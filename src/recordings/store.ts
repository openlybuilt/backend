import { randomUUID } from "node:crypto";
import type pg from "pg";

export const maxRecordingBytes = 100 * 1024 * 1024;
const chunkBytes = 1024 * 1024;
export class InvalidRecordingError extends Error {}
export type RecordingStatus = "pending" | "saved" | "failed" | "deleted";
export type RecordingMetadata = {
  status: RecordingStatus;
  browserId: string | null;
  replayId: string | null;
  bytes: number | null;
};
const columns =
  'status, browser_id AS "browserId", replay_id AS "replayId", COALESCE(byte_count,octet_length(video))::double precision AS bytes';

export class RecordingStore {
  constructor(private readonly pool: pg.Pool) {}
  async get(id: string): Promise<RecordingMetadata | undefined> {
    const result = await this.pool.query(
      `SELECT ${columns} FROM run_recordings WHERE run_id=$1`,
      [id],
    );
    return result.rows[0] as RecordingMetadata | undefined;
  }
  async report(
    id: string,
    status: "pending" | "failed",
    browserId?: string,
    replayId?: string,
  ): Promise<RecordingMetadata | undefined> {
    const result = await this.pool.query(
      `INSERT INTO run_recordings (run_id,status,browser_id,replay_id) SELECT id,$2,$3,$4 FROM backend_runs WHERE id=$1 AND recording_enabled ON CONFLICT (run_id) DO UPDATE SET status=CASE WHEN run_recordings.status='deleted' THEN 'deleted' ELSE EXCLUDED.status END,browser_id=COALESCE(EXCLUDED.browser_id,run_recordings.browser_id),replay_id=COALESCE(EXCLUDED.replay_id,run_recordings.replay_id),updated_at=now() WHERE run_recordings.status IN ('pending','deleted') RETURNING ${columns}`,
      [id, status, browserId ?? null, replayId ?? null],
    );
    return (result.rows[0] as RecordingMetadata | undefined) ?? this.get(id);
  }
  async save(
    id: string,
    video: Buffer,
  ): Promise<RecordingMetadata | undefined> {
    const result = await this.pool.query(
      `UPDATE run_recordings SET status='saved',video=$2,updated_at=now() WHERE run_id=$1 AND status='pending' RETURNING ${columns}`,
      [id, video],
    );
    return (result.rows[0] as RecordingMetadata | undefined) ?? this.get(id);
  }
  async upload(
    id: string,
    source: AsyncIterable<Uint8Array>,
    assertCurrent: () => void,
  ): Promise<RecordingMetadata | undefined> {
    const client = await this.pool.connect();
    let broken = false;
    try {
      assertCurrent();
      await client.query("BEGIN");
      assertCurrent();
      const existing = await client.query(
        `SELECT ${columns} FROM run_recordings WHERE run_id=$1`,
        [id],
      );
      assertCurrent();
      if (existing.rows[0]?.status !== "pending") {
        await client.query("ROLLBACK");
        return existing.rows[0] as RecordingMetadata | undefined;
      }
      const uploadId = randomUUID();
      let block = Buffer.allocUnsafe(chunkBytes);
      let used = 0,
        sequence = 0,
        total = 0;
      const header = Buffer.alloc(16);
      const write = async () => {
        assertCurrent();
        await client.query(
          "INSERT INTO recording_chunks(run_id,upload_id,sequence,data) VALUES($1,$2,$3,$4)",
          [id, uploadId, sequence++, block.subarray(0, used)],
        );
        assertCurrent();
        block = Buffer.allocUnsafe(chunkBytes);
        used = 0;
      };
      for await (const value of source) {
        assertCurrent();
        const data = Buffer.from(
          value.buffer,
          value.byteOffset,
          value.byteLength,
        );
        if (total < 16) data.copy(header, total, 0, 16 - total);
        total += data.length;
        if (
          total >= 16 &&
          (header.toString("ascii", 4, 8) !== "ftyp" ||
            header.readUInt32BE(0) < 16)
        )
          throw new InvalidRecordingError("Invalid MP4 recording");
        for (let offset = 0; offset < data.length; ) {
          const count = Math.min(chunkBytes - used, data.length - offset);
          data.copy(block, used, offset, offset + count);
          used += count;
          offset += count;
          if (used === chunkBytes) await write();
        }
      }
      assertCurrent();
      if (
        total < 16 ||
        header.toString("ascii", 4, 8) !== "ftyp" ||
        header.readUInt32BE(0) < 16 ||
        header.readUInt32BE(0) > total
      )
        throw new InvalidRecordingError("Invalid MP4 recording");
      if (used) await write();
      assertCurrent();
      const saved = await client.query(
        `UPDATE run_recordings SET status='saved',video=NULL,byte_count=$2,upload_id=$3,updated_at=now() WHERE run_id=$1 AND status='pending' RETURNING ${columns}`,
        [id, total, uploadId],
      );
      assertCurrent();
      await client.query(saved.rowCount ? "COMMIT" : "ROLLBACK");
      assertCurrent();
      return (saved.rows[0] as RecordingMetadata | undefined) ?? this.get(id);
    } catch (error) {
      await client.query("ROLLBACK").catch(() => (broken = true));
      throw error;
    } finally {
      client.release(broken);
    }
  }
  async *stream(
    id: string,
    offset: number,
    bytes: number,
    assertCurrent: () => void,
  ): AsyncGenerator<Buffer> {
    assertCurrent();
    const result = await this.pool.query(
      "SELECT upload_id FROM run_recordings WHERE run_id=$1 AND status='saved'",
      [id],
    );
    assertCurrent();
    if (!result.rows.length) throw new Error("Recording not available");
    const uploadId = result.rows[0].upload_id as string | null;
    while (bytes > 0) {
      const within = uploadId ? offset % chunkBytes : offset;
      const count = Math.min(
        bytes,
        uploadId ? chunkBytes - within : chunkBytes,
      );
      assertCurrent();
      const part = await this.pool.query(
        uploadId
          ? "SELECT substring(data FROM $3::integer FOR $4::integer) AS video FROM recording_chunks WHERE run_id=$1 AND upload_id=$2 AND sequence=$5"
          : "SELECT substring(video FROM $3::integer FOR $4::integer) AS video FROM run_recordings WHERE run_id=$1 AND status='saved' AND $2::uuid IS NULL",
        [
          id,
          uploadId,
          within + 1,
          count,
          ...(uploadId ? [Math.floor(offset / chunkBytes)] : []),
        ],
      );
      assertCurrent();
      const video = part.rows[0]?.video as Buffer | undefined;
      if (!video || video.length !== count)
        throw new Error("Recording data incomplete");
      yield video;
      offset += count;
      bytes -= count;
    }
  }
  async read(
    id: string,
    offset: number,
    bytes: number,
  ): Promise<Buffer | undefined> {
    const result = await this.pool.query(
      "SELECT substring(video FROM $2::integer FOR $3::integer) AS video FROM run_recordings WHERE run_id=$1 AND status='saved'",
      [id, offset + 1, bytes],
    );
    return result.rows[0]?.video as Buffer | undefined;
  }
  async delete(id: string): Promise<void> {
    await this.pool.query(
      `INSERT INTO run_recordings (run_id,status) SELECT id,'deleted' FROM backend_runs WHERE id=$1 ON CONFLICT (run_id) DO UPDATE SET status='deleted',video=NULL,updated_at=now()`,
      [id],
    );
  }
  async expire(): Promise<void> {
    await this.pool.query(
      `UPDATE run_recordings SET status='deleted',video=NULL,updated_at=now() WHERE status='saved' AND updated_at <= now()-interval '30 days'`,
    );
    await this.pool.query(
      "UPDATE run_recordings r SET status='failed',updated_at=now() FROM backend_runs b WHERE r.run_id=b.id AND r.status='pending' AND ((b.status IN ('completed','failed','cancelled','interrupted','outcome_unknown') AND b.updated_at < now()-interval '120 seconds') OR b.created_at + (b.timeout_ms+180000)*interval '1 millisecond' < now())",
    );
  }
}
