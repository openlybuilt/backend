import { randomUUID } from "node:crypto";
import type pg from "pg";

const chunkBytes = 1024 * 1024;
export class InvalidRecordingError extends Error {}
async function* recordingBody(
  source: AsyncIterable<Uint8Array>,
): AsyncGenerator<Uint8Array> {
  try {
    yield* source;
  } catch {
    throw new InvalidRecordingError("Upload interrupted");
  }
}
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
  private activeUploads = 0;
  private readonly waitingUploads: {
    signal: AbortSignal;
    resolve: (release: () => void) => void;
    reject: (reason: unknown) => void;
    onAbort: () => void;
  }[] = [];

  constructor(private readonly pool: pg.Pool) {}

  private async acquireUpload(signal: AbortSignal): Promise<() => void> {
    signal.throwIfAborted();
    if (this.activeUploads < 4) {
      this.activeUploads++;
      return this.releaseUpload();
    }
    return new Promise((resolve, reject) => {
      const entry = {
        signal,
        resolve,
        reject,
        onAbort: () => {
          const index = this.waitingUploads.indexOf(entry);
          if (index >= 0) this.waitingUploads.splice(index, 1);
          reject(signal.reason);
        },
      };
      this.waitingUploads.push(entry);
      signal.addEventListener("abort", entry.onAbort, { once: true });
      if (signal.aborted) entry.onAbort();
    });
  }

  private releaseUpload(): () => void {
    let released = false;
    return () => {
      if (released) return;
      released = true;
      const next = this.waitingUploads.shift();
      if (next) {
        next.signal.removeEventListener("abort", next.onAbort);
        next.resolve(this.releaseUpload());
      } else this.activeUploads--;
    };
  }
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
  async upload(
    id: string,
    source: AsyncIterable<Uint8Array>,
    assertCurrent: () => void,
    interrupt: () => void,
    signal: AbortSignal,
  ): Promise<RecordingMetadata | undefined> {
    const assertUpload = () => {
      signal.throwIfAborted();
      assertCurrent();
    };
    const releaseUpload = await this.acquireUpload(signal);
    let client: pg.PoolClient;
    try {
      assertUpload();
      client = await this.pool.connect();
    } catch (error) {
      releaseUpload();
      throw error;
    }
    let broken = false;
    let failure: Error | undefined;
    const onError = (error: Error) => {
      broken = true;
      failure = error;
      interrupt();
    };
    client.on("error", onError);
    try {
      assertUpload();
      await client.query("BEGIN");
      assertUpload();
      const existing = await client.query(
        `SELECT ${columns} FROM run_recordings WHERE run_id=$1`,
        [id],
      );
      assertUpload();
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
        assertUpload();
        await client.query(
          "INSERT INTO recording_chunks(run_id,upload_id,sequence,data) VALUES($1,$2,$3,$4)",
          [id, uploadId, sequence++, block.subarray(0, used)],
        );
        assertUpload();
        block = Buffer.allocUnsafe(chunkBytes);
        used = 0;
      };
      for await (const value of recordingBody(source)) {
        assertUpload();
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
      assertUpload();
      if (
        total < 16 ||
        header.toString("ascii", 4, 8) !== "ftyp" ||
        header.readUInt32BE(0) < 16 ||
        header.readUInt32BE(0) > total
      )
        throw new InvalidRecordingError("Invalid MP4 recording");
      if (used) await write();
      assertUpload();
      const saved = await client.query(
        `UPDATE run_recordings SET status='saved',video=NULL,byte_count=$2,upload_id=$3,updated_at=now() WHERE run_id=$1 AND status='pending' RETURNING ${columns}`,
        [id, total, uploadId],
      );
      assertUpload();
      await client.query(saved.rowCount ? "COMMIT" : "ROLLBACK");
      assertUpload();
      return (saved.rows[0] as RecordingMetadata | undefined) ?? this.get(id);
    } catch (error) {
      await client.query("ROLLBACK").catch(() => (broken = true));
      throw failure ?? error;
    } finally {
      client.off("error", onError);
      client.release(broken);
      releaseUpload();
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
  async delete(id: string): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(
        `INSERT INTO run_recordings (run_id,status) SELECT id,'deleted' FROM backend_runs WHERE id=$1 ON CONFLICT (run_id) DO UPDATE SET status='deleted',video=NULL,byte_count=NULL,upload_id=NULL,updated_at=now()`,
        [id],
      );
      await client.query("DELETE FROM recording_chunks WHERE run_id=$1", [id]);
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  async expire(): Promise<void> {
    await this.pool.query(
      `WITH expired AS (UPDATE run_recordings SET status='deleted',video=NULL,byte_count=NULL,upload_id=NULL,updated_at=now() WHERE status='saved' AND updated_at <= now()-interval '30 days' RETURNING run_id) DELETE FROM recording_chunks WHERE run_id IN (SELECT run_id FROM expired)`,
    );
    await this.pool.query(
      "UPDATE run_recordings r SET status='failed',updated_at=now() FROM backend_runs b WHERE r.run_id=b.id AND r.status='pending' AND ((b.status IN ('completed','failed','cancelled','interrupted','outcome_unknown') AND b.updated_at < now()-interval '120 seconds') OR b.created_at + (b.timeout_ms+180000)*interval '1 millisecond' < now())",
    );
  }
}
