import type pg from "pg";

export const maxRecordingBytes = 100 * 1024 * 1024;
export type RecordingStatus = "pending" | "saved" | "failed" | "deleted";
export type RecordingMetadata = {
  status: RecordingStatus;
  browserId: string | null;
  replayId: string | null;
  bytes: number | null;
};
const columns =
  'status, browser_id AS "browserId", replay_id AS "replayId", octet_length(video) AS bytes';

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
      `INSERT INTO run_recordings (run_id,status,browser_id,replay_id) SELECT id,$2,$3,$4 FROM backend_runs WHERE id=$1 AND recording_enabled ON CONFLICT (run_id) DO UPDATE SET status=EXCLUDED.status,browser_id=COALESCE(EXCLUDED.browser_id,run_recordings.browser_id),replay_id=COALESCE(EXCLUDED.replay_id,run_recordings.replay_id),updated_at=now() WHERE run_recordings.status='pending' RETURNING ${columns}`,
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
