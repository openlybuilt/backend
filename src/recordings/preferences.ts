import type pg from "pg";

export class RecordingPreferences {
  constructor(private readonly pool: pg.Pool) {}

  async get(workspaceId: string): Promise<boolean> {
    const result = await this.pool.query(
      "SELECT recordings_allowed FROM workspaces WHERE id=$1",
      [workspaceId],
    );
    if (!result.rows[0]) throw new Error("Workspace not found");
    return result.rows[0].recordings_allowed;
  }

  async stopping(workspaceId: string): Promise<boolean> {
    const result = await this.pool.query(
      "SELECT 1 FROM backend_runs WHERE workspace_id=$1 AND recording_stop_requested LIMIT 1",
      [workspaceId],
    );
    return !!result.rowCount;
  }

  async set(workspaceId: string, enabled: boolean): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(
        "UPDATE workspaces SET recordings_allowed=$2 WHERE id=$1",
        [workspaceId, enabled],
      );
      if (!enabled) {
        await client.query(
          `UPDATE backend_runs SET recording_stop_requested=true WHERE workspace_id=$1 AND recording_enabled AND (status NOT IN ('completed','failed','cancelled','interrupted','outcome_unknown') OR EXISTS (SELECT 1 FROM run_recordings WHERE run_id=backend_runs.id AND status='pending'))`,
          [workspaceId],
        );
        await client.query(
          `INSERT INTO run_recordings (run_id,status) SELECT id,'deleted' FROM backend_runs WHERE workspace_id=$1 AND recording_stop_requested ON CONFLICT (run_id) DO UPDATE SET status='deleted',video=NULL,updated_at=now()`,
          [workspaceId],
        );
      }
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }
}
