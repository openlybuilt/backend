import { randomUUID } from "node:crypto";
import type pg from "pg";
import { defaultWorkflow } from "../workflows/default-workflow.js";

const LOCAL_WORKSPACE_ID = "00000000-0000-4000-8000-000000000001";

export class WorkspaceStore {
  constructor(private readonly pool: pg.Pool) {}

  async getOrCreate(): Promise<string> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const result = await client.query(
        "INSERT INTO workspaces (id) VALUES ($1) ON CONFLICT (id) DO NOTHING RETURNING id",
        [LOCAL_WORKSPACE_ID],
      );
      if (result.rowCount) {
        const workflowId = randomUUID();
        await client.query(
          "INSERT INTO workflows (id,workspace_id) VALUES ($1,$2)",
          [workflowId, LOCAL_WORKSPACE_ID],
        );
        await client.query(
          "INSERT INTO workflow_versions (workflow_id,version,definition) VALUES ($1,1,$2::jsonb)",
          [workflowId, JSON.stringify(defaultWorkflow)],
        );
      }
      await client.query("COMMIT");
      return LOCAL_WORKSPACE_ID;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }
}
