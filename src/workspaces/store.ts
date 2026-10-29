import { randomUUID } from "node:crypto";
import type pg from "pg";
import { defaultWorkflow } from "../workflows/default-workflow.js";

export type VerifiedIdentity = { issuer: string; subject: string };

export class WorkspaceStore {
  constructor(private readonly pool: pg.Pool) {}

  async getOrCreate(identity: VerifiedIdentity): Promise<string> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const person = await client.query<{ id: string }>(
        "INSERT INTO identities (id,issuer,subject) VALUES ($1,$2,$3) ON CONFLICT (issuer,subject) DO UPDATE SET subject=EXCLUDED.subject RETURNING id",
        [randomUUID(), identity.issuer, identity.subject],
      );
      const workspace = await client.query<{ id: string }>(
        "INSERT INTO workspaces (id,identity_id) VALUES ($1,$2) ON CONFLICT (identity_id) DO NOTHING RETURNING id",
        [randomUUID(), person.rows[0]!.id],
      );
      let workspaceId = workspace.rows[0]?.id;
      if (workspaceId) {
        const workflowId = randomUUID();
        await client.query(
          "INSERT INTO workflows (id,workspace_id) VALUES ($1,$2)",
          [workflowId, workspaceId],
        );
        await client.query(
          "INSERT INTO workflow_versions (workflow_id,version,definition) VALUES ($1,1,$2::jsonb)",
          [workflowId, JSON.stringify(defaultWorkflow)],
        );
      } else {
        const existing = await client.query<{ id: string }>(
          "SELECT id FROM workspaces WHERE identity_id=$1",
          [person.rows[0]!.id],
        );
        workspaceId = existing.rows[0]!.id;
      }
      await client.query("COMMIT");
      return workspaceId;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }
}
