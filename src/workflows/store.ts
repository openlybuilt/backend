import { randomUUID } from "node:crypto";
import type pg from "pg";
import type { WorkflowDefinition } from "./definition.js";
export type WorkflowVersion = {
  id: string;
  version: number;
  definition: WorkflowDefinition;
  createdAt: string;
};
export class WorkflowStore {
  constructor(private readonly pool: pg.Pool) {}
  async create(
    definition: WorkflowDefinition,
    workspaceId: string,
  ): Promise<WorkflowVersion> {
    const id = randomUUID();
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(
        "INSERT INTO workflows(id,workspace_id) VALUES ($1,$2)",
        [id, workspaceId],
      );
      const result = await client.query(
        'INSERT INTO workflow_versions(workflow_id,version,definition) VALUES ($1,1,$2::jsonb) RETURNING workflow_id AS id, version, definition, created_at AS "createdAt"',
        [id, JSON.stringify(definition)],
      );
      await client.query("COMMIT");
      return result.rows[0] as WorkflowVersion;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }
  async addVersion(
    id: string,
    definition: WorkflowDefinition,
    workspaceId: string,
    expectedVersion?: number,
  ): Promise<WorkflowVersion | undefined> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const owner = await client.query(
        "SELECT id FROM workflows WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
        [id, workspaceId],
      );
      if (!owner.rowCount) {
        await client.query("ROLLBACK");
        return undefined;
      }
      if (expectedVersion !== undefined) {
        const latest = await client.query(
          "SELECT COALESCE(MAX(version),0)::integer AS version FROM workflow_versions WHERE workflow_id=$1",
          [id],
        );
        if (latest.rows[0].version !== expectedVersion)
          throw new Error("Workflow version conflict");
      }
      const result = await client.query(
        'INSERT INTO workflow_versions(workflow_id,version,definition) SELECT $1,COALESCE(MAX(version),0)+1,$2::jsonb FROM workflow_versions WHERE workflow_id=$1 RETURNING workflow_id AS id, version, definition, created_at AS "createdAt"',
        [id, JSON.stringify(definition)],
      );
      await client.query("COMMIT");
      return result.rows[0] as WorkflowVersion;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }
  async get(
    id: string,
    workspaceId: string,
    version?: number,
  ): Promise<WorkflowVersion | undefined> {
    const result = await this.pool.query(
      'SELECT v.workflow_id AS id,v.version,v.definition,v.created_at AS "createdAt" FROM workflow_versions v JOIN workflows w ON w.id=v.workflow_id WHERE v.workflow_id=$1 AND w.workspace_id=$2 AND ($3::integer IS NULL OR v.version=$3) ORDER BY v.version DESC LIMIT 1',
      [id, workspaceId, version ?? null],
    );
    return result.rows[0] as WorkflowVersion | undefined;
  }
  async list(workspaceId: string): Promise<WorkflowVersion[]> {
    const result = await this.pool.query(
      'SELECT DISTINCT ON (v.workflow_id) v.workflow_id AS id,v.version,v.definition,v.created_at AS "createdAt" FROM workflow_versions v JOIN workflows w ON w.id=v.workflow_id WHERE w.workspace_id=$1 ORDER BY v.workflow_id,v.version DESC',
      [workspaceId],
    );
    return result.rows as WorkflowVersion[];
  }
}
