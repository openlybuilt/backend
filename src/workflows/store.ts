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
  async create(definition: WorkflowDefinition): Promise<WorkflowVersion> {
    const id = randomUUID();
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query("INSERT INTO workflows(id) VALUES ($1)", [id]);
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
  ): Promise<WorkflowVersion | undefined> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const owner = await client.query(
        "SELECT id FROM workflows WHERE id=$1 FOR UPDATE",
        [id],
      );
      if (!owner.rowCount) {
        await client.query("ROLLBACK");
        return undefined;
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
    version?: number,
  ): Promise<WorkflowVersion | undefined> {
    const result = await this.pool.query(
      'SELECT workflow_id AS id,version,definition,created_at AS "createdAt" FROM workflow_versions WHERE workflow_id=$1 AND ($2::integer IS NULL OR version=$2) ORDER BY version DESC LIMIT 1',
      [id, version ?? null],
    );
    return result.rows[0] as WorkflowVersion | undefined;
  }
  async list(): Promise<WorkflowVersion[]> {
    const result = await this.pool.query(
      'SELECT DISTINCT ON (workflow_id) workflow_id AS id,version,definition,created_at AS "createdAt" FROM workflow_versions ORDER BY workflow_id,version DESC',
    );
    return result.rows as WorkflowVersion[];
  }
}
