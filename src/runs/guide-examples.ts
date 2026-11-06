import type pg from "pg";

export class GuideExampleStore {
  constructor(private readonly pool: pg.Pool) {}

  async create(
    id: string,
    workflowId: string,
    version: number,
    workspaceId: string,
  ): Promise<boolean> {
    const result = await this.pool.query(
      `INSERT INTO guide_example_runs (id, workspace_id, workflow_id, workflow_version)
       SELECT $1, $2, w.id, v.version FROM workflows w
       JOIN workflow_versions v ON v.workflow_id=w.id AND v.version=$4
       WHERE w.id=$3 AND w.workspace_id=$2 AND w.deleted_at IS NULL
       ON CONFLICT (id) DO NOTHING RETURNING id`,
      [id, workspaceId, workflowId, version],
    );
    if (result.rowCount) return true;
    const existing = await this.pool.query(
      "SELECT 1 FROM guide_example_runs WHERE id=$1 AND workspace_id=$2 AND workflow_id=$3 AND workflow_version=$4",
      [id, workspaceId, workflowId, version],
    );
    return Boolean(existing.rowCount);
  }

  async list(workspaceId: string, workflowId?: string): Promise<object[]> {
    const result = await this.pool.query(
      `SELECT e.id, e.workflow_id AS "workflowId", e.workflow_version AS "workflowVersion", e.created_at AS "createdAt",
       v.definition->>'name' AS "workflowName", 'completed' AS status, true AS example,
       jsonb_build_object('message', 'Recorded example') AS result,
       jsonb_build_object('id',e.workflow_id,'version',v.version,'definition',v.definition,'createdAt',v.created_at) AS "exampleWorkflow"
       FROM guide_example_runs e JOIN workflow_versions v ON v.workflow_id=e.workflow_id AND v.version=e.workflow_version
       WHERE e.workspace_id=$1 AND ($2::uuid IS NULL OR e.workflow_id=$2)
       ORDER BY e.created_at DESC, e.id DESC LIMIT 100`,
      [workspaceId, workflowId ?? null],
    );
    return result.rows;
  }
}
