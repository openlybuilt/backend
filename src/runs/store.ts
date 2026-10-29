import { isDeepStrictEqual } from "node:util";
import { readFile } from "node:fs/promises";
import pg from "pg";
import type { RunRecord, RunRequest } from "./contracts.js";

export type BackendRun = RunRecord & {
  cancelRequested: boolean;
  recordingStatus?: "pending" | "saved" | "failed" | "unavailable";
};
export type WorkflowRun = Pick<
  BackendRun,
  | "id"
  | "status"
  | "result"
  | "steps"
  | "workflowId"
  | "workflowVersion"
  | "createdAt"
  | "updatedAt"
  | "recordingStatus"
>;
export type RecentWorkflowRun = WorkflowRun & { workflowName: string };

function withRecordingStatus<T extends WorkflowRun>(record: T): T {
  if (
    record.result &&
    typeof record.result === "object" &&
    !Array.isArray(record.result)
  )
    record.result = {
      ...record.result,
      recordingStatus: record.recordingStatus,
    };
  return record;
}

export class RunStore {
  readonly pool: pg.Pool;
  private readonly lock: pg.Client;
  constructor(url: string) {
    this.pool = new pg.Pool({ connectionString: url });
    this.lock = new pg.Client({ connectionString: url });
    this.lock.on("error", () => {
      console.error("Database ownership connection lost");
      process.exit(1);
    });
  }
  async initialize(): Promise<void> {
    await this.lock.connect();
    const lock = await this.lock.query(
      "SELECT pg_try_advisory_lock(78422117) AS acquired",
    );
    if (!lock.rows[0].acquired)
      throw new Error("Another backend service owns this database");
    const sql = await readFile(
      new URL("../../migrations/001_backend_runs.sql", import.meta.url),
      "utf8",
    );
    await this.pool.query(sql);
    const workflowsSql = await readFile(
      new URL("../../migrations/002_workflows.sql", import.meta.url),
      "utf8",
    );
    await this.pool.query(workflowsSql);
    await this.pool.query(
      await readFile(
        new URL("../../migrations/003_run_live_view.sql", import.meta.url),
        "utf8",
      ),
    );
    await this.pool.query(
      await readFile(
        new URL("../../migrations/004_recordings.sql", import.meta.url),
        "utf8",
      ),
    );
    await this.pool.query(
      await readFile(
        new URL("../../migrations/005_run_events.sql", import.meta.url),
        "utf8",
      ),
    );
    await this.pool.query(
      "UPDATE backend_runs SET status = 'dispatch_unknown', updated_at = now() WHERE status = 'dispatch_pending'",
    );
  }
  async create(request: RunRequest): Promise<BackendRun> {
    await this.pool.query(
      "INSERT INTO backend_runs (id, instruction, max_steps, timeout_ms, status,workflow,workflow_id,workflow_version,workflow_inputs) VALUES ($1,$2,$3,$4,'dispatch_pending',$5::jsonb,$6::uuid,$7::integer,$8::jsonb) ON CONFLICT (id) DO NOTHING",
      [
        request.id,
        request.instruction,
        request.maxSteps,
        request.timeoutMs,
        request.workflow ? JSON.stringify(request.workflow) : null,
        request.workflowId ?? null,
        request.workflowVersion ?? null,
        request.workflowInputs ? JSON.stringify(request.workflowInputs) : null,
      ],
    );
    const record = await this.get(request.id);
    if (!record) throw new Error("Run insert failed");
    if (
      record.instruction !== request.instruction ||
      record.maxSteps !== request.maxSteps ||
      record.timeoutMs !== request.timeoutMs ||
      !isDeepStrictEqual(record.workflow ?? null, request.workflow ?? null) ||
      (record.workflowId ?? null) !== (request.workflowId ?? null) ||
      (record.workflowVersion ?? null) !== (request.workflowVersion ?? null) ||
      !isDeepStrictEqual(
        record.workflowInputs ?? null,
        request.workflowInputs ?? null,
      )
    )
      throw new Error("Run ID already has different inputs");
    return record;
  }
  async get(id: string): Promise<BackendRun | undefined> {
    const result = await this.pool.query(
      `SELECT id, instruction, max_steps AS "maxSteps", timeout_ms AS "timeoutMs", status, cancel_requested AS "cancelRequested", result, workflow, workflow_id AS "workflowId", workflow_version AS "workflowVersion", workflow_inputs AS "workflowInputs", steps, live_view_url AS "liveViewUrl", created_at AS "createdAt", updated_at AS "updatedAt", COALESCE((SELECT status FROM run_recordings WHERE run_id=backend_runs.id),'unavailable') AS "recordingStatus" FROM backend_runs WHERE id=$1`,
      [id],
    );
    const record = result.rows[0] as BackendRun | undefined;
    return record ? withRecordingStatus(record) : undefined;
  }
  async listRecent(): Promise<RecentWorkflowRun[]> {
    const result = await this.pool.query(
      `SELECT r.id,r.status,r.result,r.steps,r.workflow_id AS "workflowId",r.workflow_version AS "workflowVersion",r.created_at AS "createdAt",r.updated_at AS "updatedAt",v.definition->>'name' AS "workflowName",COALESCE((SELECT status FROM run_recordings WHERE run_id=r.id),'unavailable') AS "recordingStatus" FROM backend_runs r JOIN workflow_versions v ON v.workflow_id=r.workflow_id AND v.version=r.workflow_version ORDER BY r.created_at DESC,r.id DESC LIMIT 100`,
    );
    return result.rows.map((record: RecentWorkflowRun) =>
      withRecordingStatus(record),
    );
  }
  async listForWorkflow(workflowId: string): Promise<WorkflowRun[]> {
    const result = await this.pool.query(
      `SELECT id,status,result,steps,workflow_id AS "workflowId",workflow_version AS "workflowVersion",created_at AS "createdAt",updated_at AS "updatedAt",COALESCE((SELECT status FROM run_recordings WHERE run_id=backend_runs.id),'unavailable') AS "recordingStatus" FROM backend_runs WHERE workflow_id=$1 ORDER BY created_at DESC,id DESC LIMIT 100`,
      [workflowId],
    );
    return result.rows.map((record: WorkflowRun) =>
      withRecordingStatus(record),
    );
  }
  async requestCancel(id: string): Promise<void> {
    await this.pool.query(
      "UPDATE backend_runs SET cancel_requested=true, status=CASE WHEN status='dispatch_pending' THEN 'cancelled' ELSE status END, result=CASE WHEN status='dispatch_pending' THEN '{\"status\":\"cancelled\",\"message\":\"Run cancelled before execution\"}'::jsonb ELSE result END, updated_at=now() WHERE id=$1 AND status NOT IN ('completed','failed','cancelled','interrupted','outcome_unknown')",
      [id],
    );
  }
  async beginDispatch(id: string): Promise<boolean> {
    const result = await this.pool.query(
      "UPDATE backend_runs SET status='provisioning', updated_at=now() WHERE id=$1 AND status='dispatch_pending' RETURNING id",
      [id],
    );
    return result.rowCount === 1;
  }
  async activeIds(): Promise<string[]> {
    const result = await this.pool.query(
      "SELECT id FROM backend_runs WHERE status IN ('dispatch_unknown','provisioning','running','cleaning_up','cancelling')",
    );
    return result.rows.map((row: { id: string }) => row.id);
  }
  async update(
    id: string,
    status: string,
    result: unknown,
    steps?: unknown,
    liveViewUrl?: string | null,
  ): Promise<void> {
    await this.pool.query(
      "UPDATE backend_runs SET status=$2, result=COALESCE($3::jsonb,result), steps=COALESCE($4::jsonb,steps), live_view_url=CASE WHEN $2 IN ('completed','failed','cancelled','interrupted','outcome_unknown') THEN NULL WHEN $6 THEN $5 ELSE live_view_url END, updated_at=now() WHERE id=$1 AND status NOT IN ('completed','failed','cancelled','interrupted','outcome_unknown') AND (CASE $2 WHEN 'dispatch_pending' THEN 0 WHEN 'dispatch_unknown' THEN 1 WHEN 'provisioning' THEN 1 WHEN 'running' THEN 2 WHEN 'cancelling' THEN 3 WHEN 'cleaning_up' THEN 3 ELSE 4 END) >= (CASE status WHEN 'dispatch_pending' THEN 0 WHEN 'dispatch_unknown' THEN 1 WHEN 'provisioning' THEN 1 WHEN 'running' THEN 2 WHEN 'cancelling' THEN 3 WHEN 'cleaning_up' THEN 3 ELSE 4 END)",
      [
        id,
        status,
        result === undefined || result === null ? null : JSON.stringify(result),
        steps === undefined || steps === null ? null : JSON.stringify(steps),
        liveViewUrl ?? null,
        liveViewUrl !== undefined,
      ],
    );
  }
  async close(): Promise<void> {
    await this.pool.end();
    await this.lock.end();
  }
}
