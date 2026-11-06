import { isDeepStrictEqual } from "node:util";
import { readFile } from "node:fs/promises";
import type pg from "pg";
import type { RunRecord, RunRequest } from "./contracts.js";

export type BackendRun = RunRecord & {
  cancelRequested: boolean;
  recordingStatus?: "pending" | "saved" | "failed" | "deleted" | "unavailable";
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
  | "recordingEnabled"
  | "executionTarget"
  | "provider"
>;
export type RecentWorkflowRun = WorkflowRun & { workflowName: string };

function workflowIdentity(workflow: RunRequest["workflow"] | null): unknown {
  return workflow
    ? {
        ...workflow,
        executionTarget:
          workflow.executionTarget === undefined
            ? "browser"
            : workflow.executionTarget,
      }
    : null;
}

function withRecordingStatus<T extends WorkflowRun>(record: T): T {
  record.provider ??=
    record.executionTarget === "windows" ? "daytona" : "onkernel";
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
  constructor(readonly pool: pg.Pool) {}
  async initialize(): Promise<void> {
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
      await readFile(
        new URL("../../migrations/006_workspaces.sql", import.meta.url),
        "utf8",
      ),
    );
    await this.pool.query(
      await readFile(
        new URL("../../migrations/007_placeholders.sql", import.meta.url),
        "utf8",
      ),
    );
    await this.pool.query(
      await readFile(
        new URL("../../migrations/008_placeholder_runs.sql", import.meta.url),
        "utf8",
      ),
    );
    await this.pool.query(
      await readFile(
        new URL("../../migrations/009_recording_controls.sql", import.meta.url),
        "utf8",
      ),
    );
    await this.pool.query(
      await readFile(
        new URL("../../migrations/011_recording_chunks.sql", import.meta.url),
        "utf8",
      ),
    );
    await this.pool.query(
      await readFile(
        new URL("../../migrations/012_workflow_schedules.sql", import.meta.url),
        "utf8",
      ),
    );
    await this.pool.query(
      await readFile(
        new URL("../../migrations/013_weekday_schedules.sql", import.meta.url),
        "utf8",
      ),
    );
    await this.pool.query(
      await readFile(
        new URL("../../migrations/014_local_workspace.sql", import.meta.url),
        "utf8",
      ),
    );
    await this.pool.query(
      await readFile(
        new URL("../../migrations/015_workflow_deletion.sql", import.meta.url),
        "utf8",
      ),
    );
    await this.pool.query(
      await readFile(
        new URL("../../migrations/016_run_provider.sql", import.meta.url),
        "utf8",
      ),
    );
    await this.pool.query(
      await readFile(
        new URL(
          "../../migrations/017_guide_example_history.sql",
          import.meta.url,
        ),
        "utf8",
      ),
    );
    // These reservations never reached beginDispatch, so no agent request was sent.
    await this.pool
      .query(`DELETE FROM backend_runs b USING schedule_occurrences o
      WHERE o.run_id=b.id AND o.status='dispatching' AND b.status='dispatch_pending'`);
    await this.pool
      .query(`UPDATE schedule_occurrences o SET status='queued',run_id=NULL
      WHERE o.status='dispatching' AND NOT EXISTS (SELECT 1 FROM backend_runs b WHERE b.id=o.run_id)`);
    await this.pool.query(
      `UPDATE backend_runs SET status='interrupted', result='{"status":"interrupted","message":"Run interrupted before execution"}'::jsonb, updated_at=now() WHERE status='dispatch_pending'`,
    );
  }
  async create(request: RunRequest): Promise<BackendRun> {
    if (!request.workspaceId) throw new Error("Workspace is required");
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await this.insert(request, client);
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
    return this.verify(request);
  }
  async insert(request: RunRequest, client: pg.PoolClient): Promise<void> {
    if (!request.workspaceId) throw new Error("Workspace is required");
    const preference = await client.query(
      "SELECT recordings_allowed FROM workspaces WHERE id=$1 FOR UPDATE",
      [request.workspaceId],
    );
    if (!preference.rows[0]) throw new Error("Workspace not found");
    if (request.workflowId) {
      const workflow = await client.query(
        "SELECT id FROM workflows WHERE id=$1 AND workspace_id=$2 AND deleted_at IS NULL FOR UPDATE",
        [request.workflowId, request.workspaceId],
      );
      if (!workflow.rowCount) throw new Error("Workflow not found");
    }
    request.recordingEnabled =
      request.recordingEnabled !== false &&
      preference.rows[0].recordings_allowed;
    await client.query(
      "INSERT INTO backend_runs (id, instruction, max_steps, timeout_ms, status,workflow,workflow_id,workflow_version,workflow_inputs,workspace_id,request_fingerprint,contains_secrets,recording_enabled,provider) VALUES ($1,$2,$3,$4,'dispatch_pending',$5::jsonb,$6::uuid,$7::integer,$8::jsonb,$9::uuid,$10,$11,$12,$13) ON CONFLICT (id) DO NOTHING",
      [
        request.id,
        request.instruction,
        request.maxSteps,
        request.timeoutMs,
        request.requestFingerprint
          ? null
          : request.workflow
            ? JSON.stringify(request.workflow)
            : null,
        request.workflowId ?? null,
        request.workflowVersion ?? null,
        request.requestFingerprint
          ? null
          : request.workflowInputs
            ? JSON.stringify(request.workflowInputs)
            : null,
        request.workspaceId,
        request.requestFingerprint ?? null,
        request.containsSecrets ?? false,
        request.recordingEnabled !== false,
        request.workflow?.provider ??
          (request.workflow?.executionTarget === "windows"
            ? "daytona"
            : "onkernel"),
      ],
    );
  }
  private async verify(request: RunRequest): Promise<BackendRun> {
    const record = await this.get(request.id);
    if (!record) throw new Error("Run insert failed");
    const stored = await this.pool.query<{
      request_fingerprint: string | null;
    }>("SELECT request_fingerprint FROM backend_runs WHERE id=$1", [
      request.id,
    ]);
    if (
      record.workspaceId !== request.workspaceId ||
      (stored.rows[0]?.request_fingerprint ?? null) !==
        (request.requestFingerprint ?? null) ||
      record.instruction !== request.instruction ||
      record.maxSteps !== request.maxSteps ||
      record.timeoutMs !== request.timeoutMs ||
      record.recordingEnabled !== (request.recordingEnabled !== false) ||
      (request.requestFingerprint
        ? stored.rows[0]?.request_fingerprint !== request.requestFingerprint
        : !isDeepStrictEqual(
            workflowIdentity(record.workflow),
            workflowIdentity(request.workflow),
          )) ||
      (record.workflowId ?? null) !== (request.workflowId ?? null) ||
      (record.workflowVersion ?? null) !== (request.workflowVersion ?? null) ||
      (!request.requestFingerprint &&
        !isDeepStrictEqual(
          record.workflowInputs ?? null,
          request.workflowInputs ?? null,
        ))
    )
      throw new Error("Run ID already has different inputs");
    return record;
  }
  async get(id: string): Promise<BackendRun | undefined> {
    const result = await this.pool.query(
      `SELECT id, instruction, max_steps AS "maxSteps", timeout_ms AS "timeoutMs", recording_enabled AS "recordingEnabled",COALESCE(workflow->>'executionTarget',(SELECT v.definition->>'executionTarget' FROM workflow_versions v JOIN workflows w ON w.id=v.workflow_id WHERE v.workflow_id=backend_runs.workflow_id AND v.version=backend_runs.workflow_version AND w.workspace_id=backend_runs.workspace_id),'browser') AS "executionTarget", provider AS "provider", status, cancel_requested AS "cancelRequested", result, workflow, workflow_id AS "workflowId", workflow_version AS "workflowVersion", workflow_inputs AS "workflowInputs", workspace_id AS "workspaceId", steps, live_view_url AS "liveViewUrl", created_at AS "createdAt", updated_at AS "updatedAt", COALESCE((SELECT status FROM run_recordings WHERE run_id=backend_runs.id),'unavailable') AS "recordingStatus" FROM backend_runs WHERE id=$1`,

      [id],
    );
    const record = result.rows[0] as BackendRun | undefined;
    return record ? withRecordingStatus(record) : undefined;
  }
  async getForWorkspace(
    id: string,
    workspaceId: string,
  ): Promise<BackendRun | undefined> {
    const record = await this.get(id);
    return record?.workspaceId === workspaceId ? record : undefined;
  }
  async listRecent(workspaceId: string): Promise<RecentWorkflowRun[]> {
    const result = await this.pool.query(
      `SELECT r.id,r.status,r.result,r.steps,r.workflow_id AS "workflowId",r.workflow_version AS "workflowVersion",r.created_at AS "createdAt",r.updated_at AS "updatedAt",r.recording_enabled AS "recordingEnabled",COALESCE(r.workflow->>'executionTarget',v.definition->>'executionTarget','browser') AS "executionTarget",r.provider AS "provider",v.definition->>'name' AS "workflowName",COALESCE((SELECT status FROM run_recordings WHERE run_id=r.id),'unavailable') AS "recordingStatus" FROM backend_runs r JOIN workflows w ON w.id=r.workflow_id AND w.workspace_id=r.workspace_id JOIN workflow_versions v ON v.workflow_id=r.workflow_id AND v.version=r.workflow_version WHERE r.workspace_id=$1 ORDER BY r.created_at DESC,r.id DESC LIMIT 100`,
      [workspaceId],
    );
    return result.rows.map((record: RecentWorkflowRun) =>
      withRecordingStatus(record),
    );
  }
  async listForWorkflow(
    workflowId: string,
    workspaceId: string,
  ): Promise<WorkflowRun[]> {
    const result = await this.pool.query(
      `SELECT id,status,result,steps,workflow_id AS "workflowId",workflow_version AS "workflowVersion",created_at AS "createdAt",updated_at AS "updatedAt",recording_enabled AS "recordingEnabled",COALESCE(workflow->>'executionTarget',(SELECT v.definition->>'executionTarget' FROM workflow_versions v JOIN workflows w ON w.id=v.workflow_id WHERE v.workflow_id=backend_runs.workflow_id AND v.version=backend_runs.workflow_version AND w.workspace_id=backend_runs.workspace_id),'browser') AS "executionTarget",provider AS "provider",COALESCE((SELECT status FROM run_recordings WHERE run_id=backend_runs.id),'unavailable') AS "recordingStatus" FROM backend_runs WHERE workflow_id=$1 AND workspace_id=$2 ORDER BY created_at DESC,id DESC LIMIT 100`,
      [workflowId, workspaceId],
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
  async failBeforeDispatch(id: string): Promise<void> {
    await this.pool.query(
      `UPDATE backend_runs SET status='failed',result='{"status":"failed","message":"The selected provider is currently unavailable."}'::jsonb,updated_at=now() WHERE id=$1 AND status='dispatch_pending' AND NOT cancel_requested`,
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
  async activeIds(includePending = false): Promise<string[]> {
    const result = await this.pool.query(
      "SELECT id FROM backend_runs WHERE status IN ('dispatch_unknown','provisioning','running','cleaning_up','cancelling') OR ($1::boolean AND status='dispatch_pending')",
      [includePending],
    );
    return result.rows.map((row: { id: string }) => row.id);
  }
  async interrupt(ids: string[]): Promise<void> {
    await this.pool.query(
      `WITH interrupted AS (
        UPDATE backend_runs SET status='interrupted',cancel_requested=true,
        result='{"status":"interrupted","message":"Execution interrupted by database outage","cleanupError":"Cleanup could not be confirmed after database outage"}'::jsonb,
        live_view_url=NULL,updated_at=now()
        WHERE id=ANY($1::uuid[]) AND status NOT IN ('completed','failed','cancelled','interrupted','outcome_unknown') RETURNING id
      ) UPDATE run_recordings SET status='failed',updated_at=now() WHERE run_id IN (SELECT id FROM interrupted) AND status='pending'`,
      [ids],
    );
  }
  async update(
    id: string,
    status: string,
    result: unknown,
    steps?: unknown,
    liveViewUrl?: string | null,
  ): Promise<void> {
    const sensitivity = await this.pool.query<{ contains_secrets: boolean }>(
      "SELECT contains_secrets FROM backend_runs WHERE id=$1",
      [id],
    );
    if (sensitivity.rows[0]?.contains_secrets) {
      const safeSteps = (value: unknown): unknown =>
        Array.isArray(value)
          ? value.map((step) => {
              if (!step || typeof step !== "object" || Array.isArray(step))
                return {};
              const s = step as Record<string, unknown>;
              return { index: s.index, type: s.type, status: s.status };
            })
          : undefined;
      steps = safeSteps(steps);
      if (result && typeof result === "object" && !Array.isArray(result)) {
        const r = result as Record<string, unknown>;
        result = {
          status,
          ...(r.logsPartial === true ? { logsPartial: true } : {}),
          ...(Array.isArray(r.steps) ? { steps: safeSteps(r.steps) } : {}),
        };
      }
    }
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
}
