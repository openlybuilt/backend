import { createHash, randomUUID } from "node:crypto";
import type pg from "pg";
import { parseDefinition, resolveWorkflow } from "../workflows/definition.js";
import type { PlaceholderStore } from "../placeholders/store.js";
import { isDatabaseFailure } from "../database.js";
import type { ScheduleInput } from "./contracts.js";

export class ScheduleConflict extends Error {
  constructor(
    readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

type Row = {
  id: string;
  workflow_id: string;
  workspace_id: string;
  kind: "once" | "interval";
  run_at: Date | null;
  interval_minutes: number | null;
  enabled: boolean;
  recording_enabled: boolean;
  required_secret_names: string[];
  approved_secret_names: string[];
  next_run_at: Date | null;
  issue: string | null;
  created_at: Date;
  last_run_id: string | null;
  last_occurrence_status: string | null;
  last_run_status: string | null;
};
const select = `SELECT s.*,
  (SELECT run_id FROM schedule_occurrences o WHERE o.schedule_id=s.id AND o.run_id IS NOT NULL ORDER BY scheduled_at DESC LIMIT 1) AS last_run_id,
  (SELECT status FROM schedule_occurrences o WHERE o.schedule_id=s.id ORDER BY scheduled_at DESC LIMIT 1) AS last_occurrence_status,
  (SELECT b.status FROM schedule_occurrences o JOIN backend_runs b ON b.id=o.run_id WHERE o.schedule_id=s.id ORDER BY o.scheduled_at DESC LIMIT 1) AS last_run_status
  FROM workflow_schedules s`;
function serialize(row: Row) {
  return {
    id: row.id,
    workflowId: row.workflow_id,
    kind: row.kind,
    runAt: row.run_at?.toISOString() ?? null,
    intervalMinutes: row.interval_minutes,
    enabled: row.enabled,
    nextRunAt: row.next_run_at?.toISOString() ?? null,
    status: row.issue
      ? "needs_attention"
      : !row.enabled
        ? "paused"
        : row.kind === "once" && row.last_occurrence_status === "missed"
          ? "missed"
          : row.kind === "once" &&
              (row.last_occurrence_status === "failed" ||
                ["failed", "cancelled", "interrupted"].includes(
                  row.last_run_status ?? "",
                ))
            ? "failed"
            : row.kind === "once" && row.last_run_status === "completed"
              ? "completed"
              : "active",
    issue: row.issue,
    lastRunId: row.last_run_id,
    createdAt: row.created_at.toISOString(),
    secretNames: row.required_secret_names,
    recordingEnabled: row.recording_enabled,
  };
}
export type ScheduleView = ReturnType<typeof serialize>;
export class ScheduleStore {
  constructor(
    readonly pool: pg.Pool,
    private readonly placeholders: PlaceholderStore,
  ) {}
  async timezone(workspaceId: string): Promise<string | null> {
    const result = await this.pool.query(
      "SELECT timezone FROM workspaces WHERE id=$1",
      [workspaceId],
    );
    if (!result.rows.length) throw new Error("Workspace not found");
    return result.rows[0].timezone as string | null;
  }
  async setTimezone(
    workspaceId: string,
    timezone: string,
    onlyIfUnset = false,
  ): Promise<string> {
    const result = await this.pool.query(
      "UPDATE workspaces SET timezone=CASE WHEN $3 THEN COALESCE(timezone,$2) ELSE $2 END WHERE id=$1 RETURNING timezone",
      [workspaceId, timezone, onlyIfUnset],
    );
    if (!result.rows.length) throw new Error("Workspace not found");
    return result.rows[0].timezone as string;
  }
  async list(
    workspaceId: string,
    workflowId: string,
  ): Promise<ScheduleView[] | undefined> {
    const workflow = await this.pool.query(
      "SELECT 1 FROM workflows WHERE id=$1 AND workspace_id=$2",
      [workflowId, workspaceId],
    );
    if (!workflow.rowCount) return undefined;
    const result = await this.pool.query<Row>(
      `${select} WHERE s.workspace_id=$1 AND s.workflow_id=$2 ORDER BY s.created_at,s.id`,
      [workspaceId, workflowId],
    );
    return result.rows.map(serialize);
  }
  private async currentSecrets(
    client: pg.PoolClient,
    workspaceId: string,
    workflowId: string,
  ) {
    const lock = await client.query(
      "SELECT 1 FROM workflows WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
      [workflowId, workspaceId],
    );
    if (!lock.rowCount) return undefined;
    const version = await client.query<{
      version: number;
      definition: unknown;
    }>(
      "SELECT version,definition FROM workflow_versions WHERE workflow_id=$1 ORDER BY version DESC LIMIT 1",
      [workflowId],
    );
    if (!version.rows[0])
      throw new ScheduleConflict(
        "invalid_workflow",
        "Fix the workflow before resuming.",
      );
    let definition: ReturnType<typeof parseDefinition>;
    try {
      definition = parseDefinition(version.rows[0].definition);
    } catch {
      throw new ScheduleConflict(
        "invalid_workflow",
        "Fix the workflow before resuming.",
      );
    }
    const secrets = await client.query<{ name: string; secret: boolean }>(
      "SELECT name,secret FROM placeholders WHERE workspace_id=$1 AND name=ANY($2::text[]) ORDER BY name FOR SHARE",
      [workspaceId, definition.inputs],
    );
    return {
      version: version.rows[0].version,
      definition,
      names: secrets.rows.filter((row) => row.secret).map((row) => row.name),
      missing: definition.inputs.filter(
        (name) => !secrets.rows.some((row) => row.name === name),
      ),
    };
  }
  private async checkResolved(
    client: pg.PoolClient,
    workspaceId: string,
    definition: ReturnType<typeof parseDefinition>,
  ) {
    let values: Awaited<ReturnType<PlaceholderStore["resolve"]>>;
    try {
      values = await this.placeholders.resolve(
        workspaceId,
        definition.inputs,
        client,
      );
    } catch (error) {
      const code =
        error && typeof error === "object" && "code" in error
          ? (error as { code?: unknown }).code
          : undefined;
      if (
        isDatabaseFailure(error) ||
        (typeof code === "string" && /^[0-9A-Z]{5}$/.test(code))
      )
        throw error;
      throw new ScheduleConflict(
        "invalid_workflow",
        "Fix the workflow or its placeholder values before resuming.",
      );
    }
    if (values.missing.length)
      throw new ScheduleConflict(
        "missing_placeholders",
        "Save all required placeholder values before resuming.",
      );
    try {
      resolveWorkflow(definition, values.values);
    } catch {
      throw new ScheduleConflict(
        "invalid_workflow",
        "Fix the workflow or its placeholder values before resuming.",
      );
    }
  }
  async create(
    workspaceId: string,
    workflowId: string,
    input: ScheduleInput,
  ): Promise<ScheduleView | undefined> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query("SELECT id FROM workspaces WHERE id=$1 FOR UPDATE", [
        workspaceId,
      ]);
      const fingerprint = createHash("sha256")
        .update(JSON.stringify({ workflowId, ...input }))
        .digest("hex");
      const prior = await client.query<{
        fingerprint: string;
        schedule_id: string | null;
      }>(
        "SELECT fingerprint,schedule_id FROM schedule_creation_requests WHERE workspace_id=$1 AND creation_key=$2",
        [workspaceId, input.idempotencyKey],
      );
      if (prior.rows[0]) {
        if (prior.rows[0].fingerprint !== fingerprint)
          throw new ScheduleConflict(
            "idempotency_conflict",
            "This request key was used for different schedule details.",
          );
        if (!prior.rows[0].schedule_id)
          throw new ScheduleConflict(
            "schedule_deleted",
            "This schedule was already created and later deleted.",
          );
        const existing = await client.query<Row>(
          `${select} WHERE s.id=$1 AND s.workspace_id=$2`,
          [prior.rows[0].schedule_id, workspaceId],
        );
        if (!existing.rows[0])
          throw new Error("Schedule creation record is inconsistent");
        await client.query("COMMIT");
        return serialize(existing.rows[0]);
      }
      if (
        input.kind === "once" &&
        new Date(input.runAt!).getTime() <= Date.now()
      )
        throw new Error("Choose a time in the future");
      const current = await this.currentSecrets(
        client,
        workspaceId,
        workflowId,
      );
      if (!current) {
        await client.query("ROLLBACK");
        return undefined;
      }
      if (
        current.names.length &&
        (!input.acknowledgeSecretExposure ||
          input.acknowledgedWorkflowVersion !== current.version)
      )
        throw new ScheduleConflict(
          "secret_acknowledgment_required",
          "Confirm the current workflow's secret exposure before scheduling.",
        );
      if (
        input.acknowledgedWorkflowVersion !== undefined &&
        input.acknowledgedWorkflowVersion !== current.version
      )
        throw new ScheduleConflict(
          "workflow_version_changed",
          "The workflow changed. Review the latest version.",
        );
      const id = randomUUID();
      await client.query(
        `INSERT INTO workflow_schedules(id,workspace_id,workflow_id,kind,run_at,interval_minutes,recording_enabled,approved_secret_names,required_secret_names,next_run_at)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$8,CASE WHEN $4='once' THEN $5::timestamptz ELSE now()+($6::integer * interval '1 minute') END)`,
        [
          id,
          workspaceId,
          workflowId,
          input.kind,
          input.runAt ?? null,
          input.intervalMinutes ?? null,
          input.recordingEnabled,
          current.names,
        ],
      );
      await client.query(
        "INSERT INTO schedule_creation_requests(workspace_id,creation_key,fingerprint,schedule_id) VALUES($1,$2,$3,$4)",
        [workspaceId, input.idempotencyKey, fingerprint, id],
      );
      const saved = await client.query<Row>(`${select} WHERE s.id=$1`, [id]);
      await client.query("COMMIT");
      return serialize(saved.rows[0]!);
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }
  async update(
    workspaceId: string,
    id: string,
    input: {
      enabled?: boolean;
      acknowledgeSecretExposure?: boolean;
      acknowledgedWorkflowVersion?: number;
    },
  ): Promise<ScheduleView | undefined> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const row = (
        await client.query<Row>(
          `${select} WHERE s.id=$1 AND s.workspace_id=$2 FOR UPDATE OF s`,
          [id, workspaceId],
        )
      ).rows[0];
      if (!row) {
        await client.query("ROLLBACK");
        return undefined;
      }
      if (input.acknowledgeSecretExposure) {
        const current = await this.currentSecrets(
          client,
          workspaceId,
          row.workflow_id,
        );
        if (!current || input.acknowledgedWorkflowVersion !== current.version)
          throw new ScheduleConflict(
            "workflow_version_changed",
            "The workflow changed. Review the latest version.",
          );
        if (current.missing.length)
          throw new ScheduleConflict(
            "missing_placeholders",
            "Save all required placeholder values before resuming.",
          );
        await this.checkResolved(client, workspaceId, current.definition);
        await client.query(
          "UPDATE workflow_schedules SET approved_secret_names=$2,required_secret_names=$2,issue=NULL,enabled=true,next_run_at=CASE WHEN kind='interval' AND issue IS NOT NULL THEN now()+(interval_minutes * interval '1 minute') ELSE next_run_at END WHERE id=$1",
          [id, current.names],
        );
      } else if (input.enabled !== undefined) {
        if (input.enabled && row.issue) {
          const current = await this.currentSecrets(
            client,
            workspaceId,
            row.workflow_id,
          );
          if (!current) throw new Error("Workflow not found");
          if (current.missing.length)
            throw new ScheduleConflict(
              "missing_placeholders",
              "Save all required placeholder values before resuming.",
            );
          await this.checkResolved(client, workspaceId, current.definition);
          if (
            current.names.some(
              (name) => !row.approved_secret_names.includes(name),
            )
          ) {
            await client.query(
              "UPDATE workflow_schedules SET issue='secret_acknowledgment_required', required_secret_names=$2 WHERE id=$1",
              [id, current.names],
            );
            const saved = await client.query<Row>(`${select} WHERE s.id=$1`, [
              id,
            ]);
            await client.query("COMMIT");
            return serialize(saved.rows[0]!);
          }
          await client.query(
            "UPDATE workflow_schedules SET issue=NULL,next_run_at=CASE WHEN kind='interval' THEN now()+(interval_minutes * interval '1 minute') ELSE next_run_at END WHERE id=$1",
            [id],
          );
        }
        if (input.enabled && !row.enabled && row.kind === "interval")
          await client.query(
            "UPDATE workflow_schedules SET enabled=true,next_run_at=now()+(interval_minutes * interval '1 minute') WHERE id=$1",
            [id],
          );
        else if (
          input.enabled &&
          !row.enabled &&
          row.kind === "once" &&
          row.next_run_at &&
          row.next_run_at.getTime() <= Date.now()
        ) {
          await client.query(
            "INSERT INTO schedule_occurrences(id,schedule_id,scheduled_at,status,reason) VALUES($1,$2,$3,'missed','paused') ON CONFLICT(schedule_id,scheduled_at) DO NOTHING",
            [randomUUID(), id, row.next_run_at],
          );
          await client.query(
            "UPDATE workflow_schedules SET enabled=true,next_run_at=NULL WHERE id=$1",
            [id],
          );
        } else
          await client.query(
            "UPDATE workflow_schedules SET enabled=$2 WHERE id=$1",
            [id, input.enabled],
          );
      }
      const saved = await client.query<Row>(`${select} WHERE s.id=$1`, [id]);
      await client.query("COMMIT");
      return serialize(saved.rows[0]!);
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }
  async delete(workspaceId: string, id: string): Promise<boolean> {
    const result = await this.pool.query(
      "DELETE FROM workflow_schedules WHERE id=$1 AND workspace_id=$2",
      [id, workspaceId],
    );
    return !!result.rowCount;
  }
  async occurrences(workspaceId: string, id: string) {
    const exists = await this.pool.query(
      "SELECT 1 FROM workflow_schedules WHERE id=$1 AND workspace_id=$2",
      [id, workspaceId],
    );
    if (!exists.rowCount) return undefined;
    const result = await this.pool.query<{
      id: string;
      scheduled_at: Date;
      status: string;
      run_id: string | null;
      reason: string | null;
    }>(
      "SELECT id,scheduled_at,status,run_id,reason FROM schedule_occurrences WHERE schedule_id=$1 ORDER BY scheduled_at DESC LIMIT 100",
      [id],
    );
    return result.rows.map((row) => ({
      id: row.id,
      scheduledAt: row.scheduled_at.toISOString(),
      status: row.status,
      runId: row.run_id,
      reason: row.reason,
    }));
  }
}
