import { randomUUID } from "node:crypto";
import type pg from "pg";
import { Database, isDatabaseFailure } from "../database.js";
import { PlaceholderStore } from "../placeholders/store.js";
import { parseRun, type RunRequest } from "../runs/contracts.js";
import { RunStore } from "../runs/store.js";
import { parseDefinition, resolveWorkflow } from "../workflows/definition.js";
import {
  prewarmPlaceholderNames,
  resolvePrewarm,
} from "../workflows/prewarm.js";
import { agentRequestBody } from "../runs/agents-client.js";

type Candidate = {
  id: string;
  workspace_id: string;
};
type QueueRow = {
  schedule_id: string;
  workflow_id: string;
  workspace_id: string;
  kind: "once" | "interval" | "weekly";
  recording_enabled: boolean;
  approved_secret_names: string[];
  enabled: boolean;
  issue: string | null;
  status: string;
};
const terminal =
  "'completed','failed','cancelled','interrupted','outcome_unknown'";
export async function reserveNext(
  pool: pg.Pool,
  database: Database,
  placeholders: PlaceholderStore,
  runs: RunStore,
  signal: AbortSignal,
): Promise<{ request: RunRequest; occurrenceId: string } | undefined> {
  database.assertReady(signal);
  const candidate =
    await pool.query<Candidate>(`SELECT o.id,s.workspace_id FROM schedule_occurrences o
    JOIN workflow_schedules s ON s.id=o.schedule_id
    WHERE o.status='queued' AND s.enabled AND s.issue IS NULL
      AND (s.kind IN ('once','weekly') OR NOT EXISTS (SELECT 1 FROM backend_runs b WHERE b.workflow_id=s.workflow_id AND b.workspace_id=s.workspace_id AND b.status NOT IN (${terminal})))
    ORDER BY o.scheduled_at,o.id LIMIT 1`);
  if (!candidate.rows.length) return undefined;
  const item = candidate.rows[0]!;
  const client = await pool.connect();
  try {
    database.assertReady(signal);
    await client.query("BEGIN");
    await client.query("SELECT id FROM workspaces WHERE id=$1 FOR UPDATE", [
      item.workspace_id,
    ]);
    const found = await client.query<QueueRow>(
      `SELECT s.id AS schedule_id,s.workflow_id,s.workspace_id,s.kind,s.recording_enabled,s.approved_secret_names,s.enabled,s.issue,o.status
      FROM schedule_occurrences o JOIN workflow_schedules s ON s.id=o.schedule_id WHERE o.id=$1 FOR UPDATE OF s,o`,
      [item.id],
    );
    const schedule = found.rows[0];
    if (
      !schedule ||
      schedule.status !== "queued" ||
      !schedule.enabled ||
      schedule.issue
    ) {
      await client.query("ROLLBACK");
      return undefined;
    }
    if (schedule.kind === "interval") {
      const active = await client.query(
        `SELECT 1 FROM backend_runs WHERE workflow_id=$1 AND workspace_id=$2 AND status NOT IN (${terminal}) LIMIT 1`,
        [schedule.workflow_id, schedule.workspace_id],
      );
      if (active.rowCount) {
        await client.query("ROLLBACK");
        return undefined;
      }
    }
    const owner = await client.query(
      "SELECT id FROM workflows WHERE id=$1 AND workspace_id=$2 AND deleted_at IS NULL FOR UPDATE",
      [schedule.workflow_id, schedule.workspace_id],
    );
    if (!owner.rowCount) {
      await client.query("ROLLBACK");
      return undefined;
    }
    const version = await client.query<{
      version: number;
      definition: unknown;
    }>(
      "SELECT version,definition FROM workflow_versions WHERE workflow_id=$1 ORDER BY version DESC LIMIT 1",
      [schedule.workflow_id],
    );
    let latest: { version: number; definition: unknown };
    let definition: ReturnType<typeof parseDefinition>;
    let inputs: Awaited<ReturnType<PlaceholderStore["resolve"]>>;
    let prewarmInputs: Awaited<ReturnType<PlaceholderStore["resolve"]>>;
    try {
      if (!version.rows[0]) throw new Error("Workflow version missing");
      latest = version.rows[0];
      definition = parseDefinition(latest.definition);
      inputs = await placeholders.resolve(
        schedule.workspace_id,
        definition.inputs,
        client,
      );
      prewarmInputs = await placeholders.resolve(
        schedule.workspace_id,
        prewarmPlaceholderNames(definition.prewarm ?? []),
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
      database.assertReady(signal);
      await client.query(
        "UPDATE workflow_schedules SET issue='invalid_workflow' WHERE id=$1",
        [schedule.schedule_id],
      );
      await client.query("COMMIT");
      return undefined;
    }
    if (inputs.missing.length || prewarmInputs.missing.length) {
      await client.query(
        "UPDATE workflow_schedules SET issue='missing_placeholders',required_secret_names=$2 WHERE id=$1",
        [schedule.schedule_id, inputs.secretNames],
      );
      database.assertReady(signal);
      await client.query("COMMIT");
      return undefined;
    }
    if (
      inputs.secretNames.some(
        (name) => !schedule.approved_secret_names.includes(name),
      )
    ) {
      await client.query(
        "UPDATE workflow_schedules SET issue='secret_acknowledgment_required',required_secret_names=$2 WHERE id=$1",
        [schedule.schedule_id, inputs.secretNames],
      );
      database.assertReady(signal);
      await client.query("COMMIT");
      return undefined;
    }
    let resolved;
    let prewarm;
    try {
      resolved = resolveWorkflow(definition, inputs.values);
      prewarm = definition.prewarm
        ? resolvePrewarm(definition.prewarm, prewarmInputs.values)
        : undefined;
    } catch {
      await client.query(
        "UPDATE workflow_schedules SET issue='invalid_workflow' WHERE id=$1",
        [schedule.schedule_id],
      );
      database.assertReady(signal);
      await client.query("COMMIT");
      return undefined;
    }
    const input = parseRun({
      id: randomUUID(),
      instruction: "Guided workflow",
      timeoutMs: 3600000,
      recordingEnabled: schedule.recording_enabled,
    });
    const request: RunRequest = {
      ...input,
      workspaceId: schedule.workspace_id,
      workflow: resolved,
      ...(prewarm ? { prewarm } : {}),
      workflowId: schedule.workflow_id,
      workflowVersion: latest.version,
      containsSecrets:
        inputs.secretNames.length > 0 || prewarmInputs.secretNames.length > 0,
      requestFingerprint: placeholders.fingerprint({
        workspaceId: schedule.workspace_id,
        workflowId: schedule.workflow_id,
        version: latest.version,
        input,
        resolved,
        prewarm,
      }),
    };
    try {
      if (prewarm) agentRequestBody({ ...request, recordingEnabled: false });
    } catch {
      await client.query(
        "UPDATE workflow_schedules SET issue='invalid_workflow' WHERE id=$1",
        [schedule.schedule_id],
      );
      database.assertReady(signal);
      await client.query("COMMIT");
      return undefined;
    }
    await runs.insert(request, client);
    await client.query(
      "UPDATE schedule_occurrences SET status='dispatching',run_id=$2 WHERE id=$1",
      [item.id, request.id],
    );
    database.assertReady(signal);
    await client.query("COMMIT");
    return { request, occurrenceId: item.id };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}
