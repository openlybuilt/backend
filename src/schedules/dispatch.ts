import { randomUUID } from "node:crypto";
import type pg from "pg";
import { AccountGate } from "../accounts/deletion.js";
import { Database } from "../database.js";
import { PlaceholderStore } from "../placeholders/store.js";
import { parseRun, type RunRequest } from "../runs/contracts.js";
import { RunStore } from "../runs/store.js";
import { parseDefinition, resolveWorkflow } from "../workflows/definition.js";

type Candidate = {
  id: string;
  workspace_id: string;
  issuer: string;
  subject: string;
};
type QueueRow = {
  schedule_id: string;
  workflow_id: string;
  workspace_id: string;
  kind: "once" | "interval";
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
  gate: AccountGate,
  placeholders: PlaceholderStore,
  runs: RunStore,
  signal: AbortSignal,
): Promise<
  { request: RunRequest; occurrenceId: string; release: () => void } | undefined
> {
  database.assertReady(signal);
  const candidate =
    await pool.query<Candidate>(`SELECT o.id,s.workspace_id,i.issuer,i.subject FROM schedule_occurrences o
    JOIN workflow_schedules s ON s.id=o.schedule_id JOIN workspaces w ON w.id=s.workspace_id JOIN identities i ON i.id=w.identity_id
    WHERE o.status='queued' AND s.enabled AND s.issue IS NULL
      AND NOT EXISTS (SELECT 1 FROM account_deletions d WHERE d.workspace_id=s.workspace_id)
      AND (s.kind='once' OR NOT EXISTS (SELECT 1 FROM backend_runs b WHERE b.workflow_id=s.workflow_id AND b.workspace_id=s.workspace_id AND b.status NOT IN (${terminal})))
    ORDER BY o.scheduled_at,o.id LIMIT 1`);
  if (!candidate.rows.length) return undefined;
  const item = candidate.rows[0]!;
  const release = gate.enter({ issuer: item.issuer, subject: item.subject });
  if (!release) return undefined;
  const client = await pool.connect().catch((error) => {
    release();
    throw error;
  });
  try {
    database.assertReady(signal);
    await client.query("BEGIN");
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
      release();
      return undefined;
    }
    await client.query("SELECT id FROM workspaces WHERE id=$1 FOR UPDATE", [
      schedule.workspace_id,
    ]);
    const deleted = await client.query(
      "SELECT 1 FROM account_deletions WHERE workspace_id=$1",
      [schedule.workspace_id],
    );
    if (deleted.rowCount) {
      await client.query("ROLLBACK");
      release();
      return undefined;
    }
    if (schedule.kind === "interval") {
      const active = await client.query(
        `SELECT 1 FROM backend_runs WHERE workflow_id=$1 AND workspace_id=$2 AND status NOT IN (${terminal}) LIMIT 1`,
        [schedule.workflow_id, schedule.workspace_id],
      );
      if (active.rowCount) {
        await client.query("ROLLBACK");
        release();
        return undefined;
      }
    }
    const owner = await client.query(
      "SELECT id FROM workflows WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
      [schedule.workflow_id, schedule.workspace_id],
    );
    if (!owner.rowCount) {
      await client.query("ROLLBACK");
      release();
      return undefined;
    }
    const version = await client.query<{
      version: number;
      definition: unknown;
    }>(
      "SELECT version,definition FROM workflow_versions WHERE workflow_id=$1 ORDER BY version DESC LIMIT 1",
      [schedule.workflow_id],
    );
    if (!version.rows[0]) throw new Error("Workflow version missing");
    const latest = version.rows[0];
    const definition = parseDefinition(latest.definition);
    const inputs = await placeholders.resolve(
      schedule.workspace_id,
      definition.inputs,
      client,
    );
    if (inputs.missing.length) {
      await client.query(
        "UPDATE workflow_schedules SET issue='missing_placeholders',required_secret_names=$2 WHERE id=$1",
        [schedule.schedule_id, inputs.secretNames],
      );
      await client.query("COMMIT");
      release();
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
      await client.query("COMMIT");
      release();
      return undefined;
    }
    let resolved;
    try {
      resolved = resolveWorkflow(definition, inputs.values);
    } catch {
      await client.query(
        "UPDATE schedule_occurrences SET status='failed',reason='invalid_workflow' WHERE id=$1",
        [item.id],
      );
      await client.query(
        "UPDATE workflow_schedules SET issue='invalid_workflow' WHERE id=$1",
        [schedule.schedule_id],
      );
      await client.query("COMMIT");
      release();
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
      workflowId: schedule.workflow_id,
      workflowVersion: latest.version,
      containsSecrets: inputs.secretNames.length > 0,
      requestFingerprint: placeholders.fingerprint({
        workspaceId: schedule.workspace_id,
        workflowId: schedule.workflow_id,
        version: latest.version,
        input,
        resolved,
      }),
    };
    await runs.insert(request, client);
    await client.query(
      "UPDATE schedule_occurrences SET status='dispatching',run_id=$2 WHERE id=$1",
      [item.id, request.id],
    );
    database.assertReady(signal);
    await client.query("COMMIT");
    return { request, occurrenceId: item.id, release };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    release();
    throw error;
  } finally {
    client.release();
  }
}
