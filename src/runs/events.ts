import type pg from "pg";
import type { WorkflowStep } from "../workflows/definition.js";

export type RunEvent = {
  sequence: number;
  timestamp: string;
  status: "running" | "completed" | "failed" | "cancelled";
  stepIndex?: number;
  durationMs?: number;
} & (
  | { kind: "step"; stepIndex: number; stepType: WorkflowStep["type"] }
  | { kind: "model"; usage?: { inputTokens: number; outputTokens: number } }
  | {
      kind: "action";
      action: "navigate" | "click" | "type" | "press_keys" | "scroll";
    }
);

function object(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === "object" && !Array.isArray(value));
}
function integer(
  value: unknown,
  min = 0,
  max = Number.MAX_SAFE_INTEGER,
): boolean {
  return (
    typeof value === "number" &&
    Number.isSafeInteger(value) &&
    value >= min &&
    value <= max
  );
}
export function parseEventBatch(value: unknown): RunEvent[] {
  if (
    !object(value) ||
    Object.keys(value).length !== 1 ||
    !Array.isArray(value.events) ||
    value.events.length < 1 ||
    value.events.length > 32
  )
    throw new Error("Expected 1 to 32 events");
  const sequences = new Set<number>();
  return value.events.map((event: unknown): RunEvent => {
    if (
      !object(event) ||
      !integer(event.sequence, 1, 10000) ||
      typeof event.timestamp !== "string" ||
      event.timestamp.length !== 24 ||
      !Number.isFinite(Date.parse(event.timestamp)) ||
      new Date(event.timestamp).toISOString() !== event.timestamp ||
      typeof event.kind !== "string" ||
      !["step", "model", "action"].includes(event.kind) ||
      typeof event.status !== "string" ||
      !["running", "completed", "failed", "cancelled"].includes(event.status) ||
      (event.stepIndex !== undefined && !integer(event.stepIndex, 0, 29)) ||
      (event.durationMs !== undefined && !integer(event.durationMs))
    )
      throw new Error("Invalid event metadata");
    const fields = [
      "sequence",
      "timestamp",
      "kind",
      "status",
      "stepIndex",
      "durationMs",
      ...(event.kind === "step"
        ? ["stepType"]
        : event.kind === "model"
          ? ["usage"]
          : ["action"]),
    ];
    if (Object.keys(event).some((key) => !fields.includes(key)))
      throw new Error("Unknown event field");
    if (
      event.kind === "step" &&
      (!integer(event.stepIndex, 0, 29) ||
        typeof event.stepType !== "string" ||
        ![
          "navigate",
          "type",
          "wait",
          "scroll",
          "press_keys",
          "click",
          "ai",
        ].includes(event.stepType))
    )
      throw new Error("Invalid step event");
    if (
      event.kind === "action" &&
      (typeof event.action !== "string" ||
        !["navigate", "click", "type", "press_keys", "scroll"].includes(
          event.action,
        ))
    )
      throw new Error("Invalid action event");
    if (
      event.usage !== undefined &&
      (!object(event.usage) ||
        Object.keys(event.usage).length !== 2 ||
        !integer(event.usage.inputTokens) ||
        !integer(event.usage.outputTokens))
    )
      throw new Error("Invalid event usage");
    const sequence = event.sequence as number;
    if (sequences.has(sequence))
      throw new Error("Duplicate event sequence in batch");
    sequences.add(sequence);
    return event as RunEvent;
  });
}

export class EventStore {
  constructor(private readonly pool: pg.Pool) {}
  async list(id: string): Promise<RunEvent[] | undefined> {
    const run = await this.pool.query(
      "SELECT id FROM backend_runs WHERE id=$1",
      [id],
    );
    if (!run.rowCount) return undefined;
    const result = await this.pool.query(
      "SELECT event FROM run_events WHERE run_id=$1 ORDER BY sequence LIMIT 10000",
      [id],
    );
    return result.rows.map((row: { event: RunEvent }) => row.event);
  }
  async append(
    id: string,
    events: RunEvent[],
  ): Promise<"saved" | "missing" | "conflict"> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const run = await client.query(
        "SELECT id FROM backend_runs WHERE id=$1 FOR KEY SHARE",
        [id],
      );
      if (!run.rowCount) {
        await client.query("ROLLBACK");
        return "missing";
      }
      const saved = await client.query(
        "INSERT INTO run_events (run_id,sequence,event) SELECT $1,(event->>'sequence')::integer,event FROM jsonb_array_elements($2::jsonb) AS event ORDER BY (event->>'sequence')::integer ON CONFLICT (run_id,sequence) DO UPDATE SET event=run_events.event WHERE run_events.event=EXCLUDED.event RETURNING sequence",
        [id, JSON.stringify(events)],
      );
      if (saved.rowCount !== events.length) {
        await client.query("ROLLBACK");
        return "conflict";
      }
      await client.query("COMMIT");
      return "saved";
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }
}
