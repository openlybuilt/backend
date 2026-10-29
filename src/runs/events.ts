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

export class EventStore {
  constructor(private readonly pool: pg.Pool) {}
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
