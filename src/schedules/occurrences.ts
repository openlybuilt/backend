import { randomUUID } from "node:crypto";
import type pg from "pg";
import { Database } from "../database.js";

type Due = {
  id: string;
  kind: "once" | "interval";
  interval_minutes: number | null;
  next_run_at: Date;
};
export async function materializeDue(
  pool: pg.Pool,
  database: Database,
  boundary: Date,
  signal: AbortSignal,
): Promise<void> {
  const client = await pool.connect();
  try {
    database.assertReady(signal);
    await client.query("BEGIN");
    const result =
      await client.query<Due>(`SELECT id,kind,interval_minutes,next_run_at FROM workflow_schedules
      WHERE enabled AND issue IS NULL AND next_run_at<=now() ORDER BY next_run_at LIMIT 50 FOR UPDATE SKIP LOCKED`);
    let budget = 500;
    for (const schedule of result.rows) {
      database.assertReady(signal);
      let next: Date | null = schedule.next_run_at;
      const now = Date.now();
      while (next && next.getTime() <= now && budget > 0) {
        const missed = next.getTime() < boundary.getTime();
        await client.query(
          `INSERT INTO schedule_occurrences(id,schedule_id,scheduled_at,status,reason)
          VALUES($1,$2,$3,$4,$5) ON CONFLICT(schedule_id,scheduled_at) DO NOTHING`,
          [
            randomUUID(),
            schedule.id,
            next,
            missed ? "missed" : "queued",
            missed ? "scheduler_unavailable" : null,
          ],
        );
        budget--;
        next =
          schedule.kind === "once"
            ? null
            : new Date(next.getTime() + schedule.interval_minutes! * 60000);
      }
      await client.query(
        "UPDATE workflow_schedules SET next_run_at=$2 WHERE id=$1",
        [schedule.id, next],
      );
      if (!budget) break;
    }
    database.assertReady(signal);
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}
