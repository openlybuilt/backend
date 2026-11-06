import { randomUUID } from "node:crypto";
import type pg from "pg";
import { nextWeekly } from "./weekly.js";
import { Database } from "../database.js";

type Due = {
  id: string;
  kind: "once" | "interval" | "weekly";
  interval_minutes: number | null;
  weekdays: number[] | null;
  local_time: string | null;
  timezone: string | null;
  next_run_at: Date;
};
export async function recoverReservations(
  pool: pg.Pool,
  database: Database,
  signal: AbortSignal,
): Promise<void> {
  const client = await pool.connect();
  try {
    database.assertReady(signal);
    await client.query("BEGIN");
    // No agent call is made until beginDispatch changes this state.
    await client.query(`DELETE FROM backend_runs b USING schedule_occurrences o
      WHERE o.run_id=b.id AND o.status='dispatching' AND b.status='dispatch_pending'`);
    await client.query(`UPDATE schedule_occurrences o SET status='queued',run_id=NULL
      WHERE o.status='dispatching' AND NOT EXISTS (SELECT 1 FROM backend_runs b WHERE b.id=o.run_id)`);
    // A completed callback is conclusive even if the scheduler's acknowledgement was lost.
    await client.query(`UPDATE schedule_occurrences o SET status=CASE WHEN b.status='failed' THEN 'failed' ELSE 'started' END
      FROM backend_runs b WHERE o.status='dispatching' AND o.run_id=b.id AND b.status IN ('completed','failed','cancelled')`);
    await client.query(`WITH uncertain AS (
      UPDATE schedule_occurrences o SET status='outcome_unknown',reason='dispatch_unknown'
      FROM backend_runs b WHERE o.status='dispatching' AND o.run_id=b.id AND b.status<>'dispatch_pending'
      RETURNING o.schedule_id)
      UPDATE workflow_schedules s SET issue='dispatch_unknown' FROM uncertain u WHERE s.id=u.schedule_id`);
    database.assertReady(signal);
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}
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
      await client.query<Due>(`SELECT s.id,s.kind,s.interval_minutes,s.weekdays,s.local_time,s.next_run_at,w.timezone FROM workflow_schedules s JOIN workspaces w ON w.id=s.workspace_id
      WHERE s.enabled AND s.issue IS NULL AND s.next_run_at<=now() ORDER BY s.next_run_at LIMIT 50 FOR UPDATE OF s SKIP LOCKED`);
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
            : schedule.kind === "weekly"
              ? nextWeekly(
                  next,
                  schedule.timezone!,
                  schedule.weekdays!,
                  schedule.local_time!,
                )
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
