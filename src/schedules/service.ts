import { Database, isDatabaseFailure } from "../database.js";
import { PlaceholderStore } from "../placeholders/store.js";
import { RunService } from "../runs/service.js";
import { RunStore } from "../runs/store.js";
import { reserveNext } from "./dispatch.js";
import { materializeDue, recoverReservations } from "./occurrences.js";

export class ScheduleService {
  private timer?: ReturnType<typeof setInterval>;
  private sweep?: Promise<void>;
  private epoch?: AbortSignal;
  private boundary = new Date();
  constructor(
    private readonly database: Database,
    private readonly placeholders: PlaceholderStore,
    private readonly store: RunStore,
    private readonly runs: RunService,
  ) {}
  start(): void {
    const tick = () => {
      if (this.sweep || !this.database.ready) return;
      const signal = this.database.signal;
      this.sweep = this.tick(signal)
        .catch((error: unknown) => {
          if (isDatabaseFailure(error)) this.database.unavailable();
          else if (!signal.aborted) console.error("Schedule processing failed");
        })
        .finally(() => {
          this.sweep = undefined;
        });
    };
    this.timer = setInterval(tick, 1000);
    this.timer.unref();
    tick();
  }
  async stop(): Promise<void> {
    clearInterval(this.timer);
    await this.sweep;
  }
  private async tick(signal: AbortSignal): Promise<void> {
    this.database.assertReady(signal);
    if (this.epoch !== signal) {
      this.epoch = signal;
      this.boundary = new Date();
    }
    // Only one sweep runs at a time; any leftover reservation is no longer in flight here.
    await recoverReservations(this.store.pool, this.database, signal);
    await materializeDue(this.store.pool, this.database, this.boundary, signal);
    for (let i = 0; i < 50; i++) {
      this.database.assertReady(signal);
      const reserved = await reserveNext(
        this.store.pool,
        this.database,
        this.placeholders,
        this.store,
        signal,
      );
      if (!reserved) break;
      const run = await this.runs.dispatchPrepared(reserved.request);
      this.database.assertReady(signal);
      if (
        run.status === "dispatch_unknown" ||
        run.status === "outcome_unknown"
      ) {
        await this.store.pool.query(
          `WITH updated AS (UPDATE schedule_occurrences SET status='outcome_unknown',reason='dispatch_unknown' WHERE id=$1 RETURNING schedule_id)
            UPDATE workflow_schedules s SET issue='dispatch_unknown' FROM updated u WHERE s.id=u.schedule_id`,
          [reserved.occurrenceId],
        );
      } else {
        await this.store.pool.query(
          "UPDATE schedule_occurrences SET status='started' WHERE id=$1 AND status='dispatching'",
          [reserved.occurrenceId],
        );
      }
    }
  }
}
