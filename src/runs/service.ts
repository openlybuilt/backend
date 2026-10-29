import { RecordingStore } from "../recordings/store.js";
import { AgentsClient } from "./agents-client.js";
import type { RunRequest } from "./contracts.js";
import { RunStore, type BackendRun } from "./store.js";
import type { RunUpdate } from "./updates.js";

const terminal = new Set([
  "completed",
  "failed",
  "cancelled",
  "interrupted",
  "outcome_unknown",
]);
export class RunService {
  private readonly dispatches = new Map<string, Promise<BackendRun>>();
  private timer?: ReturnType<typeof setInterval>;
  private sweep?: Promise<void>;
  constructor(
    private readonly store: RunStore,
    private readonly agents: AgentsClient,
    private readonly recordings: RecordingStore,
  ) {}
  start(): void {
    const tick = () => {
      if (this.sweep) return;
      this.sweep = this.reconcileActive()
        .catch(() => console.error("Run reconciliation failed"))
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
    await Promise.allSettled(this.dispatches.values());
  }
  async report(id: string, update: RunUpdate): Promise<BackendRun | undefined> {
    await this.store.update(
      id,
      update.status,
      update.result,
      update.steps,
      update.liveViewUrl,
    );
    const saved = await this.store.get(id);
    if (saved && terminal.has(saved.status) && saved.status !== update.status)
      throw new Error("Run already finished");
    return saved;
  }
  async create(request: RunRequest): Promise<BackendRun> {
    const current = await this.store.create(request);
    const pending = this.dispatches.get(request.id);
    if (pending) return pending;
    if (current.status !== "dispatch_pending" || current.cancelRequested)
      return current;
    const task = this.dispatch(request).finally(() => {
      this.dispatches.delete(request.id);
    });
    this.dispatches.set(request.id, task);
    return task;
  }
  private async dispatch(request: RunRequest): Promise<BackendRun> {
    if (await this.store.beginDispatch(request.id))
      try {
        const current = await this.store.get(request.id);
        if (current?.cancelRequested) {
          await this.store.update(request.id, "cancelled", {
            status: "cancelled",
            message: "Run cancelled before execution",
          });
        } else {
          const preference = await this.store.pool.query(
            "SELECT recording_stop_requested FROM backend_runs WHERE id=$1",
            [request.id],
          );
          const remote = await this.agents.request(
            "/runs",
            "POST",
            preference.rows[0]?.recording_stop_requested
              ? { ...request, recordingEnabled: false }
              : request,
          );
          if (!remote || remote.id !== request.id)
            throw new Error("Invalid agents response");
          // The acknowledgement never overwrites progress already reported.
          const fresh = await this.store.get(request.id);
          if (fresh?.cancelRequested && !terminal.has(fresh.status))
            await this.agents.request(`/runs/${request.id}/cancel`, "POST");
        }
      } catch {
        await this.store.update(request.id, "dispatch_unknown", null);
      }
    const saved = await this.store.get(request.id);
    if (!saved) throw new Error("Run missing");
    return saved;
  }
  private async reconcileActive(): Promise<void> {
    for (const id of await this.store.activeIds()) await this.reconcile(id);
    const pendingStops = await this.store.pool.query<{ id: string }>(
      "SELECT id FROM backend_runs WHERE recording_stop_requested",
    );
    for (const { id } of pendingStops.rows) {
      try {
        const stopped = await this.agents.request(
          `/runs/${id}/recording/stop`,
          "POST",
        );
        const current = await this.store.get(id);
        if (stopped || (current && terminal.has(current.status)))
          await this.store.pool.query(
            "UPDATE backend_runs SET recording_stop_requested=false WHERE id=$1",
            [id],
          );
      } catch {
        /* Retry until the run ends. */
      }
    }
    await this.recordings.expire();
  }
  private async reconcile(id: string): Promise<void> {
    if (this.dispatches.has(id)) return;
    const current = await this.store.get(id);
    if (!current || terminal.has(current.status)) return;
    const cleanupGrace = current.recordingStatus === "pending" ? 60000 : 15000;
    const expired =
      Date.now() >
      new Date(current.createdAt).getTime() + current.timeoutMs + cleanupGrace;
    try {
      const path = `/runs/${id}`;
      const remote = await this.agents.request(path, "GET");
      const fresh = await this.store.get(id);
      if (!fresh || terminal.has(fresh.status)) return;
      if (remote && remote.id !== id)
        throw new Error("Invalid agents response");
      if (remote && (fresh.cancelRequested || expired)) {
        await this.agents.request(`${path}/cancel`, "POST");
        await this.store.update(id, "cancelling", null);
      }
      if (!remote || expired) {
        const status =
          current.status === "dispatch_unknown"
            ? "outcome_unknown"
            : "interrupted";
        await this.store.update(id, status, {
          status,
          message: expired
            ? "Execution did not finish before its deadline"
            : "Agents no longer has the active execution",
        });
      }
    } catch {
      // A temporary connection error does not erase the last reported state.
      if (expired)
        await this.store.update(id, "interrupted", {
          status: "interrupted",
          message: "Execution could not be confirmed before its deadline",
        });
    }
  }
  async cancel(id: string): Promise<BackendRun | undefined> {
    const current = await this.store.get(id);
    if (!current || terminal.has(current.status)) return current;
    await this.store.requestCancel(id);
    await this.reconcile(id);
    return this.store.get(id);
  }
}
