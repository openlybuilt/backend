import { RecordingStore } from "../recordings/store.js";
import { Database, isDatabaseFailure } from "../database.js";
import { AgentsClient } from "./agents-client.js";
import type { RunRequest } from "./contracts.js";
import { RunStore, type BackendRun } from "./store.js";
import type { RunUpdate } from "./updates.js";
import { workflowCleanupGraceMs } from "./timing.js";

const terminal = new Set([
  "completed",
  "failed",
  "cancelled",
  "interrupted",
  "outcome_unknown",
]);
class ProviderUnavailableError extends Error {}
class CancelledBeforeDispatchError extends Error {}
export class RunService {
  private readonly dispatches = new Map<string, Promise<BackendRun>>();
  private readonly owned = new Set<string>();
  private timer?: ReturnType<typeof setInterval>;
  private sweep?: Promise<void>;
  constructor(
    private readonly store: RunStore,
    private readonly agents: AgentsClient,
    private readonly recordings: RecordingStore,
    private readonly database: Database,
  ) {}
  start(): void {
    const tick = () => {
      if (this.sweep || !this.database.ready) return;
      this.sweep = this.reconcileActive()
        .catch((error: unknown) => {
          if (isDatabaseFailure(error)) this.database.unavailable();
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
    await Promise.allSettled(this.dispatches.values());
  }
  async pause(): Promise<void> {
    const ids = [...this.owned];
    this.owned.clear();
    await Promise.allSettled([
      ...ids.map((id) => this.agents.request(`/runs/${id}/cancel`, "POST")),
      ...this.dispatches.values(),
      ...(this.sweep ? [this.sweep] : []),
    ]);
  }
  async recover(signal: AbortSignal): Promise<void> {
    await this.pause();
    signal.throwIfAborted();
    const ids = await this.store.activeIds(true);
    signal.throwIfAborted();
    await Promise.allSettled(
      ids.map((id) =>
        this.agents.request(`/runs/${id}/cancel`, "POST", undefined, signal),
      ),
    );
    signal.throwIfAborted();
    await this.store.interrupt(ids);
    signal.throwIfAborted();
  }
  async report(id: string, update: RunUpdate): Promise<BackendRun | undefined> {
    const signal = this.database.signal;
    this.database.assertReady(signal);
    this.owned.add(id);
    await this.store.update(
      id,
      update.status,
      update.result,
      update.steps,
      update.liveViewUrl,
    );
    this.database.assertReady(signal);
    const saved = await this.store.get(id);
    this.database.assertReady(signal);
    if (!saved || terminal.has(saved.status)) this.owned.delete(id);
    if (saved && terminal.has(saved.status) && saved.status !== update.status)
      throw new Error("Run already finished");
    return saved;
  }
  async create(request: RunRequest): Promise<BackendRun> {
    const signal = this.database.signal;
    this.database.assertReady(signal);
    const current = await this.store.create(request);
    this.database.assertReady(signal);
    if (!terminal.has(current.status)) this.owned.add(request.id);
    const pending = this.dispatches.get(request.id);
    if (pending) return pending;
    if (current.status !== "dispatch_pending" || current.cancelRequested)
      return current;
    const task = this.dispatch(request, signal).finally(() => {
      this.dispatches.delete(request.id);
    });
    this.dispatches.set(request.id, task);
    return task;
  }
  async dispatchPrepared(request: RunRequest): Promise<BackendRun> {
    const signal = this.database.signal;
    this.database.assertReady(signal);
    const current = await this.store.get(request.id);
    this.database.assertReady(signal);
    if (!current) throw new Error("Run missing");
    if (!terminal.has(current.status)) this.owned.add(request.id);
    const pending = this.dispatches.get(request.id);
    if (pending) return pending;
    if (current.status !== "dispatch_pending" || current.cancelRequested)
      return current;
    const task = this.dispatch(request, signal).finally(() => {
      this.dispatches.delete(request.id);
    });
    this.dispatches.set(request.id, task);
    return task;
  }
  private async dispatch(
    request: RunRequest,
    signal: AbortSignal,
  ): Promise<BackendRun> {
    this.database.assertReady(signal);
    if (await this.store.beginDispatch(request.id))
      try {
        this.database.assertReady(signal);
        const current = await this.store.get(request.id);
        this.database.assertReady(signal);
        if (current?.cancelRequested) {
          await this.store.update(request.id, "cancelled", {
            status: "cancelled",
            message: "Run cancelled before execution",
          });
        } else {
          if (request.workflow?.provider) {
            let available = false;
            try {
              const { providers } =
                await this.agents.providerCapabilities(signal);
              available = providers.some(
                (provider) =>
                  provider.id === request.workflow!.provider &&
                  provider.executionTargets.includes(
                    request.workflow!.executionTarget,
                  ),
              );
            } catch (error) {
              if (signal.aborted) throw error;
            }
            this.database.assertReady(signal);
            if (!available) throw new ProviderUnavailableError();
            const afterCheck = await this.store.get(request.id);
            this.database.assertReady(signal);
            if (afterCheck?.cancelRequested)
              throw new CancelledBeforeDispatchError();
          }
          const preference = await this.store.pool.query(
            "SELECT recording_stop_requested FROM backend_runs WHERE id=$1",
            [request.id],
          );
          this.database.assertReady(signal);
          const remote = await this.agents.request(
            "/runs",
            "POST",
            preference.rows[0]?.recording_stop_requested
              ? { ...request, recordingEnabled: false }
              : request,
            signal,
          );
          this.database.assertReady(signal);
          if (!remote || remote.id !== request.id)
            throw new Error("Invalid agents response");
          // The acknowledgement never overwrites progress already reported.
          const fresh = await this.store.get(request.id);
          this.database.assertReady(signal);
          if (fresh?.cancelRequested && !terminal.has(fresh.status))
            await this.agents.request(
              `/runs/${request.id}/cancel`,
              "POST",
              undefined,
              signal,
            );
        }
      } catch (error) {
        this.database.assertReady(signal);
        if (
          error instanceof ProviderUnavailableError ||
          error instanceof CancelledBeforeDispatchError
        ) {
          const current = await this.store.get(request.id);
          this.database.assertReady(signal);
          const status = current?.cancelRequested ? "cancelled" : "failed";
          await this.store.update(request.id, status, {
            status,
            message:
              status === "cancelled"
                ? "Run cancelled before execution"
                : "The selected provider is currently unavailable.",
          });
        } else await this.store.update(request.id, "dispatch_unknown", null);
      }
    this.database.assertReady(signal);
    const saved = await this.store.get(request.id);
    this.database.assertReady(signal);
    if (!saved) throw new Error("Run missing");
    return saved;
  }
  private async reconcileActive(): Promise<void> {
    const signal = this.database.signal;
    this.database.assertReady(signal);
    const ids = await this.store.activeIds();
    this.database.assertReady(signal);
    for (const id of ids) this.owned.add(id);
    for (const id of ids) await this.reconcile(id, signal);
    this.database.assertReady(signal);
    const pendingStops = await this.store.pool.query<{ id: string }>(
      "SELECT id FROM backend_runs WHERE recording_stop_requested",
    );
    this.database.assertReady(signal);
    for (const { id } of pendingStops.rows) {
      try {
        const stopped = await this.agents.request(
          `/runs/${id}/recording/stop`,
          "POST",
          undefined,
          signal,
        );
        this.database.assertReady(signal);
        const current = await this.store.get(id);
        this.database.assertReady(signal);
        if (stopped || (current && terminal.has(current.status)))
          await this.store.pool.query(
            "UPDATE backend_runs SET recording_stop_requested=false WHERE id=$1",
            [id],
          );
      } catch (error) {
        if (signal.aborted) throw error;
      }
    }
    this.database.assertReady(signal);
    await this.recordings.expire();
  }
  private async reconcile(id: string, signal: AbortSignal): Promise<void> {
    this.database.assertReady(signal);
    if (this.dispatches.has(id)) return;
    const current = await this.store.get(id);
    this.database.assertReady(signal);
    if (!current || terminal.has(current.status)) {
      this.owned.delete(id);
      return;
    }
    const cleanupGrace =
      current.workflow || current.workflowId
        ? current.recordingEnabled
          ? workflowCleanupGraceMs
          : 12 * 60 * 1000
        : current.recordingStatus === "pending"
          ? 60000
          : 15000;
    const expired =
      Date.now() >
      new Date(current.createdAt).getTime() + current.timeoutMs + cleanupGrace;
    try {
      const path = `/runs/${id}`;
      const remote = await this.agents.request(path, "GET", undefined, signal);
      this.database.assertReady(signal);
      const fresh = await this.store.get(id);
      this.database.assertReady(signal);
      if (!fresh || terminal.has(fresh.status)) return;
      if (remote && remote.id !== id)
        throw new Error("Invalid agents response");
      if (remote && (fresh.cancelRequested || expired)) {
        await this.agents.request(`${path}/cancel`, "POST", undefined, signal);
        this.database.assertReady(signal);
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
      this.database.assertReady(signal);
      // A temporary connection error does not erase the last reported state.
      if (expired)
        await this.store.update(id, "interrupted", {
          status: "interrupted",
          message: "Execution could not be confirmed before its deadline",
        });
    }
  }
  async cancel(id: string): Promise<BackendRun | undefined> {
    const signal = this.database.signal;
    this.database.assertReady(signal);
    const current = await this.store.get(id);
    this.database.assertReady(signal);
    if (!current || terminal.has(current.status)) return current;
    await this.store.requestCancel(id);
    this.database.assertReady(signal);
    await this.reconcile(id, signal);
    this.database.assertReady(signal);
    const saved = await this.store.get(id);
    this.database.assertReady(signal);
    return saved;
  }
}
