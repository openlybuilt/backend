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
  constructor(
    private readonly store: RunStore,
    private readonly agents: AgentsClient,
  ) {}
  async report(id: string, update: RunUpdate): Promise<BackendRun | undefined> {
    await this.store.update(
      id,
      update.status,
      update.result,
      update.steps,
      update.liveViewUrl,
    );
    return this.store.get(id);
  }
  async create(request: RunRequest): Promise<BackendRun> {
    const current = await this.store.create(request);
    if (terminal.has(current.status) || current.cancelRequested) return current;
    if (
      ["provisioning", "running", "cleaning_up", "cancelling"].includes(
        current.status,
      )
    ) {
      return (await this.reconcile(request.id)) ?? current;
    }
    try {
      const remote = await this.agents.request("/runs", "POST", request);
      if (!remote || remote.id !== request.id)
        throw new Error("Invalid agents response");
      await this.store.update(
        request.id,
        remote.status,
        remote.result,
        remote.steps,
      );
      const fresh = await this.store.get(request.id);
      if (fresh?.cancelRequested && !terminal.has(remote.status))
        await this.reconcile(request.id);
    } catch {
      await this.store.update(request.id, "dispatch_unknown", null);
    }
    const saved = await this.store.get(request.id);
    if (!saved) throw new Error("Run missing");
    return saved;
  }
  async reconcile(
    id: string,
  ): Promise<BackendRun | undefined> {
    const current = await this.store.get(id);
    if (!current || terminal.has(current.status)) return current;
    let liveViewUrl: string | undefined;
    try {
      const path = `/runs/${id}`;
      let remote = await this.agents.request(path, "GET");
      if (remote && current.cancelRequested && !terminal.has(remote.status))
        remote = await this.agents.request(`${path}/cancel`, "POST");
      if (remote && remote.id === id) {
        if (
          !terminal.has(remote.status) &&
          typeof remote.liveViewUrl === "string"
        ) {
          try {
            const u = new URL(remote.liveViewUrl);
            if (u.protocol === "https:" && !u.username && !u.password)
              liveViewUrl = u.href;
          } catch {
            /* Ignore invalid provider URLs. */
          }
        }
        await this.store.update(id, remote.status, remote.result, remote.steps);
      } else if (
        !remote &&
        ["provisioning", "running", "cleaning_up", "cancelling"].includes(
          current.status,
        )
      ) {
        await this.store.update(id, "outcome_unknown", null);
      }
    } catch {
      /* Keep the last known state when agents is unavailable. */
    }
    const saved = await this.store.get(id);
    return saved && liveViewUrl && !terminal.has(saved.status)
      ? { ...saved, liveViewUrl }
      : saved;
  }
  async cancel(id: string): Promise<BackendRun | undefined> {
    const current = await this.store.get(id);
    if (!current || terminal.has(current.status)) return current;
    await this.store.requestCancel(id);
    return this.reconcile(id);
  }
}
