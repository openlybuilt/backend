import { AgentsClient } from "./agents-client.js";
import type { RunRequest } from "./contracts.js";
import { RunStore, type BackendRun } from "./store.js";

const terminal = new Set(["completed", "failed", "cancelled", "interrupted"]);
export class RunService {
  constructor(
    private readonly store: RunStore,
    private readonly agents: AgentsClient,
  ) {}
  async create(request: RunRequest): Promise<BackendRun> {
    const current = await this.store.create(request);
    if (terminal.has(current.status) || current.cancelRequested) return current;
    try {
      const remote = await this.agents.request("/runs", "POST", request);
      if (!remote || remote.id !== request.id)
        throw new Error("Invalid agents response");
      await this.store.update(request.id, remote.status, remote.result);
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
  async reconcile(id: string): Promise<BackendRun | undefined> {
    const current = await this.store.get(id);
    if (!current || terminal.has(current.status)) return current;
    try {
      const path = `/runs/${id}`;
      let remote = await this.agents.request(path, "GET");
      if (remote && current.cancelRequested && !terminal.has(remote.status))
        remote = await this.agents.request(`${path}/cancel`, "POST");
      if (remote && remote.id === id)
        await this.store.update(id, remote.status, remote.result);
    } catch {
      /* Keep the last known state when agents is unavailable. */
    }
    return this.store.get(id);
  }
  async cancel(id: string): Promise<BackendRun | undefined> {
    const current = await this.store.get(id);
    if (!current || terminal.has(current.status)) return current;
    await this.store.requestCancel(id);
    return this.reconcile(id);
  }
}
