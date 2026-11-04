import type { RunRequest } from "./contracts.js";
import type { ExecutionTarget, ProviderId } from "../workflows/definition.js";
export type ProviderCapability = {
  id: ProviderId;
  executionTargets: ExecutionTarget[];
};
export type AgentRun = {
  id: string;
  status: string;
};
export class AgentsClient {
  constructor(
    private readonly base: string,
    private readonly token: string,
  ) {
    if (!/^[A-Za-z0-9_-]{32,256}$/.test(token))
      throw new Error("AGENTS_API_TOKEN must be 32 to 256 URL-safe characters");
    const url = new URL(base);
    if (
      url.protocol !== "http:" ||
      url.hostname !== "127.0.0.1" ||
      url.pathname !== "/" ||
      url.search ||
      url.hash ||
      url.username ||
      url.password
    )
      throw new Error("AGENTS_URL must be a loopback HTTP origin");
  }
  async capabilities(
    signal?: AbortSignal,
  ): Promise<{ executionTargets: ExecutionTarget[] }> {
    const response = await fetch(new URL("/capabilities", this.base), {
      headers: { authorization: `Bearer ${this.token}` },
      signal: signal
        ? AbortSignal.any([signal, AbortSignal.timeout(10000)])
        : AbortSignal.timeout(10000),
      redirect: "error",
    });
    if (!response.ok || !response.body) {
      await response.body?.cancel();
      throw new Error("Execution capabilities unavailable");
    }
    const chunks: Uint8Array[] = [];
    let bytes = 0;
    for await (const chunk of response.body) {
      bytes += chunk.byteLength;
      if (bytes > 1024) throw new Error("Invalid execution capabilities");
      chunks.push(chunk);
    }
    const data: unknown = JSON.parse(Buffer.concat(chunks).toString("utf8"));
    if (!data || typeof data !== "object" || Array.isArray(data))
      throw new Error("Invalid execution capabilities");
    const targets = (data as Record<string, unknown>).executionTargets;
    if (
      Object.keys(data).length !== 1 ||
      !Array.isArray(targets) ||
      targets.length > 2 ||
      new Set(targets).size !== targets.length ||
      targets.some((target) => target !== "browser" && target !== "windows")
    )
      throw new Error("Invalid execution capabilities");
    return { executionTargets: targets as ExecutionTarget[] };
  }
  async providerCapabilities(
    signal?: AbortSignal,
  ): Promise<{ providers: ProviderCapability[] }> {
    const response = await fetch(new URL("/provider-capabilities", this.base), {
      headers: { authorization: `Bearer ${this.token}` },
      signal: signal
        ? AbortSignal.any([signal, AbortSignal.timeout(10000)])
        : AbortSignal.timeout(10000),
      redirect: "error",
    });
    if (!response.ok || !response.body) {
      await response.body?.cancel();
      throw new Error("Provider capabilities unavailable");
    }
    const chunks: Uint8Array[] = [];
    let bytes = 0;
    for await (const chunk of response.body) {
      bytes += chunk.byteLength;
      if (bytes > 4096) throw new Error("Invalid provider capabilities");
      chunks.push(chunk);
    }
    const value: unknown = JSON.parse(Buffer.concat(chunks).toString("utf8"));
    if (!value || typeof value !== "object" || Array.isArray(value))
      throw new Error("Invalid provider capabilities");
    const data = value as Record<string, unknown>;
    if (
      Object.keys(data).length > 16 ||
      !Array.isArray(data.providers) ||
      data.providers.length > 32
    )
      throw new Error("Invalid provider capabilities");
    const providers: ProviderCapability[] = [];
    const seen = new Set<string>();
    for (const item of data.providers) {
      if (!item || typeof item !== "object" || Array.isArray(item))
        throw new Error("Invalid provider capabilities");
      const entry = item as Record<string, unknown>;
      if (
        Object.keys(entry).length > 16 ||
        typeof entry.id !== "string" ||
        !/^[a-z][a-z0-9-]{0,63}$/.test(entry.id) ||
        seen.has(entry.id) ||
        !Array.isArray(entry.executionTargets) ||
        entry.executionTargets.length < 1 ||
        entry.executionTargets.length > 16 ||
        new Set(entry.executionTargets).size !==
          entry.executionTargets.length ||
        entry.executionTargets.some(
          (target) =>
            typeof target !== "string" ||
            !/^[a-z][a-z0-9-]{0,63}$/.test(target),
        )
      )
        throw new Error("Invalid provider capabilities");
      seen.add(entry.id);
      const supported = entry.executionTargets.filter(
        (target): target is ExecutionTarget =>
          target === "browser" || target === "windows",
      );
      if (supported.length)
        providers.push({ id: entry.id, executionTargets: supported });
    }
    return { providers };
  }
  async request(
    path: string,
    method: "GET" | "POST",
    body?: RunRequest,
    signal?: AbortSignal,
  ): Promise<AgentRun | undefined> {
    const response = await fetch(new URL(path, this.base), {
      method,
      headers: {
        authorization: `Bearer ${this.token}`,
        ...(body ? { "content-type": "application/json" } : {}),
      },
      body: body
        ? JSON.stringify({
            id: body.id,
            instruction: body.instruction,
            maxSteps: body.maxSteps,
            timeoutMs: body.timeoutMs,
            ...(body.recordingEnabled === false
              ? { recordingEnabled: false }
              : {}),
            ...(body.workflow ? { workflow: body.workflow } : {}),
          })
        : undefined,
      signal: AbortSignal.any([
        AbortSignal.timeout(10000),
        ...(signal ? [signal] : []),
      ]),
      redirect: "error",
    });
    if (response.status === 404) return undefined;
    if (!response.ok) throw new Error("Agents request failed");
    const data: unknown = await response.json();
    if (
      !data ||
      typeof data !== "object" ||
      typeof (data as AgentRun).id !== "string" ||
      typeof (data as AgentRun).status !== "string"
    )
      throw new Error("Invalid agents response");
    return data as AgentRun;
  }
}
