import type { RunRequest } from "./contracts.js";
import type { ExecutionTarget } from "../workflows/definition.js";
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
  async capabilities(): Promise<{ executionTargets: ExecutionTarget[] }> {
    const response = await fetch(new URL("/capabilities", this.base), {
      headers: { authorization: `Bearer ${this.token}` },
      signal: AbortSignal.timeout(10000),
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
  async request(
    path: string,
    method: "GET" | "POST",
    body?: RunRequest,
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
      signal: AbortSignal.timeout(10000),
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
