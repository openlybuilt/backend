import type { RunRequest } from "./contracts.js";
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
