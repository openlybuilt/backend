import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import { AgentsClient } from "./runs/agents-client.js";
import { parseRun } from "./runs/contracts.js";
import { RunService } from "./runs/service.js";
import { RunStore } from "./runs/store.js";

const rawPort = process.env.PORT ?? "3000";
if (!/^\d+$/.test(rawPort) || Number(rawPort) < 1 || Number(rawPort) > 65535)
  throw new Error("PORT must be an integer between 1 and 65535");
if (!process.env.DATABASE_URL) throw new Error("DATABASE_URL is required");
const store = new RunStore(process.env.DATABASE_URL);
await store.initialize();
const runs = new RunService(
  store,
  new AgentsClient(process.env.AGENTS_URL ?? "http://127.0.0.1:3100"),
);
function send(response: ServerResponse, status: number, value: unknown): void {
  response.writeHead(status, { "content-type": "application/json" });
  response.end(JSON.stringify(value));
}
async function readJson(request: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  let bytes = 0;
  for await (const chunk of request) {
    const data = Buffer.from(chunk);
    bytes += data.length;
    if (bytes > 32000) throw new Error("Request body too large");
    chunks.push(data);
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw new Error("Invalid JSON");
  }
}
const server = createServer((request, response) => {
  void (async () => {
    if (request.method === "GET" && request.url === "/health")
      return send(response, 200, { status: "ok" });
    if (request.method === "POST" && request.url === "/runs") {
      let input;
      try {
        input = parseRun(await readJson(request));
      } catch (error) {
        return send(response, 400, {
          error: error instanceof Error ? error.message : "Invalid request",
        });
      }
      try {
        return send(response, 202, await runs.create(input));
      } catch (error) {
        if (
          error instanceof Error &&
          error.message === "Run ID already has different inputs"
        )
          return send(response, 409, { error: error.message });
        throw error;
      }
    }
    const match =
      /^\/runs\/([0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})(\/cancel)?$/i.exec(
        request.url ?? "",
      );
    if (match && request.method === "GET" && !match[2]) {
      const record = await runs.reconcile(match[1]!.toLowerCase());
      return record
        ? send(response, 200, record)
        : send(response, 404, { error: "Run not found" });
    }
    if (match && request.method === "POST" && match[2]) {
      const record = await runs.cancel(match[1]!.toLowerCase());
      return record
        ? send(response, 200, record)
        : send(response, 404, { error: "Run not found" });
    }
    send(response, 404, { error: "Not found" });
  })().catch(() => {
    if (!response.headersSent)
      send(response, 500, { error: "Internal service error" });
    else response.end();
  });
});
server.listen(Number(rawPort), "127.0.0.1", () =>
  console.log(`Backend listening on 127.0.0.1:${rawPort}`),
);
for (const signal of ["SIGINT", "SIGTERM"] as const)
  process.once(signal, () => {
    server.close(() => {
      void store.close().catch(() => {
        process.exitCode = 1;
      });
    });
  });
