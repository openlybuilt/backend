import { timingSafeEqual } from "node:crypto";
import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import { AgentsClient } from "./runs/agents-client.js";
import { parseRun } from "./runs/contracts.js";
import { parseUpdate } from "./runs/updates.js";
import { RunService } from "./runs/service.js";
import { RunStore } from "./runs/store.js";
import { WorkflowStore } from "./workflows/store.js";
import { parseDefinition, resolveWorkflow } from "./workflows/definition.js";

const rawPort = process.env.PORT ?? "3000";
if (!/^\d+$/.test(rawPort) || Number(rawPort) < 1 || Number(rawPort) > 65535)
  throw new Error("PORT must be an integer between 1 and 65535");
if (!process.env.DATABASE_URL) throw new Error("DATABASE_URL is required");
const apiToken = process.env.BACKEND_API_TOKEN ?? "";
if (!/^[A-Za-z0-9_-]{32,256}$/.test(apiToken))
  throw new Error("BACKEND_API_TOKEN must be 32 to 256 URL-safe characters");
const store = new RunStore(process.env.DATABASE_URL);
await store.initialize();
const workflows = new WorkflowStore(store.pool);
const runs = new RunService(
  store,
  new AgentsClient(
    process.env.AGENTS_URL ?? "http://127.0.0.1:3100",
    process.env.AGENTS_API_TOKEN ?? "",
  ),
);
function send(response: ServerResponse, status: number, value: unknown): void {
  response.writeHead(status, { "content-type": "application/json" });
  response.end(JSON.stringify(value));
}
async function readJson(
  request: IncomingMessage,
  limit = 32000,
): Promise<unknown> {
  const chunks: Buffer[] = [];
  let bytes = 0;
  for await (const chunk of request) {
    const data = Buffer.from(chunk);
    bytes += data.length;
    if (bytes > limit) throw new Error("Request body too large");
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
    const updateMatch =
      /^\/internal\/runs\/([0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})\/update$/i.exec(
        request.url ?? "",
      );
    if (request.method === "POST" && updateMatch) {
      if (
        !/^application\/json(?:\s*;|$)/i.test(
          request.headers["content-type"] ?? "",
        )
      )
        return send(response, 415, { error: "JSON content type required" });
      let update;
      try {
        update = parseUpdate(await readJson(request, 1024 * 1024));
      } catch (error) {
        return send(response, 400, {
          error: error instanceof Error ? error.message : "Invalid run update",
        });
      }
      const record = await runs.report(updateMatch[1]!.toLowerCase(), update);
      return record
        ? send(response, 200, record)
        : send(response, 404, { error: "Run not found" });
    }
    const authorization = request.headers.authorization ?? "";
    const provided = Buffer.from(authorization);
    const expected = Buffer.from(`Bearer ${apiToken}`);
    if (
      request.headers.origin !== undefined ||
      provided.length !== expected.length ||
      !timingSafeEqual(provided, expected)
    ) {
      return send(response, 401, { error: "Unauthorized" });
    }
    if (request.method === "GET" && request.url === "/workflows")
      return send(response, 200, await workflows.list());
    if (request.method === "POST" && request.url === "/workflows") {
      if (
        !/^application\/json(?:\s*;|$)/i.test(
          request.headers["content-type"] ?? "",
        )
      )
        return send(response, 415, { error: "JSON content type required" });
      let definition;
      try {
        definition = parseDefinition(await readJson(request));
      } catch (error) {
        return send(response, 400, {
          error: error instanceof Error ? error.message : "Invalid workflow",
        });
      }
      return send(response, 201, await workflows.create(definition));
    }
    const versionMatch =
      /^\/workflows\/([0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})\/versions\/([1-9][0-9]{0,9})$/i.exec(
        request.url ?? "",
      );
    if (versionMatch && request.method === "GET") {
      const number = Number(versionMatch[2]);
      if (number > 2147483647)
        return send(response, 400, { error: "Invalid workflow version" });
      const found = await workflows.get(versionMatch[1]!.toLowerCase(), number);
      return found
        ? send(response, 200, found)
        : send(response, 404, { error: "Workflow version not found" });
    }
    const workflowMatch =
      /^\/workflows\/([0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})(?:\/(versions|runs))?$/i.exec(
        request.url ?? "",
      );
    if (workflowMatch) {
      const workflowId = workflowMatch[1]!.toLowerCase();
      if (request.method === "GET" && !workflowMatch[2]) {
        const found = await workflows.get(workflowId);
        return found
          ? send(response, 200, found)
          : send(response, 404, { error: "Workflow not found" });
      }
      if (request.method === "POST" && workflowMatch[2] === "versions") {
        if (
          !/^application\/json(?:\s*;|$)/i.test(
            request.headers["content-type"] ?? "",
          )
        )
          return send(response, 415, { error: "JSON content type required" });
        let definition;
        try {
          definition = parseDefinition(await readJson(request));
        } catch (error) {
          return send(response, 400, {
            error: error instanceof Error ? error.message : "Invalid workflow",
          });
        }
        const version = await workflows.addVersion(workflowId, definition);
        return version
          ? send(response, 201, version)
          : send(response, 404, { error: "Workflow not found" });
      }
      if (request.method === "POST" && workflowMatch[2] === "runs") {
        if (
          !/^application\/json(?:\s*;|$)/i.test(
            request.headers["content-type"] ?? "",
          )
        )
          return send(response, 415, { error: "JSON content type required" });
        let raw: unknown;
        try {
          raw = await readJson(request);
        } catch {
          return send(response, 400, { error: "Invalid JSON" });
        }
        if (!raw || typeof raw !== "object" || Array.isArray(raw))
          return send(response, 400, { error: "Invalid workflow run" });
        const body = raw as Record<string, unknown>;
        if (
          Object.keys(body).some(
            (key) =>
              !["id", "version", "inputs", "maxSteps", "timeoutMs"].includes(
                key,
              ),
          ) ||
          typeof body.version !== "number" ||
          !Number.isInteger(body.version) ||
          body.version < 1 ||
          body.version > 2147483647
        )
          return send(response, 400, { error: "Invalid workflow run" });
        const version = await workflows.get(workflowId, body.version);
        if (!version)
          return send(response, 404, { error: "Workflow version not found" });
        let input;
        let resolved;
        try {
          input = parseRun({
            id: body.id,
            instruction: "Guided workflow",
            maxSteps: body.maxSteps,
            timeoutMs: body.timeoutMs,
          });
          resolved = resolveWorkflow(version.definition, body.inputs);
        } catch (error) {
          return send(response, 400, {
            error:
              error instanceof Error ? error.message : "Invalid workflow run",
          });
        }
        try {
          return send(
            response,
            202,
            await runs.create({
              ...input,
              workflow: resolved,
              workflowId,
              workflowVersion: body.version,
              workflowInputs: body.inputs as Record<string, string>,
            }),
          );
        } catch (error) {
          if (
            error instanceof Error &&
            error.message === "Run ID already has different inputs"
          )
            return send(response, 409, { error: error.message });
          throw error;
        }
      }
    }
    if (request.method === "POST" && request.url === "/runs") {
      if (
        !/^application\/json(?:\s*;|$)/i.test(
          request.headers["content-type"] ?? "",
        )
      )
        return send(response, 415, { error: "JSON content type required" });
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
