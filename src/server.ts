import { timingSafeEqual } from "node:crypto";
import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import { RecordingStore } from "./recordings/store.js";
import { RecordingPreferences } from "./recordings/preferences.js";
import { recordingPlayback, recordingUpload } from "./recordings/routes.js";
import { EventStore, parseEventBatch } from "./runs/events.js";
import { AgentsClient } from "./runs/agents-client.js";
import { parseRun } from "./runs/contracts.js";
import { parseUpdate } from "./runs/updates.js";
import { RunService } from "./runs/service.js";
import { RunStore } from "./runs/store.js";
import { WorkflowStore } from "./workflows/store.js";
import { parseDefinition, resolveWorkflow } from "./workflows/definition.js";
import { IdentityClient } from "./identity/client.js";
import { WorkspaceStore } from "./workspaces/store.js";
import { AccountDeletionService, AccountGate } from "./accounts/deletion.js";
import { PlaceholderStore } from "./placeholders/store.js";

const rawPort = process.env.PORT ?? "3000";
if (!/^\d+$/.test(rawPort) || Number(rawPort) < 1 || Number(rawPort) > 65535)
  throw new Error("PORT must be an integer between 1 and 65535");
if (!process.env.DATABASE_URL) throw new Error("DATABASE_URL is required");
const identityClient = new IdentityClient(
  process.env.AUTH_VERIFY_URL ?? "",
  process.env.AUTH_SERVICE_TOKEN ?? "",
);
const callbackToken = process.env.AGENTS_CALLBACK_TOKEN ?? "";
if (!/^[A-Za-z0-9_-]{32,256}$/.test(callbackToken))
  throw new Error(
    "AGENTS_CALLBACK_TOKEN must be 32 to 256 URL-safe characters",
  );
if (callbackToken === process.env.AUTH_SERVICE_TOKEN)
  throw new Error(
    "Agent callback and identity service credentials must differ",
  );
const store = new RunStore(process.env.DATABASE_URL);
await store.initialize();
const workflows = new WorkflowStore(store.pool);
const workspaces = new WorkspaceStore(store.pool);
const placeholders = new PlaceholderStore(
  store.pool,
  process.env.PLACEHOLDER_ENCRYPTION_KEY ?? "",
);
const events = new EventStore(store.pool);
const recordings = new RecordingStore(store.pool);
const recordingPreferences = new RecordingPreferences(store.pool);
const agents = new AgentsClient(
  process.env.AGENTS_URL ?? "http://127.0.0.1:3100",
  process.env.AGENTS_API_TOKEN ?? "",
);
const runs = new RunService(store, agents, recordings);
const accountGate = new AccountGate();
const accountDeletion = new AccountDeletionService(
  store.pool,
  agents,
  identityClient,
);
accountDeletion.start();
runs.start();
function send(response: ServerResponse, status: number, value: unknown): void {
  response.writeHead(status, {
    "content-type": "application/json",
    "cache-control": "no-store",
  });
  response.end(JSON.stringify(value));
}
async function readJson(
  request: IncomingMessage,
  limit = 32000,
): Promise<unknown> {
  if (Number(request.headers["content-length"]) > limit) {
    request.resume();
    throw new Error("Request body too large");
  }
  const chunks: Buffer[] = [];
  let bytes = 0;
  for await (const chunk of request.iterator({ destroyOnReturn: false })) {
    const data = Buffer.from(chunk);
    bytes += data.length;
    if (bytes > limit) {
      request.resume();
      throw new Error("Request body too large");
    }
    chunks.push(data);
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw new Error("Invalid JSON");
  }
}
const server = createServer((request, response) => {
  let releaseAccount: (() => void) | undefined;
  void (async () => {
    if (request.method === "GET" && request.url === "/health")
      return send(response, 200, { status: "ok" });
    if (/^\/internal\//i.test(request.url ?? "")) {
      const provided = Buffer.from(request.headers.authorization ?? "");
      const expected = Buffer.from(`Bearer ${callbackToken}`);
      if (
        request.headers.origin !== undefined ||
        provided.length !== expected.length ||
        !timingSafeEqual(provided, expected)
      )
        return send(response, 401, { error: "Unauthorized" });
    }
    if (await recordingUpload(request, response, recordings)) return;
    const internalEvents =
      /^\/internal\/runs\/([0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})\/events$/i.exec(
        request.url ?? "",
      );
    if (request.method === "POST" && internalEvents) {
      if (
        !/^application\/json(?:\s*;|$)/i.test(
          request.headers["content-type"] ?? "",
        )
      )
        return send(response, 415, { error: "JSON content type required" });
      let batch;
      try {
        batch = parseEventBatch(await readJson(request, 32768));
      } catch (error) {
        request.resume();
        const message =
          error instanceof Error ? error.message : "Invalid event batch";
        return send(
          response,
          message === "Request body too large" ? 413 : 400,
          { error: message },
        );
      }
      const outcome = await events.append(
        internalEvents[1]!.toLowerCase(),
        batch,
      );
      return outcome === "saved"
        ? send(response, 200, { accepted: batch.length })
        : send(response, outcome === "missing" ? 404 : 409, {
            error:
              outcome === "missing"
                ? "Run not found"
                : "Event sequence already has different data",
          });
    }
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
      try {
        const record = await runs.report(updateMatch[1]!.toLowerCase(), update);
        return record
          ? send(response, 200, record)
          : send(response, 404, { error: "Run not found" });
      } catch (error) {
        if (error instanceof Error && error.message === "Run already finished")
          return send(response, 409, { error: error.message });
        throw error;
      }
    }
    const authorization = /^Bearer ([A-Za-z0-9._~-]{1,10000})$/.exec(
      request.headers.authorization ?? "",
    );
    if (request.headers.origin !== undefined || !authorization) {
      return send(response, 401, { error: "Unauthorized" });
    }
    let identity;
    try {
      identity = await identityClient.verify(authorization[1]!);
    } catch {
      return send(response, 503, {
        error: "Identity verification unavailable",
      });
    }
    if (!identity) return send(response, 401, { error: "Unauthorized" });
    if (request.method === "GET" && request.url === "/account/deletion") {
      return send(response, 200, {
        status: (await accountDeletion.status(identity)) ?? "none",
      });
    }
    if (request.method === "DELETE" && request.url === "/account") {
      let verified;
      try {
        verified = await identityClient.verifyDeletion(authorization[1]!);
      } catch {
        return send(response, 503, {
          error: "Account verification unavailable",
        });
      }
      if (
        !verified ||
        verified.issuer !== identity.issuer ||
        verified.subject !== identity.subject
      )
        return send(response, 403, {
          code: "reauthentication_required",
          error: "Recent verification required",
        });
      try {
        await accountGate.block(identity);
      } catch {
        return send(response, 409, {
          error: "Account deletion is being requested",
        });
      }
      try {
        const workspaceId = await workspaces.getOrCreate(identity);
        await accountDeletion.queue(identity, workspaceId);
      } catch (error) {
        if (
          error instanceof Error &&
          error.message === "Account deletion requested"
        )
          return send(response, 202, { status: "pending" });
        accountGate.unblock(identity);
        throw error;
      }
      return send(response, 202, { status: "pending" });
    }
    releaseAccount = accountGate.enter(identity);
    if (!releaseAccount)
      return send(response, 409, { error: "Account deletion requested" });
    let workspaceId: string;
    try {
      workspaceId = await workspaces.getOrCreate(identity);
    } catch (error) {
      if (
        error instanceof Error &&
        error.message === "Account deletion requested"
      )
        return send(response, 409, { error: error.message });
      throw error;
    }
    if (request.url === "/settings/recordings" && request.method === "GET")
      return send(response, 200, {
        enabled: await recordingPreferences.get(workspaceId),
        stopping: await recordingPreferences.stopping(workspaceId),
      });
    if (request.url === "/settings/recordings" && request.method === "POST") {
      if (
        !/^application\/json(?:\s*;|$)/i.test(
          request.headers["content-type"] ?? "",
        )
      )
        return send(response, 415, { error: "JSON content type required" });
      let body: unknown;
      try {
        body = await readJson(request);
      } catch {
        return send(response, 400, { error: "Invalid JSON" });
      }
      if (
        !body ||
        typeof body !== "object" ||
        Array.isArray(body) ||
        Object.keys(body).length !== 1 ||
        typeof (body as { enabled?: unknown }).enabled !== "boolean"
      )
        return send(response, 400, { error: "Invalid recording preference" });
      const enabled = (body as { enabled: boolean }).enabled;
      await recordingPreferences.set(workspaceId, enabled);
      return send(response, 200, {
        enabled,
        stopping: await recordingPreferences.stopping(workspaceId),
      });
    }
    if (request.method === "GET" && request.url === "/placeholders")
      return send(response, 200, await placeholders.list(workspaceId));
    if (request.method === "POST" && request.url === "/placeholders") {
      if (
        !/^application\/json(?:\s*;|$)/i.test(
          request.headers["content-type"] ?? "",
        )
      )
        return send(response, 415, { error: "JSON content type required" });
      try {
        return send(
          response,
          200,
          await placeholders.save(workspaceId, await readJson(request)),
        );
      } catch (error) {
        if (
          error instanceof Error &&
          [
            "Invalid placeholder",
            "Invalid JSON",
            "Request body too large",
            "Placeholder already exists",
          ].includes(error.message)
        )
          return send(
            response,
            error.message === "Request body too large"
              ? 413
              : error.message === "Placeholder already exists"
                ? 409
                : 400,
            { error: error.message },
          );
        throw error;
      }
    }
    const placeholderMatch =
      /^\/placeholders\/([A-Za-z][A-Za-z0-9_]{0,49})$/.exec(request.url ?? "");
    if (request.method === "DELETE" && placeholderMatch)
      return (await placeholders.delete(workspaceId, placeholderMatch[1]!))
        ? send(response, 200, { deleted: true })
        : send(response, 404, { error: "Placeholder not found" });
    const recordingMatch =
      /^\/runs\/([0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})\/recording$/i.exec(
        request.url ?? "",
      );
    if (
      recordingMatch &&
      !(await store.getForWorkspace(
        recordingMatch[1]!.toLowerCase(),
        workspaceId,
      ))
    )
      return send(response, 404, { error: "Run not found" });
    if (recordingMatch && request.method === "DELETE") {
      await recordings.delete(recordingMatch[1]!.toLowerCase());
      return send(response, 200, { deleted: true });
    }
    if (await recordingPlayback(request, response, recordings)) return;
    const eventsMatch =
      /^\/runs\/([0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})\/events$/i.exec(
        request.url ?? "",
      );
    if (request.method === "GET" && eventsMatch) {
      if (
        !(await store.getForWorkspace(
          eventsMatch[1]!.toLowerCase(),
          workspaceId,
        ))
      )
        return send(response, 404, { error: "Run not found" });
      const saved = await events.list(eventsMatch[1]!.toLowerCase());
      return saved
        ? send(response, 200, saved)
        : send(response, 404, { error: "Run not found" });
    }
    if (request.method === "GET" && request.url === "/runs")
      return send(response, 200, await store.listRecent(workspaceId));
    if (request.method === "GET" && request.url === "/workflows")
      return send(response, 200, await workflows.list(workspaceId));
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
      return send(
        response,
        201,
        await workflows.create(definition, workspaceId),
      );
    }
    const versionMatch =
      /^\/workflows\/([0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})\/versions\/([1-9][0-9]{0,9})$/i.exec(
        request.url ?? "",
      );
    if (versionMatch && request.method === "GET") {
      const number = Number(versionMatch[2]);
      if (number > 2147483647)
        return send(response, 400, { error: "Invalid workflow version" });
      const found = await workflows.get(
        versionMatch[1]!.toLowerCase(),
        workspaceId,
        number,
      );
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
        const found = await workflows.get(workflowId, workspaceId);
        return found
          ? send(response, 200, found)
          : send(response, 404, { error: "Workflow not found" });
      }
      if (request.method === "GET" && workflowMatch[2] === "runs") {
        if (!(await workflows.get(workflowId, workspaceId)))
          return send(response, 404, { error: "Workflow not found" });
        return send(
          response,
          200,
          await store.listForWorkflow(workflowId, workspaceId),
        );
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
        const header = request.headers["if-match"];
        if (
          header !== undefined &&
          (typeof header !== "string" ||
            !/^[1-9][0-9]{0,9}$/.test(header) ||
            Number(header) > 2147483647)
        )
          return send(response, 400, { error: "Invalid workflow version" });
        let version;
        try {
          version = await workflows.addVersion(
            workflowId,
            definition,
            workspaceId,
            header === undefined ? undefined : Number(header),
          );
        } catch (error) {
          if (
            error instanceof Error &&
            error.message === "Workflow version conflict"
          )
            return send(response, 409, {
              error:
                "This workflow changed in another session. Reload before saving.",
            });
          throw error;
        }
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
              ![
                "id",
                "version",
                "inputs",
                "maxSteps",
                "timeoutMs",
                "acknowledgeSecretExposure",
                "recordingEnabled",
              ].includes(key),
          ) ||
          typeof body.version !== "number" ||
          !Number.isInteger(body.version) ||
          body.version < 1 ||
          body.version > 2147483647
        )
          return send(response, 400, { error: "Invalid workflow run" });
        const version = await workflows.get(
          workflowId,
          workspaceId,
          body.version,
        );
        if (!version)
          return send(response, 404, { error: "Workflow version not found" });
        if (
          body.inputs !== undefined &&
          (!body.inputs ||
            typeof body.inputs !== "object" ||
            Array.isArray(body.inputs) ||
            Object.keys(body.inputs).length !== 0)
        )
          return send(response, 400, {
            error: "Save placeholder values before running",
          });
        if (
          body.acknowledgeSecretExposure !== undefined &&
          typeof body.acknowledgeSecretExposure !== "boolean"
        )
          return send(response, 400, { error: "Invalid acknowledgment" });
        const resolvedInputs = await placeholders.resolve(
          workspaceId,
          version.definition.inputs,
        );
        if (resolvedInputs.missing.length)
          return send(response, 400, {
            error: "Missing placeholder values",
            missing: resolvedInputs.missing,
          });
        if (
          resolvedInputs.secretNames.length &&
          body.acknowledgeSecretExposure !== true
        )
          return send(response, 400, {
            error: "Secret exposure acknowledgment required",
          });
        let input;
        let resolved;
        try {
          input = parseRun({
            id: body.id,
            instruction: "Guided workflow",
            maxSteps: body.maxSteps,
            timeoutMs: body.timeoutMs,
            recordingEnabled: body.recordingEnabled,
          });
          resolved = resolveWorkflow(version.definition, resolvedInputs.values);
          if (body.timeoutMs === undefined)
            input.timeoutMs = Math.min(
              600000,
              input.timeoutMs +
                Math.ceil(
                  resolved.steps.reduce(
                    (total, step) =>
                      total + (step.type === "wait" ? step.seconds * 1000 : 0),
                    0,
                  ),
                ),
            );
        } catch (error) {
          return send(response, 400, {
            error: resolvedInputs.secretNames.length
              ? "Workflow contains an invalid placeholder value"
              : error instanceof Error
                ? error.message
                : "Invalid workflow run",
          });
        }
        try {
          return send(
            response,
            202,
            await runs.create({
              ...input,
              workspaceId,
              workflow: resolved,
              workflowId,
              workflowVersion: body.version,
              containsSecrets: resolvedInputs.secretNames.length > 0,
              requestFingerprint: placeholders.fingerprint({
                workspaceId,
                workflowId,
                version: body.version,
                input,
                resolved,
              }),
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
        return send(
          response,
          202,
          await runs.create({ ...input, workspaceId }),
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
    const match =
      /^\/runs\/([0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})(\/cancel)?$/i.exec(
        request.url ?? "",
      );
    if (match && request.method === "GET" && !match[2]) {
      const record = await store.getForWorkspace(
        match[1]!.toLowerCase(),
        workspaceId,
      );
      return record
        ? send(response, 200, record)
        : send(response, 404, { error: "Run not found" });
    }
    if (match && request.method === "POST" && match[2]) {
      if (!(await store.getForWorkspace(match[1]!.toLowerCase(), workspaceId)))
        return send(response, 404, { error: "Run not found" });
      const record = await runs.cancel(match[1]!.toLowerCase());
      return record
        ? send(response, 200, record)
        : send(response, 404, { error: "Run not found" });
    }
    send(response, 404, { error: "Not found" });
  })()
    .catch(() => {
      if (!response.headersSent)
        send(response, 500, { error: "Internal service error" });
      else response.end();
    })
    .finally(() => releaseAccount?.());
});
server.listen(Number(rawPort), "127.0.0.1", () =>
  console.log(`Backend listening on 127.0.0.1:${rawPort}`),
);
for (const signal of ["SIGINT", "SIGTERM"] as const)
  process.once(signal, () => {
    server.close(() => {
      void runs
        .stop()
        .then(() => accountDeletion.stop())
        .then(() => store.close())
        .catch(() => {
          process.exitCode = 1;
        });
    });
  });
