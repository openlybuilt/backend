import type { IncomingMessage, ServerResponse } from "node:http";
import { parseSchedule, parseTimezone } from "./contracts.js";
import { ScheduleConflict, ScheduleStore } from "./store.js";

const uuid =
  "([0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})";
type Context = {
  workspaceId: string;
  store: ScheduleStore;
  readJson: (request: IncomingMessage) => Promise<unknown>;
  whileReady: <T>(work: () => Promise<T>) => Promise<T>;
  send: (response: ServerResponse, status: number, value: unknown) => void;
};
function parsePatch(value: unknown): {
  enabled?: boolean;
  acknowledgeSecretExposure?: boolean;
  acknowledgedWorkflowVersion?: number;
} {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new Error("Invalid schedule update");
  const body = value as Record<string, unknown>;
  const keys = Object.keys(body);
  if (keys.length === 1 && typeof body.enabled === "boolean")
    return { enabled: body.enabled };
  if (
    keys.length === 2 &&
    body.acknowledgeSecretExposure === true &&
    typeof body.acknowledgedWorkflowVersion === "number" &&
    Number.isInteger(body.acknowledgedWorkflowVersion) &&
    body.acknowledgedWorkflowVersion >= 1 &&
    body.acknowledgedWorkflowVersion <= 2147483647
  )
    return {
      acknowledgeSecretExposure: true,
      acknowledgedWorkflowVersion: body.acknowledgedWorkflowVersion,
    };
  throw new Error("Invalid schedule update");
}
export async function scheduleRoute(
  request: IncomingMessage,
  response: ServerResponse,
  context: Context,
): Promise<boolean> {
  const { workspaceId, store, readJson, whileReady, send } = context;
  const path = request.url ?? "";
  const workflows = new RegExp(`^/workflows/${uuid}/schedules$`, "i").exec(
    path,
  );
  const single = new RegExp(`^/schedules/${uuid}(/occurrences)?$`, "i").exec(
    path,
  );
  if (path !== "/settings/timezone" && !workflows && !single) return false;
  try {
    if (
      ["POST", "PATCH"].includes(request.method ?? "") &&
      !/^application\/json(?:\s*;|$)/i.test(
        request.headers["content-type"] ?? "",
      )
    ) {
      send(response, 415, { error: "JSON content type required" });
      return true;
    }
    if (path === "/settings/timezone" && request.method === "GET") {
      send(response, 200, {
        timezone: await whileReady(() => store.timezone(workspaceId)),
      });
      return true;
    }
    if (path === "/settings/timezone" && request.method === "POST") {
      const { timezone, onlyIfUnset } = parseTimezone(
        await whileReady(() => readJson(request)),
      );
      send(response, 200, {
        timezone: await whileReady(() =>
          store.setTimezone(workspaceId, timezone, onlyIfUnset),
        ),
      });
      return true;
    }
    if (workflows && request.method === "GET") {
      const rows = await whileReady(() =>
        store.list(workspaceId, workflows[1]!.toLowerCase()),
      );
      send(
        response,
        rows ? 200 : 404,
        rows ? { schedules: rows } : { error: "Workflow not found" },
      );
      return true;
    }
    if (workflows && request.method === "POST") {
      const input = parseSchedule(await whileReady(() => readJson(request)));
      const row = await whileReady(() =>
        store.create(workspaceId, workflows[1]!.toLowerCase(), input),
      );
      send(response, row ? 201 : 404, row ?? { error: "Workflow not found" });
      return true;
    }
    if (single && single[2] === "/occurrences" && request.method === "GET") {
      const rows = await whileReady(() =>
        store.occurrences(workspaceId, single[1]!.toLowerCase()),
      );
      send(
        response,
        rows ? 200 : 404,
        rows ? { occurrences: rows } : { error: "Schedule not found" },
      );
      return true;
    }
    if (single && !single[2] && request.method === "PATCH") {
      const input = parsePatch(await whileReady(() => readJson(request)));
      const row = await whileReady(() =>
        store.update(workspaceId, single[1]!.toLowerCase(), input),
      );
      send(response, row ? 200 : 404, row ?? { error: "Schedule not found" });
      return true;
    }
    if (single && !single[2] && request.method === "DELETE") {
      const deleted = await whileReady(() =>
        store.delete(workspaceId, single[1]!.toLowerCase()),
      );
      if (deleted) response.writeHead(204).end();
      else send(response, 404, { error: "Schedule not found" });
      return true;
    }
    send(response, 405, { error: "Method not allowed" });
    return true;
  } catch (error) {
    if (error instanceof ScheduleConflict) {
      send(response, 409, { code: error.code, error: error.message });
      return true;
    }
    if (
      error instanceof Error &&
      (error instanceof SyntaxError ||
        /^Invalid |^Choose a time|^Scheduled time/.test(error.message))
    ) {
      send(response, 400, { error: error.message });
      return true;
    }
    throw error;
  }
}
