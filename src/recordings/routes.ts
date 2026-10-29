import type { IncomingMessage, ServerResponse } from "node:http";
import { RecordingStore, maxRecordingBytes } from "./store.js";

const runId =
  "([0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})";
function send(response: ServerResponse, status: number, value: unknown): void {
  response.writeHead(status, { "content-type": "application/json" });
  response.end(JSON.stringify(value));
}
class BodyError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}
function readBody(request: IncomingMessage, limit: number): Promise<Buffer> {
  if (Number(request.headers["content-length"]) > limit) {
    request.resume();
    return Promise.reject(new BodyError(413, "Recording body too large"));
  }
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let bytes = 0;
    const onData = (chunk: Buffer) => {
      bytes += chunk.length;
      if (bytes > limit) {
        request.off("data", onData);
        chunks.length = 0;
        request.resume();
        reject(new BodyError(413, "Recording body too large"));
      } else chunks.push(chunk);
    };
    request.on("data", onData);
    request.once("end", () => resolve(Buffer.concat(chunks)));
    request.once("error", reject);
    request.once("aborted", () =>
      reject(new BodyError(400, "Upload interrupted")),
    );
  });
}
export async function recordingUpload(
  request: IncomingMessage,
  response: ServerResponse,
  recordings: RecordingStore,
): Promise<boolean> {
  const match = new RegExp(
    `^/internal/runs/${runId}/recording(/status)?$`,
    "i",
  ).exec(request.url ?? "");
  if (request.method !== "POST" || !match) return false;
  const id = match[1]!.toLowerCase();
  const contentType = request.headers["content-type"] ?? "";
  try {
    if (match[2]) {
      if (!/^application\/json(?:\s*;|$)/i.test(contentType))
        throw new BodyError(415, "JSON content type required");
      let value: Record<string, unknown>;
      try {
        value = JSON.parse((await readBody(request, 2048)).toString("utf8"));
      } catch (error) {
        if (error instanceof BodyError) throw error;
        throw new BodyError(400, "Invalid recording status");
      }
      if (
        !value ||
        typeof value !== "object" ||
        Array.isArray(value) ||
        Object.keys(value).some(
          (key) => !["status", "browserId", "replayId"].includes(key),
        ) ||
        (value.status !== "pending" && value.status !== "failed") ||
        [value.browserId, value.replayId].some(
          (id) =>
            id !== undefined &&
            (typeof id !== "string" || !/^[A-Za-z0-9_.:-]{1,256}$/.test(id)),
        )
      )
        throw new BodyError(400, "Invalid recording status");
      const saved = await recordings.report(
        id,
        value.status,
        value.browserId as string | undefined,
        value.replayId as string | undefined,
      );
      send(response, saved ? 200 : 404, saved ?? { error: "Run not found" });
    } else {
      const existing = await recordings.get(id);
      if (!existing) throw new BodyError(404, "Pending recording not found");
      if (existing.status !== "pending") {
        request.resume();
        send(response, existing.status === "saved" ? 200 : 409, existing);
        return true;
      }
      if (!/^video\/mp4(?:\s*;|$)/i.test(contentType))
        throw new BodyError(415, "MP4 content type required");
      const video = await readBody(request, maxRecordingBytes);
      if (
        video.length < 16 ||
        video.toString("ascii", 4, 8) !== "ftyp" ||
        video.readUInt32BE(0) < 16 ||
        video.readUInt32BE(0) > video.length
      )
        throw new BodyError(400, "Invalid MP4 recording");
      const saved = await recordings.save(id, video);
      send(
        response,
        saved?.status === "saved" ? 200 : 409,
        saved ?? { error: "Recording no longer pending" },
      );
    }
  } catch (error) {
    if (!(error instanceof BodyError)) throw error;
    request.resume();
    send(response, error.status, { error: error.message });
  }
  return true;
}

export async function recordingPlayback(
  request: IncomingMessage,
  response: ServerResponse,
  recordings: RecordingStore,
): Promise<boolean> {
  const match = new RegExp(`^/runs/${runId}/recording$`, "i").exec(
    request.url ?? "",
  );
  if (!match || !["GET", "HEAD"].includes(request.method ?? "")) return false;
  const id = match[1]!.toLowerCase();
  const metadata = await recordings.get(id);
  if (metadata?.status !== "saved" || !metadata.bytes) {
    send(response, 404, { error: "Recording not available" });
    return true;
  }
  const size = metadata.bytes;
  let start = 0,
    end = size - 1;
  const range = request.headers.range;
  if (range !== undefined) {
    const parsed = /^bytes=(\d*)-(\d*)$/.exec(range);
    let valid = Boolean(parsed && (parsed[1] || parsed[2]));
    if (parsed && valid) {
      if (parsed[1]) {
        start = Number(parsed[1]);
        end = parsed[2] ? Number(parsed[2]) : size - 1;
      } else {
        const suffix = Number(parsed[2]);
        valid = Number.isSafeInteger(suffix) && suffix > 0;
        start = Math.max(0, size - suffix);
      }
      valid =
        valid &&
        Number.isSafeInteger(start) &&
        Number.isSafeInteger(end) &&
        start < size &&
        start <= end;
      end = Math.min(end, size - 1);
    }
    if (!valid) {
      response.writeHead(416, {
        "content-range": `bytes */${size}`,
        "accept-ranges": "bytes",
        "content-length": "0",
      });
      response.end();
      return true;
    }
  }
  const video =
    request.method === "HEAD"
      ? undefined
      : await recordings.read(id, start, end - start + 1);
  if (request.method !== "HEAD" && !video) {
    send(response, 404, { error: "Recording not available" });
    return true;
  }
  response.writeHead(range === undefined ? 200 : 206, {
    "content-type": "video/mp4",
    "content-length": end - start + 1,
    "accept-ranges": "bytes",
    "cache-control": "private, no-store",
    ...(range === undefined
      ? {}
      : { "content-range": `bytes ${start}-${end}/${size}` }),
  });
  response.end(video);
  return true;
}
