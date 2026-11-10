import type {
  ExecutionTarget,
  ProviderId,
  ResolvedWorkflow,
} from "../workflows/definition.js";
import type { ResolvedPrewarmStep } from "../workflows/prewarm.js";
export type RunRequest = {
  id: string;
  instruction: string;
  maxSteps: number;
  timeoutMs: number;
  workflow?: ResolvedWorkflow;
  prewarm?: ResolvedPrewarmStep[];
  workflowId?: string;
  workflowVersion?: number;
  workflowInputs?: Record<string, string>;
  workspaceId?: string;
  requestFingerprint?: string;
  containsSecrets?: boolean;
  recordingEnabled?: boolean;
};
export type RunRecord = RunRequest & {
  executionTarget: ExecutionTarget;
  provider: ProviderId;
  status: string;
  result: unknown;
  createdAt: string;
  updatedAt: string;
  steps?: unknown;
  liveViewUrl?: string | null;
};
export function parseRun(value: unknown): RunRequest {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new Error("Expected a JSON object");
  const r = value as Record<string, unknown>;
  if (
    Object.keys(r).some(
      (key) =>
        ![
          "id",
          "instruction",
          "maxSteps",
          "timeoutMs",
          "recordingEnabled",
        ].includes(key),
    )
  )
    throw new Error("Unknown run field");
  if (
    typeof r.id !== "string" ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
      r.id,
    )
  )
    throw new Error("Invalid run ID");
  if (
    typeof r.instruction !== "string" ||
    !r.instruction.trim() ||
    r.instruction.length > 10000
  )
    throw new Error("Invalid instruction");
  const maxSteps = r.maxSteps ?? 20,
    timeoutMs = r.timeoutMs ?? 120000;
  if (
    typeof maxSteps !== "number" ||
    !Number.isInteger(maxSteps) ||
    maxSteps < 1 ||
    maxSteps > 100
  )
    throw new Error("Invalid maxSteps");
  if (
    typeof timeoutMs !== "number" ||
    !Number.isInteger(timeoutMs) ||
    timeoutMs < 1000 ||
    timeoutMs > 3600000
  )
    throw new Error("Invalid timeoutMs");
  if (
    r.recordingEnabled !== undefined &&
    typeof r.recordingEnabled !== "boolean"
  )
    throw new Error("Invalid recordingEnabled");
  return {
    recordingEnabled: r.recordingEnabled !== false,
    id: r.id.toLowerCase(),
    instruction: r.instruction,
    maxSteps,
    timeoutMs,
  };
}
