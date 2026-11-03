export type ScheduleInput = {
  idempotencyKey: string;
  kind: "once" | "interval" | "weekly";
  runAt?: string;
  intervalMinutes?: number;
  weekdays?: number[];
  localTime?: string;
  recordingEnabled: boolean;
  acknowledgeSecretExposure: boolean;
  acknowledgedWorkflowVersion?: number;
};

function record(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new Error("Invalid schedule");
  return value as Record<string, unknown>;
}

export function parseInstant(value: unknown): string {
  if (typeof value !== "string") throw new Error("Invalid scheduled time");
  const match =
    /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d{1,3})?(Z|[+-]\d{2}:\d{2})$/.exec(
      value,
    );
  if (!match) throw new Error("Scheduled time needs an explicit UTC offset");
  const [, year, month, day, hour, minute, second, offset] = match;
  const parts = [year, month, day, hour, minute, second].map(Number);
  const [y, m, d, h, min, sec] = parts as [
    number,
    number,
    number,
    number,
    number,
    number,
  ];
  const local = new Date(Date.UTC(y, m - 1, d, h, min, sec));
  if (
    y < 100 ||
    local.getUTCFullYear() !== y ||
    local.getUTCMonth() !== m - 1 ||
    local.getUTCDate() !== d ||
    h > 23 ||
    min > 59 ||
    sec > 59
  )
    throw new Error("Invalid scheduled time");
  if (offset !== "Z") {
    const oh = Number(offset!.slice(1, 3)),
      om = Number(offset!.slice(4, 6));
    if (oh > 14 || om > 59 || (oh === 14 && om !== 0))
      throw new Error("Invalid UTC offset");
  }
  const instant = new Date(value);
  if (!Number.isFinite(instant.getTime()))
    throw new Error("Invalid scheduled time");
  return instant.toISOString();
}

export function parseSchedule(value: unknown): ScheduleInput {
  const body = record(value);
  const allowed = [
    "idempotencyKey",
    "kind",
    "runAt",
    "intervalMinutes",
    "weekdays",
    "localTime",
    "recordingEnabled",
    "acknowledgeSecretExposure",
    "acknowledgedWorkflowVersion",
  ];
  if (Object.keys(body).some((key) => !allowed.includes(key)))
    throw new Error("Invalid schedule");
  if (
    typeof body.idempotencyKey !== "string" ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
      body.idempotencyKey,
    )
  )
    throw new Error("Invalid idempotency key");
  if (
    body.recordingEnabled !== undefined &&
    typeof body.recordingEnabled !== "boolean"
  )
    throw new Error("Invalid recording preference");
  if (
    body.acknowledgeSecretExposure !== undefined &&
    typeof body.acknowledgeSecretExposure !== "boolean"
  )
    throw new Error("Invalid acknowledgment");
  const version = body.acknowledgedWorkflowVersion;
  if (
    version !== undefined &&
    (!Number.isInteger(version) ||
      (version as number) < 1 ||
      (version as number) > 2147483647)
  )
    throw new Error("Invalid workflow version");
  const common = {
    idempotencyKey: body.idempotencyKey.toLowerCase(),
    recordingEnabled: body.recordingEnabled !== false,
    acknowledgeSecretExposure: body.acknowledgeSecretExposure === true,
    ...(version === undefined
      ? {}
      : { acknowledgedWorkflowVersion: version as number }),
  };
  if (
    body.kind === "once" &&
    body.intervalMinutes === undefined &&
    body.weekdays === undefined &&
    body.localTime === undefined
  )
    return { ...common, kind: "once", runAt: parseInstant(body.runAt) };
  if (
    body.kind === "interval" &&
    body.runAt === undefined &&
    body.weekdays === undefined &&
    body.localTime === undefined &&
    Number.isInteger(body.intervalMinutes) &&
    (body.intervalMinutes as number) >= 1 &&
    (body.intervalMinutes as number) <= 525600
  )
    return {
      ...common,
      kind: "interval",
      intervalMinutes: body.intervalMinutes as number,
    };
  if (
    body.kind === "weekly" &&
    body.runAt === undefined &&
    body.intervalMinutes === undefined &&
    Array.isArray(body.weekdays) &&
    body.weekdays.length >= 1 &&
    body.weekdays.length <= 7 &&
    body.weekdays.every(
      (day) => Number.isInteger(day) && day >= 0 && day <= 6,
    ) &&
    new Set(body.weekdays).size === body.weekdays.length &&
    typeof body.localTime === "string" &&
    /^([01][0-9]|2[0-3]):[0-5][0-9]$/.test(body.localTime)
  )
    return {
      ...common,
      kind: "weekly",
      weekdays: [...body.weekdays].sort(),
      localTime: body.localTime,
    };
  throw new Error("Invalid schedule");
}

export function parseTimezone(value: unknown): {
  timezone: string;
  onlyIfUnset: boolean;
} {
  const body = record(value);
  if (
    Object.keys(body).some(
      (key) => key !== "timezone" && key !== "onlyIfUnset",
    ) ||
    (body.onlyIfUnset !== undefined && typeof body.onlyIfUnset !== "boolean") ||
    typeof body.timezone !== "string" ||
    body.timezone.length > 100
  )
    throw new Error("Invalid time zone");
  try {
    const timezone = new Intl.DateTimeFormat("en", {
      timeZone: body.timezone,
    }).resolvedOptions().timeZone;
    return { timezone, onlyIfUnset: body.onlyIfUnset === true };
  } catch {
    throw new Error("Invalid time zone");
  }
}
