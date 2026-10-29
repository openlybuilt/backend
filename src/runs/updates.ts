export type RunUpdate = {
  status: string;
  steps?: unknown[];
  result?: Record<string, unknown>;
  liveViewUrl?: string | null;
};

export function parseUpdate(value: unknown): RunUpdate {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new Error("Expected a run update");
  const update = value as Record<string, unknown>;
  if (
    Object.keys(update).some(
      (key) => !["status", "steps", "result", "liveViewUrl"].includes(key),
    ) ||
    typeof update.status !== "string" ||
    ![
      "provisioning",
      "running",
      "cleaning_up",
      "completed",
      "failed",
      "cancelled",
      "interrupted",
    ].includes(update.status)
  )
    throw new Error("Invalid run status");
  const steps =
    update.steps ??
    (update.result as Record<string, unknown> | undefined)?.steps;
  if (steps !== undefined) {
    if (!Array.isArray(steps) || steps.length > 100)
      throw new Error("Invalid step updates");
    for (const [index, step] of steps.entries()) {
      if (
        !step ||
        typeof step !== "object" ||
        Array.isArray(step) ||
        step.index !== index ||
        ![
          "navigate",
          "type",
          "wait",
          "scroll",
          "press_keys",
          "click",
          "ai",
        ].includes(step.type) ||
        !["running", "completed", "failed", "cancelled"].includes(
          step.status,
        ) ||
        (step.message !== undefined &&
          (typeof step.message !== "string" || step.message.length > 10000))
      )
        throw new Error("Invalid step update");
    }
  }
  if (
    update.result !== undefined &&
    (!update.result ||
      typeof update.result !== "object" ||
      Array.isArray(update.result) ||
      (update.result as Record<string, unknown>).status !== update.status)
  )
    throw new Error("Invalid run result");
  if (update.liveViewUrl !== undefined && update.liveViewUrl !== null) {
    if (
      typeof update.liveViewUrl !== "string" ||
      update.liveViewUrl.length > 4000
    )
      throw new Error("Invalid live view URL");
    const url = new URL(update.liveViewUrl);
    if (url.protocol !== "https:" || url.username || url.password)
      throw new Error("Invalid live view URL");
  }
  return { ...update, ...(steps === undefined ? {} : { steps }) } as RunUpdate;
}
