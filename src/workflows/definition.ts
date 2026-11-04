export type ExecutionTarget = "browser" | "windows";
export type ProviderId = "onkernel" | "daytona";
export type WorkflowStep =
  | { type: "navigate"; url: string }
  | { type: "type"; text: string; target?: string; region?: string }
  | { type: "wait"; seconds: number }
  | {
      type: "scroll";
      direction: "up" | "down" | "left" | "right";
      pixels: number;
    }
  | { type: "scroll"; direction: "up" | "down"; wheelTicks: number }
  | { type: "press_keys"; keys: string[] }
  | { type: "click"; x: number; y: number }
  | { type: "click"; target: string }
  | { type: "ai"; instruction: string };
export type ResolvedWorkflow = {
  executionTarget: ExecutionTarget;
  provider?: ProviderId;
  steps: WorkflowStep[];
};

function parseExecutionTarget(value: unknown): ExecutionTarget {
  if (value === undefined) return "browser";
  if (value === "browser" || value === "windows") return value;
  throw new Error("Invalid execution target");
}

function parseProvider(
  value: unknown,
  target: ExecutionTarget,
): ProviderId | undefined {
  if (value === undefined) return undefined;
  if (
    (value === "onkernel" && target === "browser") ||
    (value === "daytona" && target === "windows")
  )
    return value;
  throw new Error("Provider is incompatible with the selected VM type");
}

export function parseWorkflow(input: unknown): ResolvedWorkflow {
  if (!input || typeof input !== "object" || Array.isArray(input))
    throw new Error("Invalid workflow");
  const value = input as Record<string, unknown>;
  const executionTarget = parseExecutionTarget(value.executionTarget);
  const provider = parseProvider(value.provider, executionTarget);
  if (
    Object.keys(value).some(
      (key) => !["steps", "executionTarget", "provider"].includes(key),
    ) ||
    !Array.isArray(value.steps) ||
    value.steps.length < 1 ||
    value.steps.length > 30
  )
    throw new Error("Workflow requires 1 to 30 steps");
  const steps = value.steps.map((raw): WorkflowStep => {
    if (!raw || typeof raw !== "object" || Array.isArray(raw))
      throw new Error("Invalid workflow step");
    const step = raw as Record<string, unknown>;
    const keys = Object.keys(step);
    const exact = (...names: string[]) =>
      keys.length === names.length &&
      names.every((name) => names.includes(name) && keys.includes(name));
    if (
      step.type === "navigate" &&
      exact("type", "url") &&
      typeof step.url === "string" &&
      step.url.length <= 2048
    ) {
      const url = new URL(step.url);
      if (url.protocol !== "http:" && url.protocol !== "https:")
        throw new Error("Invalid URL protocol");
      return { type: "navigate", url: url.href };
    }
    if (
      step.type === "type" &&
      typeof step.text === "string" &&
      step.text.length <= 10000 &&
      (exact("type", "text") ||
        (typeof step.target === "string" &&
          step.target.trim().length > 0 &&
          step.target.length <= 1000 &&
          (exact("type", "text", "target") ||
            (exact("type", "text", "target", "region") &&
              typeof step.region === "string" &&
              step.region.trim().length > 0 &&
              step.region.length <= 200))))
    )
      return {
        type: "type",
        text: step.text,
        ...(typeof step.target === "string" ? { target: step.target } : {}),
        ...(typeof step.region === "string" ? { region: step.region } : {}),
      };
    if (
      step.type === "wait" &&
      exact("type", "seconds") &&
      typeof step.seconds === "number" &&
      Number.isFinite(step.seconds) &&
      step.seconds > 0 &&
      step.seconds <= 600
    )
      return { type: "wait", seconds: step.seconds };
    if (step.type === "scroll") {
      if (
        executionTarget === "windows" &&
        exact("type", "direction", "wheelTicks") &&
        (step.direction === "up" || step.direction === "down") &&
        typeof step.wheelTicks === "number" &&
        Number.isInteger(step.wheelTicks) &&
        step.wheelTicks >= 1 &&
        step.wheelTicks <= 100
      )
        return {
          type: "scroll",
          direction: step.direction,
          wheelTicks: step.wheelTicks,
        };
      if (
        executionTarget === "browser" &&
        exact("type", "direction", "pixels") &&
        (step.direction === "up" ||
          step.direction === "down" ||
          step.direction === "left" ||
          step.direction === "right") &&
        typeof step.pixels === "number" &&
        Number.isInteger(step.pixels) &&
        step.pixels >= 1 &&
        step.pixels <= 100000
      )
        return {
          type: "scroll",
          direction: step.direction,
          pixels: step.pixels,
        };
      throw new Error(
        executionTarget === "windows"
          ? "Windows scrolling requires up or down and 1 to 100 wheel ticks"
          : "Browser scrolling requires a direction and 1 to 100000 pixels",
      );
    }
    if (
      step.type === "ai" &&
      exact("type", "instruction") &&
      typeof step.instruction === "string" &&
      step.instruction.trim().length > 0 &&
      step.instruction.length <= 10000
    )
      return { type: "ai", instruction: step.instruction };
    if (
      step.type === "press_keys" &&
      exact("type", "keys") &&
      Array.isArray(step.keys) &&
      step.keys.length > 0 &&
      step.keys.length <= 10 &&
      step.keys.every(
        (key) => typeof key === "string" && key.length > 0 && key.length <= 40,
      )
    )
      return { type: "press_keys", keys: step.keys as string[] };
    if (
      step.type === "click" &&
      exact("type", "target") &&
      typeof step.target === "string" &&
      step.target.trim().length > 0 &&
      step.target.length <= 10000
    )
      return { type: "click", target: step.target };
    if (
      step.type === "click" &&
      exact("type", "x", "y") &&
      typeof step.x === "number" &&
      Number.isInteger(step.x) &&
      step.x >= 0 &&
      step.x <= 16384 &&
      typeof step.y === "number" &&
      Number.isInteger(step.y) &&
      step.y >= 0 &&
      step.y <= 16384
    )
      return { type: "click", x: step.x, y: step.y };
    throw new Error("Invalid workflow step");
  });
  return {
    executionTarget,
    ...(provider === undefined ? {} : { provider }),
    steps,
  };
}

export type WorkflowDefinition = {
  executionTarget?: ExecutionTarget;
  provider?: ProviderId;
  name: string;
  description: string;
  inputs: string[];
  steps: WorkflowStep[];
};
const inputName = /^[A-Za-z][A-Za-z0-9_]{0,49}$/;
const template = /\{\{([A-Za-z][A-Za-z0-9_]{0,49})\}\}/g;
function replace(value: string, values: Record<string, string>): string {
  return value.replace(template, (_, name: string) => {
    const item = values[name];
    if (item === undefined) throw new Error(`Missing input: ${name}`);
    return item;
  });
}
export function resolveWorkflow(
  definition: WorkflowDefinition,
  inputs: unknown,
): ResolvedWorkflow {
  if (!inputs || typeof inputs !== "object" || Array.isArray(inputs))
    throw new Error("Inputs must be an object");
  const values = inputs as Record<string, unknown>;
  if (
    Object.keys(values).length !== definition.inputs.length ||
    definition.inputs.some(
      (name) =>
        typeof values[name] !== "string" ||
        (values[name] as string).length > 10000,
    ) ||
    Object.keys(values).some((name) => !definition.inputs.includes(name))
  )
    throw new Error("Inputs do not match the workflow");
  const strings = values as Record<string, string>;
  return parseWorkflow({
    executionTarget: definition.executionTarget,
    ...(definition.provider === undefined
      ? {}
      : { provider: definition.provider }),
    steps: definition.steps.map((step) => {
      if (step.type === "navigate")
        return { ...step, url: replace(step.url, strings) };
      if (step.type === "type")
        return {
          ...step,
          text: replace(step.text, strings),
          ...(step.target ? { target: replace(step.target, strings) } : {}),
          ...(step.region ? { region: replace(step.region, strings) } : {}),
        };
      if (step.type === "ai")
        return { ...step, instruction: replace(step.instruction, strings) };
      if (step.type === "press_keys")
        return { ...step, keys: step.keys.map((key) => replace(key, strings)) };
      if (step.type === "click" && "target" in step)
        return { ...step, target: replace(step.target, strings) };
      return step;
    }),
  });
}
export function parseDefinition(input: unknown): WorkflowDefinition {
  if (!input || typeof input !== "object" || Array.isArray(input))
    throw new Error("Invalid workflow definition");
  const value = input as Record<string, unknown>;
  const executionTarget = parseExecutionTarget(value.executionTarget);
  const provider = parseProvider(value.provider, executionTarget);
  if (
    Object.keys(value).some(
      (key) =>
        ![
          "name",
          "description",
          "inputs",
          "steps",
          "executionTarget",
          "provider",
        ].includes(key),
    ) ||
    typeof value.name !== "string" ||
    (value.description !== undefined &&
      (typeof value.description !== "string" ||
        value.description.length > 1000)) ||
    !value.name.trim() ||
    value.name.length > 120 ||
    !Array.isArray(value.inputs) ||
    value.inputs.length > 20 ||
    value.inputs.some(
      (name) => typeof name !== "string" || !inputName.test(name),
    ) ||
    new Set(value.inputs).size !== value.inputs.length ||
    !Array.isArray(value.steps)
  )
    throw new Error("Invalid workflow definition");
  const inputs = value.inputs as string[];
  const source = JSON.stringify(value.steps);
  const references = [...source.matchAll(template)].map((match) => match[1]);
  if (references.some((name) => !inputs.includes(name!)))
    throw new Error("Unknown workflow input");
  const placeholders = Object.fromEntries(
    inputs.map((name) => [name, "example.com"]),
  );
  const sample = (value.steps as Array<Record<string, unknown>>).map((step) => {
    const copy = { ...step };
    for (const key of ["url", "text", "instruction", "target", "region"]) {
      if (typeof copy[key] === "string") {
        const original = copy[key] as string;
        copy[key] =
          key === "url" && /^\{\{[A-Za-z][A-Za-z0-9_]{0,49}\}\}$/.test(original)
            ? "https://example.com"
            : replace(original, placeholders);
      }
    }
    if (Array.isArray(copy.keys))
      copy.keys = copy.keys.map((key) =>
        typeof key === "string" ? replace(key, placeholders) : key,
      );
    return copy;
  });
  if (sample.length > 0)
    parseWorkflow({
      executionTarget,
      ...(provider === undefined ? {} : { provider }),
      steps: sample,
    });
  return {
    ...(value.executionTarget === undefined ? {} : { executionTarget }),
    ...(provider === undefined ? {} : { provider }),
    name: value.name.trim(),
    description: (value.description as string | undefined) ?? "",
    inputs,
    steps: value.steps as WorkflowStep[],
  };
}
