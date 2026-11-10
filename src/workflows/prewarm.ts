export type PrewarmStep =
  | { type: "goto"; url: string }
  | { type: "fill"; selector: string; placeholder: string }
  | { type: "click"; selector: string }
  | { type: "wait_for"; selector: string; timeout: number };
export type ResolvedPrewarmStep =
  | Exclude<PrewarmStep, { type: "fill" }>
  | { type: "fill"; selector: string; value: string };
const obj = (v: unknown): v is Record<string, unknown> =>
  !!v && typeof v === "object" && !Array.isArray(v);
const exact = (v: Record<string, unknown>, keys: string[]) =>
  Object.keys(v).length === keys.length &&
  keys.every((key) => Object.hasOwn(v, key));
const text = (v: unknown): v is string =>
  typeof v === "string" && v.trim().length > 0 && v.length <= 2048;
export function parsePrewarm(input: unknown): PrewarmStep[] {
  if (!Array.isArray(input) || input.length < 2 || input.length > 30)
    throw new Error("Prewarming requires 2 to 30 steps");
  const steps = input.map((v): PrewarmStep => {
    if (!obj(v)) throw new Error("Invalid prewarming step");
    if (v.type === "goto" && exact(v, ["type", "url"]) && text(v.url)) {
      const url = new URL(v.url);
      if (url.protocol === "https:" && !url.username && !url.password)
        return { type: "goto", url: url.href };
    }
    if (
      v.type === "fill" &&
      exact(v, ["type", "selector", "placeholder"]) &&
      text(v.selector) &&
      typeof v.placeholder === "string" &&
      /^[A-Za-z][A-Za-z0-9_]{0,49}$/.test(v.placeholder)
    )
      return { type: "fill", selector: v.selector, placeholder: v.placeholder };
    if (
      v.type === "click" &&
      exact(v, ["type", "selector"]) &&
      text(v.selector)
    )
      return { type: "click", selector: v.selector };
    if (
      v.type === "wait_for" &&
      exact(v, ["type", "selector", "timeout"]) &&
      text(v.selector) &&
      Number.isInteger(v.timeout) &&
      (v.timeout as number) >= 1 &&
      (v.timeout as number) <= 30000
    )
      return {
        type: "wait_for",
        selector: v.selector,
        timeout: v.timeout as number,
      };
    throw new Error("Invalid prewarming step");
  });
  if (steps[0]?.type !== "goto" || steps.at(-1)?.type !== "wait_for")
    throw new Error(
      "Prewarming must open a page and verify the signed-in screen",
    );
  return steps;
}
export function prewarmPlaceholderNames(steps: PrewarmStep[]): string[] {
  return [
    ...new Set(
      steps.flatMap((step) => (step.type === "fill" ? [step.placeholder] : [])),
    ),
  ];
}
export function resolvePrewarm(
  steps: PrewarmStep[],
  values: Record<string, string>,
): ResolvedPrewarmStep[] {
  return steps.map((step) => {
    if (step.type !== "fill") return step;
    if (!Object.hasOwn(values, step.placeholder) || !values[step.placeholder])
      throw new Error("Missing prewarming placeholder");
    return {
      type: "fill",
      selector: step.selector,
      value: values[step.placeholder]!,
    };
  });
}
