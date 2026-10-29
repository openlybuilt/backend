import { parseDefinition } from "./definition.js";

export const defaultWorkflow = parseDefinition({
  name: "Browser workflow demo",
  steps: [
    {
      url: "https://www.selenium.dev/selenium/web/web-form.html",
      type: "navigate",
    },
    {
      type: "wait",
      seconds: 15,
    },
    {
      text: "Hello from Virela!",
      type: "type",
      region: "Top left",
      target: "The Text input field",
    },
    {
      keys: ["Ctrl+a", "Right"],
      type: "press_keys",
    },
    {
      type: "scroll",
      pixels: 300,
      direction: "down",
    },
    {
      type: "click",
      target: "The Default checkbox. Do not submit the form.",
    },
    {
      type: "ai",
      instruction:
        "Confirm the Text input field contains exactly Hello from Virela! and the Default checkbox is selected. Do not submit the form or change anything.",
    },
  ],
  inputs: [],
  description:
    "Try navigation, waiting, text entry, keys, scrolling, clicking, and AI checks without submitting a form.",
});
