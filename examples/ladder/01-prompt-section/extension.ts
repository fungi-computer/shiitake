import { defineExtension } from "@fungi.computer/shiitake/extensions";

/** The smallest extension: one named section in every system prompt. */
export const houseStyle = defineExtension("house-style").define({
  prompt: () => [{ name: "voice", text: "Answer in one short sentence." }],
});
