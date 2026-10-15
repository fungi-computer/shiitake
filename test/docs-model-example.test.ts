import { expect, it } from "vitest";
import { deterministicModelReply } from "../examples/recipes/deterministic-model.js";

it("the public-helper docs example completes one deterministic model call", async () => {
  const { outcome, calls } = await deterministicModelReply();
  expect(outcome.status).toBe("completed");
  expect(outcome.message.content).toEqual([{ type: "text", text: "pong" }]);
  expect(calls).toBe(1);
});
