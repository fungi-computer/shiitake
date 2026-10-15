import { expect, it } from "vitest";

it("the settled hook counts completed runs once across reconciliations", async () => {
  const { settledCounterRoundTrip } =
    await import("../examples/recipes/settled-counter.js");
  const result = await settledCounterRoundTrip();
  expect(result.first.extensions["settled-counter"]).toMatchObject({
    status: "running",
    value: 1,
  });
  expect(result.second.extensions["settled-counter"]).toMatchObject({
    status: "running",
    value: 2,
  });
  expect(result.repeated.extensions["settled-counter"]).toMatchObject({
    value: 2,
  });
});

it("the read-grant docs example refuses a write tool before it runs", async () => {
  const { readGrantAdd } = await import("../examples/bookmarks/read-grant.js");
  expect(await readGrantAdd()).toMatchObject({
    refused: true,
    module: "bookmarks",
    operation: "add",
    reason: "read-only",
  });
});
