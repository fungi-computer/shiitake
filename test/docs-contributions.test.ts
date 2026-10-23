import { expect, it } from "vitest";
// Static imports: loading the Extension and testing barrels is the slow part
// (~570 ms cold, against ~180 ms for the round trip), and it belongs to
// collection, not to the 5 s the test body gets.
import { readGrantAdd } from "../examples/bookmarks/read-grant.js";
import { settledCounterRoundTrip } from "../examples/recipes/settled-counter.js";

it("the settled hook counts completed runs once across reconciliations", async () => {
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
  expect(await readGrantAdd()).toMatchObject({
    refused: true,
    module: "bookmarks",
    operation: "add",
    reason: "read-only",
  });
});
