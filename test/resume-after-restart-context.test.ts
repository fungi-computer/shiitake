import { expect, it } from "vitest";
import {
  killed,
  finished,
  withDatabase,
  expectOutcome,
  expectVerifiedAnswer,
} from "./fixtures/restart.fixture.js";

for (const boundary of ["steer", "retry", "compaction", "summary"]) {
  it(
    `resends an interrupted model after ${boundary} from durable context`,
    () =>
      withDatabase(async (path) => {
        await killed(path, `crash-${boundary}`, "model-started");
        const snapshot = await finished(path, `finish-${boundary}`);
        expectOutcome(snapshot, "completed");
        expectVerifiedAnswer(snapshot);
      }),
    30_000,
  );
}

it(
  "keeps cancellation committed before the original process dies",
  () =>
    withDatabase(async (path) => {
      await killed(path, "crash-cancel", "cancelled");
      const snapshot = await finished(path);
      expectOutcome(snapshot, "cancelled");
      expect(snapshot.messages.map(({ value }) => value.role)).toEqual([
        "user",
      ]);
    }),
  30_000,
);

it(
  "keeps the assistant call paired with its result after recovered work fails",
  () =>
    withDatabase(async (path) => {
      await killed(path, "crash-tool", "tool-started");
      expectOutcome(await finished(path, "fail-context"), "failed");
      expectVerifiedAnswer(await finished(path, "finish-history"));
    }),
  45_000,
);
