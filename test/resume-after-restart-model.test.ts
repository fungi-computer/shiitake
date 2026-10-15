import { expect, it } from "vitest";
import {
  killed,
  finished,
  withDatabase,
  expectOutcome,
} from "./fixtures/restart.fixture.js";

it(
  "continues an accepted turn after the process is killed mid-model-call",
  () =>
    withDatabase(async (path) => {
      await killed(path, "crash-model", "model-started");
      const snapshot = await finished(path);
      expectOutcome(snapshot, "completed");
      expect(snapshot.messages.map(({ value }) => value.role)).toEqual([
        "user",
        "assistant",
      ]);
    }),
  30_000,
);

it(
  "exhausts two recoveries with an interrupted diagnostic",
  () =>
    withDatabase(async (path) => {
      for (let attempt = 0; attempt < 3; attempt += 1)
        await killed(path, "crash-model", "model-started");
      const snapshot = await finished(path);
      expect(snapshot.terminals).toHaveLength(1);
      expect(snapshot.terminals[0]).toMatchObject({
        outcome: "interrupted",
        diagnostic: "Restart recovery budget exhausted (2 recoveries)",
      });
    }),
  60_000,
);

it(
  "never resumes a cancelled turn after restart",
  () =>
    withDatabase(async (path) => {
      await killed(path, "crash-model", "model-started");
      for (const mode of ["cancel", "finish"])
        expectOutcome(await finished(path, mode), "cancelled");
    }),
  45_000,
);

it(
  "reacquires host model permission on recovery",
  () =>
    withDatabase(async (path) => {
      await killed(path, "crash-model", "model-started");
      const snapshot = await finished(path, "paused");
      expectOutcome(snapshot, "failed");
      expect(snapshot.messages.map(({ value }) => value.role)).toEqual([
        "user",
      ]);
    }),
  30_000,
);
