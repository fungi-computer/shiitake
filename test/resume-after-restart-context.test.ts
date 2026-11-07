import { readFile } from "node:fs/promises";
import * as Schema from "effect/Schema";
import { expect, it } from "vitest";
import {
  killed,
  finished,
  finishedValue,
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
        if (boundary === "retry") {
          const dispatches = await retryDispatches(path);
          expect(dispatches.map(({ attempt }) => attempt)).toEqual([1, 2, 3]);
        }
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

const RetryDispatch = Schema.fromJsonString(
  Schema.Struct({ at: Schema.Number, attempt: Schema.Number }),
);
async function retryDispatches(path: string) {
  return (await readFile(path + ".retry-requests", "utf8"))
    .trim()
    .split("\n")
    .map((line) => Schema.decodeUnknownSync(RetryDispatch)(line));
}

it(
  "retains the absolute retry deadline across SIGKILL during backoff",
  () =>
    withDatabase(async (path) => {
      await killed(path, "crash-backoff", "retry-waiting");
      const dueAt = Number(await readFile(path + ".retry-due", "utf8"));
      expectOutcome(await finished(path, "finish-retry"), "completed");
      const dispatches = await retryDispatches(path);
      expect(dispatches.map(({ attempt }) => attempt)).toEqual([1, 2]);
      expect(dispatches[1]?.at).toBeGreaterThanOrEqual(dueAt);
    }),
  30_000,
);

it(
  "retains retry intent when killed immediately after the failed assistant commit",
  () =>
    withDatabase(async (path) => {
      await killed(path, "crash-retry-decision", "retry-decided");
      const dueAt = Number(await readFile(path + ".retry-due", "utf8"));
      expect(dueAt).toBeGreaterThan(0);
      expectOutcome(await finished(path, "finish-retry"), "completed");
      const dispatches = await retryDispatches(path);
      expect(dispatches.map(({ attempt }) => attempt)).toEqual([1, 2]);
      expect(dispatches[1]?.at).toBeGreaterThanOrEqual(dueAt);
    }),
  30_000,
);

it.each(["initial", "after-retry"])(
  "does not renew an exhausted retry budget after two SIGKILLs (%s)",
  async (boundary) =>
    withDatabase(async (path) => {
      await killed(path, `crash-exhausted-${boundary}`, "model-started");
      await killed(
        path,
        boundary === "initial"
          ? "crash-exhausted-decision-zero"
          : "crash-exhausted-decision-one",
        "retry-exhausted",
      );
      expectOutcome(await finished(path, "finish-exhausted"), "failed");
      expect(
        (await retryDispatches(path)).map(({ attempt }) => attempt),
      ).toEqual(boundary === "initial" ? [1] : [1, 2]);
    }),
  45_000,
);

it(
  "fires a timer once after three SIGKILLs after claim without charging the Agent crash quota",
  () =>
    withDatabase(async (path) => {
      const started = Date.now();
      await killed(path, "crash-timer-arm", "timer-claimed");
      await killed(path, "crash-timer-recover", "timer-claimed");
      await killed(path, "crash-timer-recover", "timer-claimed");
      expect(Date.now() - started).toBeLessThan(60_000);
      const facts = Schema.decodeUnknownSync(
        Schema.Struct({
          fires: Schema.Int,
          pointers: Schema.Int,
          jobs: Schema.Array(
            Schema.Struct({
              state: Schema.String,
              outcome: Schema.String,
              recovery_count: Schema.Int,
              attempt_count: Schema.Int,
            }),
          ),
          pending: Schema.Struct({
            pending: Schema.Boolean,
            nextWakeAt: Schema.NullOr(Schema.Number),
          }),
        }),
      )(await finishedValue(path, "finish-timer"));
      expect(facts).toEqual({
        fires: 1,
        pointers: 0,
        jobs: [
          {
            state: "settled",
            outcome: "completed",
            recovery_count: 0,
            attempt_count: 0,
          },
        ],
        pending: { pending: false, nextWakeAt: null },
      });
    }),
  60_000,
);
