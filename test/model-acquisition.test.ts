import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Exit from "effect/Exit";
import * as Cause from "effect/Cause";
import { expect, it } from "vitest";

import { fauxModel } from "@fungi.computer/mule/testing";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import type { ModelAcquisition } from "../src/internal/contracts.js";
import { ShiitakeError } from "../src/errors.js";
import {
  acquireModelPromise,
  acquireOwnedModel,
} from "../src/internal/model-acquisition.js";

function resource(release: () => Promise<void>): ModelAcquisition {
  const faux = fauxModel([]);
  return { model: faux.model, retryable: faux.retryable, release };
}

it.each([
  { withStream: false, withRetryAfter: false },
  { withStream: true, withRetryAfter: false },
  { withStream: false, withRetryAfter: true },
  { withStream: true, withRetryAfter: true },
])(
  "preserves the typed retry decision with stream=$withStream, Retry-After=$withRetryAfter",
  async ({ withStream, withRetryAfter }) => {
    const faux = fauxModel([]);
    const refused = fauxAssistantMessage("", { stopReason: "error" });
    const unknown = fauxAssistantMessage("", { stopReason: "error" });
    const retryable: ModelAcquisition["retryable"] = (message) =>
      message === refused;
    let acquisition: ModelAcquisition = {
      ...resource(() => Promise.resolve()),
      retryable,
      rejection: (message) =>
        message === refused ? { code: "usage-limit-reached" } : undefined,
    };
    if (withStream) acquisition = { ...acquisition, stream: faux.stream };
    if (withRetryAfter)
      acquisition = { ...acquisition, retryAfterMs: () => 1_000 };
    const acquired = await Effect.runPromise(
      acquireModelPromise(() => Promise.resolve(acquisition)),
    );
    try {
      expect(acquired.retryable).toBe(retryable);
      expect(acquired.retryable(refused)).toBe(true);
      expect(acquired.retryable(unknown)).toBe(false);
      expect(acquired.rejection?.(refused)).toEqual({
        code: "usage-limit-reached",
      });
      expect(acquired.rejection?.(unknown)).toBeUndefined();
    } finally {
      await Effect.runPromise(acquired.release);
    }
  },
);

it("waits for a late acquisition and releases it after interruption", async () => {
  let resolveAcquisition!: (value: ModelAcquisition) => void;
  let resolveSignalAbort!: () => void;
  let releases = 0;
  const signalAborted = new Promise<void>((resolve) => {
    resolveSignalAbort = resolve;
  });
  const acquisition = new Promise<ModelAcquisition>((resolve) => {
    resolveAcquisition = resolve;
  });
  const fiber = Effect.runFork(
    acquireModelPromise((signal) => {
      signal.addEventListener("abort", resolveSignalAbort, { once: true });
      return acquisition;
    }),
  );
  await Promise.resolve();
  const interrupted = Effect.runPromise(Fiber.interrupt(fiber));
  await signalAborted;
  resolveAcquisition(
    resource(() => {
      releases += 1;
      return Promise.resolve();
    }),
  );
  await interrupted;
  expect(releases).toBe(1);
});

it("transfers a successful acquisition before an interrupt and releases once", async () => {
  let resolveAcquired!: () => void;
  const acquiredReady = new Promise<void>((resolve) => {
    resolveAcquired = resolve;
  });
  let releases = 0;
  const run = Effect.scoped(
    Effect.gen(function* () {
      const acquired = yield* acquireOwnedModel(
        acquireModelPromise(() =>
          Promise.resolve(
            resource(() => {
              releases += 1;
              return Promise.resolve();
            }),
          ),
        ),
      );
      expect(acquired).toBeDefined();
      resolveAcquired();
      yield* Effect.never;
    }),
  );
  const fiber = Effect.runFork(run);
  await acquiredReady;
  await Effect.runPromise(Fiber.interrupt(fiber));
  expect(releases).toBe(1);
});

it("reports a typed fatal boundary when late interruption cleanup fails", async () => {
  let resolveAcquisition!: (value: ModelAcquisition) => void;
  let resolveSignalAbort!: () => void;
  const signalAborted = new Promise<void>((resolve) => {
    resolveSignalAbort = resolve;
  });
  const acquisition = new Promise<ModelAcquisition>((resolve) => {
    resolveAcquisition = resolve;
  });
  const fiber = Effect.runFork(
    acquireModelPromise((signal) => {
      signal.addEventListener("abort", resolveSignalAbort, { once: true });
      return acquisition;
    }),
  );
  await Promise.resolve();
  const interrupted = Effect.runPromise(Fiber.interrupt(fiber));
  await signalAborted;
  resolveAcquisition(
    resource(() => Promise.reject(new Error("late release failed"))),
  );
  await interrupted;
  const result = await Effect.runPromise(Fiber.await(fiber));
  expect(Exit.isFailure(result)).toBe(true);
  if (Exit.isFailure(result))
    expect(Cause.pretty(result.cause)).toContain("Model release failed");
});

it("preserves ordinary acquisition, rejection, and release outcomes", async () => {
  let releases = 0;
  const acquired = await Effect.runPromise(
    acquireModelPromise(() =>
      Promise.resolve(
        resource(() => {
          releases += 1;
          return Promise.resolve();
        }),
      ),
    ),
  );
  await Effect.runPromise(acquired.release);
  expect(releases).toBe(1);

  expect(
    await Effect.runPromise(
      Effect.result(
        acquireModelPromise(() => Promise.reject(new Error("acquire"))),
      ),
    ),
  ).toMatchObject({
    _tag: "Failure",
    failure: { boundary: "model", cause: "rejected" },
  });

  const releaseError = new ShiitakeError(
    "model",
    "rejected",
    "Model release failed",
  );
  const failing = await Effect.runPromise(
    acquireModelPromise(() =>
      Promise.resolve(resource(() => Promise.reject(releaseError))),
    ),
  );
  expect(await Effect.runPromise(Effect.result(failing.release))).toMatchObject(
    {
      _tag: "Failure",
      failure: { boundary: "model", cause: "rejected" },
    },
  );
});
