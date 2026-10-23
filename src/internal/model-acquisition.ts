import * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";

import type { ModelAcquisition, ModelSource } from "./contracts.js";
import type {
  ModelAcquisitionEffect,
  ModelSourceEffect,
} from "./effect-contracts.js";
import { ShiitakeError } from "../errors.js";
import { reportCatchFailure } from "./failure.js";

function acquisitionFailure(cause?: unknown): ShiitakeError {
  if (cause instanceof ShiitakeError) return cause;
  return new ShiitakeError(
    "model",
    "rejected",
    "Model acquisition failed",
    undefined,
    cause,
  );
}

function releaseFailure(cause?: unknown): ShiitakeError {
  return new ShiitakeError(
    "model",
    "rejected",
    "Model release failed",
    undefined,
    cause,
  );
}

function asEffectAcquisition(
  acquisition: ModelAcquisition,
): ModelAcquisitionEffect {
  const release = Effect.tryPromise({
    try: () => acquisition.release(),
    catch: releaseFailure,
  });
  let shared: Pick<
    ModelAcquisitionEffect,
    "model" | "release" | "failureReason" | "retryable"
  > = {
    model: acquisition.model,
    release,
    retryable: acquisition.retryable,
  };
  if (acquisition.failureReason !== undefined)
    shared = { ...shared, failureReason: acquisition.failureReason };
  if (acquisition.stream === undefined)
    return acquisition.retryAfterMs === undefined
      ? Object.freeze(shared)
      : Object.freeze({ ...shared, retryAfterMs: acquisition.retryAfterMs });
  return acquisition.retryAfterMs === undefined
    ? Object.freeze({ ...shared, stream: acquisition.stream })
    : Object.freeze({
        ...shared,
        stream: acquisition.stream,
        retryAfterMs: acquisition.retryAfterMs,
      });
}

/**
 * Adapt a host Promise while keeping an interrupted late result owned.
 *
 * Effect's interruptible acquisition can stop waiting before a foreign
 * Promise settles. The returned async cleanup therefore waits for that
 * Promise and releases a result which was never transferred to the run
 * scope. A successful result resumes the Effect only after the signal check,
 * so the run scope's finalizer owns it exactly once.
 */
export function acquireModelPromise(
  acquire: (signal: AbortSignal) => Promise<ModelAcquisition>,
): ReturnType<ModelSourceEffect["acquire"]> {
  return Effect.callback((resume, signal) => {
    if (signal.aborted) return;

    let acquisition: Promise<ModelAcquisition>;
    try {
      acquisition = Promise.resolve(acquire(signal));
    } catch (cause) {
      resume(Effect.fail(acquisitionFailure(cause)));
      return;
    }

    const settled = acquisition.then(
      (resource) => {
        if (signal.aborted) {
          // An interrupted foreign Promise may still produce a resource. Keep
          // cancellation pending until that resource has been released.
          return Promise.resolve(resource.release()).then(() => undefined);
        }
        resume(Effect.succeed(asEffectAcquisition(resource)));
        return undefined;
      },
      (cause: unknown) => {
        if (!signal.aborted) resume(Effect.fail(acquisitionFailure(cause)));
        else reportCatchFailure("late-model-acquisition", cause);
      },
    );

    // Effect runs this cleanup when its interruptible async wait is stopped.
    // `settled` also observes late rejection, preventing an unhandled Promise.
    return Effect.tryPromise({
      try: () => settled,
      catch: releaseFailure,
    }).pipe(Effect.orDie);
  });
}

/**
 * Transfer one interruptible acquisition into its run scope without an
 * interruption gap between success and ownership registration.
 */
export function acquireOwnedModel<E, R>(
  acquire: Effect.Effect<ModelAcquisitionEffect, E, R>,
): Effect.Effect<ModelAcquisitionEffect, E, R | Scope.Scope> {
  let owned: ModelAcquisitionEffect | undefined;
  const transfer = Effect.uninterruptibleMask((restore) =>
    Effect.flatMap(restore(acquire), (acquisition) =>
      Effect.sync(() => {
        owned = acquisition;
        return acquisition;
      }),
    ),
  );
  return Effect.acquireRelease(
    transfer,
    () =>
      owned === undefined ? Effect.void : owned.release.pipe(Effect.orDie),
    { interruptible: true },
  );
}

/** Lift one host model source into the Effect runtime boundary. */
export function adaptModelSource(source: ModelSource): ModelSourceEffect {
  const acquire: ModelSourceEffect["acquire"] = ({ sessionId, preference }) =>
    acquireModelPromise((signal) =>
      source.acquire({ sessionId, preference, signal }),
    );
  const describe = source.describe;
  return Object.freeze(
    describe === undefined
      ? { acquire }
      : {
          acquire,
          describe: ({
            sessionId,
            preference,
          }: Parameters<NonNullable<ModelSourceEffect["describe"]>>[0]) =>
            Effect.tryPromise({
              try: (signal) => describe({ sessionId, preference, signal }),
              catch: acquisitionFailure,
            }),
        },
  );
}
