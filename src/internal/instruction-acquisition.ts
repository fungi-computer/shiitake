import * as Effect from "effect/Effect";

import { ShiitakeError } from "../errors.js";
import { reportCatchFailure } from "./failure.js";

/** A snapshot a late, interrupted load may still hand over, with its release. */
export type ReleasableSnapshot = Readonly<{
  release?: (() => void) | undefined;
}>;

function releaseLate(snapshot: ReleasableSnapshot): void {
  try {
    snapshot.release?.();
  } catch (cause) {
    reportCatchFailure("late-instruction-release", cause);
  }
}

/**
 * Adapt a host Promise while keeping an interrupted late result owned.
 *
 * Interrupting the wait does not stop a foreign Promise, which may still hold
 * run-scoped host resources and later return a snapshot nobody owns. The
 * interruption itself finishes at once, as a host that ignores cancellation
 * must not hold the run open. Instead the still-pending Promise is handed to
 * `track`, so whoever must prove the run drained can wait for it, and a late
 * snapshot is released as soon as it arrives.
 */
export function acquireInstructionPromise<A extends ReleasableSnapshot>(
  load: (signal: AbortSignal) => Promise<A>,
  failure: () => ShiitakeError,
  track?: (pending: Promise<void>) => void,
): Effect.Effect<A, ShiitakeError> {
  return Effect.callback((resume, signal) => {
    if (signal.aborted) return;

    let loading: Promise<A>;
    try {
      loading = Promise.resolve(load(signal));
    } catch {
      resume(Effect.fail(failure()));
      return;
    }

    const settled = loading.then(
      (value) => {
        if (signal.aborted) releaseLate(value);
        else resume(Effect.succeed(value));
      },
      (cause: unknown) => {
        if (!signal.aborted) resume(Effect.fail(failure()));
        else reportCatchFailure("late-instruction-acquisition", cause);
      },
    );

    return Effect.sync(() => {
      track?.(settled);
    });
  });
}
