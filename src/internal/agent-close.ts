import * as Effect from "effect/Effect";
import * as Latch from "effect/Latch";
import * as Exit from "effect/Exit";

import type { ShiitakeError } from "../errors.js";

type RunningClose = {
  readonly _tag: "running";
  readonly settled: Latch.Latch;
  result?: Exit.Exit<void, ShiitakeError>;
};

type CloseState =
  { readonly _tag: "open" } | RunningClose | { readonly _tag: "closed" };

function replayCloseResult(
  result: Exit.Exit<void, ShiitakeError>,
): Effect.Effect<void, ShiitakeError> {
  return Exit.match(result, {
    onFailure: Effect.failCause,
    onSuccess: () => Effect.void,
  });
}

function closeCoordinatorThenExtensions(
  coordinatorClose: Effect.Effect<void, ShiitakeError>,
  extensionClose: Effect.Effect<void, ShiitakeError>,
): Effect.Effect<void, ShiitakeError> {
  return Effect.matchCauseEffect(coordinatorClose, {
    onFailure: (coordinatorCause) =>
      Effect.exit(extensionClose).pipe(
        Effect.andThen(Effect.failCause(coordinatorCause)),
      ),
    onSuccess: () => extensionClose,
  });
}

/** Compose one idempotent close action for the constructed agent. */
export function composeAgentClose(
  coordinatorClose: Effect.Effect<void, ShiitakeError>,
  extensionClose: Effect.Effect<void, ShiitakeError>,
): Effect.Effect<void, ShiitakeError> {
  let state: CloseState = { _tag: "open" };

  const runClose = (
    running: RunningClose,
  ): Effect.Effect<void, ShiitakeError> =>
    closeCoordinatorThenExtensions(coordinatorClose, extensionClose).pipe(
      Effect.exit,
      Effect.flatMap((result) =>
        Effect.sync(() => {
          running.result = result;
          state = { _tag: "closed" };
        }).pipe(
          Effect.andThen(running.settled.open),
          Effect.andThen(replayCloseResult(result)),
        ),
      ),
      Effect.uninterruptible,
    );

  return Effect.suspend(() => {
    if (state._tag === "closed") return Effect.void;
    if (state._tag !== "open") {
      const running = state;
      return running.settled.await.pipe(
        Effect.andThen(
          Effect.suspend(() => {
            const result = running.result;
            return result === undefined
              ? Effect.die(new Error("Agent close settlement was not recorded"))
              : replayCloseResult(result);
          }),
        ),
      );
    }

    const running: RunningClose = {
      _tag: "running",
      settled: Latch.makeUnsafe(false),
    };
    state = running;
    return runClose(running);
  });
}
