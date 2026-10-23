import * as Effect from "effect/Effect";

import type { SessionContext } from "../client.js";
import { estimateContextTokens } from "@earendil-works/pi-ai/utils/estimate";
import type { ShiitakeEffectOptions } from "./effect-contracts.js";
import { reportCatchFailure } from "./failure.js";
import {
  buildSessionContext,
  readSessionContextPath,
  readSessionModelPreference,
} from "./woodstock/index.js";

/**
 * The context size compaction measures against the model's window, for the
 * Session's settled context: the next run starts from it. Null before the
 * Session has any context.
 */
function settledContextTokens(
  store: ShiitakeEffectOptions["store"],
  sessionId: string,
): number | null {
  const path = readSessionContextPath(store, { sessionId });
  const messages = buildSessionContext(path);
  return messages.length === 0 ? null : estimateContextTokens(messages).tokens;
}

/**
 * The Session's context use as compaction measures it, against the window of
 * the model a run would select now. Undefined while the Session has no
 * context or its context cannot be read; a window the host cannot describe is
 * null, never guessed.
 */
export function readSessionContext(
  options: ShiitakeEffectOptions,
  sessionId: string,
): Effect.Effect<SessionContext | undefined> {
  return Effect.gen(function* () {
    // A context too large to read safely has no footer figure.
    const read = yield* Effect.result(
      Effect.try({
        try: () => ({
          tokens: settledContextTokens(options.store, sessionId),
          preference: readSessionModelPreference(options.store, sessionId),
        }),
        catch: (error) => error,
      }),
    );
    if (read._tag === "Failure" || read.success.tokens === null)
      return undefined;
    const { tokens, preference } = read.success;
    const describe = options.model.describe;
    if (describe === undefined) return Object.freeze({ tokens, window: null });
    // A null Session override follows a mutable Agent default. Resolve through
    // the host's turn-model selector on every read, without acquiring a turn.
    const window = yield* describe({ sessionId, preference }).pipe(
      Effect.map(({ contextWindow }) => contextWindow),
      Effect.catch((error) =>
        Effect.sync(() => {
          reportCatchFailure("session-context.describe", error);
          return null;
        }),
      ),
    );
    return Object.freeze({ tokens, window });
  });
}
