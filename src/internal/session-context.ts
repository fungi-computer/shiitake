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

type CachedContext = Readonly<{
  cursor: string | null | undefined;
  preference: string;
  value: SessionContext | undefined;
}>;

const contextByOwner = new WeakMap<
  ShiitakeEffectOptions,
  Map<string, CachedContext>
>();

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
 * Read the stored figure synchronously with the Session baseline. Only model
 * description remains in the returned Effect, outside the SQLite transaction
 * and the request's ancestry scope. Undefined without readable context; a model
 * window the host cannot describe is null, never guessed.
 */
export function prepareSessionContext(
  options: ShiitakeEffectOptions,
  sessionId: string,
  cursor: string | null,
  cache = true,
): Effect.Effect<SessionContext | undefined> {
  let preference: ReturnType<typeof readSessionModelPreference>;
  try {
    preference = readSessionModelPreference(options.store, sessionId);
  } catch (error) {
    reportCatchFailure("session-context.preference", error);
    return Effect.succeed(undefined);
  }
  const preferenceKey = JSON.stringify(preference);
  const ownerCache =
    contextByOwner.get(options) ?? new Map<string, CachedContext>();
  contextByOwner.set(options, ownerCache);
  const previous = cache ? ownerCache.get(sessionId) : undefined;
  const describe = options.model.describe;
  if (previous?.cursor === cursor && previous.preference === preferenceKey) {
    if (previous.value === undefined || describe === undefined)
      return Effect.succeed(previous.value);
    const previousValue = previous.value;
    // A null Session override follows a mutable Agent default. Check the
    // selected window without decoding history again; a changed window is
    // a distinct effective preference for this cursor.
    return describe({ sessionId, preference }).pipe(
      Effect.map(({ contextWindow }) => {
        if (previousValue.window === contextWindow) return previousValue;
        const value = Object.freeze({
          tokens: previousValue.tokens,
          window: contextWindow,
        });
        if (cache && ownerCache.get(sessionId)?.cursor === cursor)
          ownerCache.set(sessionId, {
            cursor,
            preference: preferenceKey,
            value,
          });
        return value;
      }),
      Effect.catch((error) =>
        Effect.sync(() => {
          reportCatchFailure("session-context.describe", error);
          return previousValue;
        }),
      ),
    );
  }

  // At most one cached whole-window read per Session, cursor, and
  // preference: a late describe may publish only while this cursor still
  // owns the cache entry. Reads before the tip bypass the cache and each read
  // the whole window. A cold-cache connection also reads the whole kept
  // window.
  let tokens: number | null;
  try {
    tokens = settledContextTokens(options.store, sessionId);
  } catch (error) {
    reportCatchFailure("session-context.figure", error);
    return Effect.succeed(undefined);
  }
  if (tokens === null) {
    if (cache)
      ownerCache.set(sessionId, {
        cursor,
        preference: preferenceKey,
        value: undefined,
      });
    return Effect.succeed(undefined);
  }
  if (describe === undefined) {
    const value = Object.freeze({ tokens, window: null });
    if (cache)
      ownerCache.set(sessionId, { cursor, preference: preferenceKey, value });
    return Effect.succeed(value);
  }
  // Publish the decoded figure before yielding to the host's model
  // description. Concurrent watchers then reuse these tokens rather than
  // decoding the same Session window while `describe` is in flight.
  if (cache)
    ownerCache.set(sessionId, {
      cursor,
      preference: preferenceKey,
      value: Object.freeze({ tokens, window: null }),
    });
  // A null Session override follows a mutable Agent default. Resolve through
  // the host's turn-model selector on every read, without acquiring a turn.
  return describe({
    sessionId,
    preference,
  }).pipe(
    Effect.map(({ contextWindow }) => contextWindow),
    Effect.catch((error) =>
      Effect.sync(() => {
        reportCatchFailure("session-context.describe", error);
        return null;
      }),
    ),
    Effect.map((window) => {
      const value = Object.freeze({ tokens, window });
      if (cache && ownerCache.get(sessionId)?.cursor === cursor)
        ownerCache.set(sessionId, { cursor, preference: preferenceKey, value });
      return value;
    }),
  );
}
