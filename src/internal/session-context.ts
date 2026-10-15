import * as Effect from "effect/Effect";

import type { SessionContext } from "../client.js";
import { estimateThresholdContextTokens } from "./compaction/session-compaction.js";
import type { ShiitakeEffectOptions } from "./effect-contracts.js";
import { reportCatchFailure } from "./failure.js";
import type { RuntimeCoordinator } from "./runtime-coordinator.js";
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
  return messages.length === 0
    ? null
    : estimateThresholdContextTokens(path, messages);
}

type WindowEntry = Readonly<{ preference: string; window: number | null }>;

/**
 * Each runtime keeps the last described window per Session. A saved model
 * preference changes the key, so `/model` is described again at once.
 */
const describedWindows = new WeakMap<
  RuntimeCoordinator,
  Map<string, WindowEntry>
>();

function windowsFor(runtime: RuntimeCoordinator): Map<string, WindowEntry> {
  const existing = describedWindows.get(runtime);
  if (existing !== undefined) return existing;
  const created = new Map<string, WindowEntry>();
  describedWindows.set(runtime, created);
  return created;
}

/**
 * The Session's context use as compaction measures it, against the window of
 * the model a run would select now. Undefined while the Session has no
 * context or its context cannot be read; a window the host cannot describe is
 * null, never guessed.
 */
export function readSessionContext(
  options: ShiitakeEffectOptions,
  runtime: RuntimeCoordinator,
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
    const windows = windowsFor(runtime);
    const key = JSON.stringify(preference);
    const cached = windows.get(sessionId);
    if (cached?.preference === key)
      return Object.freeze({ tokens, window: cached.window });
    const describe = options.model.describe;
    if (describe === undefined) return Object.freeze({ tokens, window: null });
    const described = yield* describe({ sessionId, preference }).pipe(
      Effect.map(({ contextWindow }) => contextWindow),
      Effect.catch((error) =>
        Effect.sync(() => {
          reportCatchFailure("session-context.describe", error);
          return undefined;
        }),
      ),
    );
    // A failed description is retried on the next read, never cached.
    if (described === undefined) return Object.freeze({ tokens, window: null });
    windows.set(sessionId, { preference: key, window: described });
    return Object.freeze({ tokens, window: described });
  });
}
