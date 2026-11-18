import { loadPatternRedactor } from "@fungi.computer/veil/redaction";
import * as Effect from "effect/Effect";

import { ShiitakeError } from "../errors.js";
import type { SessionRedactor } from "./contracts.js";

/**
 * Refuse prompt text that holds a secret, before anything is admitted. Two
 * matchers, both owned by Veil, in this order: the maintained provider key
 * patterns, then the owner's stored values through the host's Vault port
 * (`stored`, yielding `undefined` when the host has no Vault). The pattern
 * check runs first and needs no port, so a recognisable key is refused as
 * `secret_in_prompt` even while the Vault is down. A matcher that cannot
 * answer refuses too, as `contract`/`unavailable`: the text was refused at
 * the door and nothing was admitted or stored.
 *
 * Owner-side only: the client entry must never reach Veil's matcher, so this
 * stays apart from the shared client boundary.
 */
export function refuseSecretInPrompt(
  text: string,
  stored: Effect.Effect<SessionRedactor | undefined, ShiitakeError>,
): Effect.Effect<void, ShiitakeError> {
  const refuse = Effect.fail(
    new ShiitakeError(
      "contract",
      "secret_in_prompt",
      "This message contains a key or password, so it was not sent or saved. Add the key in settings instead.",
    ),
  );
  const unchecked = (cause: unknown) =>
    new ShiitakeError(
      "contract",
      "unavailable",
      "This message could not be checked for keys, so it was not sent or saved. Try again shortly.",
      undefined,
      cause,
    );
  if (text.length === 0) return Effect.void;
  return Effect.tryPromise({
    try: () => loadPatternRedactor(),
    catch: unchecked,
  }).pipe(
    Effect.flatMap((patterns) =>
      patterns.contains(text)
        ? refuse
        : stored.pipe(
            Effect.mapError(unchecked),
            Effect.flatMap((redactor) =>
              redactor === undefined
                ? Effect.void
                : Effect.tryPromise({
                    try: () => redactor.contains(text),
                    catch: unchecked,
                  }).pipe(
                    Effect.flatMap((found) => (found ? refuse : Effect.void)),
                  ),
            ),
          ),
    ),
  );
}
