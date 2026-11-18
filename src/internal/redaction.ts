import type { MuleError, MuleToolResult } from "@fungi.computer/mule";
import type { JsonValue } from "@fungi.computer/mule/effect";
import * as Effect from "effect/Effect";

import { ShiitakeError } from "../errors.js";
import type {
  SessionRedactionResult,
  SessionRedactor,
  SqliteOwner,
} from "./contracts.js";
import type { ShiitakeEffectOptions } from "./effect-contracts.js";

/**
 * The Session's one place that applies the host's Vault redaction. The host
 * builds the matcher from fingerprints of its stored values; Shiitake never
 * sees a value. A host without the port stores no values. A bound port that
 * cannot load fails closed: output is withheld and the failure is typed.
 */
type OwnerRedaction = {
  readonly load: Effect.Effect<SessionRedactor, ShiitakeError>;
  /** The last matcher that loaded; a run's writes use it after its load. */
  current: SessionRedactor | undefined;
};

const byOwner = new WeakMap<SqliteOwner, OwnerRedaction>();

declare const redacted: unique symbol;

/**
 * Output that passed this owner's Vault matcher. The brand is held at
 * runtime too, so a store write takes only a value `redactOwned` (or
 * `retainedOutput`) returned, never fresh output.
 */
export type RedactedOutput<A extends object> = A &
  Readonly<{ [redacted]: true }>;

/** Redacted text, such as a compaction summary. */
export type RedactedText = RedactedOutput<Readonly<{ text: string }>>;

const redactedValues = new WeakSet();

function isRedacted<A extends object>(value: A): value is RedactedOutput<A> {
  return redactedValues.has(value);
}

function branded<A extends object>(value: A): RedactedOutput<A> {
  redactedValues.add(value);
  if (isRedacted(value)) return value;
  throw new RedactionUnavailableError();
}

/**
 * Bytes already in this store (a fork's copy, a re-keyed row, a summary
 * assembled from redacted text). Never fresh model, tool or extension output.
 */
export function retainedOutput<A extends object>(value: A): RedactedOutput<A> {
  return branded(value);
}

/** A closed, persistable reason: Vault redaction could not run. */
export class RedactionUnavailableError extends ShiitakeError {
  constructor(cause?: unknown) {
    super(
      "runtime",
      "unavailable",
      "Session redaction is unavailable",
      undefined,
      cause,
    );
  }

  readonly diagnostic = "redaction-unavailable";
}

/** The Promise hosts' port, lifted once into Shiitake's Effect owner. */
export function liftRedaction(
  port: Readonly<{ load: () => Promise<SessionRedactor> }>,
): NonNullable<ShiitakeEffectOptions["redaction"]> {
  return {
    load: Effect.tryPromise({
      try: () => port.load(),
      catch: (cause) => new RedactionUnavailableError(cause),
    }),
  };
}

export function bindSessionRedaction(
  owner: SqliteOwner,
  load: Effect.Effect<SessionRedactor, ShiitakeError>,
): void {
  byOwner.set(owner, { load, current: undefined });
}

/**
 * Load the host's current matcher. `undefined` means this host has no Vault;
 * a bound port that fails is a typed `RedactionUnavailableError`.
 */
export function loadSessionRedaction(
  owner: SqliteOwner,
): Effect.Effect<SessionRedactor | undefined, RedactionUnavailableError> {
  const state = byOwner.get(owner);
  if (state === undefined) return Effect.succeed(undefined);
  return state.load.pipe(
    Effect.tap((redactor) =>
      Effect.sync(() => {
        state.current = redactor;
      }),
    ),
    Effect.mapError((cause) => new RedactionUnavailableError(cause)),
  );
}

/**
 * The matcher persistence uses now: `undefined` when the host has no Vault,
 * `null` while a bound port has not loaded (nothing may be persisted).
 */
function currentSessionRedaction(
  owner: SqliteOwner,
): SessionRedactor | null | undefined {
  const state = byOwner.get(owner);
  return state === undefined ? undefined : (state.current ?? null);
}

/** Text through one matcher call; an unavailable matcher withholds it. */
function settled(
  call: () => Promise<SessionRedactionResult>,
): Effect.Effect<string, RedactionUnavailableError> {
  return Effect.tryPromise({
    try: call,
    catch: (cause) => new RedactionUnavailableError(cause),
  }).pipe(
    Effect.flatMap((result) =>
      result.status === "unavailable"
        ? Effect.fail(new RedactionUnavailableError())
        : Effect.succeed(result.text),
    ),
  );
}

/** Redact whole text with the owner's current matcher, failing closed. */
export function redactOwnedText(
  owner: SqliteOwner,
  text: string,
): Effect.Effect<RedactedText, RedactionUnavailableError> {
  const redactor = currentSessionRedaction(owner);
  if (redactor === undefined) return Effect.sync(() => branded({ text }));
  if (redactor === null) return Effect.fail(new RedactionUnavailableError());
  return settled(() => redactor.redact(text)).pipe(
    Effect.map((value) => branded({ text: value })),
  );
}

/** One JSON representation, written and parsed by its owner's schema. */
export type RedactionCodec<A> = Readonly<{
  encode: (value: A) => string;
  decode: (text: string) => A;
}>;

/**
 * Redact one value through its JSON text with the owner's current matcher.
 * The mask replaces whole matches inside string literals; text whose mask
 * crossed JSON structure no longer decodes, and that withholds it too.
 */
export function redactOwned<A extends object>(
  owner: SqliteOwner,
  value: A,
  codec: RedactionCodec<A>,
): Effect.Effect<RedactedOutput<A>, RedactionUnavailableError> {
  const redactor = currentSessionRedaction(owner);
  if (redactor === undefined) return Effect.sync(() => branded(value));
  if (redactor === null) return Effect.fail(new RedactionUnavailableError());
  return redactWith(redactor, value, codec).pipe(
    Effect.map((result) => branded(result)),
  );
}

function redactWith<A>(
  redactor: SessionRedactor,
  value: A,
  codec: RedactionCodec<A>,
): Effect.Effect<A, RedactionUnavailableError> {
  return Effect.try({
    try: () => codec.encode(value),
    catch: (cause) => new RedactionUnavailableError(cause),
  }).pipe(
    Effect.flatMap((encoded) =>
      Effect.tryPromise({
        try: () => redactor.redact(encoded),
        catch: (cause) => new RedactionUnavailableError(cause),
      }),
    ),
    Effect.flatMap((result) => {
      if (result.status === "clean") return Effect.succeed(value);
      if (result.status === "unavailable")
        return Effect.fail(new RedactionUnavailableError());
      return Effect.try({
        try: () => codec.decode(result.text),
        catch: (cause) => new RedactionUnavailableError(cause),
      });
    }),
  );
}

const muleFailure = (cause: unknown): MuleError => ({
  _tag: "MuleError",
  phase: "redaction",
  message: "Session redaction is unavailable",
  cause,
});

/**
 * Mule's tool-result door for this owner: each text part and the result's
 * application data. A terminal result is redacted whole. A live update
 * snapshot also holds back its last possible secret prefix, since the next
 * snapshot may complete it.
 */
export function toolResultRedaction(
  owner: SqliteOwner,
  data: RedactionCodec<JsonValue>,
) {
  if (!byOwner.has(owner)) return undefined;
  return (
    result: MuleToolResult,
    phase: "update" | "result",
  ): Effect.Effect<MuleToolResult, MuleError> =>
    loadSessionRedaction(owner).pipe(
      Effect.flatMap((redactor) => {
        if (redactor === undefined) return Effect.succeed(result);
        const text = (value: string) =>
          settled(() =>
            phase === "result"
              ? redactor.redact(value)
              : redactor.stream().write(value),
          );
        return Effect.all({
          content: Effect.forEach(
            result.content,
            (
              part,
            ): Effect.Effect<
              MuleToolResult["content"][number],
              RedactionUnavailableError
            > =>
              part.type === "text"
                ? text(part.text).pipe(
                    Effect.map((value) => ({ ...part, text: value })),
                  )
                : Effect.succeed(part),
          ),
          data:
            result.data === undefined
              ? Effect.succeed(undefined)
              : redactWith(redactor, result.data, data),
        }).pipe(
          Effect.map(({ content, data: redactedData }) =>
            redactedData === undefined
              ? { ...result, content }
              : { ...result, content, data: redactedData },
          ),
        );
      }),
      Effect.mapError(muleFailure),
    );
}

/**
 * A live model text stream: emits only text that cannot begin a match. Each
 * write completes before the next, as the stream requires. An unavailable
 * matcher withholds live text; the recorded message is redacted whole.
 */
export type LiveTextRedaction = (delta: string) => Effect.Effect<string>;

export function liveTextRedaction(owner: SqliteOwner): LiveTextRedaction {
  const redactor = currentSessionRedaction(owner);
  if (redactor === undefined) return (delta) => Effect.succeed(delta);
  if (redactor === null) return () => Effect.succeed("");
  const stream = redactor.stream();
  return (delta) =>
    settled(() => stream.write(delta)).pipe(Effect.orElseSucceed(() => ""));
}
