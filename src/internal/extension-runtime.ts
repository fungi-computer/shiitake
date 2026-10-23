import type { WatchdogQueueHead } from "@fungi.computer/watchdog/effect";
import {
  AttentionItemSchema,
  maximumExtensionAttention,
} from "../session-attention.js";
import type { ExtensionRuntimePortV0 } from "./extension-context.js";
import type { DocumentSpecV0, DocumentHandleV0 } from "../session-documents.js";
import { encodeDocument } from "./document-codec.js";
import type { FileTool } from "@fungi.computer/knapsack";
import type { ModuleRegistration } from "@fungi.computer/mycelium";
import * as Cause from "effect/Cause";
import * as Result from "effect/Result";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";

import { ShiitakeError } from "../errors.js";
import {
  AdmissionError,
  ExtensionCommandError,
  ExtensionRead,
  ExtensionStateError,
  ExtensionStop,
  ExtensionTx,
  type SendInputV0,
  type SendResultV0,
  type AnyAgentHooksV0,
  type CollectionSpecV0,
  type CollectionV0,
  type CompactionHandoffV0,
  type ExtensionCapabilityV0,
  type ExtensionFingerprint,
  type ExtensionId,
  type ExtensionSlotV0,
  type ExtensionStatusV0,
  type Json,
  type PromptSectionV0,
  type RunIdentityV0,
  type SettledRunV0,
  type StoredRecordV0,
  type TimersV0,
  type AttentionV0,
  type UpdateCollectionV0,
} from "../extensions.js";
import type {
  AdmissionBoundary,
  AdmissionDoor,
  AdmitWithin,
} from "./admission-door.js";
import { decodeClientSessionId } from "./client-boundary.js";
import type { SqliteOwner } from "./contracts.js";
import type { AdmittedExtensionV0 } from "./extension-roster.js";
import {
  AsyncExtensionTransactionError,
  invokeExtensionHook,
  invokeSyncHandler,
} from "./extension-hook.js";
import {
  setSessionAttention,
  clearSessionAttention,
  projectSessionAttention,
  readAttentionSessionPage,
  replaceAttentionGrants,
  hasStoredSession,
  readSessionLineage,
  hasPendingSessionSteer,
  hasSuccessfulToolResult,
  advanceCursor,
  declareSessionDocument,
  readSessionDocument,
  writeSessionDocument,
  deleteAllRecords,
  deleteRecord,
  deleteTimer,
  deleteExtensionTimers,
  readAllRecords,
  readCursor,
  readDueTimers,
  readRecord,
  readRecordPage,
  readStateLedger,
  readStatus,
  readTimer,
  readTimerWake,
  writeRecord,
  writeStateLedger,
  writeStatus,
  writeTimer,
} from "./woodstock/index.js";
import { JsonValueSchema } from "./json-value.js";
import { publishOwnerObservation } from "./observation-kernel.js";

/** The settled consumer name core itself uses for mail and control repair. */
export const coreConsumer = "core";

const maximumKeyLength = 256;
const maximumRecordBytes = 1024 * 1024;
const maximumReasonLength = 512;
const maximumPage = 64;
const encoder = new TextEncoder();
const decodePromptSections = Schema.decodeUnknownResult(
  Schema.Array(
    Schema.Struct({
      name: Schema.String.pipe(
        Schema.check(Schema.isPattern(/^[a-z][A-Za-z0-9_-]{0,63}$/u)),
      ),
      text: Schema.String.pipe(
        Schema.check(Schema.isMinLength(1)),
        Schema.check(
          Schema.makeFilter((text) => encoder.encode(text).byteLength <= 8192),
        ),
      ),
    }),
  ).pipe(Schema.check(Schema.isMaxLength(16))),
);

type Hooks = AnyAgentHooksV0;
type ReadService = Readonly<{ extensionId: ExtensionId }>;

/** Per-transaction effects collected while one extension body runs. */
type TransactionFrame = {
  readonly extensionId: ExtensionId;
  readonly admit: AdmitWithin;
  readonly observed: Set<string>;
  timersChanged: boolean;
};

type Member = {
  readonly admitted: AdmittedExtensionV0;
  scope?: Scope.Closeable;
  hooks: Hooks | undefined;
  status: "running" | "stopped";
  reason: string | null;
  stoppedAt: number | null;
  stateResetRequired?: ShiitakeError;
};

/** A declared migration failed or returned records the new state cannot decode. */
class IncompatibleStateMigrationError extends Error {}

/** Core's generic view of rostered v1 extensions. */
export type ExtensionRuntime = Readonly<{
  /** Prepare additions without replacing any live member; apply only between turns. */
  stage: (admitted: readonly AdmittedExtensionV0[]) => Effect.Effect<
    Readonly<{
      apply: Effect.Effect<void, ShiitakeError>;
      discard: Effect.Effect<void>;
    }>,
    ShiitakeError
  >;
  /** Running extensions that participate in runs, with their fingerprints. */
  runParticipants: () => readonly (readonly [
    ExtensionId,
    ExtensionFingerprint,
  ])[];
  /** Failed pure state migrations make execution unsafe until repaired. */
  incompatibleMigrations: () => readonly ExtensionId[];
  /** Per-run Mycelium modules of the captured participants. */
  modulesForRun: (
    run: RunIdentityV0,
    captured: ReadonlySet<string>,
  ) => Effect.Effect<readonly ModuleRegistration[], ShiitakeError>;
  /** The exclusive holder's direct Files tools for these captured participants. */
  filesForRun: (
    run: RunIdentityV0,
    captured: ReadonlySet<string>,
  ) => Effect.Effect<readonly FileTool[], ShiitakeError>;
  /** Ordered, bounded sections rebuilt for every model request. */
  promptSections: (
    run: RunIdentityV0,
    captured: ReadonlySet<string>,
  ) => Effect.Effect<readonly PromptSectionV0[], ShiitakeError>;
  /** The captured participant's exclusive compaction handoff. */
  compactionHandoff: (
    captured: ReadonlySet<string>,
  ) => CompactionHandoffV0 | undefined;
  /** Running settled consumers and their durable cursors. */
  settledConsumers: () => readonly Readonly<{ id: string; cursor: number }>[];
  /** Deliver each consumer its unseen slice of core-processed runs. */
  deliverSettled: (
    runs: readonly SettledRunV0[],
    throughSequence: number,
  ) => Effect.Effect<void, ShiitakeError>;
  /** Fire up to 64 due timers; reports whether more may be due. */
  fireDueTimers: (now: number) => Effect.Effect<boolean, ShiitakeError>;
  /** Timer wake state across running extensions. */
  timerWake: (
    now: number,
  ) => Readonly<{ due: boolean; nextWakeAt: number | null }>;
  /** Projection slots for one Session; call inside the snapshot transaction. */
  project: (sessionId: string) => Readonly<Record<string, ExtensionSlotV0>>;
  /** Run one command in its own transaction and return its encoded output. */
  call: (
    target: Readonly<
      { scope: "session"; sessionId: string } | { scope: "agent" }
    >,
    extensionId: string,
    command: string,
    input: Json,
  ) => Effect.Effect<Json, ShiitakeError>;
  list: () => readonly ExtensionStatusV0[];
  resume: (id: string) => Effect.Effect<ExtensionStatusV0, ShiitakeError>;
  close: Effect.Effect<void, ShiitakeError>;
}>;

function boundReason(reason: string): string {
  return reason.length <= maximumReasonLength
    ? reason
    : `${reason.slice(0, maximumReasonLength - 1)}…`;
}

const decodeStoredJson = Schema.decodeSync(
  Schema.fromJsonString(JsonValueSchema),
);

/** Typed failures an extension body may return. */
type ExtensionFailure =
  ExtensionStop | ExtensionStateError | AdmissionError | ExtensionCommandError;

function failureText(value: ExtensionFailure): string {
  return value instanceof ExtensionStop
    ? value.reason
    : `${value._tag}: ${value.message}`;
}

function isExtensionFailure<E>(value: E): value is E & ExtensionFailure {
  return (
    value instanceof ExtensionStop ||
    value instanceof ExtensionStateError ||
    value instanceof AdmissionError ||
    value instanceof ExtensionCommandError
  );
}

/** A bounded, caller-safe reason for one stopping cause. */
function stopReason<E>(hook: string, cause: Cause.Cause<E>): string {
  const failure = Cause.findErrorOption(cause);
  if (Option.isSome(failure)) {
    const value = failure.value;
    if (value instanceof ExtensionStop) return boundReason(value.reason);
    return boundReason(
      isExtensionFailure(value)
        ? `${hook}: ${failureText(value)}`
        : `${hook} failed`,
    );
  }
  const defect = Result.getSuccess(Cause.findDefect(cause));
  if (Option.isSome(defect)) {
    if (defect.value instanceof Error)
      return boundReason(
        defect.value.name === "AsyncFiberError"
          ? `${hook} crossed an asynchronous boundary inside a core transaction`
          : `${hook} defect: ${defect.value.message}`,
      );
    return boundReason(`${hook} defect`);
  }
  return boundReason(`${hook} was interrupted`);
}

function shouldStop(cause: Cause.Cause<unknown>): boolean {
  const failure = Cause.findErrorOption(cause);
  return Option.isNone(failure) || failure.value instanceof ExtensionStop;
}

function stateError(
  reason: ExtensionStateError["reason"],
  message: string,
): ExtensionStateError {
  return new ExtensionStateError({ reason, message });
}

function boundedKey(
  key: string,
  what: string,
): Effect.Effect<string, ExtensionStateError> {
  return key.length >= 1 && key.length <= maximumKeyLength
    ? Effect.succeed(key)
    : Effect.fail(stateError("bound", `${what} must be 1..256 characters`));
}

/** Typed failures a command handler may return to its caller. */
type CommandFailure =
  ExtensionCommandError | ExtensionStateError | AdmissionError;

/** Map one typed extension command failure to Shiitake's public error. */
function commandFailure(failure: CommandFailure): ShiitakeError {
  const message = boundReason(failure.message);
  if (failure instanceof ExtensionCommandError)
    switch (failure.reason) {
      case "invalid":
        return new ShiitakeError("contract", "invalid", message);
      case "conflict":
        return new ShiitakeError("store", "conflict", message);
      case "not_found":
        return new ShiitakeError("store", "rejected", message);
      case "rejected":
        return new ShiitakeError("extensions", "rejected", message);
    }
  if (failure instanceof AdmissionError)
    switch (failure.reason) {
      case "invalid":
        return new ShiitakeError("contract", "invalid", message);
      case "conflict":
        return new ShiitakeError("contract", "conflict", message);
      case "closed":
        return new ShiitakeError("runtime", "closed", message);
    }
  return failure.reason === "bound"
    ? new ShiitakeError("contract", "invalid", message)
    : new ShiitakeError("store", "unavailable", message);
}

/** Activate the admitted roster and bind it to core's doors and tables. */
export function openExtensionRuntime(
  input: Readonly<{
    owner: SqliteOwner;
    door: AdmissionDoor;
    readQueueHead: (sessionId: string) => WatchdogQueueHead;
    admitted: readonly AdmittedExtensionV0[];
    requestWake: Effect.Effect<void, ShiitakeError>;
  }>,
): Effect.Effect<ExtensionRuntime, ShiitakeError> {
  const { owner } = input;
  const frames = new WeakMap<ReadService, TransactionFrame>();
  const members = new Map<string, Member>();
  const incompatibleMigrations = new Set<ExtensionId>();
  let projecting: ExtensionId | undefined;
  const updating = new Set<ExtensionId>();
  const transacting = new Set<ExtensionId>();
  let documentFrame: TransactionFrame | undefined;

  const readServiceFor = (id: ExtensionId): ReadService =>
    Object.freeze({ extensionId: id });

  const requireRead = (id: ExtensionId) =>
    Effect.flatMap(ExtensionRead, (service) =>
      service.extensionId === id
        ? Effect.void
        : Effect.die(
            new Error("Extension read used another extension's context"),
          ),
    );

  const requireFrame = (id: ExtensionId) =>
    Effect.flatMap(ExtensionTx, (service) => {
      const frame = frames.get(service);
      return frame?.extensionId === id
        ? Effect.succeed(frame)
        : Effect.die(
            new Error("Extension write used outside its own core transaction"),
          );
    });

  const storeStep = <A>(run: () => A): Effect.Effect<A, ExtensionStateError> =>
    Effect.try({
      try: run,
      catch: () => stateError("unavailable", "Extension state is unavailable"),
    });

  const grantGuard = (
    member: Member,
    capability: ExtensionCapabilityV0,
  ): Effect.Effect<void, ExtensionStateError> =>
    member.admitted.grants.has(capability)
      ? Effect.void
      : Effect.fail(
          stateError("unavailable", `Extension is not granted ${capability}`),
        );

  function collectionHandle<A, I>(
    member: Member,
    name: string,
    spec: CollectionSpecV0<A, I>,
  ): CollectionV0<A> {
    const id = member.admitted.id;
    const decode = Schema.decodeResult(Schema.fromJsonString(spec.schema));
    const encode = Schema.encodeResult(spec.schema);
    const decodeJson = (
      json: string,
    ): Effect.Effect<A, ExtensionStateError> => {
      const decoded = decode(json);
      return decoded._tag === "Failure"
        ? Effect.fail(stateError("decode", `Stored ${name} record is invalid`))
        : Effect.succeed(decoded.success);
    };
    return Object.freeze({
      peek: (key: string) => {
        if (projecting !== id || !member.admitted.grants.has("state"))
          throw new Error("Extension read used outside its own projection");
        const json = readRecord(owner, id, name, key);
        if (json === undefined) return Option.none();
        const decoded = decode(json);
        if (decoded._tag === "Failure")
          throw stateError("decode", `Stored ${name} record is invalid`);
        return Option.some(decoded.success);
      },
      peekPage: (query) => {
        if (projecting !== id || !member.admitted.grants.has("state"))
          throw new Error("Extension read used outside its own projection");
        if (
          !Number.isSafeInteger(query.limit) ||
          query.limit < 1 ||
          query.limit > maximumPage
        )
          throw stateError("bound", "Page limit must be 1..64");
        return readRecordPage(owner, id, name, query).map((json) => {
          const decoded = decode(json);
          if (decoded._tag === "Failure")
            throw stateError("decode", `Stored ${name} record is invalid`);
          return decoded.success;
        });
      },
      get: (key: string) =>
        requireRead(id).pipe(
          Effect.andThen(grantGuard(member, "state")),
          Effect.andThen(storeStep(() => readRecord(owner, id, name, key))),
          Effect.flatMap((json) =>
            json === undefined
              ? Effect.succeed(Option.none())
              : decodeJson(json).pipe(Effect.map(Option.some)),
          ),
        ),
      page: (query) =>
        requireRead(id).pipe(
          Effect.andThen(grantGuard(member, "state")),
          Effect.andThen(
            Number.isSafeInteger(query.limit) &&
              query.limit >= 1 &&
              query.limit <= maximumPage
              ? Effect.void
              : Effect.fail(stateError("bound", "Page limit must be 1..64")),
          ),
          Effect.andThen(
            storeStep(() => readRecordPage(owner, id, name, query)),
          ),
          Effect.flatMap((rows) => Effect.forEach(rows, decodeJson)),
        ),
      put: (value: A) =>
        requireFrame(id).pipe(
          Effect.andThen(grantGuard(member, "state")),
          Effect.andThen(
            Effect.suspend(() => {
              const encoded = encode(value);
              if (encoded._tag === "Failure")
                return Effect.fail(
                  stateError(
                    "encode",
                    `The ${name} record does not match its schema`,
                  ),
                );
              const json = JSON.stringify(encoded.success);
              if (encoder.encode(json).byteLength > maximumRecordBytes)
                return Effect.fail(
                  stateError("bound", `The ${name} record exceeds 1 MiB`),
                );
              const index = spec.index?.(value) ?? null;
              return Effect.all([
                boundedKey(spec.key(value), "A record key"),
                index === null
                  ? Effect.succeed(null)
                  : boundedKey(index, "A record index"),
              ]).pipe(
                Effect.flatMap(([key, boundedIndex]) =>
                  storeStep(() => {
                    writeRecord(owner, id, name, key, boundedIndex, json);
                  }),
                ),
              );
            }),
          ),
        ),
      remove: (key: string) =>
        requireFrame(id).pipe(
          Effect.andThen(grantGuard(member, "state")),
          Effect.andThen(
            storeStep(() => {
              deleteRecord(owner, id, name, key);
            }),
          ),
        ),
    });
  }

  /** Synchronous collection operations exist only inside state.update. */
  function updateCollection<A, I>(
    member: Member,
    name: string,
    spec: CollectionSpecV0<A, I>,
  ): UpdateCollectionV0<A> {
    const id = member.admitted.id;
    const decode = Schema.decodeResult(Schema.fromJsonString(spec.schema));
    const encode = Schema.encodeResult(spec.schema);
    const guard = () => {
      if (!updating.has(id) || !member.admitted.grants.has("state"))
        throw stateError(
          "unavailable",
          "Extension state update is unavailable",
        );
    };
    const key = (value: string) => {
      if (value.length < 1 || value.length > maximumKeyLength)
        throw stateError("bound", "A record key must be 1..256 characters");
      return value;
    };
    const decoded = (json: string): A => {
      const result = decode(json);
      if (result._tag === "Failure")
        throw stateError("decode", `Stored ${name} record is invalid`);
      return result.success;
    };
    return Object.freeze({
      get: (recordKey: string) => {
        guard();
        const json = readRecord(owner, id, name, key(recordKey));
        return json === undefined ? Option.none() : Option.some(decoded(json));
      },
      page: (query) => {
        guard();
        if (
          !Number.isSafeInteger(query.limit) ||
          query.limit < 1 ||
          query.limit > maximumPage
        )
          throw stateError("bound", "Page limit must be 1..64");
        return readRecordPage(owner, id, name, query).map(decoded);
      },
      put: (value: A) => {
        guard();
        const result = encode(value);
        if (result._tag === "Failure")
          throw stateError(
            "encode",
            `The ${name} record does not match its schema`,
          );
        const json = JSON.stringify(result.success);
        if (encoder.encode(json).byteLength > maximumRecordBytes)
          throw stateError("bound", `The ${name} record exceeds 1 MiB`);
        const index = spec.index?.(value) ?? null;
        if (index !== null) key(index);
        writeRecord(owner, id, name, key(spec.key(value)), index, json);
      },
      remove: (recordKey: string) => {
        guard();
        deleteRecord(owner, id, name, key(recordKey));
      },
    });
  }

  function timersFor(member: Member): TimersV0 {
    const id = member.admitted.id;
    return Object.freeze({
      set: (key: string, dueAt: number) =>
        requireFrame(id).pipe(
          Effect.tap(() => grantGuard(member, "timers")),
          Effect.tap(() => boundedKey(key, "A timer key")),
          Effect.tap(() =>
            Number.isSafeInteger(dueAt) && dueAt >= 0
              ? Effect.void
              : Effect.fail(stateError("bound", "A timer instant is invalid")),
          ),
          Effect.flatMap((frame) =>
            storeStep(() => {
              writeTimer(owner, id, key, dueAt);
              frame.timersChanged = true;
            }),
          ),
        ),
      cancel: (key: string) =>
        requireFrame(id).pipe(
          Effect.tap(() => grantGuard(member, "timers")),
          Effect.flatMap((frame) =>
            storeStep(() => {
              deleteTimer(owner, id, key);
              frame.timersChanged = true;
            }),
          ),
        ),
      read: (key: string) =>
        requireRead(id).pipe(
          Effect.andThen(grantGuard(member, "timers")),
          Effect.andThen(storeStep(() => readTimer(owner, id, key))),
          Effect.map(Option.fromNullishOr),
        ),
    });
  }

  function attentionFor(member: Member): AttentionV0 {
    const id = member.admitted.id;
    const mutate = (sessionId: string, write: () => void) =>
      requireFrame(id).pipe(
        Effect.tap(() => grantGuard(member, "attention")),
        Effect.flatMap((frame) =>
          Effect.try({
            try: () => {
              decodeClientSessionId(sessionId);
              write();
              projectSessionAttention(
                owner,
                sessionId,
                input.readQueueHead(sessionId),
              );
              frame.observed.add(sessionId);
            },
            catch: (error) =>
              error instanceof ExtensionStateError &&
              error.reason === "session_absent"
                ? error
                : error instanceof ShiitakeError && error.cause === "invalid"
                  ? stateError("bound", error.message)
                  : stateError(
                      "unavailable",
                      "Attention update is unavailable",
                    ),
          }),
        ),
      );
    return {
      set: (sessionId, item) => {
        const decoded = Schema.decodeUnknownResult(AttentionItemSchema, {
          onExcessProperty: "error",
        })(item);
        return decoded._tag === "Failure"
          ? Effect.fail(stateError("bound", "Attention item is invalid"))
          : mutate(sessionId, () => {
              if (!hasStoredSession(owner, sessionId))
                throw stateError(
                  "session_absent",
                  "Attention Session is absent",
                );
              setSessionAttention(owner, sessionId, id, decoded.success);
            });
      },
      clear: (sessionId, itemId) =>
        boundedKey(itemId, "An attention id").pipe(
          Effect.andThen(
            mutate(sessionId, () => {
              clearSessionAttention(owner, sessionId, id, itemId);
            }),
          ),
        ),
    };
  }

  function sendFor(member: Member) {
    const id = member.admitted.id;
    return (
      request: SendInputV0,
    ): Effect.Effect<SendResultV0, AdmissionError, ExtensionTx> =>
      requireFrame(id).pipe(
        Effect.flatMap((frame) =>
          member.admitted.grants.has("send")
            ? Effect.try({
                try: (): SendResultV0 => {
                  const { requireExistingSession = false } = request;
                  const result = frame.admit({
                    sessionId: request.sessionId,
                    requireExistingSession,
                    command: {
                      type: "prompt",
                      requestId: request.prompt.requestId,
                      delivery: request.prompt.delivery,
                      text: request.prompt.text,
                    },
                    when: request.when,
                    origin: {
                      kind: "extension",
                      extensionId: id,
                      correlation: request.correlation ?? null,
                    },
                  });
                  return result.status === "accepted"
                    ? {
                        _tag: "Accepted",
                        commandId: result.commandId,
                        delivery: result.delivery,
                      }
                    : { _tag: "NotReady" };
                },
                catch: (error) =>
                  new AdmissionError({
                    reason:
                      error instanceof ShiitakeError
                        ? error.cause === "conflict"
                          ? "conflict"
                          : error.cause === "closed"
                            ? "closed"
                            : "invalid"
                        : "invalid",
                    message:
                      error instanceof ShiitakeError
                        ? error.message
                        : "Session command is invalid",
                  }),
              })
            : Effect.fail(
                new AdmissionError({
                  reason: "closed",
                  message: "Extension is not granted send",
                }),
              ),
        ),
      );
  }

  /**
   * Run one extension body in its own owner transaction. A failed body rolls
   * back only this transaction; its Exit is returned for the caller's policy.
   */
  function runTransaction<A, E>(
    member: Member,
    body: Effect.Effect<A, E, ExtensionTx | ExtensionRead>,
    boundary: AdmissionBoundary,
  ): Effect.Effect<Exit.Exit<A, E>, ShiitakeError> {
    return Effect.suspend(() => {
      let bodyExit: Exit.Exit<A, E> | undefined;
      let frame: TransactionFrame | undefined;
      const rollback = new Error("Extension transaction rolled back");
      return input.door
        .transact((admit) => {
          const service = readServiceFor(member.admitted.id);
          frame = {
            extensionId: member.admitted.id,
            admit,
            observed: new Set(),
            timersChanged: false,
          };
          frames.set(service, frame);
          transacting.add(member.admitted.id);
          const previousDocumentFrame = documentFrame;
          documentFrame = frame;
          try {
            bodyExit = Effect.runSyncExit(
              body.pipe(
                Effect.provideService(ExtensionTx, service),
                Effect.provideService(ExtensionRead, service),
              ),
            );
          } finally {
            documentFrame = previousDocumentFrame;
            frames.delete(service);
            transacting.delete(member.admitted.id);
          }
          if (Exit.isFailure(bodyExit)) throw rollback;
          return bodyExit.value;
        }, boundary)
        .pipe(
          Effect.map((): Exit.Exit<A, E> => {
            if (bodyExit === undefined)
              throw new Error("Extension transaction returned no exit");
            return bodyExit;
          }),
          Effect.catch((error) =>
            bodyExit !== undefined && Exit.isFailure(bodyExit)
              ? Effect.succeed(bodyExit)
              : Effect.fail(error),
          ),
          Effect.tap((exit) =>
            Exit.isSuccess(exit) && frame !== undefined
              ? Effect.gen(function* () {
                  for (const sessionId of frame?.observed ?? [])
                    publishOwnerObservation(owner, sessionId);
                  if (boundary === "request" && frame?.timersChanged === true)
                    yield* input.requestWake;
                })
              : Effect.void,
          ),
        );
    });
  }

  /** Refresh at most the extension's outstanding resource ceiling, in bounded pages. */
  function refreshAttentionOwner(extensionId: string): void {
    let after = "";
    for (
      let count = 0;
      count < maximumExtensionAttention;
      count += maximumPage
    ) {
      const sessions = readAttentionSessionPage(owner, extensionId, after);
      for (const sessionId of sessions) {
        projectSessionAttention(
          owner,
          sessionId,
          input.readQueueHead(sessionId),
        );
        // Only a latency hint: readers re-read after the synchronous transaction.
        queueMicrotask(() => {
          publishOwnerObservation(owner, sessionId);
        });
      }
      const last = sessions.at(-1);
      if (sessions.length < maximumPage || last === undefined) return;
      after = last;
    }
  }

  /**
   * Record a stop and log the full cause. The in-memory stop applies
   * immediately either way, so the extension stays isolated until the next
   * open even when nothing durable landed. A failed durable write is never
   * swallowed: it fails this Effect with a `store`/`unavailable`
   * `ShiitakeError` so the operation that triggered the stop learns the
   * record did not actually land.
   */
  function recordStop(
    member: Member,
    reason: string,
    cause: Cause.Cause<unknown> | undefined,
    within: "own-transaction" | "enclosing-transaction",
  ): Effect.Effect<void, ShiitakeError> {
    const stoppedAt = Date.now();
    member.status = "stopped";
    member.reason = reason;
    member.stoppedAt = stoppedAt;
    const write = Effect.try({
      try: () => {
        const run = () => {
          writeStatus(owner, member.admitted.id, {
            status: "stopped",
            reason,
            stoppedAt,
            revision: member.admitted.revision,
          });
          refreshAttentionOwner(member.admitted.id);
        };
        if (within === "own-transaction") owner.transactionSync(run);
        else run();
      },
      catch: (error) =>
        new ShiitakeError(
          "store",
          "unavailable",
          `Extension ${member.admitted.id} stop status is unavailable`,
          undefined,
          error,
        ),
    });
    return Effect.gen(function* () {
      const exit = yield* Effect.exit(write);
      yield* Effect.logError("shiitake_extension_stopped").pipe(
        Effect.annotateLogs({
          extensionId: member.admitted.id,
          reason,
          cause: cause === undefined ? null : Redacted.make(cause),
          durable: Exit.isSuccess(exit),
        }),
      );
      return yield* exit;
    });
  }

  /** Persist a synchronous stop, in or outside an enclosing owner transaction. */
  function recordStopSynchronously(
    member: Member,
    reason: string,
    within: "own-transaction" | "enclosing-transaction",
  ): void {
    member.status = "stopped";
    member.reason = reason;
    member.stoppedAt = Date.now();
    const run = () => {
      writeStatus(owner, member.admitted.id, {
        status: "stopped",
        reason,
        stoppedAt: member.stoppedAt,
        revision: member.admitted.revision,
      });
      refreshAttentionOwner(member.admitted.id);
    };
    try {
      if (within === "own-transaction") owner.transactionSync(run);
      else run();
    } catch (error) {
      throw new ShiitakeError(
        "store",
        "unavailable",
        `Extension ${member.admitted.id} stop status is unavailable`,
        undefined,
        error,
      );
    }
  }

  function documentHandle<A, I>(
    member: Member,
    name: string,
    spec: DocumentSpecV0<A, I>,
  ): DocumentHandleV0<A> {
    const check = (sessionId: string) => {
      if (
        documentFrame?.extensionId !== member.admitted.id ||
        !member.admitted.grants.has("state")
      )
        throw stateError(
          "unavailable",
          "Session documents require the extension's state transaction",
        );
      decodeClientSessionId(sessionId);
      return documentFrame;
    };
    const read = (sessionId: string) => {
      check(sessionId);
      const json =
        readSessionDocument(owner, sessionId, member.admitted.id, name) ??
        encodeDocument(spec, spec.initial);
      if (encoder.encode(json).byteLength > spec.maxBytes)
        throw stateError("bound", "Session document exceeds maxBytes");
      const decoded = Schema.decodeUnknownResult(
        Schema.fromJsonString(spec.schema),
      )(json);
      if (decoded._tag === "Failure")
        throw stateError(
          "decode",
          "Stored session document does not match its schema",
        );
      const value: A = decoded.success;
      return value;
    };
    const handle: DocumentHandleV0<A> = {
      read,
      update: (sessionId, change) => {
        const frame = check(sessionId);
        const value = change(read(sessionId));
        writeSessionDocument(
          owner,
          sessionId,
          member.admitted.id,
          name,
          encodeDocument(spec, value),
        );
        frame.observed.add(sessionId);
      },
    };
    return Object.freeze(handle);
  }

  function contextFor(member: Member): ExtensionRuntimePortV0 {
    const observed = new Set<string>();
    const update = <A>(body: () => A): A => {
      const id = member.admitted.id;
      if (
        member.status !== "running" ||
        !member.admitted.grants.has("state") ||
        updating.has(id) ||
        transacting.has(id) ||
        projecting === id
      )
        throw stateError(
          "unavailable",
          "Extension state update is unavailable",
        );
      try {
        const result = owner.transactionSync(() => {
          updating.add(id);
          try {
            return invokeSyncHandler(body, undefined);
          } finally {
            updating.delete(id);
          }
        });
        for (const sessionId of observed)
          publishOwnerObservation(owner, sessionId);
        return result;
      } catch (error) {
        if (!(error instanceof AsyncExtensionTransactionError)) throw error;
        const reason = error.message;
        recordStopSynchronously(member, reason, "own-transaction");
        throw new ExtensionStop({ reason });
      } finally {
        observed.clear();
      }
    };
    return Object.freeze({
      collection: <A, I>(name: string, spec: CollectionSpecV0<A, I>) =>
        collectionHandle(member, name, spec),
      updateCollection: <A, I>(name: string, spec: CollectionSpecV0<A, I>) =>
        updateCollection(member, name, spec),
      document: <A, I>(name: string, spec: DocumentSpecV0<A, I>) =>
        documentHandle(member, name, spec),
      update,
      observeUpdate: (sessionId: string) => {
        if (!updating.has(member.admitted.id))
          throw stateError(
            "unavailable",
            "Extension observation is unavailable",
          );
        observed.add(decodeClientSessionId(sessionId));
      },
      context: Object.freeze({
        manifest: member.admitted.manifest,
        revision: member.admitted.revision,
        grants: Object.freeze([...member.admitted.grants]),
        send: sendFor(member),
        timers: timersFor(member),
        attention: attentionFor(member),
        sessionPosition: (sessionId: string) =>
          requireRead(member.admitted.id).pipe(
            Effect.andThen(grantGuard(member, "state")),
            Effect.andThen(
              storeStep(() => {
                const id = decodeClientSessionId(sessionId);
                if (!hasStoredSession(owner, id))
                  throw stateError(
                    "unavailable",
                    "Session timeline is unavailable",
                  );
                return (
                  readSessionLineage(owner, { sessionId: id })
                    ?.newestSequence ?? 0
                );
              }),
            ),
          ),
        hasPendingSteer: (run: Pick<RunIdentityV0, "sessionId" | "workId">) =>
          requireRead(member.admitted.id).pipe(
            Effect.andThen(grantGuard(member, "run_tools")),
            Effect.andThen(
              storeStep(() =>
                hasPendingSessionSteer(
                  owner,
                  decodeClientSessionId(run.sessionId),
                  run.workId,
                ),
              ),
            ),
          ),
        hasSuccessfulToolResult: (
          run: Pick<RunIdentityV0, "sessionId" | "workId">,
          toolCallId: string,
        ) =>
          requireRead(member.admitted.id).pipe(
            Effect.andThen(grantGuard(member, "run_tools")),
            Effect.andThen(
              storeStep(() =>
                hasSuccessfulToolResult(
                  owner,
                  decodeClientSessionId(run.sessionId),
                  run.workId,
                  toolCallId,
                ),
              ),
            ),
          ),
        observe: (sessionId: string) =>
          requireFrame(member.admitted.id).pipe(
            Effect.map((frame) => {
              frame.observed.add(sessionId);
            }),
          ),
        transact: <A, E>(
          body: Effect.Effect<A, E, ExtensionTx | ExtensionRead>,
        ): Effect.Effect<A, E | ExtensionStop | ShiitakeError> => {
          const run: Effect.Effect<
            Exit.Exit<A, E>,
            ExtensionStop
          > = member.status === "running"
            ? runTransaction(member, body, "request").pipe(Effect.orDie)
            : Effect.fail(
                new ExtensionStop({
                  reason: member.reason ?? "Extension is stopped",
                }),
              );
          return run.pipe(
            Effect.flatMap(
              (exit): Effect.Effect<A, E | ExtensionStop | ShiitakeError> => {
                if (Exit.isSuccess(exit)) return Effect.succeed(exit.value);
                if (!shouldStop(exit.cause))
                  return Effect.failCause(exit.cause);
                const reason = stopReason("transact", exit.cause);
                return recordStop(
                  member,
                  reason,
                  exit.cause,
                  "own-transaction",
                ).pipe(
                  Effect.flatMap(() =>
                    Effect.fail(new ExtensionStop({ reason })),
                  ),
                );
              },
            ),
          );
        },
      }),
    });
  }

  /** Pure per-version migrations; prepare owns their transaction with declarations. */
  function migrate(member: Member): void {
    const { id, definition, revision } = member.admitted;
    const declared = definition.state.version;
    const ledger = readStateLedger(owner, id);
    if (ledger !== undefined && ledger.version > declared)
      throw new ShiitakeError(
        "extensions",
        "conflict",
        `Stored ${id} state version ${String(ledger.version)} is newer than ${String(declared)}`,
      );
    if (ledger !== undefined && ledger.version < declared) {
      let rows: readonly StoredRecordV0[] = readAllRecords(owner, id).map(
        (row): StoredRecordV0 => ({
          collection: row.collection,
          key: row.key,
          value: decodeStoredJson(row.valueJson),
        }),
      );
      for (let version = ledger.version; version < declared; version += 1) {
        const step = definition.state.migrations[version];
        if (step === undefined)
          throw new ShiitakeError(
            "extensions",
            "state_reset_required",
            `State reset required for ${id}: stored version ${String(ledger.version)}, declared version ${String(declared)}. An operator must authorize the reset; stored records have not been changed.`,
          );
        try {
          rows = rows.flatMap((row) => step(row));
        } catch (cause) {
          throw new IncompatibleStateMigrationError(
            `Failed ${id} state migration from ${String(version)}`,
            { cause },
          );
        }
      }
      deleteAllRecords(owner, id);
      for (const row of rows) {
        const declaredSpec = definition.state.collections[row.collection];
        if (declaredSpec === undefined)
          throw new IncompatibleStateMigrationError(
            `Migrated ${id} record names an undeclared collection`,
          );
        const decoded = declaredSpec.indexFromEncoded(row.value);
        if (decoded._tag === "Failure")
          throw new IncompatibleStateMigrationError(
            `Migrated ${id} ${row.collection} record is invalid`,
          );
        writeRecord(
          owner,
          id,
          row.collection,
          row.key,
          decoded.success,
          JSON.stringify(row.value),
        );
      }
    }
    writeStateLedger(owner, id, declared, revision);
  }

  function activateMember(
    member: Member,
    scope: Scope.Closeable,
    strict = false,
  ): Effect.Effect<void, ShiitakeError> {
    const context = contextFor(member);
    return Effect.gen(function* () {
      // Each activation owns its resources. Closing a failed candidate must
      // not wait for Agent shutdown or release another member's resources.
      const activationScope = yield* Scope.fork(scope);
      member.scope = activationScope;
      const exit = yield* Effect.suspend(() =>
        member.admitted.definition.activate(context),
      ).pipe(Effect.provideService(Scope.Scope, activationScope), Effect.exit);
      if (Exit.isSuccess(exit)) {
        member.hooks = exit.value;
        return;
      }
      yield* Scope.close(activationScope, exit);
      if (strict)
        return yield* Effect.fail(
          new ShiitakeError(
            "extensions",
            "rejected",
            `Extension ${member.admitted.id} failed to start: ${Cause.pretty(exit.cause)}`,
          ),
        );
      yield* recordStop(
        member,
        stopReason("activation", exit.cause),
        exit.cause,
        "own-transaction",
      );
    });
  }

  const running = (capability: ExtensionCapabilityV0) =>
    [...members.values()].filter(
      (member) =>
        member.status === "running" &&
        member.hooks !== undefined &&
        member.admitted.grants.has(capability),
    );

  function statusOf(member: Member): ExtensionStatusV0 {
    return Object.freeze({
      id: member.admitted.id,
      version: member.admitted.manifest.version,
      revision: member.admitted.revision,
      status: member.status,
      reason: member.reason,
      stoppedAt: member.stoppedAt,
    });
  }

  function isRunParticipant(member: Member): boolean {
    return (
      member.status === "running" &&
      member.hooks !== undefined &&
      (member.admitted.grants.has("run_tools") ||
        member.admitted.grants.has("files") ||
        member.admitted.grants.has("prompt") ||
        member.admitted.grants.has("compaction_handoff"))
    );
  }

  /** Run a read-only network or tool hook outside the write transaction. */
  function runRead<A>(
    member: Member,
    hook: string,
    effect: Effect.Effect<A, unknown, ExtensionRead>,
  ): Effect.Effect<Option.Option<A>, ShiitakeError> {
    return Effect.exit(
      effect.pipe(
        Effect.provideService(
          ExtensionRead,
          readServiceFor(member.admitted.id),
        ),
      ),
    ).pipe(
      Effect.flatMap((exit) =>
        Exit.isSuccess(exit)
          ? Effect.succeed(Option.some(exit.value))
          : recordStop(
              member,
              stopReason(hook, exit.cause),
              exit.cause,
              "own-transaction",
            ).pipe(Effect.as(Option.none())),
      ),
    );
  }

  const stopFromExit = <A, E>(
    member: Member,
    hook: string,
    exit: Exit.Exit<A, E>,
  ): Effect.Effect<void, ShiitakeError> =>
    Exit.isFailure(exit)
      ? recordStop(
          member,
          stopReason(hook, exit.cause),
          exit.cause,
          "own-transaction",
        )
      : Effect.void;

  /**
   * Run pending state migrations. A stored version newer than the declared one
   * fails construction. A missing path isolates this extension with its rows
   * retained; a failed declared migration also guards runs as incompatible.
   */
  function prepare(member: Member): Effect.Effect<boolean, ShiitakeError> {
    return Effect.try({
      try: () => {
        owner.transactionSync(() => {
          migrate(member);
          for (const [name, spec] of Object.entries(member.admitted.documents))
            declareSessionDocument(owner, {
              extensionId: member.admitted.id,
              name,
              initialJson: encodeDocument(spec, spec.initial),
              fork: spec.fork,
              maxBytes: spec.maxBytes,
            });
        });
      },
      catch: (error) => error,
    }).pipe(
      Effect.tap(() =>
        Effect.sync(() => {
          delete member.stateResetRequired;
        }),
      ),
      Effect.as(true),
      Effect.catch((error) => {
        if (error instanceof ShiitakeError) {
          if (error.cause !== "state_reset_required") return Effect.fail(error);
          member.stateResetRequired = error;
          return recordStop(
            member,
            boundReason(error.message),
            Cause.fail(error),
            "own-transaction",
          ).pipe(Effect.as(false));
        }
        if (
          error instanceof IncompatibleStateMigrationError &&
          (member.admitted.grants.has("run_tools") ||
            member.admitted.grants.has("files") ||
            member.admitted.grants.has("prompt") ||
            member.admitted.grants.has("compaction_handoff"))
        )
          incompatibleMigrations.add(member.admitted.id);
        return recordStop(
          member,
          boundReason(
            error instanceof Error
              ? `migration: ${error.message}`
              : "migration failed",
          ),
          Cause.die(error),
          "own-transaction",
        ).pipe(Effect.as(false));
      }),
    );
  }

  return Effect.gen(function* () {
    const scope = yield* Scope.make();
    function startMember(admitted: AdmittedExtensionV0, strict: boolean) {
      return Effect.gen(function* () {
        const stored = readStatus(owner, admitted.id);
        const keepStopped =
          stored?.status === "stopped" && stored.revision === admitted.revision;
        const member: Member = {
          admitted,
          hooks: undefined,
          status: keepStopped ? "stopped" : "running",
          reason: keepStopped ? stored.reason : null,
          stoppedAt: keepStopped ? stored.stoppedAt : null,
        };
        // Every settled consumer has a cursor from its first roster on, so a
        // later resume replays only what it missed.
        yield* Effect.try({
          try: () => {
            if (admitted.grants.has("settled"))
              owner.transactionSync(() => {
                if (readCursor(owner, admitted.id) === undefined)
                  advanceCursor(
                    owner,
                    admitted.id,
                    readCursor(owner, coreConsumer) ?? 0,
                  );
              });
          },
          catch: () =>
            new ShiitakeError(
              "store",
              "unavailable",
              "Settled cursor is unavailable",
            ),
        });
        const prepared = yield* prepare(member);
        if (!prepared || keepStopped) {
          if (strict)
            return yield* Effect.fail(
              new ShiitakeError(
                "extensions",
                "rejected",
                `Extension ${admitted.id} cannot start: ${member.reason ?? "stopped"}`,
              ),
            );
          return member;
        }
        yield* Effect.try({
          try: () => {
            owner.transactionSync(() => {
              writeStatus(owner, admitted.id, {
                status: "running",
                reason: null,
                stoppedAt: null,
                revision: admitted.revision,
              });
              refreshAttentionOwner(admitted.id);
            });
          },
          catch: () =>
            new ShiitakeError(
              "store",
              "unavailable",
              "Extension status is unavailable",
            ),
        });
        yield* activateMember(member, scope, strict);
        return member;
      });
    }
    for (const admitted of input.admitted) {
      const member = yield* startMember(admitted, false).pipe(
        Effect.onError(() => Scope.close(scope, Exit.void)),
      );
      members.set(admitted.id, member);
    }

    yield* Effect.try({
      try: () => {
        owner.transactionSync(() => {
          const changed = replaceAttentionGrants(
            owner,
            input.admitted
              .filter((member) => member.grants.has("attention"))
              .map((member) => member.id),
          );
          for (const id of changed) refreshAttentionOwner(id);
        });
      },
      catch: () =>
        new ShiitakeError(
          "store",
          "unavailable",
          "Attention grants are unavailable",
        ),
    });

    const runtime: ExtensionRuntime = {
      stage: (admitted) =>
        Effect.gen(function* () {
          const changed = admitted.filter((next) => {
            const previous = members.get(next.id);
            return (
              previous !== undefined &&
              previous.admitted.fingerprint !== next.fingerprint
            );
          });
          if (changed.length > 0)
            return yield* Effect.fail(
              new ShiitakeError(
                "extensions",
                "rejected",
                `Extensions ${changed.map(({ id }) => id).join(", ")}: this change needs a restart: run fungi stop, then reopen fungi`,
              ),
            );
          const additions = new Map<string, Member>();
          const discard = Effect.suspend(() =>
            Effect.forEach(
              [...additions.values()].reverse(),
              (member) =>
                Effect.exit(
                  member.scope === undefined
                    ? Effect.void
                    : Scope.close(member.scope, Exit.void),
                ),
              { concurrency: 1 },
            ).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  for (const next of admitted)
                    if (!members.has(next.id))
                      incompatibleMigrations.delete(next.id);
                }),
              ),
              Effect.flatMap(Exit.asVoidAll),
            ),
          );
          yield* Effect.gen(function* () {
            for (const next of admitted)
              if (!members.has(next.id))
                additions.set(next.id, yield* startMember(next, true));
          }).pipe(Effect.onError(() => discard));
          const target = new Set(admitted.map(({ id }) => id));
          let settled = false;
          return {
            discard: Effect.suspend(() => {
              if (settled) return Effect.void;
              settled = true;
              return discard;
            }),
            apply: Effect.gen(function* () {
              if (settled)
                return yield* Effect.fail(
                  new ShiitakeError(
                    "extensions",
                    "rejected",
                    "Roster transition already settled",
                  ),
                );
              // Finish fallible store work atomically before releasing live
              // resources. A rolled-back publication must leave both the old
              // timers and the old member scopes available.
              yield* Effect.try({
                try: () => {
                  owner.transactionSync(() => {
                    for (const [id, member] of members)
                      if (!target.has(member.admitted.id))
                        deleteExtensionTimers(owner, id);
                    const changed = replaceAttentionGrants(
                      owner,
                      admitted
                        .filter((entry) => entry.grants.has("attention"))
                        .map(({ id }) => id),
                    );
                    for (const id of changed) refreshAttentionOwner(id);
                  });
                },
                catch: () =>
                  new ShiitakeError(
                    "store",
                    "unavailable",
                    "Roster publication storage is unavailable",
                  ),
              });
              for (const [id, member] of members)
                if (!target.has(member.admitted.id)) {
                  if (member.scope !== undefined)
                    yield* Scope.close(member.scope, Exit.void);
                  members.delete(id);
                  incompatibleMigrations.delete(member.admitted.id);
                }
              for (const [id, member] of additions) members.set(id, member);
              settled = true;
            }),
          };
        }),
      runParticipants: () =>
        [...members.values()]
          .filter(isRunParticipant)
          .map(
            (member) =>
              [member.admitted.id, member.admitted.fingerprint] as const,
          ),
      incompatibleMigrations: () => [...incompatibleMigrations],
      modulesForRun: (run, captured) =>
        Effect.forEach(running("run_tools"), (member) => {
          const hook = member.hooks?.moduleForRun;
          return !captured.has(member.admitted.id) || hook === undefined
            ? Effect.succeed(Option.none<ModuleRegistration>())
            : runRead(
                member,
                "moduleForRun",
                Effect.suspend(() => hook(run)),
              ).pipe(Effect.map(Option.flatten));
        }).pipe(Effect.map((modules) => modules.flatMap(Option.toArray))),
      filesForRun: (run, captured) =>
        Effect.gen(function* () {
          const member = running("files").find((entry) =>
            captured.has(entry.admitted.id),
          );
          if (member === undefined) return [];
          const hook = member.hooks?.filesForRun;
          if (hook === undefined) {
            yield* recordStop(
              member,
              "files: granted capability has no handler",
              undefined,
              "own-transaction",
            );
            return yield* Effect.fail(
              new ShiitakeError(
                "extensions",
                "rejected",
                "Extension Files are unavailable",
              ),
            );
          }
          const result = yield* runRead(
            member,
            "filesForRun",
            Effect.suspend(() => hook(run)),
          );
          if (Option.isNone(result))
            return yield* Effect.fail(
              new ShiitakeError(
                "extensions",
                "rejected",
                "Extension Files capture failed",
              ),
            );
          return Object.freeze([...Option.getOrElse(result.value, () => [])]);
        }),
      promptSections: (run, captured) =>
        Effect.gen(function* () {
          const sections: PromptSectionV0[] = [];
          let totalBytes = 0;
          for (const member of running("prompt")) {
            if (!captured.has(member.admitted.id)) continue;
            const hook = member.hooks?.prompt;
            if (hook === undefined) {
              yield* recordStop(
                member,
                "prompt: granted capability has no handler",
                undefined,
                "own-transaction",
              );
              return yield* Effect.fail(
                new ShiitakeError(
                  "extensions",
                  "rejected",
                  "Extension prompt is unavailable",
                ),
              );
            }
            const result = yield* Effect.exit(
              invokeExtensionHook(() => hook(run)).pipe(
                Effect.provideService(
                  ExtensionRead,
                  readServiceFor(member.admitted.id),
                ),
              ),
            );
            if (Exit.isFailure(result)) {
              yield* recordStop(
                member,
                stopReason("prompt", result.cause),
                result.cause,
                "own-transaction",
              );
              return yield* Effect.fail(
                new ShiitakeError(
                  "extensions",
                  "rejected",
                  "Extension prompt failed",
                ),
              );
            }
            const decoded = decodePromptSections(result.value);
            const values = decoded._tag === "Success" ? decoded.success : [];
            totalBytes += values.reduce(
              (size, section) => size + encoder.encode(section.text).byteLength,
              0,
            );
            if (
              decoded._tag === "Failure" ||
              new Set(values.map(({ name }) => name)).size !== values.length ||
              totalBytes > 65536
            ) {
              yield* recordStop(
                member,
                "prompt: section is invalid or exceeds its bound",
                undefined,
                "own-transaction",
              );
              return yield* Effect.fail(
                new ShiitakeError(
                  "extensions",
                  "rejected",
                  "Extension prompt is invalid",
                ),
              );
            }
            sections.push(
              ...[...values]
                .sort((left, right) =>
                  left.name < right.name ? -1 : left.name > right.name ? 1 : 0,
                )
                .map((section) => ({
                  name: `${member.admitted.id}.${section.name}`,
                  text: section.text,
                })),
            );
          }
          return sections;
        }),
      compactionHandoff: (captured) =>
        running("compaction_handoff").find(({ admitted }) =>
          captured.has(admitted.id),
        )?.hooks?.compactionHandoff,
      settledConsumers: () =>
        running("settled").map((member) => ({
          id: member.admitted.id,
          cursor: readCursor(owner, member.admitted.id) ?? 0,
        })),
      deliverSettled: (runs, throughSequence) =>
        Effect.forEach(
          running("settled"),
          (member) =>
            Effect.suspend(() => {
              const cursor = readCursor(owner, member.admitted.id) ?? 0;
              if (throughSequence <= cursor) return Effect.void;
              const slice = runs.filter(({ sequence }) => sequence > cursor);
              const hook = member.hooks?.onSettled;
              const body = (
                hook === undefined || slice.length === 0
                  ? Effect.void
                  : hook(slice)
              ).pipe(
                Effect.andThen(
                  Effect.sync(() => {
                    advanceCursor(owner, member.admitted.id, throughSequence);
                  }),
                ),
              );
              return runTransaction(member, body, "reconcile").pipe(
                Effect.flatMap((exit) =>
                  stopFromExit(member, "onSettled", exit),
                ),
              );
            }),
          { discard: true },
        ),
      fireDueTimers: (now) =>
        Effect.gen(function* () {
          const ids = running("timers").map(({ admitted }) => admitted.id);
          const due = readDueTimers(owner, ids, now, maximumPage);
          for (const timer of due) {
            const member = members.get(timer.extensionId);
            if (member?.status !== "running") continue;
            const hook = member.hooks?.onTimer;
            const body = Effect.suspend(() => {
              if (
                readTimer(owner, timer.extensionId, timer.key) !== timer.dueAt
              )
                return Effect.void;
              deleteTimer(owner, timer.extensionId, timer.key);
              return hook === undefined
                ? Effect.void
                : hook({ key: timer.key, dueAt: timer.dueAt, firedAt: now });
            });
            const exit = yield* runTransaction(member, body, "reconcile");
            yield* stopFromExit(member, "onTimer", exit);
          }
          return due.length === maximumPage;
        }),
      timerWake: (now) =>
        readTimerWake(
          owner,
          running("timers").map(({ admitted }) => admitted.id),
          now,
        ),
      project: (sessionId) => {
        const slots: Record<string, ExtensionSlotV0> = {};
        for (const member of members.values()) {
          const projects = member.admitted.grants.has("projection");
          let catalog = member.admitted.grants.has("commands")
            ? member.admitted.definition.catalog
            : undefined;
          if (!projects && catalog === undefined) continue;
          const revision = member.admitted.revision;
          const projection = member.admitted.definition.contract.projection;
          const hook = member.hooks?.project;
          if (member.status === "running" && member.hooks !== undefined) {
            const value =
              !projects || hook === undefined || projection === null
                ? Option.some(null)
                : (() => {
                    let output: Json | null = null;
                    let failure: unknown;
                    projecting = member.admitted.id;
                    try {
                      const projected = invokeSyncHandler(
                        () => hook(sessionId),
                        undefined,
                      );
                      if (Option.isSome(projected)) {
                        const encoded = Schema.encodeResult(projection.schema)(
                          projected.value,
                        );
                        if (encoded._tag === "Failure")
                          throw new Error("Extension projection is invalid");
                        const json = Schema.decodeUnknownResult(
                          JsonValueSchema,
                        )(encoded.success);
                        if (json._tag === "Failure")
                          throw new Error("Extension projection is not JSON");
                        output = json.success;
                        const projectCatalog = member.hooks.projectCatalog;
                        if (
                          catalog !== undefined &&
                          projectCatalog !== undefined
                        )
                          catalog = Schema.decodeUnknownSync(JsonValueSchema)(
                            invokeSyncHandler(
                              () => projectCatalog(json.success),
                              undefined,
                            ),
                          );
                      }
                    } catch (error) {
                      failure = error;
                    } finally {
                      projecting = undefined;
                    }
                    if (failure !== undefined) {
                      recordStopSynchronously(
                        member,
                        boundReason(
                          failure instanceof Error
                            ? `project defect: ${failure.message}`
                            : "project defect",
                        ),
                        "enclosing-transaction",
                      );
                      return Option.none<Json | null>();
                    }
                    return Option.some(output);
                  })();
            if (Option.isSome(value)) {
              slots[member.admitted.id] = Object.freeze(
                catalog === undefined
                  ? { status: "running", revision, value: value.value }
                  : {
                      status: "running",
                      revision,
                      value: value.value,
                      catalog,
                    },
              );
              continue;
            }
          }
          slots[member.admitted.id] = Object.freeze({
            status: "stopped",
            revision,
            reason: member.reason ?? "Extension did not activate",
          });
        }
        return Object.freeze(slots);
      },
      call: (target, extensionId, command, raw) =>
        Effect.gen(function* () {
          const member = members.get(extensionId);
          if (member?.admitted.grants.has("commands") !== true)
            return yield* Effect.fail(
              new ShiitakeError(
                "extensions",
                "unavailable",
                "Extension is unavailable",
              ),
            );
          if (member.status !== "running" || member.hooks === undefined)
            return yield* Effect.fail(
              member.stateResetRequired ??
                new ShiitakeError(
                  "extensions",
                  "unavailable",
                  `Extension ${extensionId} is stopped: ${member.reason ?? "not activated"}`,
                ),
            );
          const commands = member.admitted.definition.contract.commands;
          const spec = Object.hasOwn(commands, command)
            ? commands[command]
            : undefined;
          if (spec?.scope !== target.scope)
            return yield* Effect.fail(
              new ShiitakeError(
                "contract",
                "invalid",
                "Extension command is unknown",
              ),
            );
          const sessionId =
            target.scope === "session"
              ? yield* Effect.try({
                  try: () => decodeClientSessionId(target.sessionId),
                  catch: () =>
                    new ShiitakeError(
                      "contract",
                      "invalid",
                      "Session id is invalid",
                    ),
                })
              : null;
          const handler = member.hooks.commands.get(command);
          if (handler === undefined) {
            const decoded = Schema.decodeUnknownResult(spec.input, {
              onExcessProperty: "error",
            })(raw);
            if (decoded._tag === "Failure")
              return yield* Effect.fail(
                new ShiitakeError(
                  "contract",
                  "invalid",
                  "Extension command input is invalid",
                ),
              );
            yield* recordStop(
              member,
              `command ${command} has no handler`,
              undefined,
              "own-transaction",
            );
            return yield* Effect.fail(
              new ShiitakeError(
                "extensions",
                "unavailable",
                "Extension is stopped",
              ),
            );
          }
          const prepared = handler({ sessionId, input: raw });
          if (prepared._tag === "Failure")
            return yield* Effect.fail(
              new ShiitakeError(
                "contract",
                "invalid",
                "Extension command input is invalid",
              ),
            );
          const body = prepared.success();
          const exit = yield* runTransaction(member, body, "request");
          if (Exit.isSuccess(exit)) return exit.value;
          const failure = Cause.findErrorOption(exit.cause);
          if (Option.isSome(failure) && !shouldStop(exit.cause))
            return yield* Effect.fail(commandFailure(failure.value));
          const reason = stopReason(`command ${command}`, exit.cause);
          yield* recordStop(member, reason, exit.cause, "own-transaction");
          return yield* Effect.fail(
            new ShiitakeError(
              "extensions",
              "unavailable",
              `Extension ${extensionId} is stopped: ${reason}`,
            ),
          );
        }),
      list: () => [...members.values()].map(statusOf),
      resume: (id) =>
        Effect.gen(function* () {
          const member = members.get(id);
          if (member === undefined)
            return yield* Effect.fail(
              new ShiitakeError(
                "extensions",
                "unavailable",
                "Extension is unavailable",
              ),
            );
          if (member.status === "stopped") {
            yield* Effect.try({
              try: () => {
                owner.transactionSync(() => {
                  writeStatus(owner, member.admitted.id, {
                    status: "running",
                    reason: null,
                    stoppedAt: null,
                    revision: member.admitted.revision,
                  });
                  refreshAttentionOwner(member.admitted.id);
                });
              },
              catch: () =>
                new ShiitakeError(
                  "store",
                  "unavailable",
                  "Extension status is unavailable",
                ),
            });
            member.status = "running";
            member.reason = null;
            member.stoppedAt = null;
            if (member.hooks === undefined && (yield* prepare(member)))
              yield* activateMember(member, scope);
            yield* input.requestWake;
          }
          return statusOf(member);
        }),
      close: Scope.close(scope, Exit.void),
    };
    return Object.freeze(runtime);
  });
}
