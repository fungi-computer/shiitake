import {
  isContextOverflow,
  type Message,
  type Model,
  type Api,
  type MuleRunOutcome,
  type MuleStreamFunction,
} from "@fungi.computer/mule/effect";
import { isRecoverableLength } from "@earendil-works/pi-ai/utils/overflow";
import * as Effect from "effect/Effect";

import type { CompactionHandoffV0 } from "../../extensions.js";
import { ShiitakeError } from "../../errors.js";
import {
  borrowedView,
  compact,
  prepareCompaction,
  type CompactionSourceEntry,
} from "./compaction.js";
import {
  estimateContextTokens,
  estimateTokens,
  shouldCompact,
  type CompactionSettings,
} from "./utils.js";
import {
  appendCompactionNotice,
  appendCompactionTransition,
  beginCompactionActivity,
  buildSessionContext,
  endCompactionActivity,
  withdrawnWorkIds,
  contextMessageIds,
  commandContextMessage,
  operatorContextMessage,
  promptCommand,
  readSessionContextPath,
  type CompactionTrigger,
  type StoredSessionRow,
} from "../woodstock/index.js";
import type { SqliteOwner } from "../contracts.js";

function runtimeFailure(message: string): ShiitakeError {
  return new ShiitakeError("runtime", "rejected", message);
}

const commitFailures = {
  handoff:
    "The compaction handoff failed; the conversation context is unchanged",
  conflict:
    "The Session changed during compaction; the conversation context is unchanged",
} as const;

/**
 * A commit that stopped before its CAS append took effect. The phase is
 * owner-closed, so its diagnostic is safe to record and show.
 */
export class CompactionCommitError extends ShiitakeError {
  constructor(readonly phase: keyof typeof commitFailures) {
    super(
      phase === "conflict" ? "store" : "runtime",
      phase === "conflict" ? "conflict" : "rejected",
      commitFailures[phase],
    );
  }

  get diagnostic(): string {
    return commitFailures[this.phase];
  }
}

function sourceEntries(
  path: readonly StoredSessionRow[],
  activeWorkId: string,
): CompactionSourceEntry[] {
  const visible = contextMessageIds(path, activeWorkId);
  const withdrawn = withdrawnWorkIds(path);
  const entries: CompactionSourceEntry[] = [];
  for (const row of path) {
    const prompt = promptCommand(row.entry);
    if (prompt !== undefined && !withdrawn.has(prompt.workId)) {
      entries.push({
        id: row.id,
        kind: "message",
        message: commandContextMessage(prompt),
      });
      continue;
    }
    if (row.entry.kind === "operator") {
      entries.push({
        id: row.id,
        kind: "message",
        message: operatorContextMessage(row.entry),
      });
      continue;
    }
    if (row.entry.kind === "message") {
      if (visible.has(row.id))
        entries.push({ id: row.id, kind: "message", message: row.entry.value });
      continue;
    }
    if (row.entry.kind === "compaction") {
      const compaction = {
        id: row.id,
        kind: "compaction",
        timestamp: row.timestamp,
        firstKeptEntryId: row.entry.firstKeptEntryId,
        summary: row.entry.summary,
        usage: row.entry.usage,
      } as const;
      entries.push(
        row.entry.details === undefined
          ? compaction
          : { ...compaction, details: row.entry.details },
      );
    }
  }
  return entries;
}

/** The current context rows: the latest kept window, never the whole lineage. */
function readPath(
  store: SqliteOwner,
  sessionId: string,
  workId: string,
): Effect.Effect<readonly StoredSessionRow[], ShiitakeError> {
  return Effect.try({
    try: () => readSessionContextPath(store, { sessionId, workId }),
    catch: (error) =>
      error instanceof ShiitakeError && error.cause === "context_too_large"
        ? error
        : runtimeFailure("Woodstock selected lineage is unavailable"),
  });
}

/**
 * Replace the context before a running work's next assistant request, or
 * keep it (`undefined`). Run scope hands this to Mule's between-turn point.
 */
export type BetweenTurns = (
  context: readonly Message[],
  newMessageCount: number,
) => Effect.Effect<readonly Message[] | undefined, ShiitakeError>;

export type SessionCompactionInput = Readonly<{
  store: SqliteOwner;
  sessionId: string;
  workId: string;
  model: Model<Api>;
  stream?: MuleStreamFunction | undefined;
  handoff?: CompactionHandoffV0 | undefined;
  /** This model's resolved budgets and the global automatic switch. */
  settings: CompactionSettings;
  path: readonly StoredSessionRow[];
  messages: readonly Message[];
  /** Finish the durable assistant/tool boundary before compacting it. */
  resuming?: boolean;
  run: (
    messages: readonly Message[],
    betweenTurns: BetweenTurns,
  ) => Effect.Effect<MuleRunOutcome, ShiitakeError>;
}>;

type CompactionContext = Readonly<{
  path: readonly StoredSessionRow[];
  messages: readonly Message[];
}>;

/**
 * The compaction owner's context estimate: Pi's usage-plus-trailing count,
 * excluding provider usage that belongs before Woodstock's latest boundary.
 *
 * A provider's reported usage measures the context it actually read, so it
 * is authoritative only while nothing has reshaped that context since. Once
 * a compaction lands, every kept message's own usage figure is stale: it
 * priced the pre-compaction history, not the summary that replaced it. The
 * one safe fallback is a fresh chars/4 estimate of every message still in
 * view, the same method `tokensAfterCompaction` uses when it records the
 * marker's own `tokensAfter` at commit time; trailing-after-usage alone
 * would undercount (even to zero, when the kept tail's own reply is the
 * newest message and nothing followed it).
 */
export function estimateThresholdContextTokens(
  path: readonly StoredSessionRow[],
  messages: readonly Message[],
): number {
  const boundary = path.reduce<number | undefined>(
    (latest, row) => (row.entry.kind === "compaction" ? row.timestamp : latest),
    undefined,
  );
  const estimate = estimateContextTokens(messages);
  const usageIndex = estimate.lastUsageIndex;
  const usageMessage = usageIndex === null ? undefined : messages[usageIndex];
  if (
    boundary === undefined ||
    usageMessage?.role !== "assistant" ||
    usageMessage.timestamp > boundary
  )
    return estimate.tokens;
  return messages.reduce((sum, message) => sum + estimateTokens(message), 0);
}

/** What one commit needs: the Session, its model, budgets and handoff. */
type CommitInput = Pick<
  SessionCompactionInput,
  "store" | "sessionId" | "workId" | "model" | "stream" | "handoff" | "settings"
>;

type CommitRequest = Readonly<{
  trigger: CompactionTrigger;
  resumesRun: boolean;
  instructions?: string | undefined;
}>;

/** The context the model reads once a compaction row becomes the boundary. */
function tokensAfterCompaction(
  path: readonly StoredSessionRow[],
  expected: StoredSessionRow,
  entry: Extract<StoredSessionRow["entry"], { kind: "compaction" }>,
  workId: string,
): number {
  const boundary: StoredSessionRow = {
    ...expected,
    id: `${expected.id}:compaction`,
    parentId: expected.id,
    sequence: expected.sequence + 1,
    timestamp: Date.now(),
    entry,
  };
  return buildSessionContext([...path, boundary], workId).reduce(
    (sum, message) => sum + estimateTokens(message),
    0,
  );
}

/** One exact preparation, handoff, CAS append, and authoritative rebuild. */
function commitCompaction(
  input: CommitInput,
  path: readonly StoredSessionRow[],
  request: CommitRequest,
): Effect.Effect<CompactionContext | undefined, ShiitakeError> {
  return Effect.suspend(() => {
    beginCompactionActivity(input.store, input.sessionId, request.trigger);
    return commitCompactionEffect(input, path, request).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          endCompactionActivity(input.store, input.sessionId);
        }),
      ),
    );
  });
}

/** The commit itself; `commitCompaction` wraps it with the live activity fact. */
function commitCompactionEffect(
  input: CommitInput,
  path: readonly StoredSessionRow[],
  request: CommitRequest,
): Effect.Effect<CompactionContext | undefined, ShiitakeError> {
  return Effect.gen(function* () {
    const preparation = prepareCompaction(
      sourceEntries(path, input.workId),
      input.settings,
    );
    if (preparation === undefined) return undefined;
    const expected = path.at(-1);
    if (expected === undefined) return undefined;
    const result = yield* compact(
      preparation,
      input.model,
      input.stream,
      request.instructions,
    );
    if (input.handoff !== undefined)
      yield* input.handoff
        .consume(borrowedView(preparation, result, expected.id))
        .pipe(Effect.mapError(() => new CompactionCommitError("handoff")));
    const tokensAfter = tokensAfterCompaction(
      path,
      expected,
      {
        kind: "compaction",
        resumesRun: request.resumesRun,
        trigger: request.trigger,
        workId: input.workId,
        firstKeptEntryId: result.firstKeptEntryId,
        summary: result.summary,
        tokensBefore: result.tokensBefore,
        usage: result.usage,
        contextRevision: expected.id,
      },
      input.workId,
    );
    const committed = yield* Effect.try({
      try: () =>
        appendCompactionTransition(input.store, {
          sessionId: input.sessionId,
          resumesRun: request.resumesRun,
          trigger: request.trigger,
          tokensAfter,
          expectedLeafId: expected.id,
          workId: input.workId,
          firstKeptEntryId: result.firstKeptEntryId,
          summary: result.summary,
          tokensBefore: result.tokensBefore,
          usage: result.usage,
          contextRevision: expected.id,
          details: result.details,
        }),
      catch: (error) =>
        error instanceof ShiitakeError
          ? error.cause === "conflict"
            ? new CompactionCommitError("conflict")
            : error
          : runtimeFailure("Woodstock compaction append failed"),
    });
    const committedPath = [...path, committed];
    return {
      path: committedPath,
      messages: buildSessionContext(committedPath, input.workId),
    };
  });
}

/** A failure's display-safe reason: owner-closed text or the model's message. */
function failureDiagnostic(error: ShiitakeError): string {
  if (error instanceof CompactionCommitError) return error.diagnostic;
  if (error.boundary === "model") return error.message;
  return `Compaction failed (${error.boundary}: ${error.cause})`;
}

/** Record a compaction that committed nothing, so clients can show it. */
function recordNotice(
  input: CommitInput,
  trigger: CompactionTrigger,
  failure: ShiitakeError | undefined,
): Effect.Effect<void, ShiitakeError> {
  return Effect.try({
    try: () => {
      appendCompactionNotice(input.store, {
        sessionId: input.sessionId,
        workId: input.workId,
        trigger,
        result: failure === undefined ? "nothing_to_compact" : "failed",
        diagnostic:
          failure === undefined ? undefined : failureDiagnostic(failure),
      });
    },
    catch: (error) =>
      error instanceof ShiitakeError
        ? error
        : runtimeFailure("Compaction notice could not be recorded"),
  });
}

/**
 * Run one automatic compaction. A failure is recorded as a notice and the
 * caller keeps its current context; automatic compaction never fails a run.
 */
function autoCompaction(
  input: CommitInput,
  path: readonly StoredSessionRow[],
  request: CommitRequest,
): Effect.Effect<CompactionContext | undefined, ShiitakeError> {
  return Effect.result(commitCompaction(input, path, request)).pipe(
    Effect.flatMap((result) =>
      result._tag === "Success"
        ? Effect.succeed(result.success)
        : recordNotice(input, request.trigger, result.failure).pipe(
            Effect.as(undefined),
          ),
    ),
  );
}

/** Apply the pre-run threshold check, preserving unchanged input on failure. */
function preRunMessages(
  input: SessionCompactionInput,
): Effect.Effect<CompactionContext, ShiitakeError> {
  if (
    input.resuming === true ||
    !shouldCompact(
      estimateThresholdContextTokens(input.path, input.messages),
      input.model.contextWindow,
      input.settings,
    )
  )
    return Effect.succeed({ path: input.path, messages: input.messages });
  return autoCompaction(input, input.path, {
    trigger: "threshold",
    resumesRun: false,
  }).pipe(
    Effect.map(
      (compacted) =>
        compacted ?? { path: input.path, messages: input.messages },
    ),
  );
}

type RecoveryDecision = Readonly<{
  triggered: boolean;
  overflow: boolean;
  retry: boolean;
}>;

function classifyRecovery(
  outcome: MuleRunOutcome,
  model: Model<Api>,
): RecoveryDecision {
  const overflow = isContextOverflow(outcome.message, model.contextWindow);
  const recoverableLength = isRecoverableLength(
    outcome.message,
    model.maxTokens,
  );
  return {
    triggered: overflow || recoverableLength,
    overflow,
    retry:
      (overflow || recoverableLength) && outcome.message.stopReason !== "stop",
  };
}

function removeFailedAssistant(messages: readonly Message[]) {
  const last = messages.at(-1);
  return last?.role === "assistant" &&
    (last.stopReason === "error" || last.stopReason === "length")
    ? messages.slice(0, -1)
    : messages;
}

/**
 * The run's working context: the messages it started from, replaced when a
 * mid-run compaction rebuilt them after `newMessageCount` run messages.
 */
type WorkingContext = {
  path: readonly StoredSessionRow[];
  messages: readonly Message[];
  newMessageCount: number;
};

function postRunPath(
  input: SessionCompactionInput,
  working: WorkingContext,
  outcome: MuleRunOutcome,
  recovery: RecoveryDecision,
): Effect.Effect<readonly StoredSessionRow[] | undefined, ShiitakeError> {
  if (!input.settings.enabled) return Effect.succeed(undefined);
  const threshold = shouldCompact(
    estimateThresholdContextTokens(working.path, [
      ...working.messages,
      ...outcome.messages.slice(working.newMessageCount),
    ]),
    input.model.contextWindow,
    input.settings,
  );
  return recovery.triggered || threshold
    ? readPath(input.store, input.sessionId, input.workId)
    : Effect.succeed(undefined);
}

/**
 * Pi's between-turn threshold check: when a running work's context has grown
 * past the threshold, compact the durable lineage (which already holds this
 * turn's tool results) and continue from the rebuilt context.
 */
function betweenTurns(
  input: SessionCompactionInput,
  working: WorkingContext,
): BetweenTurns {
  return (context, newMessageCount) =>
    Effect.gen(function* () {
      if (
        !shouldCompact(
          estimateThresholdContextTokens(working.path, context),
          input.model.contextWindow,
          input.settings,
        )
      )
        return undefined;
      const path = yield* readPath(input.store, input.sessionId, input.workId);
      const compacted = yield* autoCompaction(input, path, {
        trigger: "threshold",
        resumesRun: false,
      });
      if (compacted === undefined) return undefined;
      working.path = compacted.path;
      working.messages = compacted.messages;
      working.newMessageCount = newMessageCount;
      return compacted.messages;
    });
}

/** Run terminal outcomes through post-threshold and one-time recovery policy. */
function postRunOutcome(
  input: SessionCompactionInput,
  initial: CompactionContext,
): Effect.Effect<MuleRunOutcome, ShiitakeError> {
  return Effect.gen(function* () {
    let current = initial;
    let recoveryAttempted = initial.path.some(
      ({ entry }) =>
        entry.kind === "compaction" &&
        entry.workId === input.workId &&
        entry.resumesRun === true,
    );
    for (;;) {
      const working: WorkingContext = {
        path: current.path,
        messages: current.messages,
        newMessageCount: 0,
      };
      const outcome = yield* input.run(
        working.messages,
        betweenTurns(input, working),
      );
      const recovery = classifyRecovery(outcome, input.model);
      const path = yield* postRunPath(input, working, outcome, recovery);
      if (path === undefined) return outcome;

      if (recovery.retry && recoveryAttempted) return outcome;

      const compacted = yield* autoCompaction(input, path, {
        trigger: recovery.overflow ? "overflow" : "threshold",
        resumesRun: recovery.retry,
      });
      if (compacted === undefined || !recovery.retry) return outcome;

      current = {
        path: compacted.path,
        messages: removeFailedAssistant(compacted.messages),
      };
      recoveryAttempted = true;
    }
  });
}

/** Compose the retained pre-run, between-turn and post-run compaction phases. */
export function runWithCompaction(
  input: SessionCompactionInput,
): Effect.Effect<MuleRunOutcome, ShiitakeError> {
  if (!input.settings.enabled) return postRunOutcome(input, input);
  return preRunMessages(input).pipe(
    Effect.flatMap((initial) => postRunOutcome(input, initial)),
  );
}

/** What a manual compaction did; a failure is the Effect's error. */
export type ManualCompactionOutcome = "compacted" | "nothing_to_compact";

/**
 * Compact one Session now, as accepted work: the same commit the automatic
 * triggers use, with `resumesRun: false`, the user's optional focus and no
 * model turn. "Nothing to compact" and a failure are each recorded as a
 * notice. A retried claim of the same work never compacts twice.
 */
export function compactSession(
  input: Pick<
    SessionCompactionInput,
    | "store"
    | "sessionId"
    | "workId"
    | "model"
    | "stream"
    | "handoff"
    | "settings"
    | "path"
  > &
    Readonly<{ instructions?: string | undefined }>,
): Effect.Effect<ManualCompactionOutcome, ShiitakeError> {
  if (
    input.path.some(
      ({ entry }) =>
        (entry.kind === "compaction" || entry.kind === "compaction_notice") &&
        entry.workId === input.workId,
    )
  )
    return Effect.succeed("compacted");
  return commitCompaction(input, input.path, {
    trigger: "manual",
    resumesRun: false,
    instructions: input.instructions,
  }).pipe(
    Effect.tapError((error) => recordNotice(input, "manual", error)),
    Effect.flatMap((committed) =>
      committed === undefined
        ? recordNotice(input, "manual", undefined).pipe(
            Effect.as("nothing_to_compact" as const),
          )
        : Effect.succeed("compacted" as const),
    ),
  );
}
