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
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

import type { CompactionHandoffV0 } from "../../extensions.js";
import { ShiitakeError } from "../../errors.js";
import {
  borrowedView,
  compact,
  prepareCompaction,
  type CompactionPreparation,
  type CompactionResult,
  type CompactionSourceEntry,
} from "./compaction.js";
import {
  claimSlot,
  releaseSlot,
  slotTaken,
  takeSummary,
  type CompactionSlot,
} from "./compaction-slot.js";
import {
  estimateContextTokens,
  estimateMessageTokens,
} from "@earendil-works/pi-ai/utils/estimate";
import {
  compactionSummaryMessage,
  shouldCompact,
  shouldCompactInBackground,
  type CompactionSettings,
} from "./utils.js";
import {
  appendCompactionNotice,
  appendCompactionTransition,
  beginCompactionActivity,
  buildSessionContext,
  CompactionReceiptSchema,
  compactionHeadState,
  endCompactionActivity,
  withdrawnWorkIds,
  contextMessageIds,
  commandContextMessage,
  operatorContextMessage,
  placeCompactionTransition,
  promptCommand,
  readSessionContextPath,
  type AppendCompactionInput,
  type CompactionNoticeInput,
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
  handoffResult:
    "The compaction handoff failed: it returned an invalid result; the conversation context is unchanged",
  conflict:
    "The Session changed during compaction; the conversation context is unchanged",
} as const;

/**
 * A commit that stopped before its CAS append took effect. The phase is
 * owner-closed, so its diagnostic is safe to record and show.
 */
export class CompactionCommitError extends ShiitakeError {
  constructor(
    readonly phase: keyof typeof commitFailures,
    readonly usage?: CompactionResult["usage"],
  ) {
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
        head: row.entry.head,
        model: row.entry.model[0],
        usage: row.entry.data.usage,
      } as const;
      entries.push(
        row.entry.data.details === undefined
          ? compaction
          : { ...compaction, details: row.entry.data.details },
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
    catch: () => runtimeFailure("Woodstock selected lineage is unavailable"),
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
    (sum, message) => sum + estimateMessageTokens(message),
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
    const receipt = yield* handOff(input, preparation, result, expected.id);
    return yield* appendSummary(
      input,
      path,
      result,
      request,
      expected.id,
      receipt,
    );
  });
}

/**
 * What a handoff may answer. A JS extension's result arrives untyped, so it is
 * decoded here: an `accepted` receipt must satisfy the entry's own receipt
 * rule, so an invalid one stops the compaction before anything is written.
 */
const HandoffResultSchema = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("accepted"),
    receipt: CompactionReceiptSchema,
  }),
  Schema.Struct({ type: Schema.Literal("discarded_by_policy") }),
]);
const decodeHandoffResult = Schema.decodeUnknownResult(HandoffResultSchema);

/**
 * Zookeeper's handoff with the exact arrays, before the destructive append.
 * Returns the archive receipt to store in the entry that commits, or
 * `undefined` when no handoff is installed or it discarded by policy.
 */
function handOff(
  input: CommitInput,
  preparation: CompactionPreparation,
  result: CompactionResult,
  contextRevision: string,
): Effect.Effect<string | undefined, ShiitakeError> {
  if (input.handoff === undefined) return Effect.succeed(undefined);
  return input.handoff
    .consume(
      borrowedView(preparation, result, input.sessionId, contextRevision),
    )
    .pipe(
      Effect.mapError(() => new CompactionCommitError("handoff", result.usage)),
      Effect.flatMap((handed) => {
        const decoded = decodeHandoffResult(handed);
        if (decoded._tag === "Failure")
          return Effect.fail(
            new CompactionCommitError("handoffResult", result.usage),
          );
        return Effect.succeed(
          decoded.success.type === "accepted"
            ? decoded.success.receipt
            : undefined,
        );
      }),
    );
}

/**
 * The row a compaction appends on the leaf `path` ends at, with its measured
 * `tokensAfter`. `contextRevision` names the last row the summary saw: that leaf
 * for a blocking commit, the selection tail for a background one.
 */
function compactionRow(
  input: CommitInput,
  path: readonly StoredSessionRow[],
  result: CompactionResult,
  request: CommitRequest,
  contextRevision: string,
  receipt: string | undefined,
): AppendCompactionInput | undefined {
  const expected = path.at(-1);
  if (expected === undefined) return undefined;
  const tokensAfter = tokensAfterCompaction(
    path,
    expected,
    {
      kind: "compaction",
      workId: input.workId,
      head: result.firstKeptEntryId,
      model: [compactionSummaryMessage(result.summary, Date.now())],
      data: {
        reason: request.trigger,
        resumesRun: request.resumesRun,
        tokensBefore: result.tokensBefore,
        // This probe row only measures the context; the figure it computes
        // is what the appended row records.
        tokensAfter: 0,
        usage: result.usage,
        contextRevision,
      },
    },
    input.workId,
  );
  const row = {
    sessionId: input.sessionId,
    resumesRun: request.resumesRun,
    trigger: request.trigger,
    tokensAfter,
    expectedLeafId: expected.id,
    workId: input.workId,
    head: result.firstKeptEntryId,
    summary: result.summary,
    tokensBefore: result.tokensBefore,
    usage: result.usage,
    contextRevision,
    details: result.details,
  };
  return receipt === undefined ? row : { ...row, receipt };
}

/** The context the run continues from once `committed` ends `path`. */
function rebuilt(
  input: CommitInput,
  path: readonly StoredSessionRow[],
  committed: StoredSessionRow,
): CompactionContext {
  const committedPath = [...path, committed];
  return {
    path: committedPath,
    messages: buildSessionContext(committedPath, input.workId),
  };
}

/** CAS-append a blocking compaction on the leaf `path` ends at and rebuild. */
function appendSummary(
  input: CommitInput,
  path: readonly StoredSessionRow[],
  result: CompactionResult,
  request: CommitRequest,
  contextRevision: string,
  receipt: string | undefined,
): Effect.Effect<CompactionContext | undefined, ShiitakeError> {
  const row = compactionRow(
    input,
    path,
    result,
    request,
    contextRevision,
    receipt,
  );
  if (row === undefined) return Effect.succeed(undefined);
  return Effect.try({
    try: () => appendCompactionTransition(input.store, row),
    catch: (error) =>
      error instanceof ShiitakeError
        ? error.cause === "conflict"
          ? new CompactionCommitError("conflict")
          : error
        : runtimeFailure("Woodstock compaction append failed"),
  }).pipe(Effect.map((committed) => rebuilt(input, path, committed)));
}

/** A failure's display-safe reason: owner-closed text or the model's message. */
function failureDiagnostic(error: ShiitakeError): string {
  if (error instanceof CompactionCommitError) return error.diagnostic;
  if (error.boundary === "model") return error.message;
  return `Compaction failed (${error.boundary}: ${error.cause})`;
}

/** Append one compaction notice, so clients can show what committed nothing. */
function writeNotice(
  input: CommitInput,
  notice: CompactionNoticeInput,
): Effect.Effect<void, ShiitakeError> {
  return Effect.try({
    try: () => {
      appendCompactionNotice(input.store, notice);
    },
    catch: (error) =>
      error instanceof ShiitakeError
        ? error
        : runtimeFailure("Compaction notice could not be recorded"),
  });
}

/** Record a compaction that found nothing to compact, or failed. */
function recordNotice(
  input: CommitInput,
  trigger: CompactionTrigger,
  failure: ShiitakeError | undefined,
): Effect.Effect<void, ShiitakeError> {
  const common = { sessionId: input.sessionId, workId: input.workId, trigger };
  return writeNotice(
    input,
    failure === undefined
      ? { ...common, result: "nothing_to_compact" }
      : {
          ...common,
          result: "failed",
          diagnostic: failureDiagnostic(failure),
          usage:
            failure instanceof CompactionCommitError
              ? failure.usage
              : undefined,
        },
  );
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

/** What a background compaction hands back to its run; it writes nothing. */
type BackgroundSummary = Readonly<{
  preparation: CompactionPreparation;
  result: CompactionResult;
  /** The last row the summary saw: the placed entry's `contextRevision`. */
  selectionTail: string;
}>;

/** What placement still needs once the fork-time arrays are released. */
type HandedOff = Readonly<{
  result: CompactionResult;
  selectionTail: string;
  /** The archive receipt; stored only if placement commits. */
  receipt: string | undefined;
}>;

type BackgroundSlot = CompactionSlot<BackgroundSummary, ShiitakeError>;

/**
 * One run's background compaction: the run's scope (the fiber's lifetime),
 * its slot while a summary is in flight or awaiting placement, and whether
 * this run has stopped starting them. A settled attempt that failed, found
 * nothing to compact, or went stale stops further starts in this run (a
 * stale summary means the conversation moved under it; retrying would spend
 * again on the same race), leaving the blocking threshold as the backstop.
 * Stopping is Shiitake policy, not Pi parity: Pi may start another later.
 */
type BackgroundCompaction = {
  readonly scope: Scope.Scope;
  slot: BackgroundSlot | undefined;
  stopped: boolean;
};

const thresholdRequest: CommitRequest = {
  trigger: "threshold",
  resumesRun: false,
};

/**
 * Start a background compaction when the context crossed the background
 * threshold (tokens only), the owner's slot is free and this run has not
 * stopped trying. `readContext` is the one context read, taken at fork time;
 * the fiber keeps only the preparation and hands it back with its summary.
 */
function startBackground(
  input: SessionCompactionInput,
  background: BackgroundCompaction,
  contextTokens: number,
  readContext: Effect.Effect<readonly StoredSessionRow[], ShiitakeError>,
): Effect.Effect<void, ShiitakeError> {
  if (
    background.stopped ||
    slotTaken(input.store) ||
    !shouldCompactInBackground(
      contextTokens,
      input.model.contextWindow,
      input.settings,
    )
  )
    return Effect.void;
  return readContext.pipe(
    Effect.flatMap((path) => forkBackground(input, background.scope, path)),
    Effect.map((slot) => {
      if (slot === undefined) background.stopped = true;
      else background.slot = slot;
    }),
  );
}

/**
 * Claim the owner's slot and fork the summary into the run's scope:
 * interrupting the run interrupts it, aborts the provider call and frees the
 * slot (a deliberate Shiitake difference: Pi's background compaction
 * outlives an abort). Nothing to compact forks nothing and stays silent, as
 * automatic compaction does.
 */
function forkBackground(
  input: SessionCompactionInput,
  scope: Scope.Scope,
  path: readonly StoredSessionRow[],
): Effect.Effect<BackgroundSlot | undefined> {
  const tail = path.at(-1);
  const preparation = prepareCompaction(
    sourceEntries(path, input.workId),
    input.settings,
  );
  if (tail === undefined || preparation === undefined)
    return Effect.succeed(undefined);
  return claimSlot(input.store, scope, (handBack) =>
    summarize(input, preparation, tail.id, handBack),
  );
}

/** The fiber's work: the summary call, handed back through the slot. */
function summarize(
  input: SessionCompactionInput,
  preparation: CompactionPreparation,
  selectionTail: string,
  handBack: (summary: BackgroundSummary) => void,
): Effect.Effect<void, ShiitakeError> {
  return Effect.suspend(() => {
    beginCompactionActivity(input.store, input.sessionId, "threshold");
    return compact(preparation, input.model, input.stream).pipe(
      Effect.map((result) => {
        handBack({ preparation, result, selectionTail });
      }),
      Effect.ensuring(
        Effect.sync(() => {
          endCompactionActivity(input.store, input.sessionId);
        }),
      ),
    );
  });
}

/**
 * Settle this run's background compaction and place it: at a between-turn
 * point only once it has finished (`join: false`), or by waiting for it
 * when the blocking threshold, an overflow or the end of the run arrives.
 * Joining instead of starting a second compaction is Shiitake's
 * single-flight policy; Pi does not join.
 * Undefined while it still runs, or when it settled failed or stale; a
 * summary or handoff failure becomes the ordinary `failed` notice. Background
 * notices (`failed`, `stale`) always record trigger `threshold`, the start that
 * made them; a placed entry takes `request`, standing in for the compaction
 * the run would have made at that point.
 */
function settleBackground(
  input: SessionCompactionInput,
  background: BackgroundCompaction,
  request: CommitRequest,
  join: boolean,
): Effect.Effect<CompactionContext | undefined, ShiitakeError> {
  return Effect.suspend(() => {
    const slot = background.slot;
    const fiber = slot?.fiber;
    if (
      slot === undefined ||
      fiber === undefined ||
      (!join && fiber.pollUnsafe() === undefined)
    )
      return Effect.succeed(undefined);
    background.slot = undefined;
    return Effect.result(
      Fiber.join(fiber).pipe(
        Effect.flatMap(() => handOffSummary(input, slot)),
        Effect.flatMap((handedOff) =>
          handedOff === undefined
            ? Effect.succeed(undefined)
            : appendPlaced(input, handedOff, request),
        ),
      ),
    ).pipe(
      Effect.flatMap((placed) => {
        if (placed._tag === "Success" && placed.success !== undefined)
          return Effect.succeed(placed.success);
        background.stopped = true;
        return placed._tag === "Success"
          ? Effect.succeed(undefined)
          : recordNotice(input, "threshold", placed.failure).pipe(
              Effect.as(undefined),
            );
      }),
      Effect.ensuring(
        Effect.sync(() => {
          releaseSlot(input.store, slot);
        }),
      ),
    );
  });
}

/**
 * Record a finished summary dropped unplaced: no entry, only its spend.
 * `stale`: its head moved. `insufficient`: placed, the context would still be
 * over the limit.
 */
function recordDropped(
  input: SessionCompactionInput,
  result: CompactionResult,
  reason: "stale" | "insufficient",
): Effect.Effect<undefined, ShiitakeError> {
  return writeNotice(input, {
    sessionId: input.sessionId,
    workId: input.workId,
    trigger: "threshold",
    result: reason,
    usage: result.usage,
  }).pipe(Effect.as(undefined));
}

/**
 * Placement steps 1-3 (design §3): take the summary out of the slot, check
 * its head, hand the fork-time arrays to Zookeeper, then release the slot.
 * Only the result and the selection tail come back, so the summarized prefix
 * is unreachable before placement reads the context again. A stale summary
 * never reaches the handoff.
 *
 * A summary handed off here can still fail to commit (stale inside the
 * append, insufficient, or a CAS conflict). Its accepted archive is then
 * unreferenced and Z5 sweeps it, at most one per run: see the "view" steps
 * of wiki/3-resources/shiitake-v0.0.1/zookeeper-compaction-handoff.md.
 */
function handOffSummary(
  input: SessionCompactionInput,
  slot: BackgroundSlot,
): Effect.Effect<HandedOff | undefined, ShiitakeError> {
  const summary = takeSummary(slot);
  if (summary === undefined) return Effect.succeed(undefined);
  const { result, selectionTail } = summary;
  return Effect.try({
    try: () =>
      compactionHeadState(input.store, {
        sessionId: input.sessionId,
        head: result.firstKeptEntryId,
      }),
    catch: (error) =>
      error instanceof ShiitakeError
        ? error
        : runtimeFailure("Woodstock compaction head is unavailable"),
  }).pipe(
    Effect.flatMap((head) =>
      head === "stale"
        ? recordDropped(input, result, "stale")
        : handOff(input, summary.preparation, result, selectionTail).pipe(
            Effect.map((receipt) => ({ result, selectionTail, receipt })),
          ),
    ),
    Effect.ensuring(
      Effect.sync(() => {
        releaseSlot(input.store, slot);
      }),
    ),
  );
}

/**
 * Placement steps 4-5: one fresh read, then the append on the leaf as it is
 * now, head-checked again inside the append transaction because the handoff
 * ran in between. Rows added after the selection tail stay verbatim after
 * the head; a head that moved meanwhile drops the summary as stale.
 *
 * Decided before appending (fix 3, option A): when the placed context would
 * still be over the limit, the summary is not placed. It is recorded as
 * `insufficient` and the caller's ordinary path (the blocking compaction, or the
 * overflow recovery) runs on a fresh read. Placing it would leave a
 * compaction at the tip, where Pi's preparation finds nothing to compact
 * (compaction.ts:282), so that ordinary path could never help.
 */
function appendPlaced(
  input: SessionCompactionInput,
  handedOff: HandedOff,
  request: CommitRequest,
): Effect.Effect<CompactionContext | undefined, ShiitakeError> {
  return readPath(input.store, input.sessionId, input.workId).pipe(
    Effect.flatMap((path) => {
      const row = compactionRow(
        input,
        path,
        handedOff.result,
        request,
        handedOff.selectionTail,
        handedOff.receipt,
      );
      if (row === undefined) return Effect.succeed(undefined);
      if (
        shouldCompact(
          row.tokensAfter,
          input.model.contextWindow,
          input.settings,
        )
      )
        return recordDropped(input, handedOff.result, "insufficient");
      return Effect.try({
        try: () => placeCompactionTransition(input.store, row),
        catch: (error) =>
          error instanceof ShiitakeError
            ? error.cause === "conflict"
              ? new CompactionCommitError("conflict")
              : error
            : runtimeFailure("Woodstock compaction append failed"),
      }).pipe(
        Effect.flatMap((placed) =>
          placed === "stale"
            ? recordDropped(input, handedOff.result, "stale")
            : Effect.succeed(rebuilt(input, path, placed)),
        ),
      );
    }),
  );
}

/**
 * Apply the pre-run threshold check, preserving unchanged input on failure.
 * Below it, the claimed context path may start a background compaction.
 */
function preRunMessages(
  input: SessionCompactionInput,
  background: BackgroundCompaction,
): Effect.Effect<CompactionContext, ShiitakeError> {
  const unchanged = { path: input.path, messages: input.messages };
  if (input.resuming === true) return Effect.succeed(unchanged);
  const tokens = estimateContextTokens(input.messages).tokens;
  if (!shouldCompact(tokens, input.model.contextWindow, input.settings))
    return startBackground(
      input,
      background,
      tokens,
      Effect.succeed(input.path),
    ).pipe(Effect.as(unchanged));
  return autoCompaction(input, input.path, thresholdRequest).pipe(
    Effect.map((compacted) => compacted ?? unchanged),
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
    estimateContextTokens([
      ...working.messages,
      ...outcome.messages.slice(working.newMessageCount),
    ]).tokens,
    input.model.contextWindow,
    input.settings,
  );
  return recovery.triggered || threshold
    ? readPath(input.store, input.sessionId, input.workId)
    : Effect.succeed(undefined);
}

/**
 * Pi's between-turn point, where Mule is paused and the run is the only
 * writer. A finished background summary is placed here; past the blocking
 * threshold a running one is joined, and only if that placed nothing does
 * the blocking compaction run. Below it, a background compaction may start.
 */
function betweenTurns(
  input: SessionCompactionInput,
  working: WorkingContext,
  background: BackgroundCompaction,
): BetweenTurns {
  return (context, newMessageCount) =>
    Effect.gen(function* () {
      const replace = (compacted: CompactionContext) => {
        working.path = compacted.path;
        working.messages = compacted.messages;
        working.newMessageCount = newMessageCount;
        return compacted.messages;
      };
      const tokens = estimateContextTokens(context).tokens;
      const blocking = shouldCompact(
        tokens,
        input.model.contextWindow,
        input.settings,
      );
      const placed = yield* settleBackground(
        input,
        background,
        thresholdRequest,
        blocking,
      );
      if (placed !== undefined) return replace(placed);
      const readContext = readPath(input.store, input.sessionId, input.workId);
      if (!blocking) {
        yield* startBackground(input, background, tokens, readContext);
        return undefined;
      }
      const compacted = yield* autoCompaction(
        input,
        yield* readContext,
        thresholdRequest,
      );
      return compacted === undefined ? undefined : replace(compacted);
    });
}

/** Run terminal outcomes through post-threshold and one-time recovery policy. */
function postRunOutcome(
  input: SessionCompactionInput,
  initial: CompactionContext,
  background: BackgroundCompaction,
): Effect.Effect<MuleRunOutcome, ShiitakeError> {
  return Effect.gen(function* () {
    let current = initial;
    let recoveryAttempted = initial.path.some(
      ({ entry }) =>
        entry.kind === "compaction" &&
        entry.workId === input.workId &&
        entry.data.resumesRun,
    );
    for (;;) {
      const working: WorkingContext = {
        path: current.path,
        messages: current.messages,
        newMessageCount: 0,
      };
      const outcome = yield* input.run(
        working.messages,
        betweenTurns(input, working, background),
      );
      const recovery = classifyRecovery(outcome, input.model);
      const retry = recovery.retry && !recoveryAttempted;
      // A summary still running when the run ends is waited for and placed
      // first; it then stands in for the post-run compaction.
      const placed = yield* settleBackground(
        input,
        background,
        {
          trigger: recovery.overflow ? "overflow" : "threshold",
          resumesRun: retry,
        },
        true,
      );
      if (placed !== undefined) {
        if (!retry) return outcome;
        current = {
          path: placed.path,
          messages: removeFailedAssistant(placed.messages),
        };
        recoveryAttempted = true;
        continue;
      }
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

/**
 * Compose the retained pre-run, between-turn and post-run compaction phases.
 * A background compaction is a child fiber in the run's own scope.
 */
export function runWithCompaction(
  input: SessionCompactionInput,
): Effect.Effect<MuleRunOutcome, ShiitakeError, Scope.Scope> {
  return Effect.gen(function* () {
    const background: BackgroundCompaction = {
      scope: yield* Effect.scope,
      slot: undefined,
      stopped: false,
    };
    const initial = input.settings.enabled
      ? yield* preRunMessages(input, background)
      : input;
    return yield* postRunOutcome(input, initial, background);
  });
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
