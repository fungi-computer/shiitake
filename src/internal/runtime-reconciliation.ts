import {
  agentJobFromInput,
  decodeAgentJob,
  type BirdDogEffect,
} from "@fungi.computer/bird-dog/effect";
import type { BirdDogSettlement } from "@fungi.computer/bird-dog";
import type { WatchdogSettledJob } from "@fungi.computer/watchdog";
import type {
  WatchdogRuntimeEffect,
  WatchdogSettledRecord,
} from "@fungi.computer/watchdog/effect";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";

import { agentTurnRecovery } from "./accepted-run-executor.js";
import { ShiitakeError } from "../errors.js";
import type { SettledRunV0 } from "../extensions.js";
import { ExtensionId } from "../extension-contract.js";
import type { SqliteOwner } from "./contracts.js";
import {
  coreConsumer,
  reportConsumer,
  type ExtensionRuntime,
} from "./extension-runtime.js";
import { publishOwnerObservation } from "./observation-kernel.js";
import {
  advanceCursor,
  readCursor,
  readExtensionAdmissionOrigin,
  readSettledRunFacts,
  readSettlementContent,
  readSettlementDiagnostic,
  readSessionDelegator,
  readSettlementMeta,
  readWaitingOn,
  readWaitingOnSender,
  type WaitingOnYou,
  recordWorkSettlement,
  sessionLineageRefused,
  settleControls,
} from "./woodstock/index.js";

const pageSize = 64;

function runtimeFailure(message: string): ShiitakeError {
  return new ShiitakeError("runtime", "rejected", message);
}

/** BirdDog's settlement facts for one settled job, without its correspondents. */
function settlementFacts(
  store: SqliteOwner,
  job: WatchdogSettledJob,
  /** The open items this settlement names as waiting on its recipient. */
  waitingOnYou: Effect.Effect<readonly WaitingOnYou[], ShiitakeError>,
): Effect.Effect<BirdDogSettlement, ShiitakeError> {
  return Effect.gen(function* () {
    const payload = yield* decodeJob(job);
    const content =
      job.outcome === "cancelled"
        ? ""
        : yield* readSettlementContent(store, payload.work);
    const meta = yield* readSettlementMeta(store, payload.work, job.outcome);
    const diagnostic =
      job.outcome === "completed" || job.outcome === "cancelled"
        ? undefined
        : yield* readSettlementDiagnostic(store, payload.work);
    let settled: BirdDogSettlement = { job, content };
    if (diagnostic !== undefined) settled = { ...settled, diagnostic };
    // A cancelled run names nothing; what it left open is the canceller's to see.
    const waiting = job.outcome === "cancelled" ? [] : yield* waitingOnYou;
    if (waiting.length > 0) settled = { ...settled, waitingOnYou: waiting };
    return meta === undefined ? settled : { ...settled, meta };
  });
}

function decodeJob(job: WatchdogSettledJob) {
  return Effect.try({
    try: () => decodeAgentJob(job),
    catch: () => runtimeFailure("Agent work payload is invalid"),
  });
}

function fanOut(
  mailRoom: BirdDogEffect,
  settled: BirdDogSettlement,
): Effect.Effect<void, ShiitakeError> {
  return mailRoom
    .fanOut(settled)
    .pipe(
      Effect.mapError(() =>
        runtimeFailure("Agent reply fan-out is unavailable"),
      ),
    );
}

/** Reply to the delegating mail that admitted this work, if it asked for one. */
function replyToMail(
  store: SqliteOwner,
  mailRoom: BirdDogEffect | undefined,
  job: WatchdogSettledJob,
): Effect.Effect<void, ShiitakeError> {
  if (mailRoom === undefined) return Effect.void;
  return Effect.gen(function* () {
    const payload = yield* decodeJob(job);
    if ((payload.mail?.replyTo.length ?? 0) === 0) return;
    // The reply goes to the mail's sender: name what waits on that sender.
    const waiting = Effect.try({
      try: () =>
        readWaitingOnSender(
          store,
          payload.work.sessionId,
          payload.work.commandFactId,
        ),
      catch: () => runtimeFailure("Session attention is unavailable"),
    });
    yield* fanOut(mailRoom, yield* settlementFacts(store, job, waiting));
  });
}

/**
 * Report work no delegating mail answered to the Session's delegator of
 * record. It runs after the extensions have seen the settlement, so a goal
 * or loop that admitted the next turn keeps the Session busy and the stretch
 * of turns reports once, when the Session finally rests. Another Agent's
 * original mail is that Agent's correspondence and never reported here.
 */
function reportToDelegator(
  store: SqliteOwner,
  watchdog: WatchdogRuntimeEffect,
  mailRoom: BirdDogEffect | undefined,
  job: WatchdogSettledJob,
): Effect.Effect<void, ShiitakeError> {
  if (mailRoom === undefined) return Effect.void;
  return Effect.gen(function* () {
    const payload = yield* decodeJob(job);
    const mail = payload.mail;
    if (mail !== undefined && mail.replyTo.length > 0) return;
    const head = yield* watchdog
      .readQueueHead(job.queue)
      .pipe(
        Effect.mapError(() =>
          runtimeFailure("Session queue head is unavailable"),
        ),
      );
    if (head.state !== "idle") return;
    const delegator = yield* Effect.try({
      try: () => readSessionDelegator(store, payload.work.sessionId),
      catch: () => runtimeFailure("Session delegator is unavailable"),
    });
    if (delegator === undefined) return;
    const from = mail?.reply === undefined ? mail?.from : undefined;
    const anotherAgentsMail =
      from !== undefined &&
      !(
        from.kind === "agent" &&
        delegator.replyTo.some(({ agentId }) => agentId === from.agentId)
      );
    if (anotherAgentsMail) return;
    // The report goes to the delegator: name what waits on it.
    const waiting = Effect.try({
      try: () =>
        readWaitingOn(store, payload.work.sessionId, delegator.replyTo),
      catch: () => runtimeFailure("Session attention is unavailable"),
    });
    const settled = yield* settlementFacts(store, job, waiting);
    yield* fanOut(mailRoom, { ...settled, delegator });
  });
}

/**
 * The `core-report` settled consumer: after the extensions' turn, report each
 * Session's last settled job in the page, one record at a time.
 */
function reportSettled(
  store: SqliteOwner,
  watchdog: WatchdogRuntimeEffect,
  mailRoom: BirdDogEffect | undefined,
  records: readonly WatchdogSettledRecord[],
): Effect.Effect<void, ShiitakeError> {
  return Effect.forEach(
    records,
    (record, index) =>
      Effect.gen(function* () {
        const later = records
          .slice(index + 1)
          .some(({ job }) => job.queue === record.job.queue);
        if (!later && !(yield* settledInRefusedSession(store, record.job)))
          yield* reportToDelegator(store, watchdog, mailRoom, record.job);
        yield* Effect.try({
          try: () => {
            advanceCursor(store, reportConsumer, record.sequence);
          },
          catch: () => runtimeFailure("Settled cursor is unavailable"),
        });
      }),
    { discard: true },
  );
}

function repairWorkSettlement(
  store: SqliteOwner,
  job: WatchdogSettledJob,
): Effect.Effect<void, ShiitakeError> {
  const outcome = job.outcome;
  if (
    outcome !== "interrupted" &&
    outcome !== "cancelled" &&
    outcome !== "failed"
  )
    return Effect.void;
  return Effect.gen(function* () {
    const payload = yield* Effect.try({
      try: () => decodeAgentJob(job),
      catch: () => runtimeFailure("Agent work payload is invalid"),
    });
    yield* recordWorkSettlement(store, {
      sessionId: payload.work.sessionId,
      commandFactId: payload.work.commandFactId,
      workId: job.jobId,
      outcome,
      diagnostic: job.failureReason,
    });
  });
}

function settleTargetedControls(
  store: SqliteOwner,
  watchdog: WatchdogRuntimeEffect,
  job: WatchdogSettledJob,
): Effect.Effect<void, ShiitakeError> {
  return Effect.gen(function* () {
    const payload = yield* Effect.try({
      try: () => decodeAgentJob(job),
      catch: () => runtimeFailure("Agent work payload is invalid"),
    });
    const outcome =
      job.outcome === "outcome_unknown" ? "interrupted" : job.outcome;
    yield* settleControls(store, {
      sessionId: payload.work.sessionId,
      parentWorkId: job.jobId,
      outcome,
      diagnostic: job.failureReason,
      enqueue: (work) => {
        watchdog.transactional.enqueueAcceptedJob(
          agentJobFromInput({
            work: { ...work, recovery: agentTurnRecovery },
            delivery: work.delivery,
          }),
        );
      },
    });
  });
}

function drainMail(
  mailRoom: BirdDogEffect | undefined,
): Effect.Effect<void, ShiitakeError> {
  if (mailRoom === undefined) return Effect.void;
  return mailRoom
    .drainOutbox()
    .pipe(
      Effect.mapError(() =>
        runtimeFailure("Agent reply delivery is unavailable"),
      ),
    );
}

/**
 * Whether the settled work belongs to a Session the lineage audit refused.
 * Nothing more can be recorded in that Session: its Watchdog settlement, with
 * the refusal as its failure reason, is the work's whole terminal.
 */
function settledInRefusedSession(
  store: SqliteOwner,
  job: WatchdogSettledJob,
): Effect.Effect<boolean, ShiitakeError> {
  return Effect.gen(function* () {
    const payload = yield* Effect.try({
      try: () => decodeAgentJob(job),
      catch: () => runtimeFailure("Agent work payload is invalid"),
    });
    const refused = yield* Effect.try({
      try: () => sessionLineageRefused(store, payload.work.sessionId),
      catch: () => runtimeFailure("Session lineage state is unavailable"),
    });
    if (refused)
      yield* Effect.logWarning("shiitake_settlement_session_refused").pipe(
        Effect.annotateLogs({ workId: job.jobId }),
      );
    return refused;
  });
}

/**
 * Join one core-processed settled record with its exact Woodstock facts. A
 * run of a Session the lineage audit refused reaches extensions with only
 * Watchdog's outcome and its admission origin: no terminal and no tool
 * calls can be read from that history.
 */
function settledRun(
  store: SqliteOwner,
  record: WatchdogSettledRecord,
): SettledRunV0 {
  const payload = decodeAgentJob(record.job);
  const facts = sessionLineageRefused(store, payload.work.sessionId)
    ? Object.freeze({
        terminal: "missing" as const,
        toolCalls: 0,
        origin: readExtensionAdmissionOrigin(
          store,
          payload.work.sessionId,
          payload.work.commandFactId,
          record.job.jobId,
        ),
      })
    : readSettledRunFacts(
        store,
        payload.work.sessionId,
        payload.work.commandFactId,
        record.job.jobId,
      );
  return Object.freeze({
    sequence: record.sequence,
    sessionId: payload.work.sessionId,
    workId: record.job.jobId,
    outcome: record.job.outcome,
    terminal: facts.terminal,
    toolCalls: facts.toolCalls,
    origin:
      facts.origin === null
        ? null
        : Object.freeze({
            extensionId: Schema.decodeUnknownSync(ExtensionId)(
              facts.origin.extensionId,
            ),
            correlation: facts.origin.correlation,
          }),
  });
}

/**
 * The `core` settled consumer: publish the idle owner observation, repair
 * terminals, settle targeted controls, fan out mail, then advance the durable core cursor, one record
 * at a time. Every step is idempotent, so a crash before the cursor advance
 * repeats work without duplicating a reply.
 */
function settleCore(
  store: SqliteOwner,
  watchdog: WatchdogRuntimeEffect,
  mailRoom: BirdDogEffect | undefined,
  records: readonly WatchdogSettledRecord[],
): Effect.Effect<void, ShiitakeError> {
  return Effect.forEach(
    records,
    (record) =>
      Effect.gen(function* () {
        // Watchdog has durably settled the work: its queue head is idle now.
        // Announce that before any Session repair, so a repair failure cannot
        // leave live watchers reporting the finished run as active. A replay
        // after a crash repeats the same announcement.
        publishOwnerObservation(store, record.job.queue);
        // A refused Session can neither record a repair nor give a reply
        // its content: the failed Watchdog settlement is all there is.
        if (!(yield* settledInRefusedSession(store, record.job))) {
          yield* repairWorkSettlement(store, record.job);
          yield* settleTargetedControls(store, watchdog, record.job);
          yield* replyToMail(store, mailRoom, record.job);
        }
        yield* Effect.try({
          try: () => {
            advanceCursor(store, coreConsumer, record.sequence);
          },
          catch: () => runtimeFailure("Settled cursor is unavailable"),
        });
      }),
    { discard: true },
  );
}

/**
 * One reconcile tick. Each step is its own Exit boundary: a failed extension
 * stops only itself, and mail always drains. Only a core failure fails the
 * tick, so the host backs off only for core.
 */
export function reconcileRuntime(
  input: Readonly<{
    watchdog: WatchdogRuntimeEffect;
    store: SqliteOwner;
    mailRoom: BirdDogEffect | undefined;
    extensions: ExtensionRuntime;
    requestWake: Effect.Effect<void, ShiitakeError>;
  }>,
): Effect.Effect<void, ShiitakeError> {
  return Effect.gen(function* () {
    const result = yield* input.watchdog.tick();
    if (result.status === "busy")
      return yield* Effect.fail(
        runtimeFailure("Runtime reconciliation is already active"),
      );
    const coreCursor = yield* Effect.try({
      try: () => readCursor(input.store, coreConsumer) ?? 0,
      catch: () => runtimeFailure("Settled cursor is unavailable"),
    });
    const reportCursor = yield* Effect.try({
      try: () => readCursor(input.store, reportConsumer) ?? coreCursor,
      catch: () => runtimeFailure("Settled cursor is unavailable"),
    });
    const consumers = input.extensions.settledConsumers();
    const after = Math.min(
      coreCursor,
      reportCursor,
      ...consumers.map(({ cursor }) => cursor),
    );
    const page = yield* input.watchdog.readSettled(pageSize, after);
    const core = yield* Effect.exit(
      settleCore(
        input.store,
        input.watchdog,
        input.mailRoom,
        page.filter(({ sequence }) => sequence > coreCursor),
      ),
    );
    const processed = yield* Effect.try({
      try: () => readCursor(input.store, coreConsumer) ?? 0,
      catch: () => runtimeFailure("Settled cursor is unavailable"),
    });
    const runs = yield* Effect.try({
      try: () =>
        page
          .filter(({ sequence }) => sequence <= processed)
          .map((record) => settledRun(input.store, record)),
      catch: () => runtimeFailure("Settled run facts are unavailable"),
    });
    yield* input.extensions.deliverSettled(runs, processed);
    const reports = yield* Effect.exit(
      reportSettled(
        input.store,
        input.watchdog,
        input.mailRoom,
        page.filter(
          ({ sequence }) => sequence > reportCursor && sequence <= processed,
        ),
      ),
    );
    const moreTimers = yield* input.extensions.fireDueTimers(Date.now());
    if (page.length === pageSize || moreTimers) yield* input.requestWake;
    yield* drainMail(input.mailRoom);
    if (Exit.isFailure(core)) return yield* Effect.failCause(core.cause);
    if (Exit.isFailure(reports)) return yield* Effect.failCause(reports.cause);
  }).pipe(
    Effect.mapError((failure) =>
      failure instanceof ShiitakeError
        ? failure
        : runtimeFailure("Runtime reconciliation failed"),
    ),
  );
}
