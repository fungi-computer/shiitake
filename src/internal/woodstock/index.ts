import type { SandboxFailureEvent } from "@fungi.computer/mycelium";
import type { Json } from "../../extensions.js";
import type {
  AssistantMessage,
  MuleError,
  MuleEvent,
  MuleIdentity,
} from "@fungi.computer/mule/effect";
import * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";

import type {
  ColdSessionPage,
  ColdSessionPageRequest,
} from "../../cold-sessions.js";
import type {
  ReadOptions,
  SessionExtensionSlots,
  SessionSnapshot,
} from "../../client.js";
import { ShiitakeError } from "../../errors.js";
import {
  decodeClientReadOptions,
  decodeClientSessionCreationRequestId,
  decodeClientSessionId,
} from "../client-boundary.js";
import type { CommandSettlement, SqliteOwner } from "../contracts.js";
import { publishOwnerObservation } from "../observation-kernel.js";
import { readColdSessionPageOwned } from "./cold-sessions.js";
import type { ObservedActivity } from "../session-status.js";
import {
  diagnosticsByWorkId,
  projectCommandSettlement,
  readClientReplayPage,
  readClientSnapshot,
  type ClientReplayPage,
} from "./client-projection.js";
import { sessionIds } from "./identities.js";
import { openProgress, openToolProgress } from "./progress.js";
import {
  encodeStoredSessionEntry,
  type LiveRetryEvent,
  type StoredSessionEntry,
} from "./session-entry.js";
import {
  appendSessionEntry,
  appendCompactionTransitionOwned,
  appendCompactionNoticeOwned,
  placeCompactionTransitionOwned,
  type CompactionNoticeInput,
  acceptSessionAbort,
  acceptSessionCancellation,
  completeSessionLeaf,
  prepareSessionRun,
  prepareSessionStore,
  readSessionEntriesForWork,
  readSessionCommandAdmission,
  settleSessionControls,
  takePendingSessionControls,
  readSessionEntryById,
  selectSessionBranch,
  forkSessionAt,
  type AcceptedCommand,
  type EnqueueAcceptedWork,
  type AppendCompactionInput,
  type PreparedSessionRun,
  type RunContract,
  type RecordAcceptedCancellation,
  type SessionQueueHead,
  type StoredSessionRow,
  readStoredSessionSummary as readSessionSummaryOwned,
  readStoredSessionFactPage,
  createStoredSession,
  deleteStoredSession,
  appendOperatorEntry,
} from "./session-store.js";
import {
  readSessionInboxPage,
  recordSessionInboxDeletion,
} from "./session-inbox.js";

export * from "./extension-store.js";
export { readLatestPendingPrompt } from "./client-projection.js";
export {
  createFleetDirectoryStore,
  createFleetDelegationStore,
} from "./fleet-store.js";
export * from "./session-attention.js";
export {
  deliverSessionGrants,
  prepareSessionGrants,
  readSessionGrants,
  type GrantsDelivery,
  type GrantsDeliveryResult,
} from "./session-grants.js";
export * from "./progress.js";
export * from "./compaction-activity.js";
export * from "./agent-reply.js";
export * from "./session-entry.js";
export * from "./session-store.js";
export * from "./session-inbox.js";
export { contextMessageIds } from "./run-recovery.js";
export * from "./storage-admission.js";

/** Record one compaction notice and publish it to observers. */
export function appendCompactionNotice(
  owner: SqliteOwner,
  input: CompactionNoticeInput,
): StoredSessionRow {
  const row = appendCompactionNoticeOwned(owner, input);
  publishOwnerObservation(owner, input.sessionId);
  return row;
}

/** Append compaction through Woodstock's ordinary owner-observation boundary. */
export function appendCompactionTransition(
  owner: SqliteOwner,
  input: AppendCompactionInput,
): StoredSessionRow {
  const row = appendCompactionTransitionOwned(owner, input);
  publishOwnerObservation(owner, input.sessionId);
  return row;
}

/** Place a background summary, head-checked in the append transaction. */
export function placeCompactionTransition(
  owner: SqliteOwner,
  input: AppendCompactionInput,
): StoredSessionRow | "stale" {
  const placed = placeCompactionTransitionOwned(owner, input);
  if (placed !== "stale") publishOwnerObservation(owner, input.sessionId);
  return placed;
}

function rejected(message: string): ShiitakeError {
  return new ShiitakeError("store", "rejected", message);
}

function conflict(message: string): ShiitakeError {
  return new ShiitakeError("store", "conflict", message);
}

function storeEffect<A>(operation: () => A): Effect.Effect<A, ShiitakeError> {
  return Effect.try({
    try: operation,
    catch: (error) =>
      error instanceof ShiitakeError
        ? error
        : new ShiitakeError(
            "store",
            "rejected",
            "Native Session store operation failed",
            undefined,
            error,
          ),
  });
}

/** Prepare Woodstock's retained Session and private admission schema. */
export function prepareWoodstockSchema(
  owner: SqliteOwner,
): Effect.Effect<void, ShiitakeError> {
  return storeEffect(() => {
    prepareSessionStore(owner);
  });
}

/** Atomically cancel one exact owner-issued command. */
export function cancelSession(
  owner: SqliteOwner,
  sessionId: string,
  commandId: string,
  onlyIfPending: boolean | undefined,
  readQueueHead: () => SessionQueueHead,
  record: RecordAcceptedCancellation,
  meta: Json | undefined,
) {
  return storeEffect(() => {
    const cancellation = acceptSessionCancellation(
      owner,
      sessionId,
      commandId,
      readQueueHead,
      record,
      onlyIfPending,
      meta,
    );
    if (cancellation?.delivery === "new")
      publishOwnerObservation(owner, sessionId);
    return cancellation;
  });
}

/** Deliver one pending control through an active Mule safe point. */
export function takeSessionControls(
  owner: SqliteOwner,
  input: Readonly<{
    sessionId: string;
    workId: string;
    delivery: "steer" | "followUp";
  }>,
): Effect.Effect<
  readonly Readonly<{
    workId: string;
    requestId: string;
    message: import("@fungi.computer/mule/effect").Message;
  }>[],
  ShiitakeError
> {
  return storeEffect(() => {
    const messages = takePendingSessionControls(
      owner,
      input.sessionId,
      input.delivery,
      input.workId,
    );
    if (messages.length > 0) publishOwnerObservation(owner, input.sessionId);
    return messages;
  });
}

/** Record terminals for controls delivered through one settled parent run. */
export function settleControls(
  owner: SqliteOwner,
  input: Readonly<{
    sessionId: string;
    parentWorkId: string;
    outcome: "completed" | "cancelled" | "failed" | "interrupted";
    diagnostic: string | undefined;
    enqueue: EnqueueAcceptedWork;
  }>,
): Effect.Effect<void, ShiitakeError> {
  return storeEffect(() => {
    settleSessionControls(owner, input);
    publishOwnerObservation(owner, input.sessionId);
  });
}

/** Resolve Session-scoped abort to private work and record it atomically. */
export function abortSession(
  owner: SqliteOwner,
  sessionId: string,
  readQueueHead: () => SessionQueueHead,
  record: RecordAcceptedCancellation,
  meta?: Json,
) {
  return storeEffect(() => {
    const cancellation = acceptSessionAbort(
      owner,
      sessionId,
      readQueueHead,
      record,
      meta,
    );
    publishOwnerObservation(owner, sessionId);
    return cancellation?.accepted;
  });
}

/** Select one complete entry as the active Session lineage leaf. */
export function branchSession(
  owner: SqliteOwner,
  input: Readonly<{
    sessionId: string;
    type: "branch";
    entryId: string;
    assertIdleSession: (queue: string) => void;
  }>,
): Effect.Effect<void, ShiitakeError> {
  return storeEffect(() => {
    selectSessionBranch(owner, {
      sessionId: input.sessionId,
      entryId: input.entryId,
      assertIdleSession: input.assertIdleSession,
    });
    publishOwnerObservation(owner, input.sessionId);
  });
}

/** Record one operator or party line on an idle Session; false on replay. */
export function recordOperatorLine(
  owner: SqliteOwner,
  input: Readonly<{
    sessionId: string;
    requestId: string;
    author: "operator" | "party";
    meta: Json | undefined;
    text: string;
    assertIdleSession: (queue: string) => void;
  }>,
): Effect.Effect<void, ShiitakeError> {
  return storeEffect(() => {
    if (appendOperatorEntry(owner, input))
      publishOwnerObservation(owner, input.sessionId);
  });
}

/** Fork one complete entry lineage into a new cold-readable Session. */
export function forkSession(
  owner: SqliteOwner,
  input: Readonly<{
    sessionId: string;
    type: "fork";
    entryId: string;
    assertIdleSession: (queue: string) => void;
  }>,
): Effect.Effect<Readonly<{ sessionId: string }>, ShiitakeError> {
  return storeEffect(() => {
    const forkedSessionId = forkSessionAt(owner, {
      sessionId: input.sessionId,
      entryId: input.entryId,
      assertIdleSession: input.assertIdleSession,
    });
    publishOwnerObservation(owner, forkedSessionId);
    return Object.freeze({ sessionId: forkedSessionId });
  });
}

/** Load the exact retained lineage ending at one accepted command. */
export function loadAcceptedRun(
  owner: SqliteOwner,
  input: Readonly<{ sessionId: string; commandFactId: string }>,
): Effect.Effect<PreparedSessionRun, ShiitakeError> {
  return storeEffect(() => {
    const prepared = prepareSessionRun(owner, {
      sessionId: decodeClientSessionId(input.sessionId),
      commandFactId: input.commandFactId,
    });
    if (prepared.entryDelivery === "new")
      publishOwnerObservation(owner, input.sessionId);
    return prepared;
  });
}

function assistantText(message: AssistantMessage): string {
  return message.content
    .flatMap((part) => (part.type === "text" ? [part.text] : []))
    .join("")
    .trim();
}

/** Read the last exact completed assistant text produced by accepted work. */
export function readSettlementContent(
  owner: SqliteOwner,
  input: Readonly<{ sessionId: string; commandFactId: string }>,
): Effect.Effect<string, ShiitakeError> {
  return storeEffect(() => {
    const admission = readSessionCommandAdmission(
      owner,
      input.sessionId,
      input.commandFactId,
    );
    if (admission === undefined)
      throw rejected("Accepted Session command is unavailable");
    const entries = readSessionEntriesForWork(
      owner,
      input.sessionId,
      admission.entry.workId,
    );
    for (let index = entries.length - 1; index >= 0; index -= 1) {
      const entry = entries[index]?.entry;
      if (entry?.kind === "message" && entry.value.role === "assistant")
        return assistantText(entry.value);
    }
    return "";
  });
}

/**
 * The host metadata that settles with one accepted command: the
 * cancellation's when a host-attributed cancel ended it, otherwise the
 * command's own. Absent when the host attached none.
 */
export function readSettlementMeta(
  owner: SqliteOwner,
  input: Readonly<{ sessionId: string; commandFactId: string }>,
  outcome:
    "completed" | "cancelled" | "failed" | "interrupted" | "outcome_unknown",
): Effect.Effect<Json | undefined, ShiitakeError> {
  return storeEffect(() => {
    const admission = readSessionCommandAdmission(
      owner,
      input.sessionId,
      input.commandFactId,
    );
    if (admission === undefined)
      throw rejected("Accepted Session command is unavailable");
    if (outcome !== "cancelled") return admission.entry.meta;
    const cancellation = readSessionEntriesForWork(
      owner,
      input.sessionId,
      admission.entry.workId,
    ).find(({ entry }) => entry.kind === "cancellation");
    return cancellation?.entry.kind === "cancellation"
      ? cancellation.entry.meta
      : undefined;
  });
}

/**
 * The Session's recorded terminal diagnostic for one accepted command, the
 * same text `agents.wait` and clients see; absent when none was recorded.
 */
export function readSettlementDiagnostic(
  owner: SqliteOwner,
  input: Readonly<{ sessionId: string; commandFactId: string }>,
): Effect.Effect<string | undefined, ShiitakeError> {
  return storeEffect(() => {
    const admission = readSessionCommandAdmission(
      owner,
      input.sessionId,
      input.commandFactId,
    );
    if (admission === undefined)
      throw rejected("Accepted Session command is unavailable");
    return diagnosticsByWorkId(
      readSessionEntriesForWork(owner, input.sessionId, admission.entry.workId),
    ).get(admission.entry.workId);
  });
}

/** Read one bounded public Session snapshot. */
export function readSession(
  owner: SqliteOwner,
  sessionId: string,
  activity: ObservedActivity,
  input?: ReadOptions,
  projectExtensions: () => SessionExtensionSlots = () => ({}),
): Effect.Effect<SessionSnapshot, ShiitakeError> {
  return storeEffect(() =>
    readClientSnapshot(
      owner,
      decodeClientSessionId(sessionId),
      decodeClientReadOptions(input),
      activity,
      projectExtensions,
    ),
  );
}

/** Read one bounded folder-resume summary through Woodstock's Effect boundary. */
export function readSessionSummary(
  owner: SqliteOwner,
  sessionId: string,
): Effect.Effect<
  import("./session-store.js").StoredSessionSummary | undefined,
  ShiitakeError
> {
  return storeEffect(() =>
    readSessionSummaryOwned(owner, decodeClientSessionId(sessionId)),
  );
}

/**
 * Persist or replay the Session reserved for a creation request ID.
 * `admitNew` may refuse a new Session; a replayed request is never refused.
 */
export function createSession(
  owner: SqliteOwner,
  requestId: string,
  admitNew: () => void,
): Effect.Effect<string, ShiitakeError> {
  return storeEffect(() =>
    createStoredSession(
      owner,
      decodeClientSessionCreationRequestId({ requestId }),
      admitNew,
    ),
  );
}

/** Permanently delete one idle Session; false when it does not exist. */
export function deleteSession(
  owner: SqliteOwner,
  input: Readonly<{
    sessionId: string;
    assertIdleSession: (queue: string) => void;
  }>,
): Effect.Effect<boolean, ShiitakeError> {
  return storeEffect(() =>
    deleteStoredSession(owner, {
      sessionId: decodeClientSessionId(input.sessionId),
      assertIdleSession: input.assertIdleSession,
      recordInboxDeletion: (sessionId) => {
        recordSessionInboxDeletion(owner, sessionId);
      },
    }),
  );
}

/** Read a page of stored conversation facts; status needs the Session owner. */
export function readSessionFactPage(
  owner: SqliteOwner,
  after?: string,
): Effect.Effect<
  import("./session-store.js").StoredSessionSummaryPage,
  ShiitakeError
> {
  return storeEffect(() =>
    readStoredSessionFactPage(
      owner,
      after === undefined ? undefined : decodeClientSessionId(after),
    ),
  );
}

/** Read one bounded page of Sessions with stored bytes, coldest first. */
export function readColdSessionPage(
  owner: SqliteOwner,
  request: ColdSessionPageRequest,
): Effect.Effect<ColdSessionPage, ShiitakeError> {
  return storeEffect(() => readColdSessionPageOwned(owner, request));
}

/** Read one bounded inbox page in tier, then newest-activity order. */
export function readSessionInbox(
  owner: SqliteOwner,
  after: string | undefined,
): Effect.Effect<import("../../client.js").SessionSummaryPage, ShiitakeError> {
  return storeEffect(() => readSessionInboxPage(owner, after));
}

/** Read one strict-after page from Woodstock's selected lineage. */
export function readSessionReplayPage(
  owner: SqliteOwner,
  sessionId: string,
  after: string | undefined,
  delivery: "replay" | "live",
  activity: ObservedActivity,
  projectExtensions: () => SessionExtensionSlots = () => ({}),
  includeSandboxFailures = false,
): Effect.Effect<ClientReplayPage, ShiitakeError> {
  return storeEffect(() =>
    readClientReplayPage(
      owner,
      decodeClientSessionId(sessionId),
      after,
      delivery,
      activity,
      projectExtensions,
      includeSandboxFailures,
    ),
  );
}

function sameEntry(left: StoredSessionEntry, right: StoredSessionEntry) {
  return encodeStoredSessionEntry(left) === encodeStoredSessionEntry(right);
}

function sameIdentity(left: MuleIdentity, right: MuleIdentity): boolean {
  return (
    left.kind === right.kind &&
    left.value === right.value &&
    left.parent?.kind === right.parent?.kind &&
    left.parent?.value === right.parent?.value
  );
}

function sameContract(left: RunContract, right: RunContract): boolean {
  return (
    left.core === right.core &&
    left.extensions.length === right.extensions.length &&
    left.extensions.every(([id, fingerprint], index) => {
      const selected = right.extensions[index];
      return selected?.[0] === id && selected[1] === fingerprint;
    })
  );
}

function appendRecordedEntry(
  owner: SqliteOwner,
  accepted: Pick<AcceptedCommand, "sessionId" | "commandId">,
  entry: StoredSessionEntry,
  findExisting: (row: StoredSessionRow) => boolean,
  beforeAppend?: () => void,
): void {
  const delivery = owner.transactionSync(() => {
    beforeAppend?.();
    const existing = readSessionEntriesForWork(
      owner,
      accepted.sessionId,
      accepted.commandId,
    ).find(findExisting);
    if (existing !== undefined) {
      if (
        entry.kind === "work_terminal" &&
        existing.entry.kind === "run_terminal"
      )
        return "replay" as const;
      if (sameEntry(existing.entry, entry)) return "replay" as const;
      throw conflict("Mule event identity conflicts");
    }

    for (let attempt = 0; attempt < 3; attempt += 1) {
      const factId = sessionIds.fact.generate();
      if (
        readSessionEntryById(owner, factId) !== undefined ||
        readSessionCommandAdmission(owner, accepted.sessionId, factId) !==
          undefined
      )
        continue;
      appendSessionEntry(owner, {
        sessionId: accepted.sessionId,
        id: factId,
        entry,
      });
      return "new" as const;
    }
    throw rejected("Session fact identity allocation failed");
  });
  if (delivery === "new") publishOwnerObservation(owner, accepted.sessionId);
}

/** Persist an operator-only sandbox diagnostic beside its accepted Session work. */
export function recordSandboxFailure(
  owner: SqliteOwner,
  accepted: Pick<AcceptedCommand, "sessionId" | "commandId">,
  event: SandboxFailureEvent,
): Effect.Effect<void, ShiitakeError> {
  return storeEffect(() => {
    appendRecordedEntry(
      owner,
      accepted,
      {
        kind: "sandbox_failure",
        workId: accepted.commandId,
        event,
      },
      ({ entry }) =>
        entry.kind === "sandbox_failure" && entry.event.ref === event.ref,
    );
  });
}

/** Extension that admitted one accepted command; null for other producers. */
export type ExtensionAdmissionOrigin = Readonly<{
  extensionId: string;
  correlation: string | null;
}> | null;

/** Exact durable facts core joins into one settled run for extensions. */
export type SettledRunFacts = Readonly<{
  toolCalls: number;
  /** Latest run or work terminal recorded for the work, if any. */
  terminal: "completed" | "failed" | "cancelled" | "interrupted" | "missing";
  origin: ExtensionAdmissionOrigin;
}>;

/** Read the admitting extension of one accepted command. */
export function readExtensionAdmissionOrigin(
  owner: SqliteOwner,
  sessionId: string,
  commandFactId: string,
  workId: string,
): ExtensionAdmissionOrigin {
  const admission = readSessionCommandAdmission(
    owner,
    sessionId,
    commandFactId,
  );
  if (admission?.entry.workId !== workId)
    throw rejected("Accepted Session work is unavailable");
  return admission.origin.kind === "extension"
    ? Object.freeze({
        extensionId: admission.origin.extensionId,
        correlation: admission.origin.correlation,
      })
    : null;
}

/** Read the terminal and admission origin of one settled accepted command. */
export function readSettledRunFacts(
  owner: SqliteOwner,
  sessionId: string,
  commandFactId: string,
  workId: string,
): SettledRunFacts {
  const origin = readExtensionAdmissionOrigin(
    owner,
    sessionId,
    commandFactId,
    workId,
  );
  const entries = readSessionEntriesForWork(owner, sessionId, workId).map(
    ({ entry }) => entry,
  );
  const toolCalls = new Set(
    entries.flatMap((entry) =>
      entry.kind === "tool_intent" ? [entry.call.id] : [],
    ),
  ).size;
  const terminal = entries
    .filter(
      (entry) =>
        entry.kind === "run_terminal" || entry.kind === "work_terminal",
    )
    .at(-1);
  return Object.freeze({
    terminal:
      terminal?.kind === "run_terminal" || terminal?.kind === "work_terminal"
        ? terminal.outcome
        : "missing",
    origin,
    toolCalls,
  });
}

/** Record settlement only when Mule has no matching recorded terminal. */
export function recordWorkSettlement(
  owner: SqliteOwner,
  input: Readonly<{
    sessionId: string;
    commandFactId: string;
    workId: string;
    /** `completed` settles work that runs no Mule turn, such as compaction. */
    outcome: "completed" | "interrupted" | "cancelled" | "failed";
    diagnostic?: string | undefined;
  }>,
): Effect.Effect<void, ShiitakeError> {
  return storeEffect(() => {
    const admission = readSessionCommandAdmission(
      owner,
      input.sessionId,
      input.commandFactId,
    );
    if (admission?.entry.workId !== input.workId)
      throw rejected("Settled Session work is unavailable");
    const entry: Extract<StoredSessionEntry, { kind: "work_terminal" }> = {
      workId: input.workId,
      kind: "work_terminal",
      outcome: input.outcome,
    };
    appendRecordedEntry(
      owner,
      { sessionId: input.sessionId, commandId: input.workId },
      input.diagnostic === undefined
        ? entry
        : { ...entry, diagnostic: input.diagnostic },
      ({ entry: current }) =>
        current.kind === "work_terminal" ||
        (current.kind === "run_terminal" && current.outcome === input.outcome),
      () => {
        owner.sql.exec(
          "DELETE FROM session_entries WHERE session_id = ? AND work_id = ? AND state = 'streaming' AND type = 'message_start'",
          input.sessionId,
          input.workId,
        );
      },
    );
  });
}

function recordLifecycle(
  owner: SqliteOwner,
  accepted: AcceptedCommand,
  contract: RunContract,
  event: Extract<MuleEvent, { type: "run_start" | "turn_start" | "run_end" }>,
  diagnostic?: string,
): Effect.Effect<void, ShiitakeError> {
  return storeEffect(() => {
    let entry: StoredSessionEntry =
      event.type === "run_start"
        ? {
            kind: "run_started",
            workId: accepted.commandId,
            run: event.run,
            contract,
          }
        : event.type === "turn_start"
          ? {
              kind: "turn_started",
              workId: accepted.commandId,
              run: event.run,
              turn: event.turn,
            }
          : {
              kind: "run_terminal",
              workId: accepted.commandId,
              run: event.run,
              terminal: event.terminal,
              outcome: event.outcome,
            };
    if (entry.kind === "run_terminal" && diagnostic !== undefined)
      entry = { ...entry, diagnostic };
    appendRecordedEntry(owner, accepted, entry, ({ entry: current }) => {
      if (current.kind !== entry.kind) return false;
      if (current.kind === "run_started" && entry.kind === "run_started")
        return (
          sameIdentity(current.run, entry.run) &&
          sameContract(current.contract, contract)
        );
      if (current.kind === "turn_started" && entry.kind === "turn_started")
        return sameIdentity(current.turn, entry.turn);
      if (current.kind === "run_terminal" && entry.kind === "run_terminal")
        return sameIdentity(current.terminal, entry.terminal);
      return false;
    });
  });
}

function recordMessageStart(
  owner: SqliteOwner,
  accepted: AcceptedCommand,
  event: Extract<MuleEvent, { type: "message_start" }>,
): Effect.Effect<void, ShiitakeError> {
  return storeEffect(() => {
    const entry: Extract<StoredSessionEntry, { kind: "message_start" }> = {
      kind: "message_start",
      workId: accepted.commandId,
      run: event.run,
      turn: event.turn,
      message: event.message,
      role: event.value.role,
    };
    const delivery = owner.transactionSync(() => {
      const existing = readSessionEntriesForWork(
        owner,
        accepted.sessionId,
        accepted.commandId,
      ).find(
        ({ entry: current }) =>
          (current.kind === "message_start" || current.kind === "message") &&
          sameIdentity(current.message, event.message),
      );
      if (existing !== undefined) {
        if (existing.entry.kind === "message") {
          if (
            existing.entry.workId !== entry.workId ||
            !sameIdentity(existing.entry.run, entry.run) ||
            !sameIdentity(existing.entry.turn, entry.turn) ||
            existing.entry.value.role !== entry.role
          )
            throw conflict("Mule message-start identity conflicts");
          return "replay" as const;
        }
        if (!sameEntry(existing.entry, entry))
          throw conflict("Mule message-start identity conflicts");
        return "replay" as const;
      }
      appendSessionEntry(owner, {
        sessionId: accepted.sessionId,
        id: event.message.value,
        entry,
        state: "streaming",
      });
      return "new" as const;
    });
    if (delivery === "new") publishOwnerObservation(owner, accepted.sessionId);
  });
}

/** Persist exactly the system prompt before a provider attempt is sent. */
function recordModelPrompt(
  owner: SqliteOwner,
  accepted: AcceptedCommand,
  contract: RunContract,
  event: Extract<MuleEvent, { type: "model_prompt" }>,
): Effect.Effect<void, ShiitakeError> {
  return storeEffect(() => {
    const entry: Extract<StoredSessionEntry, { kind: "model_prompt" }> = {
      kind: "model_prompt",
      workId: accepted.commandId,
      run: event.run,
      turn: event.turn,
      attempt: event.attempt,
      contract,
      systemPrompt: event.systemPrompt,
    };
    appendRecordedEntry(
      owner,
      accepted,
      entry,
      ({ entry: current }) =>
        current.kind === "model_prompt" &&
        sameIdentity(current.turn, event.turn) &&
        current.attempt === event.attempt &&
        sameContract(current.contract, contract),
    );
  });
}

function recordMessageEnd(
  owner: SqliteOwner,
  accepted: AcceptedCommand,
  event: Extract<MuleEvent, { type: "message_end" }>,
): Effect.Effect<void, ShiitakeError> {
  return storeEffect(() => {
    const entry: Extract<StoredSessionEntry, { kind: "message" }> = {
      kind: "message",
      workId: accepted.commandId,
      run: event.run,
      turn: event.turn,
      message: event.message,
      value: event.value,
    };
    const completed =
      event.data === undefined ? entry : { ...entry, data: event.data };
    const delivery = owner.transactionSync(() => {
      const existing = readSessionEntriesForWork(
        owner,
        accepted.sessionId,
        accepted.commandId,
      ).find(
        ({ entry: current }) =>
          (current.kind === "message_start" || current.kind === "message") &&
          sameIdentity(current.message, event.message),
      );
      if (existing?.entry.kind === "message") {
        if (!sameEntry(existing.entry, completed))
          throw conflict("Mule completed-message identity conflicts");
        return "replay" as const;
      }
      completeSessionLeaf(owner, {
        sessionId: accepted.sessionId,
        id: event.message.value,
        entry: completed,
      });
      return "new" as const;
    });
    if (delivery === "new") publishOwnerObservation(owner, accepted.sessionId);
  });
}

function recordToolIntent(
  owner: SqliteOwner,
  accepted: AcceptedCommand,
  contract: RunContract,
  event: Extract<MuleEvent, { type: "tool_execution_start" }>,
): Effect.Effect<void, ShiitakeError> {
  return storeEffect(() => {
    const entry: Extract<StoredSessionEntry, { kind: "tool_intent" }> = {
      kind: "tool_intent",
      replay: event.replay,
      contract,
      workId: accepted.commandId,
      run: event.run,
      turn: event.turn,
      toolCall: event.toolCall,
      call: event.call,
    };
    appendRecordedEntry(
      owner,
      accepted,
      entry,
      ({ entry: current }) =>
        current.kind === "tool_intent" &&
        sameIdentity(current.toolCall, event.toolCall) &&
        sameContract(current.contract, contract),
    );
  });
}

function recordToolOutcome(
  owner: SqliteOwner,
  accepted: AcceptedCommand,
  event: Extract<MuleEvent, { type: "tool_execution_end" }>,
): Effect.Effect<void, ShiitakeError> {
  return storeEffect(() => {
    const { data, ...result } = event.result;
    const entry: Extract<StoredSessionEntry, { kind: "tool_outcome" }> = {
      kind: "tool_outcome",
      workId: accepted.commandId,
      run: event.run,
      turn: event.turn,
      toolCall: event.toolCall,
      call: event.call,
      result,
    };
    appendRecordedEntry(
      owner,
      accepted,
      data === undefined ? entry : { ...entry, data },
      ({ entry: current }) =>
        current.kind === "tool_outcome" &&
        sameIdentity(current.toolCall, event.toolCall),
    );
  });
}

/** Record one retained live Pi retry observation on the Session lineage. */
export function recordLiveRetryEvent(
  owner: SqliteOwner,
  accepted: AcceptedCommand,
  event: LiveRetryEvent,
  turn: MuleIdentity,
): Effect.Effect<void, MuleError> {
  return storeEffect(() => {
    // Pi numbers retries per assistant turn, not per run. The retained turn
    // boundary scopes replay identity without changing older event payloads.
    const boundary = readSessionEntriesForWork(
      owner,
      accepted.sessionId,
      accepted.commandId,
    ).find(
      ({ entry }) =>
        entry.kind === "turn_started" && sameIdentity(entry.turn, turn),
    );
    if (boundary === undefined) throw conflict("Retry has no recorded turn");
    const entry: Extract<StoredSessionEntry, { kind: "live_retry" }> = {
      kind: "live_retry",
      workId: accepted.commandId,
      event,
    };
    appendRecordedEntry(
      owner,
      accepted,
      entry,
      ({ entry: current, sequence }) =>
        sequence > boundary.sequence &&
        current.kind === "live_retry" &&
        current.event.type === event.type &&
        current.event.attempt === event.attempt &&
        sameIdentity(current.event.run, event.run),
    );
  }).pipe(Effect.mapError(listenerFailure));
}

const listenerFailure = (): MuleError => ({
  _tag: "MuleError",
  phase: "listener",
  message: "Woodstock could not record the Mule event",
});

export type MuleRecorder = (
  event: MuleEvent,
  diagnostic?: string,
) => Effect.Effect<void, MuleError>;

/**
 * Open the one scoped listener that projects Mule's exact event stream into
 * retained Session facts and bounded process-local progress.
 */
export function openMuleRecorder(
  owner: SqliteOwner,
  accepted: AcceptedCommand,
  contract: RunContract,
): Effect.Effect<MuleRecorder, never, Scope.Scope> {
  return Effect.gen(function* () {
    const progress = yield* openProgress(owner, accepted, (event) =>
      recordMessageEnd(owner, accepted, event),
    );
    const toolProgress = yield* openToolProgress(owner, accepted);
    const record = (
      event: MuleEvent,
      diagnostic?: string,
    ): Effect.Effect<void, ShiitakeError> => {
      switch (event.type) {
        case "model_prompt":
          return recordModelPrompt(owner, accepted, contract, event);
        case "run_start":
        case "turn_start":
        case "run_end":
          return recordLifecycle(owner, accepted, contract, event, diagnostic);
        case "message_start":
          if (event.value.role === "user") return Effect.void;
          return recordMessageStart(owner, accepted, event).pipe(
            Effect.andThen(
              event.value.role === "assistant"
                ? progress.started(event)
                : Effect.void,
            ),
          );
        case "message_update":
          return progress.update(event);
        case "message_end":
          if (event.value.role === "user") return Effect.void;
          return event.value.role === "assistant"
            ? progress.terminal(event)
            : recordMessageEnd(owner, accepted, event);
        case "tool_execution_start":
          return recordToolIntent(owner, accepted, contract, event).pipe(
            Effect.andThen(toolProgress.started(event)),
          );
        case "tool_execution_end":
          return recordToolOutcome(owner, accepted, event).pipe(
            Effect.andThen(toolProgress.terminal(event)),
          );
        case "tool_execution_update":
          return toolProgress.update(event);
        case "turn_end":
          return Effect.void;
      }
    };
    return (event, diagnostic) =>
      record(event, diagnostic).pipe(Effect.mapError(listenerFailure));
  });
}

export { declareSessionDocument } from "./session-documents.js";

/** Read exact settled work facts through Woodstock's native command/work indexes. */
export function readCommandSettlement(
  owner: SqliteOwner,
  sessionId: string,
  commandId: string,
): Effect.Effect<CommandSettlement | undefined, ShiitakeError> {
  return storeEffect(() =>
    projectCommandSettlement(
      owner,
      decodeClientSessionId(sessionId),
      decodeClientSessionId(commandId),
    ),
  );
}
