import type { Json } from "../../extensions.js";
import { readSessionAttention } from "./session-attention.js";
import type { SessionAttention } from "../../session-attention.js";
import type { UserMessage } from "@fungi.computer/mule/effect";
import type { SandboxFailureEvent } from "@fungi.computer/mycelium";
import type {
  SessionFrame,
  SessionCompactionMarker,
  SessionCompactionNotice,
  SessionExtensionSlots,
  SessionMessage,
  SessionPendingPrompts,
  SessionSnapshot,
  SessionTerminal,
} from "../../client.js";
import { ShiitakeError } from "../../errors.js";
import type { ClientReadOptions } from "../client-boundary.js";
import type { CommandSettlement, SqliteOwner } from "../contracts.js";
import type { ObservedActivity } from "../session-status.js";
import { decodeWoodstockCursor, encodeWoodstockCursor } from "./cursor.js";
import { operatorClientMessage, promptCommand } from "./session-entry.js";
import {
  lineageEntrySequence,
  lineageHasSequence,
  lineageRowLimit,
  readLineageWindow,
  readLineageWorkFacts,
  readSessionEntriesForWork,
  readSessionLineage,
  readPendingSessionCommands,
  readSessionCommandAdmission,
  type SessionLineage,
  type StoredSessionRow,
} from "./session-store.js";

const maximumPagePayloadBytes = 2_097_152;
const replayPageLimit = 200;
const maximumRowsPerPage = lineageRowLimit;
const encoder = new TextEncoder();

function unavailable(message: string): ShiitakeError {
  return new ShiitakeError("store", "unavailable", message);
}

function commandIdsByWorkId(
  owner: SqliteOwner,
  sessionId: string,
  path: readonly StoredSessionRow[],
): ReadonlyMap<string, string> {
  const commandIds = new Map<string, string>();
  for (const row of path) {
    if (row.entry.kind === "command")
      commandIds.set(row.entry.workId, row.entry.workId);
  }
  const unresolved = path.flatMap(({ entry }) =>
    (entry.kind === "run_terminal" || entry.kind === "work_terminal") &&
    !commandIds.has(entry.workId)
      ? [entry.workId]
      : [],
  );
  for (const workId of unresolved)
    if (readSessionCommandAdmission(owner, sessionId, workId) !== undefined)
      commandIds.set(workId, workId);
  return commandIds;
}

/** What a path says about each accepted work: its command and its failure. */
type WorkFacts = Readonly<{
  commandIds: ReadonlyMap<string, string>;
  diagnostics: ReadonlyMap<string, string>;
  /** Host metadata of the cancellation that ended each work, by work id. */
  cancellations: ReadonlyMap<string, Json>;
  /** Read only for command rows admitted into the bounded message page. */
  origin: (commandId: string) => SessionMessage["origin"];
}>;

function admissionOrigin(
  owner: SqliteOwner,
  sessionId: string,
): WorkFacts["origin"] {
  return (commandId) => {
    const origin = readSessionCommandAdmission(
      owner,
      sessionId,
      commandId,
    )?.origin;
    return origin?.kind === "extension"
      ? { extensionId: origin.extensionId, correlation: origin.correlation }
      : undefined;
  };
}

/** Facts from rows already in hand: one work's own entries. */
function workFacts(
  owner: SqliteOwner,
  sessionId: string,
  path: readonly StoredSessionRow[],
): WorkFacts {
  return {
    commandIds: commandIdsByWorkId(owner, sessionId, path),
    diagnostics: diagnosticsByWorkId(path),
    cancellations: new Map(
      path.flatMap(({ entry }) =>
        entry.kind === "cancellation" && entry.meta !== undefined
          ? [[entry.targetWorkId, entry.meta] as const]
          : [],
      ),
    ),
    origin: admissionOrigin(owner, sessionId),
  };
}

/**
 * Facts for one bounded page: only the works its rows show, looked up across
 * the whole lineage without decoding it (`diagnosticsByWorkId`'s rule, in SQL).
 */
function pageWorkFacts(
  owner: SqliteOwner,
  sessionId: string,
  lineage: SessionLineage,
  rows: readonly StoredSessionRow[],
): WorkFacts {
  const shown = new Set(
    rows.flatMap(({ entry }) =>
      entry.kind === "message" ||
      entry.kind === "run_terminal" ||
      entry.kind === "work_terminal"
        ? [entry.workId]
        : [],
    ),
  );
  const facts = readLineageWorkFacts(owner, lineage, shown);
  const commandIds = new Map<string, string>();
  for (const workId of facts.commands) commandIds.set(workId, workId);
  for (const { entry } of rows)
    if (
      (entry.kind === "run_terminal" || entry.kind === "work_terminal") &&
      !commandIds.has(entry.workId) &&
      readSessionCommandAdmission(owner, sessionId, entry.workId) !== undefined
    )
      commandIds.set(entry.workId, entry.workId);
  return {
    commandIds,
    diagnostics: facts.diagnostics,
    cancellations: facts.cancellations,
    origin: admissionOrigin(owner, sessionId),
  };
}

function projectMessage(
  row: StoredSessionRow,
  work: WorkFacts,
): SessionMessage | undefined {
  // A compaction request has no message; its compaction marker and its
  // terminal are what clients see of it.
  const prompt = promptCommand(row.entry);
  if (prompt !== undefined) {
    const origin = work.origin(prompt.workId);
    const message: SessionMessage = {
      id: row.id,
      sequence: row.sequence,
      commandId: prompt.workId,
      value: prompt.message,
    };
    let projected: SessionMessage = message;
    if (prompt.meta !== undefined)
      projected = { ...projected, meta: prompt.meta };
    if (origin !== undefined) projected = { ...projected, origin };
    return Object.freeze(projected);
  }
  if (row.entry.kind === "operator") {
    const message: SessionMessage = {
      id: row.id,
      sequence: row.sequence,
      commandId: null,
      value: operatorClientMessage(row.entry),
    };
    let projected: SessionMessage = message;
    if (row.entry.meta !== undefined)
      projected = { ...projected, meta: row.entry.meta };
    return Object.freeze(projected);
  }
  if (row.entry.kind === "message")
    return Object.freeze({
      id: row.entry.message.value,
      sequence: row.sequence,
      commandId: work.commandIds.get(row.entry.workId) ?? null,
      value: row.entry.value,
    });
  return undefined;
}

/**
 * Newest failure message each work recorded: a work-terminal diagnostic, an
 * assistant error or a final retry error. Raw text; clients and the reply
 * fan-out decide how to present and bound it.
 */
export function diagnosticsByWorkId(
  path: readonly StoredSessionRow[],
): ReadonlyMap<string, string> {
  const diagnostics = new Map<string, string>();
  for (const { entry } of path) {
    if (entry.kind === "work_terminal" && entry.diagnostic !== undefined) {
      diagnostics.set(entry.workId, entry.diagnostic);
    } else if (entry.kind === "message" && entry.value.role === "assistant") {
      if (entry.value.errorMessage !== undefined)
        diagnostics.set(entry.workId, entry.value.errorMessage);
    } else if (
      entry.kind === "live_retry" &&
      entry.event.type === "auto_retry_end" &&
      entry.event.finalError !== undefined
    )
      diagnostics.set(entry.workId, entry.event.finalError);
  }
  return diagnostics;
}

function projectTerminal(
  row: StoredSessionRow,
  { commandIds, diagnostics, cancellations }: WorkFacts,
): SessionTerminal | undefined {
  if (row.entry.kind !== "run_terminal" && row.entry.kind !== "work_terminal")
    return undefined;
  const commandId = commandIds.get(row.entry.workId);
  if (commandId === undefined) return undefined;
  const terminal: SessionTerminal = {
    commandId,
    outcome: row.entry.outcome,
    sequence: row.sequence,
    diagnostic: diagnostics.get(row.entry.workId) ?? null,
  };
  const meta =
    row.entry.outcome === "cancelled"
      ? cancellations.get(row.entry.workId)
      : undefined;
  return Object.freeze(meta === undefined ? terminal : { ...terminal, meta });
}

/**
 * Full-history client projections show every durable entry, including what a
 * compaction summarized away. Each compaction entry in `rows` contributes one
 * minimal marker, positioned to sort immediately before the first entry its
 * compaction kept (the same boundary `buildSessionContext` resumes from) so
 * the client can render "the agent's memory starts below" accurately even
 * when the kept entries are not the compaction row's immediate predecessors.
 */
function compactionMarkersIn(
  owner: SqliteOwner,
  lineage: SessionLineage,
  rows: readonly StoredSessionRow[],
): readonly SessionCompactionMarker[] {
  const markers: SessionCompactionMarker[] = [];
  for (const row of rows) {
    const entry = row.entry;
    if (entry.kind !== "compaction") continue;
    const kept = lineageEntrySequence(owner, lineage, entry.firstKeptEntryId);
    if (kept === undefined)
      throw unavailable("Session compaction horizon is unavailable");
    markers.push(
      Object.freeze({
        id: row.id,
        sequence: kept,
        at: row.timestamp,
        tokensBefore: entry.tokensBefore,
        tokensAfter: entry.tokensAfter ?? null,
        trigger: entry.trigger ?? null,
      }),
    );
  }
  return Object.freeze(markers);
}

/** Each compaction that changed nothing, at its own place in the page. */
function compactionNoticesIn(
  rows: readonly StoredSessionRow[],
): readonly SessionCompactionNotice[] {
  return Object.freeze(
    rows.flatMap(({ id, sequence, timestamp, entry }) =>
      entry.kind === "compaction_notice"
        ? [
            Object.freeze({
              id,
              sequence,
              at: timestamp,
              trigger: entry.trigger,
              result: entry.result,
              diagnostic: entry.diagnostic ?? null,
            }),
          ]
        : [],
    ),
  );
}

function payloadBytes(
  value: SessionMessage | SessionTerminal | SandboxFailureEvent,
): number {
  return encoder.encode(JSON.stringify(value)).byteLength;
}

function cursorFor(generation: string, sequence: number): string {
  const cursor = encodeWoodstockCursor({ generation, sequence });
  if (cursor === undefined) throw unavailable("Session cursor is unavailable");
  return cursor;
}

type MessagePageState = {
  readonly messages: SessionMessage[];
  readonly terminals: SessionTerminal[];
  readonly sandboxFailures: SandboxFailureEvent[];
  bytes: number;
};

function admitProjectedRow(
  state: MessagePageState,
  row: StoredSessionRow,
  maximumMessages: number,
  work: WorkFacts,
  includeSandboxFailures = false,
): boolean {
  const message = projectMessage(row, work);
  const terminal = projectTerminal(row, work);
  const sandboxFailure =
    includeSandboxFailures && row.entry.kind === "sandbox_failure"
      ? row.entry.event
      : undefined;
  if (message !== undefined && state.messages.length === maximumMessages)
    return false;
  const nextBytes =
    (message === undefined ? 0 : payloadBytes(message)) +
    (terminal === undefined ? 0 : payloadBytes(terminal)) +
    (sandboxFailure === undefined ? 0 : payloadBytes(sandboxFailure));
  if (nextBytes > maximumPagePayloadBytes)
    throw unavailable("Session projection entry exceeds the read bound");
  if (state.bytes + nextBytes > maximumPagePayloadBytes) return false;
  if (message !== undefined) state.messages.push(message);
  if (terminal !== undefined) state.terminals.push(terminal);
  if (sandboxFailure !== undefined) state.sandboxFailures.push(sandboxFailure);
  state.bytes += nextBytes;
  return true;
}

function readBoundedPage(
  rows: readonly StoredSessionRow[],
  /** Whether the lineage holds candidate rows beyond `rows`. */
  more: boolean,
  maximumMessages: number,
  work: WorkFacts,
  direction: "forward" | "backward",
  includeSandboxFailures = false,
): Readonly<{
  messages: readonly SessionMessage[];
  terminals: readonly SessionTerminal[];
  sandboxFailures: readonly SandboxFailureEvent[];
  consumed: number;
  hasMore: boolean;
}> {
  const state: MessagePageState = {
    messages: [],
    terminals: [],
    sandboxFailures: [],
    bytes: 0,
  };
  let consumed = 0;
  for (let offset = 0; offset < rows.length; offset += 1) {
    if (consumed === maximumRowsPerPage) break;
    const index = direction === "forward" ? offset : rows.length - 1 - offset;
    const row = rows[index];
    if (
      row === undefined ||
      !admitProjectedRow(
        state,
        row,
        maximumMessages,
        work,
        includeSandboxFailures,
      )
    )
      break;
    consumed += 1;
  }
  if (consumed === 0 && rows.length > 0)
    throw unavailable("Session page cannot make bounded progress");
  const messages =
    direction === "backward" ? [...state.messages].reverse() : state.messages;
  const terminals =
    direction === "backward" ? [...state.terminals].reverse() : state.terminals;
  const sandboxFailures =
    direction === "backward"
      ? [...state.sandboxFailures].reverse()
      : state.sandboxFailures;
  return Object.freeze({
    messages: Object.freeze(messages),
    terminals: Object.freeze(terminals),
    sandboxFailures: Object.freeze(sandboxFailures),
    consumed,
    hasMore: more || consumed < rows.length,
  });
}

function isPromptParts(
  content: UserMessage["content"],
): content is Exclude<UserMessage["content"], string> {
  return Array.isArray(content);
}

function readPendingPrompts(
  owner: SqliteOwner,
  sessionId: string,
): SessionPendingPrompts {
  const { commands: rows, total } = readPendingSessionCommands(
    owner,
    sessionId,
    maximumRowsPerPage + 1,
    "prompts",
  );
  const prompts: SessionPendingPrompts["prompts"][number][] = [];
  let bytes = encoder.encode(
    JSON.stringify({ prompts: [], hasMore: false, total }),
  ).byteLength;
  for (const row of rows) {
    const entry = promptCommand(row.entry);
    if (entry === undefined)
      throw unavailable("Pending Session prompt is unavailable");
    const content = entry.message.content;
    const prompt: SessionPendingPrompts["prompts"][number] = isPromptParts(
      content,
    )
      ? {
          commandId: row.entry.workId,
          text: content
            .filter((part) => part.type === "text")
            .map((part) => part.text)
            .join(""),
          images: content.filter((part) => part.type === "image"),
          delivery: entry.delivery ?? "followUp",
        }
      : {
          commandId: row.entry.workId,
          text: content,
          delivery: entry.delivery ?? "followUp",
        };
    const withMeta =
      row.entry.meta === undefined
        ? prompt
        : { ...prompt, meta: row.entry.meta };
    const attributed =
      row.origin.kind === "extension"
        ? {
            ...withMeta,
            origin: {
              extensionId: row.origin.extensionId,
              correlation: row.origin.correlation,
            },
          }
        : withMeta;
    const size =
      encoder.encode(JSON.stringify(attributed)).byteLength +
      (prompts.length === 0 ? 0 : 1);
    if (
      prompts.length === maximumRowsPerPage ||
      bytes + size > maximumPagePayloadBytes
    )
      return { prompts, hasMore: true, total };
    prompts.push(attributed);
    bytes += size;
  }
  return { prompts, hasMore: false, total };
}

/**
 * The exclusive upper sequence a baseline read pages back from: none for the
 * latest page, or exactly the entry a `before` cursor names. Undefined means
 * the cursor cannot be honored (another lineage, or an entry no longer on
 * the selected path).
 */
function resolveHistoryAnchor(
  owner: SqliteOwner,
  lineage: SessionLineage,
  before: string,
): number | undefined {
  const position = decodeWoodstockCursor(before);
  if (position?.generation !== lineage.rootId) return undefined;
  return lineageHasSequence(owner, lineage, position.sequence)
    ? position.sequence
    : undefined;
}

/**
 * Read one bounded public baseline from the selected Session lineage. Only
 * the page's rows are decoded; `before` pages back through the whole saved
 * history, including what compaction summarized away.
 */
export function readClientSnapshot(
  owner: SqliteOwner,
  sessionId: string,
  options: ClientReadOptions,
  activity: ObservedActivity,
  projectExtensions: () => SessionExtensionSlots = () => ({}),
): SessionSnapshot {
  return owner.transactionSync(() => {
    const pending = readPendingPrompts(owner, sessionId);
    const lineage = readSessionLineage(owner, { sessionId });
    if (lineage === undefined)
      return Object.freeze({
        sessionId,
        cursor: null,
        hasMore: false,
        oldestCursor: null,
        messages: Object.freeze([]),
        terminals: Object.freeze([]),
        compactions: Object.freeze([]),
        compactionNotices: Object.freeze([]),
        pending,
        activity: activity.facts,
        progress: activity.progress,
        extensions: projectExtensions(),
        attention: readSessionAttention(owner, sessionId),
      });

    const generation = lineage.rootId;
    const anchor =
      options.before === undefined
        ? Number.MAX_SAFE_INTEGER
        : resolveHistoryAnchor(owner, lineage, options.before);
    if (anchor === undefined)
      throw unavailable("Session read cursor is unavailable");
    const candidates = readLineageWindow(owner, lineage, {
      before: anchor,
      from: "newest",
      rows: maximumRowsPerPage,
    });
    const page = readBoundedPage(
      candidates.rows,
      candidates.more,
      options.limit,
      pageWorkFacts(owner, sessionId, lineage, candidates.rows),
      "backward",
      options.includeSandboxFailures === true,
    );
    const consumedRows = candidates.rows.slice(
      candidates.rows.length - page.consumed,
    );
    const newestIncluded = candidates.rows.at(-1);
    const oldestIncluded = consumedRows[0];
    const snapshot = {
      sessionId,
      cursor:
        newestIncluded === undefined
          ? null
          : cursorFor(generation, newestIncluded.sequence),
      hasMore: page.hasMore,
      oldestCursor:
        page.hasMore && oldestIncluded !== undefined
          ? cursorFor(generation, oldestIncluded.sequence)
          : null,
      messages: page.messages,
      terminals: page.terminals,
      compactions: compactionMarkersIn(owner, lineage, consumedRows),
      compactionNotices: compactionNoticesIn(consumedRows),
      pending,
      activity: activity.facts,
      progress: options.before === undefined ? activity.progress : null,
      extensions: projectExtensions(),
      attention: readSessionAttention(owner, sessionId),
    };
    return Object.freeze(
      options.includeSandboxFailures === true
        ? { ...snapshot, sandboxFailures: page.sandboxFailures }
        : snapshot,
    );
  });
}

/** One bounded durable replay decision for the process-local observation stream. */
export type ClientReplayPage =
  | Readonly<{
      kind: "page";
      frames: readonly SessionFrame[];
      hasMore: boolean;
    }>
  | Readonly<{ kind: "reset" }>;

const resetReplayPage: ClientReplayPage = Object.freeze({ kind: "reset" });

/** The exclusive lower sequence a replay resumes after; undefined resets. */
function replayStart(
  owner: SqliteOwner,
  lineage: SessionLineage,
  position: ReturnType<typeof decodeWoodstockCursor>,
): number | undefined {
  if (position === undefined) return 0;
  if (position.generation !== lineage.rootId) return undefined;
  return lineageHasSequence(owner, lineage, position.sequence)
    ? position.sequence
    : undefined;
}

function replayFrames(
  owner: SqliteOwner,
  lineage: SessionLineage,
  start: number,
  delivery: "replay" | "live",
  activity: ObservedActivity,
  extensions: SessionExtensionSlots,
  attention: readonly SessionAttention[],
  pending: SessionPendingPrompts,
  includeSandboxFailures: boolean,
): ClientReplayPage {
  const candidates = readLineageWindow(owner, lineage, {
    after: start,
    from: "oldest",
    rows: maximumRowsPerPage,
  });
  const page = readBoundedPage(
    candidates.rows,
    candidates.more,
    replayPageLimit,
    pageWorkFacts(owner, lineage.sessionId, lineage, candidates.rows),
    "forward",
    includeSandboxFailures,
  );
  const consumedRows = candidates.rows.slice(0, page.consumed);
  const observationSequence =
    consumedRows.at(-1)?.sequence ?? lineage.newestSequence;
  const frame = {
    kind: "update" as const,
    delivery,
    cursor: cursorFor(lineage.rootId, observationSequence),
    messages: page.messages,
    terminals: page.terminals,
    compactions: compactionMarkersIn(owner, lineage, consumedRows),
    compactionNotices: compactionNoticesIn(consumedRows),
    pending,
    activity: activity.facts,
    extensions,
    attention,
  };
  const frames: SessionFrame[] = [
    Object.freeze(
      includeSandboxFailures
        ? { ...frame, sandboxFailures: page.sandboxFailures }
        : frame,
    ),
  ];
  if (delivery === "live" && !page.hasMore && activity.progress !== null)
    frames.push(
      Object.freeze({
        kind: "progress",
        progress: activity.progress,
        activity: activity.facts,
      }),
    );
  return Object.freeze({
    kind: "page" as const,
    frames: Object.freeze(frames),
    hasMore: page.hasMore,
  });
}

/** Read strict-after history plus the current waiting-admission view. */
export function readClientReplayPage(
  owner: SqliteOwner,
  sessionId: string,
  after: string | undefined,
  delivery: "replay" | "live",
  activity: ObservedActivity,
  projectExtensions: () => SessionExtensionSlots = () => ({}),
  includeSandboxFailures = false,
): ClientReplayPage {
  const position =
    after === undefined ? undefined : decodeWoodstockCursor(after);
  if (after !== undefined && position === undefined) return resetReplayPage;

  return owner.transactionSync(() => {
    const pending = readPendingPrompts(owner, sessionId);
    const lineage = readSessionLineage(owner, { sessionId });
    if (lineage === undefined) {
      if (after !== undefined) return resetReplayPage;
      return {
        kind: "page",
        frames: [
          {
            kind: "update",
            delivery,
            cursor: null,
            messages: [],
            terminals: [],
            compactions: [],
            compactionNotices: [],
            activity: activity.facts,
            extensions: projectExtensions(),
            attention: readSessionAttention(owner, sessionId),
            pending,
          },
        ],
        hasMore: false,
      };
    }

    const start = replayStart(owner, lineage, position);
    return start === undefined
      ? resetReplayPage
      : replayFrames(
          owner,
          lineage,
          start,
          delivery,
          activity,
          projectExtensions(),
          readSessionAttention(owner, sessionId),
          pending,
          includeSandboxFailures,
        );
  });
}

/** Project one exact command's durable facts without selecting a transcript lineage. */
export function projectCommandSettlement(
  owner: SqliteOwner,
  sessionId: string,
  commandId: string,
): CommandSettlement | undefined {
  const admission = readSessionCommandAdmission(owner, sessionId, commandId);
  if (admission === undefined)
    throw unavailable("Accepted Session command is unavailable");
  const rows = readSessionEntriesForWork(
    owner,
    sessionId,
    admission.entry.workId,
  );
  const work = workFacts(owner, sessionId, rows);
  const terminal = rows
    .flatMap((row) => {
      const value = projectTerminal(row, work);
      return value === undefined ? [] : [value];
    })
    .at(-1);
  if (terminal === undefined) return undefined;
  const answers = rows.flatMap(({ entry }) =>
    entry.kind === "message" && entry.value.role === "assistant"
      ? [entry.value]
      : [],
  );
  const last = answers.at(-1);
  const meta =
    terminal.outcome === "cancelled" ? terminal.meta : admission.entry.meta;
  return {
    terminal: meta === undefined ? terminal : { ...terminal, meta },
    content:
      terminal.outcome === "cancelled"
        ? ""
        : (last?.content
            .flatMap((part) => (part.type === "text" ? [part.text] : []))
            .join("")
            .trim() ?? ""),
    usage: answers.map(({ provider, model, usage }) => ({
      provider,
      model,
      usage,
    })),
    toolCalls: new Set(
      rows.flatMap(({ entry }) =>
        entry.kind === "tool_intent" ? [entry.call.id] : [],
      ),
    ).size,
  };
}
