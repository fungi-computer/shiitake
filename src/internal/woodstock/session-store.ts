import { prepareExtensionStore } from "./extension-store.js";
import {
  prepareSessionDocuments,
  forkDocumentEntries,
} from "./session-documents.js";
import type {
  Message,
  UserMessage,
  Usage,
  MuleResume,
} from "@fungi.computer/mule/effect";
import * as Schema from "effect/Schema";

import type { ModelPreference } from "../model-preference.js";

import type { PromptCommand } from "../../client.js";
import type { AcceptedWork } from "../client-boundary.js";
import type { Json } from "../../extensions.js";
import { sameHostMeta } from "../host-meta.js";
import { ShiitakeError } from "../../errors.js";
import { compactionSummaryMessage } from "../compaction/utils.js";
import { SessionTitleSchema, sessionTitleLimit } from "../client-boundary.js";
import type { SqliteOwner, SqliteRow as RawRow } from "../contracts.js";
import {
  commandTimestamp,
  decodeStoredSessionEntry,
  encodeStoredSessionEntry,
  isCompactCommand,
  commandContextMessage,
  operatorContextMessage,
  promptCommand,
  type CompactionTrigger,
  type OperatorSessionEntry,
  type StoredSessionEntry,
} from "./session-entry.js";
import { prepareSessionAttention } from "./session-attention.js";
import { copySessionGrants, prepareSessionGrants } from "./session-grants.js";
import { prepareSessionInbox, touchSessionInbox } from "./session-inbox.js";
import { sessionIds } from "./identities.js";
import type { VerifiedAgentReply } from "./agent-reply.js";

import {
  recoverAssistantTurn,
  abnormalTerminal,
  contextMessageIds,
} from "./run-recovery.js";
import type { SqliteValue } from "../contracts.js";

const closed = { onExcessProperty: "error" } as const;
const encoder = new TextEncoder();
const boundedId = Schema.String.pipe(
  Schema.check(Schema.isMinLength(1)),
  Schema.check(
    Schema.makeFilter((value) => encoder.encode(value).byteLength <= 256),
  ),
);
const sequence = Schema.Int.pipe(Schema.check(Schema.isGreaterThan(0)));
const timestamp = Schema.Int.pipe(
  Schema.check(Schema.isGreaterThanOrEqualTo(0)),
);
const SessionRowSchema = Schema.Struct({
  id: boundedId,
  leaf_id: Schema.NullOr(boundedId),
});
const SessionCreationReceiptRowSchema = Schema.Struct({
  session_id: boundedId,
});
const SessionSummaryRowSchema = Schema.Struct({
  id: boundedId,
  leaf_id: Schema.NullOr(boundedId),
  created_at: timestamp,
  updated_at: timestamp,
  metadata_json: Schema.NullOr(Schema.String),
});
/**
 * The Session row's display metadata: the user's title, which overrides the
 * derived first-prompt title. `NULL` means no user title.
 */
const SessionMetadataSchema = Schema.fromJsonString(
  Schema.Struct({ title: SessionTitleSchema }),
);
const decodeSessionMetadata = Schema.decodeUnknownResult(
  SessionMetadataSchema,
  closed,
);
const encodeSessionMetadata = Schema.encodeSync(SessionMetadataSchema);
const SessionIdRowSchema = Schema.Struct({ id: boundedId });
const WoodstockSchemaVersionRowSchema = Schema.Struct({
  version: Schema.Int,
  protocol: Schema.Literal("shiitake.extension/v0"),
});
const TableNameRowSchema = Schema.Struct({ name: boundedId });
const EntryRowSchema = Schema.Struct({
  id: boundedId,
  session_id: boundedId,
  parent_id: Schema.NullOr(boundedId),
  sequence,
  type: Schema.String,
  role: Schema.NullOr(Schema.Literals(["user", "assistant", "tool_result"])),
  state: Schema.Literals(["complete", "streaming"]),
  request_id: Schema.NullOr(boundedId),
  work_id: Schema.NullOr(boundedId),
  data_json: Schema.String,
  timestamp,
  completed_at: Schema.NullOr(timestamp),
});
const CommandAdmissionRowSchema = Schema.Struct({
  id: boundedId,
  session_id: boundedId,
  sequence,
  request_id: boundedId,
  work_id: boundedId,
  data_json: Schema.String,
  timestamp,
  origin_kind: Schema.Literals(["user", "extension"]),
  origin_id: Schema.NullOr(boundedId),
  origin_correlation: Schema.NullOr(boundedId),
});
/**
 * Who admitted one accepted command: a human, host or addressed caller
 * (`user`), or one rostered extension with its correlation.
 */
export const ExtensionAdmissionOriginSchema = Schema.Struct({
  kind: Schema.Literal("extension"),
  extensionId: boundedId,
  correlation: Schema.NullOr(boundedId),
});
export const AdmissionOriginSchema = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("user") }),
  ExtensionAdmissionOriginSchema,
]);
/** Durable producer tag stored with every accepted command. */
export type AdmissionOrigin = Schema.Schema.Type<typeof AdmissionOriginSchema>;
const decodeAdmissionOrigin = Schema.decodeUnknownResult(
  AdmissionOriginSchema,
  closed,
);
const equivalentAdmissionOrigin = Schema.toEquivalence(AdmissionOriginSchema);
const admissionColumns = (alias: string) =>
  [
    "id",
    "session_id",
    "sequence",
    "request_id",
    "work_id",
    "data_json",
    "timestamp",
    "origin_kind",
    "origin_id",
    "origin_correlation",
  ]
    .map((column) => alias + column)
    .join(", ");
const NextSequenceRowSchema = Schema.Struct({ next_sequence: sequence });
const EntryPresenceRowSchema = Schema.Struct({ present: Schema.Literal(1) });

type SessionRow = Schema.Schema.Type<typeof SessionRowSchema>;

const decodeSessionRow = Schema.decodeUnknownResult(SessionRowSchema, closed);
const decodeSessionCreationReceiptRow = Schema.decodeUnknownResult(
  SessionCreationReceiptRowSchema,
  closed,
);
const decodeSessionSummaryRow = Schema.decodeUnknownResult(
  SessionSummaryRowSchema,
  closed,
);
const decodeSessionIdRow = Schema.decodeUnknownResult(
  SessionIdRowSchema,
  closed,
);
const decodeWoodstockSchemaVersionRow = Schema.decodeUnknownResult(
  WoodstockSchemaVersionRowSchema,
  closed,
);
const decodeTableNameRow = Schema.decodeUnknownResult(
  TableNameRowSchema,
  closed,
);
const SummaryLineageSchema = Schema.Struct({
  candidate_id: Schema.NullOr(boundedId),
  path_count: Schema.Int.pipe(Schema.check(Schema.isGreaterThan(0))),
  root_count: Schema.Int.pipe(Schema.check(Schema.isGreaterThanOrEqualTo(0))),
  missing_parent_count: Schema.Int.pipe(
    Schema.check(Schema.isGreaterThanOrEqualTo(0)),
  ),
  leaf_state: Schema.Literals(["complete", "streaming"]),
});
const decodeSummaryLineage = Schema.decodeUnknownResult(
  SummaryLineageSchema,
  closed,
);
const decodeEntryRow = Schema.decodeUnknownResult(EntryRowSchema, closed);
const decodeCommandAdmissionRow = Schema.decodeUnknownResult(
  CommandAdmissionRowSchema,
  closed,
);
const decodeModelChoiceRow = Schema.decodeUnknownResult(
  Schema.Struct({ sequence: Schema.NullOr(sequence) }),
  closed,
);
const decodeNextSequenceRow = Schema.decodeUnknownResult(
  NextSequenceRowSchema,
  closed,
);
const decodeEntryPresenceRow = Schema.decodeUnknownResult(
  EntryPresenceRowSchema,
  closed,
);

/** One decoded row in the retained Session entry tree. */
export type StoredSessionRow = Readonly<{
  id: string;
  sessionId: string;
  parentId: string | null;
  sequence: number;
  state: "complete" | "streaming";
  timestamp: number;
  completedAt: number | null;
  entry: StoredSessionEntry;
}>;

/** Persisted conversation facts; run status belongs to the Session's owner. */
export type StoredSessionSummary = Readonly<{
  createdAt: number;
  updatedAt: number;
  title: string;
}>;

/** One persisted conversation's stored facts, or a row that could not be read. */
export type StoredSessionSummaryRow =
  | (StoredSessionSummary & Readonly<{ kind: "session"; sessionId: string }>)
  | Readonly<{ kind: "unavailable"; sessionId: string }>;

/** One page of stored conversation facts, in stable Session identity order. */
export type StoredSessionSummaryPage = Readonly<{
  sessions: readonly StoredSessionSummaryRow[];
  next: string | undefined;
}>;

/**
 * Page stored conversation facts without creating Sessions or opening hosts.
 *
 * A Session whose stored facts cannot be read becomes its own `unavailable`
 * row, so one damaged conversation never hides the others. Run status is not
 * stored here; only the Session's owner can report it.
 */
/** Attach host metadata to an entry only when the host supplied some. */
function withHostMeta<E extends object>(
  entry: E,
  meta: Json | undefined,
): E | (E & Readonly<{ meta: Json }>) {
  if (meta === undefined) return entry;
  return { ...entry, meta };
}

export function readStoredSessionFactPage(
  owner: SqliteOwner,
  after?: string,
): StoredSessionSummaryPage {
  // This is a read-page budget, never a limit on the number of saved Sessions.
  const pageSize = 200;
  const rows = owner.sql
    .exec(
      "SELECT id FROM sessions WHERE (? IS NULL OR id > ?) ORDER BY id LIMIT ?",
      after ?? null,
      after ?? null,
      pageSize,
    )
    .toArray();
  const sessions: StoredSessionSummaryRow[] = [];
  let last: string | undefined;
  for (const row of rows) {
    const decoded = decodeSessionIdRow(row);
    // Without a valid identity the row cannot be named or paged past.
    if (decoded._tag === "Failure")
      throw unavailable("Stored Session identity is invalid");
    const sessionId = decoded.success.id;
    last = sessionId;
    try {
      const summary = readStoredSessionSummary(owner, sessionId);
      if (summary !== undefined)
        sessions.push(
          Object.freeze({ kind: "session", sessionId, ...summary }),
        );
    } catch (error) {
      // Only a typed store/status unavailability is row-local.
      if (!(error instanceof ShiitakeError) || error.cause !== "unavailable")
        throw error;
      sessions.push(Object.freeze({ kind: "unavailable", sessionId }));
    }
  }
  return { sessions, next: rows.length === pageSize ? last : undefined };
}

/**
 * Read one persisted Session's stored title facts without opening its host.
 * This is the one title read: the user's title when set, otherwise the
 * title derived from the first entered prompt.
 */
export function readStoredSessionSummary(
  owner: SqliteOwner,
  sessionId: string,
): StoredSessionSummary | undefined {
  const raw = owner.sql
    .exec(
      `SELECT id, leaf_id, created_at, updated_at, metadata_json
         FROM sessions
        WHERE id = ?`,
      sessionId,
    )
    .toArray()[0];
  if (raw === undefined) return undefined;
  const decoded = decodeSessionSummaryRow(raw);
  if (decoded._tag === "Failure")
    throw unavailable("Stored Session summary row is invalid");
  const derived = readDerivedSessionSummary(owner, sessionId, decoded.success);
  const metadata = decoded.success.metadata_json;
  if (metadata === null) return derived;
  const user = decodeSessionMetadata(metadata);
  if (user._tag === "Failure")
    throw unavailable("Stored Session title is invalid");
  return Object.freeze({
    createdAt: decoded.success.created_at,
    updatedAt: decoded.success.updated_at,
    title: user.success.title,
  });
}

const decodeFirstPromptRow = Schema.decodeUnknownResult(
  Schema.Struct({ id: boundedId, sequence }),
  closed,
);

/**
 * The selected lineage's first delivered prompt, by bounded indexed reads.
 *
 * The Session's earliest delivered prompt is found in sequence order. It is
 * on the selected lineage when the leaf is at or after it and no entry
 * before it has two children: the history before it is then one chain, so
 * every later leaf descends through it. Only a branch taken before the
 * first prompt needs the selected lineage walked.
 */
function firstPromptId(
  owner: SqliteOwner,
  sessionId: string,
  leafId: string,
  leafSequence: number,
): string | null {
  const raw = owner.sql
    .exec(
      `SELECT command.id AS id, command.sequence AS sequence
         FROM session_entries AS command
        WHERE command.session_id = ? AND command.type = 'command'
          AND command.role = 'user' AND command.state = 'complete'
          AND NOT EXISTS (
            SELECT 1 FROM session_entries AS cancelled
             WHERE cancelled.work_id = command.work_id
               AND cancelled.session_id = command.session_id
               AND cancelled.type = 'cancellation'
               AND cancelled.sequence < command.sequence
          )
        ORDER BY command.sequence ASC LIMIT 1`,
      sessionId,
    )
    .toArray()[0];
  if (raw === undefined) return null;
  const decoded = decodeFirstPromptRow(raw);
  if (decoded._tag === "Failure")
    throw unavailable("Stored Session first prompt is invalid");
  const earliest = decoded.success;
  if (leafSequence < earliest.sequence) return null;
  const branched = owner.sql
    .exec(
      `SELECT 1 AS present FROM session_entries AS parent
        WHERE parent.session_id = ? AND parent.sequence < ?
          AND (SELECT COUNT(*) FROM session_entries AS child
                WHERE child.parent_id = parent.id
                  AND child.session_id = parent.session_id
                  AND child.state = 'complete') > 1
        LIMIT 1`,
      sessionId,
      earliest.sequence,
    )
    .toArray()[0];
  return branched === undefined
    ? earliest.id
    : selectedLineageFirstPromptId(owner, sessionId, leafId);
}

/** The first delivered prompt of a lineage that branched before it. */
function selectedLineageFirstPromptId(
  owner: SqliteOwner,
  sessionId: string,
  leafId: string,
): string | null {
  const lineage = owner.sql
    .exec(
      `WITH RECURSIVE branch (id, parent_id, sequence, type, role, state, work_id) AS (
         SELECT id, parent_id, sequence, type, role, state, work_id
           FROM session_entries
          WHERE session_id = ? AND id = ?
         UNION
         SELECT parent.id, parent.parent_id, parent.sequence,
                parent.type, parent.role, parent.state, parent.work_id
           FROM session_entries AS parent
           JOIN branch AS child ON parent.id = child.parent_id
          WHERE parent.session_id = ?
       )
       SELECT
         (SELECT command.id FROM branch AS command
           WHERE command.type = 'command' AND command.role = 'user'
             AND command.state = 'complete'
             AND NOT EXISTS (
               SELECT 1 FROM branch AS cancelled
                WHERE cancelled.type = 'cancellation'
                  AND cancelled.work_id = command.work_id
                  AND cancelled.sequence < command.sequence
             )
           ORDER BY command.sequence ASC LIMIT 1) AS candidate_id,
         (SELECT COUNT(*) FROM branch) AS path_count,
         (SELECT COUNT(*) FROM branch WHERE parent_id IS NULL) AS root_count,
         (SELECT COUNT(*) FROM branch AS child
           WHERE child.parent_id IS NOT NULL
             AND NOT EXISTS (
               SELECT 1 FROM session_entries AS parent
                WHERE parent.session_id = ? AND parent.id = child.parent_id
             )) AS missing_parent_count,
         (SELECT state FROM branch WHERE id = ?) AS leaf_state`,
      sessionId,
      leafId,
      sessionId,
      sessionId,
      leafId,
    )
    .one();
  const lineageDecoded = decodeSummaryLineage(lineage);
  if (lineageDecoded._tag === "Failure")
    throw unavailable("Stored Session summary lineage is invalid");
  if (
    lineageDecoded.success.leaf_state !== "complete" ||
    lineageDecoded.success.root_count !== 1 ||
    lineageDecoded.success.missing_parent_count !== 0
  )
    throw unavailable("Stored Session summary lineage is invalid");
  return lineageDecoded.success.candidate_id;
}

/** The first-prompt title the Session shows when the user has set none. */
function readDerivedSessionSummary(
  owner: SqliteOwner,
  sessionId: string,
  row: Schema.Schema.Type<typeof SessionSummaryRowSchema>,
): StoredSessionSummary | undefined {
  if (row.leaf_id === null)
    return Object.freeze({
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      title: "",
    });

  const leaf = readEntryRow(owner, sessionId, row.leaf_id);
  if (leaf?.state !== "complete")
    throw unavailable("Stored Session summary leaf is invalid");
  const candidateId = firstPromptId(
    owner,
    sessionId,
    row.leaf_id,
    leaf.sequence,
  );
  const candidate =
    candidateId === null
      ? undefined
      : readEntryRow(owner, sessionId, candidateId)?.entry;
  const firstPrompt =
    candidate === undefined ? undefined : promptCommand(candidate);
  if (firstPrompt === undefined) return undefined;
  return Object.freeze({
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    title: Array.from(
      !isPromptParts(firstPrompt.message.content)
        ? firstPrompt.message.content
        : firstPrompt.message.content
            .filter((part) => part.type === "text")
            .map((part) => part.text)
            .join("") || "Image prompt",
    )
      .slice(0, sessionTitleLimit)
      .join(""),
  });
}

/** The inbox title: "" before a first prompt, null when unreadable. */
function inboxTitle(owner: SqliteOwner, sessionId: string): string | null {
  try {
    return readStoredSessionSummary(owner, sessionId)?.title ?? "";
  } catch (error) {
    if (error instanceof ShiitakeError && error.cause === "unavailable")
      return null;
    throw error;
  }
}

export type AppendSessionEntryInput = Readonly<{
  sessionId: string;
  id: string;
  entry: StoredSessionEntry;
  state?: "complete" | "streaming";
}>;

export type CompleteSessionLeafInput = Readonly<{
  sessionId: string;
  id: string;
  entry: Extract<StoredSessionEntry, { kind: "message" }>;
}>;

export type ReadSelectedPathInput = Readonly<{
  sessionId: string;
  leafId?: string;
}>;

export type SessionQueueHead =
  | Readonly<{ state: "idle" }>
  | Readonly<{
      state: "queued" | "running";
      jobId: string;
      cancellationRequested: boolean;
    }>;

export type PrepareSessionRunInput = Readonly<{
  sessionId: string;
  commandFactId: string;
}>;

export type PreparedSessionRun = Readonly<{
  command: StoredSessionRow &
    Readonly<{
      entry: Extract<StoredSessionEntry, { kind: "command" }>;
    }>;
  modelPreference: ModelPreference | null;
  messages: readonly Message[];
  /**
   * Context rows, not the whole lineage: the latest compaction's kept window
   * through this work, after the few older facts that still decide it.
   */
  path: readonly StoredSessionRow[];
  entryDelivery: "new" | "replay";
  resume: MuleResume | undefined;
  abnormalTerminal: "failed" | "cancelled" | undefined;
}>;

export type StoredSessionCommandEntry = Extract<
  StoredSessionEntry,
  { kind: "command" }
>;

/** One stable private command admission before conversation entry. */
export type StoredCommandAdmission = Readonly<{
  id: string;
  sessionId: string;
  /** Durable acceptance-fact order; never a conversation-entry sequence. */
  sequence: number;
  timestamp: number;
  entry: StoredSessionCommandEntry;
  /** Producer that admitted this command through the accepted-command door. */
  origin: AdmissionOrigin;
}>;

export type RunContract = StoredSessionCommandEntry["contract"];

export type AppendCompactionInput = Readonly<{
  resumesRun?: boolean;
  trigger: CompactionTrigger;
  /** Estimated context tokens once this compaction is the boundary. */
  tokensAfter: number;
  sessionId: string;
  expectedLeafId: string;
  workId: string;
  firstKeptEntryId: string;
  summary: string;
  tokensBefore: number;
  usage: Usage;
  contextRevision: string;
  details?: Readonly<{
    readFiles: readonly string[];
    modifiedFiles: readonly string[];
  }>;
}>;

/** Private owner identities retained after one durable acceptance commit. */
export type AcceptedCommand = Readonly<{
  sessionId: string;
  requestId: string;
  commandId: string;
}>;

export type AcceptedSessionCommand = Readonly<{
  accepted: AcceptedCommand;
  command: StoredCommandAdmission;
  delivery: "new" | "replay";
}>;

export type AcceptedSessionCancellation = Readonly<{
  accepted: Readonly<{ commandId: string; entryId: string }>;
  delivery: "new" | "replay";
  /** True when the target is the live Watchdog execution. */
  active: boolean;
}>;

export type EnqueueAcceptedWork = (
  work: Readonly<{
    jobId: string;
    sessionId: string;
    commandFactId: string;
  }>,
) => void;

export type RecordAcceptedCancellation = (
  input: Readonly<{
    jobId: string;
    cancellationId: string;
    onlyIfPending?: boolean;
  }>,
) => Readonly<{ status: "recorded" | "replayed" | "conflict" | "missing" }>;

export type SessionTreeNode = Readonly<{
  row: StoredSessionRow;
  children: readonly SessionTreeNode[];
}>;
type MutableSessionTreeNode = {
  row: StoredSessionRow;
  children: MutableSessionTreeNode[];
};

export type SessionTreeInput = Readonly<{ sessionId: string }>;
export type SelectSessionBranchInput = Readonly<{
  sessionId: string;
  entryId: string;
  assertIdleSession?: (queue: string) => void;
}>;
export type ForkSessionAtInput = SelectSessionBranchInput;

function unavailable(message: string): ShiitakeError {
  return new ShiitakeError("store", "unavailable", message);
}

function incompatible(message: string): ShiitakeError {
  return new ShiitakeError("store", "incompatible", message);
}

function parseSessionRow(raw: RawRow): SessionRow {
  const decoded = decodeSessionRow(raw);
  if (decoded._tag === "Failure")
    throw unavailable("Stored Session row is invalid");
  return decoded.success;
}

function requestIdFor(entry: StoredSessionEntry): string | null {
  return entry.kind === "command" ||
    entry.kind === "cancellation" ||
    entry.kind === "operator"
    ? entry.requestId
    : null;
}

function roleFor(
  entry: StoredSessionEntry,
): "assistant" | "system" | "tool_result" | "user" | null {
  // A compaction request is accepted work, not a user message.
  if (entry.kind === "command") return isCompactCommand(entry) ? null : "user";
  if (entry.kind === "operator")
    return entry.author === "party" ? "user" : "assistant";
  if (entry.kind === "message_start")
    return entry.role === "toolResult" ? "tool_result" : entry.role;
  if (entry.kind !== "message") return null;
  return entry.value.role === "toolResult" ? "tool_result" : entry.value.role;
}

type MessageStartEntry = Extract<StoredSessionEntry, { kind: "message_start" }>;
type CausalIdentity = MessageStartEntry["message"];

function sameCausalIdentity(
  left: CausalIdentity,
  right: CausalIdentity,
): boolean {
  return (
    left.kind === right.kind &&
    left.value === right.value &&
    left.parent?.kind === right.parent?.kind &&
    left.parent?.value === right.parent?.value
  );
}

function parseEntryRow(raw: RawRow): StoredSessionRow {
  const decodedRow = decodeEntryRow(raw);
  if (decodedRow._tag === "Failure")
    throw unavailable("Stored Session entry row is invalid");

  const row = decodedRow.success;
  const decodedEntry = decodeStoredSessionEntry(row.data_json);
  if (decodedEntry._tag === "Failure")
    throw unavailable("Stored Session entry data is invalid");

  const entry = decodedEntry.success;
  if (
    row.type !== entry.kind ||
    row.work_id !== entry.workId ||
    row.request_id !== requestIdFor(entry) ||
    row.role !== roleFor(entry)
  )
    throw unavailable("Stored Session entry columns disagree with its data");

  return Object.freeze({
    id: row.id,
    sessionId: row.session_id,
    parentId: row.parent_id,
    sequence: row.sequence,
    state: row.state,
    timestamp: row.timestamp,
    completedAt: row.completed_at,
    entry,
  });
}

function parseCommandAdmissionRow(raw: RawRow): StoredCommandAdmission {
  const decodedRow = decodeCommandAdmissionRow(raw);
  if (decodedRow._tag === "Failure")
    throw unavailable("Stored command admission row is invalid");

  const row = decodedRow.success;
  const decodedEntry = decodeStoredSessionEntry(row.data_json);
  if (
    decodedEntry._tag === "Failure" ||
    decodedEntry.success.kind !== "command"
  )
    throw unavailable("Stored command admission data is invalid");
  const entry = decodedEntry.success;
  if (
    row.request_id !== entry.requestId ||
    row.work_id !== entry.workId ||
    row.id !== entry.receiptId ||
    row.id !== entry.workId ||
    row.timestamp !== commandTimestamp(entry)
  )
    throw unavailable(
      "Stored command admission columns disagree with its data",
    );

  const origin = decodeAdmissionOrigin(
    row.origin_kind === "user"
      ? { kind: "user" }
      : {
          kind: "extension",
          extensionId: row.origin_id,
          correlation: row.origin_correlation,
        },
  );
  if (
    origin._tag === "Failure" ||
    (row.origin_kind === "user" &&
      (row.origin_id !== null || row.origin_correlation !== null))
  )
    throw unavailable("Stored command admission origin is invalid");

  return Object.freeze({
    id: row.id,
    sessionId: row.session_id,
    sequence: row.sequence,
    timestamp: row.timestamp,
    entry,
    origin: origin.success,
  });
}

/** Bounded existence check; admission conditions never need transcript history. */
export function hasStoredSession(
  owner: SqliteOwner,
  sessionId: string,
): boolean {
  return readSessionRow(owner, sessionId) !== undefined;
}

function readSessionRow(
  owner: SqliteOwner,
  sessionId: string,
): SessionRow | undefined {
  const raw = owner.sql
    .exec("SELECT id, leaf_id FROM sessions WHERE id = ?", sessionId)
    .toArray()[0];
  return raw === undefined ? undefined : parseSessionRow(raw);
}

function readEntryRow(
  owner: SqliteOwner,
  sessionId: string,
  id: string,
): StoredSessionRow | undefined {
  const raw = owner.sql
    .exec(
      `SELECT id, session_id, parent_id, sequence, type, role, state,
              request_id, work_id, data_json, timestamp, completed_at
         FROM session_entries
        WHERE session_id = ? AND id = ?`,
      sessionId,
      id,
    )
    .toArray()[0];
  return raw === undefined ? undefined : parseEntryRow(raw);
}

function validateCompleteSessionLeaf(
  owner: SqliteOwner,
  session: SessionRow,
): StoredSessionRow | undefined {
  if (session.leaf_id === null) {
    const raw = owner.sql
      .exec(
        `SELECT 1 AS present
           FROM session_entries
          WHERE session_id = ?
          LIMIT 1`,
        session.id,
      )
      .toArray()[0];
    if (raw === undefined) return undefined;
    const decoded = decodeEntryPresenceRow(raw);
    if (decoded._tag === "Failure")
      throw unavailable("Session entry presence is invalid");
    throw unavailable("Session leaf is invalid");
  }
  const leaf = readEntryRow(owner, session.id, session.leaf_id);
  if (leaf?.state !== "complete") throw unavailable("Session leaf is invalid");
  return leaf;
}

/** Read one retained Session entry by its globally unique row identity. */
export function readSessionEntryById(
  owner: SqliteOwner,
  id: string,
): StoredSessionRow | undefined {
  const raw = owner.sql
    .exec(
      `SELECT id, session_id, parent_id, sequence, type, role, state,
              request_id, work_id, data_json, timestamp, completed_at
         FROM session_entries
        WHERE id = ?`,
      id,
    )
    .toArray()[0];
  return raw === undefined ? undefined : parseEntryRow(raw);
}

function readCommandAdmission(
  owner: SqliteOwner,
  sessionId: string,
  lookup:
    | Readonly<{ id: string }>
    | Readonly<{ requestId: string }>
    | Readonly<{ workId: string }>,
): StoredCommandAdmission | undefined {
  const [column, value] =
    "id" in lookup
      ? (["id", lookup.id] as const)
      : "requestId" in lookup
        ? (["request_id", lookup.requestId] as const)
        : (["work_id", lookup.workId] as const);
  const admitted = owner.sql
    .exec(
      `SELECT ${admissionColumns("")}
         FROM session_command_admissions
        WHERE session_id = ? AND ${column} = ?`,
      sessionId,
      value,
    )
    .toArray()[0];
  return admitted === undefined
    ? undefined
    : parseCommandAdmissionRow(admitted);
}

const pendingAdmissions = `FROM session_command_admissions AS a
  WHERE a.session_id = ?
    AND NOT EXISTS (
      SELECT 1 FROM session_entries AS e
       WHERE e.session_id = a.session_id AND e.work_id = a.work_id
         AND e.type IN ('command', 'cancellation', 'run_terminal', 'work_terminal')
    )`;
const decodePendingCount = Schema.decodeUnknownSync(
  Schema.Struct({
    total: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  }),
);

/** Read waiting admission facts without consulting or recreating scheduler order. */
export function readPendingSessionCommands(
  owner: SqliteOwner,
  sessionId: string,
  limit: number,
  only: "all" | "prompts" = "all",
) {
  // A waiting compaction request is accepted work but never a queued prompt.
  const filter =
    only === "prompts" ? "AND json_extract(a.data_json, '$.work') IS NULL" : "";
  const total = decodePendingCount(
    owner.sql
      .exec(
        `SELECT COUNT(*) AS total ${pendingAdmissions} ${filter}`,
        sessionId,
      )
      .one(),
  ).total;
  const commands = owner.sql
    .exec(
      `SELECT ${admissionColumns("a.")} ${pendingAdmissions} ${filter}
     ORDER BY a.sequence ASC LIMIT ?`,
      sessionId,
      limit,
    )
    .toArray()
    .map(parseCommandAdmissionRow);
  return { commands, total };
}

/** Read accepted steer/follow-up controls that remain waiting for delivery. */
function readPendingSessionControls(
  owner: SqliteOwner,
  sessionId: string,
  delivery: "steer" | "followUp",
  targetWorkId?: string,
  limit = 200,
): readonly StoredCommandAdmission[] {
  return owner.sql
    .exec(
      `SELECT ${admissionColumns("a.")}
       FROM session_command_admissions AS a
      WHERE a.session_id = ?
        AND json_extract(a.data_json, '$.delivery') = ?
        AND (? IS NULL OR json_extract(a.data_json, '$.targetWorkId') = ?)
        AND NOT EXISTS (
          SELECT 1 FROM session_entries AS e
           WHERE e.session_id = a.session_id AND e.work_id = a.work_id
             AND e.type IN ('command', 'run_terminal', 'work_terminal')
        )
      ORDER BY a.sequence ASC LIMIT ?`,
      sessionId,
      delivery,
      targetWorkId ?? null,
      targetWorkId ?? null,
      limit,
    )
    .toArray()
    .map(parseCommandAdmissionRow);
}

/** Observe one pending steer without materializing or consuming its admission. */
export function hasPendingSessionSteer(
  owner: SqliteOwner,
  sessionId: string,
  workId: string,
): boolean {
  return (
    readPendingSessionControls(owner, sessionId, "steer", workId, 1).length > 0
  );
}

/** Materialize controls at one Mule safe point exactly once. */
export function takePendingSessionControls(
  owner: SqliteOwner,
  sessionId: string,
  delivery: "steer" | "followUp",
  targetWorkId: string,
): readonly Readonly<{
  workId: string;
  requestId: string;
  message: Message;
}>[] {
  return owner.transactionSync(() =>
    readPendingSessionControls(owner, sessionId, delivery, targetWorkId).map(
      (admission) => {
        const existing = readEntryRow(owner, sessionId, admission.id);
        const control = promptCommand(
          existing?.entry ??
            appendSessionEntry(owner, {
              sessionId,
              id: admission.id,
              entry: admission.entry,
            }).entry,
        );
        if (control === undefined)
          throw unavailable("Session control admission is invalid");
        return {
          workId: control.workId,
          requestId: control.requestId,
          message: commandContextMessage(control),
        };
      },
    ),
  );
}

/** Settle controls delivered by one parent run, exactly once per request. */
export function settleSessionControls(
  owner: SqliteOwner,
  input: Readonly<{
    sessionId: string;
    parentWorkId: string;
    outcome: "completed" | "cancelled" | "failed" | "interrupted";
    enqueue: EnqueueAcceptedWork;
  }>,
): void {
  owner.transactionSync(() => {
    const rows = owner.sql
      .exec(
        `SELECT ${admissionColumns("a.")}
           FROM session_command_admissions AS a
          WHERE a.session_id = ?
            AND json_extract(a.data_json, '$.delivery') = 'steer'
            AND json_extract(a.data_json, '$.targetWorkId') = ?
          ORDER BY a.sequence ASC`,
        input.sessionId,
        input.parentWorkId,
      )
      .toArray()
      .map(parseCommandAdmissionRow);
    for (const row of rows) {
      const alreadySettled = owner.sql
        .exec(
          "SELECT 1 AS present FROM session_entries WHERE session_id = ? AND work_id = ? AND type IN ('run_terminal', 'work_terminal') LIMIT 1",
          input.sessionId,
          row.entry.workId,
        )
        .toArray()[0];
      if (alreadySettled !== undefined) continue;
      const materialized = readEntryRow(owner, input.sessionId, row.id);
      if (materialized === undefined) {
        input.enqueue({
          jobId: row.entry.workId,
          sessionId: input.sessionId,
          commandFactId: row.id,
        });
        continue;
      }
      appendSessionEntry(owner, {
        sessionId: input.sessionId,
        id: sessionIds.fact.generate(),
        entry: {
          kind: "work_terminal",
          workId: row.entry.workId,
          outcome: input.outcome,
        },
      });
    }
  });
}

/** Read one stable private command admission by accepted work identity. */
export function readSessionCommandAdmission(
  owner: SqliteOwner,
  sessionId: string,
  id: string,
): StoredCommandAdmission | undefined {
  return readCommandAdmission(owner, sessionId, { id });
}

/** Read one command admission by its accepted work identity. */
function readSessionCommandAdmissionByWorkId(
  owner: SqliteOwner,
  sessionId: string,
  workId: string,
): StoredCommandAdmission | undefined {
  return readCommandAdmission(owner, sessionId, { workId });
}

/** Read one stable private command admission by accepted request identity. */
export function readSessionCommandAdmissionByRequest(
  owner: SqliteOwner,
  sessionId: string,
  requestId: string,
): StoredCommandAdmission | undefined {
  return readCommandAdmission(owner, sessionId, { requestId });
}

/** Read entries correlated to one accepted work identity in Session order. */
export function readSessionEntriesForWork(
  owner: SqliteOwner,
  sessionId: string,
  workId: string,
): readonly StoredSessionRow[] {
  return Object.freeze(
    owner.sql
      .exec(
        `SELECT id, session_id, parent_id, sequence, type, role, state,
                request_id, work_id, data_json, timestamp, completed_at
           FROM session_entries
          WHERE session_id = ? AND work_id = ?
          ORDER BY sequence ASC`,
        sessionId,
        workId,
      )
      .toArray()
      .map(parseEntryRow),
  );
}

/** Read the latest retained live-retry event for one accepted work item. */
export function readLatestLiveRetryForWork(
  owner: SqliteOwner,
  sessionId: string,
  workId: string,
):
  | Readonly<{
      event: Extract<StoredSessionEntry, { kind: "live_retry" }>["event"];
      timestamp: number;
    }>
  | undefined {
  const raw = owner.sql
    .exec(
      `SELECT id, session_id, parent_id, sequence, type, role, state,
              request_id, work_id, data_json, timestamp, completed_at
         FROM session_entries
        WHERE session_id = ? AND work_id = ? AND type = 'live_retry'
        ORDER BY sequence DESC
        LIMIT 1`,
      sessionId,
      workId,
    )
    .toArray()[0];
  if (raw === undefined) return undefined;
  const row = parseEntryRow(raw);
  if (row.entry.kind !== "live_retry")
    throw unavailable("Stored live retry entry is invalid");
  return { event: row.entry.event, timestamp: row.timestamp };
}

/** An indexed existence read: recovery errors never acknowledge successful delivery. */
export function hasSuccessfulToolResult(
  owner: SqliteOwner,
  sessionId: string,
  workId: string,
  toolCallId: string,
): boolean {
  return (
    owner.sql
      .exec(
        `SELECT 1 FROM session_entries INDEXED BY idx_session_entries_successful_tool
     WHERE session_id = ? AND work_id = ? AND type = 'tool_outcome'
       AND json_extract(data_json, '$.result.isError') = 0
       AND json_extract(data_json, '$.call.id') = ? LIMIT 1`,
        sessionId,
        workId,
        toolCallId,
      )
      .toArray().length > 0
  );
}

/** Most recent tool intent without a matching outcome for the running work. */
export function readActiveToolForWork(
  owner: SqliteOwner,
  sessionId: string,
  workId: string,
): string | undefined {
  const raw = owner.sql
    .exec(
      `SELECT intent.id, intent.session_id, intent.parent_id,
              intent.sequence, intent.type, intent.role, intent.state,
              intent.request_id, intent.work_id, intent.data_json,
              intent.timestamp, intent.completed_at
         FROM session_entries AS intent
        WHERE intent.session_id = ? AND intent.work_id = ?
          AND intent.type = 'tool_intent'
          AND NOT EXISTS (
            SELECT 1 FROM session_entries AS outcome
             WHERE outcome.session_id = intent.session_id
               AND outcome.work_id = intent.work_id
               AND outcome.type = 'tool_outcome'
               AND json_extract(outcome.data_json, '$.toolCall.value') =
                   json_extract(intent.data_json, '$.toolCall.value')
          )
        ORDER BY intent.sequence DESC
        LIMIT 1`,
      sessionId,
      workId,
    )
    .toArray()[0];
  if (raw === undefined) return undefined;
  const row = parseEntryRow(raw);
  if (row.entry.kind !== "tool_intent")
    throw unavailable("Stored tool intent is invalid");
  return row.entry.call.name;
}

function nextSequence(owner: SqliteOwner, sessionId: string): number {
  const raw = owner.sql
    .exec(
      `SELECT COALESCE(MAX(sequence), 0) + 1 AS next_sequence
         FROM session_entries
        WHERE session_id = ?`,
      sessionId,
    )
    .one();
  const decoded = decodeNextSequenceRow(raw);
  if (decoded._tag === "Failure")
    throw unavailable("Next Session sequence is invalid");
  return decoded.success.next_sequence;
}

function nextAdmissionSequence(owner: SqliteOwner, sessionId: string): number {
  const raw = owner.sql
    .exec(
      `SELECT COALESCE(MAX(sequence), 0) + 1 AS next_sequence
         FROM (
           SELECT sequence
             FROM session_command_admissions
            WHERE session_id = ?
           UNION ALL
           SELECT sequence
             FROM session_entries
            WHERE session_id = ? AND type = 'command'
         )`,
      sessionId,
      sessionId,
    )
    .one();
  const decoded = decodeNextSequenceRow(raw);
  if (decoded._tag === "Failure")
    throw unavailable("Next command admission sequence is invalid");
  return decoded.success.next_sequence;
}

function ensureSession(owner: SqliteOwner, sessionId: string): SessionRow {
  const existing = readSessionRow(owner, sessionId);
  if (existing !== undefined) return existing;

  const now = Date.now();
  owner.sql.exec(
    `INSERT INTO sessions
       (id, cwd, parent_session, leaf_id, created_at, updated_at, metadata_json)
     VALUES (?, '/', NULL, NULL, ?, ?, NULL)`,
    sessionId,
    now,
    now,
  );
  touchSessionInbox(owner, { sessionId, activityAt: now, title: "" });
  const created = readSessionRow(owner, sessionId);
  if (created === undefined)
    throw unavailable("Created Session row is unavailable");
  return created;
}

/** Reserve or replay a durable Session identity before its first prompt. */
export function createStoredSession(
  owner: SqliteOwner,
  requestId: string,
  /** Refuses a new Session under storage pressure; replays skip it. */
  admitNew: () => void,
): string {
  return owner.transactionSync(() => {
    const existing = owner.sql
      .exec(
        "SELECT session_id FROM session_creation_receipts WHERE request_id = ?",
        requestId,
      )
      .toArray();
    const first = existing[0];
    if (first !== undefined) {
      const decoded = decodeSessionCreationReceiptRow(first);
      if (decoded._tag === "Failure")
        throw unavailable("Session creation receipt is invalid");
      return decoded.success.session_id;
    }

    admitNew();
    const sessionId = sessionIds.session.generate();
    ensureSession(owner, sessionId);
    owner.sql.exec(
      "INSERT INTO session_creation_receipts (request_id, session_id) VALUES (?, ?)",
      requestId,
      sessionId,
    );
    return sessionId;
  });
}

/**
 * Permanently remove one idle Session: its entries, command admissions,
 * creation receipt and inbox row, so a retried creation request reserves a
 * fresh Session.
 * Returns false when the Session does not exist.
 */
export function deleteStoredSession(
  owner: SqliteOwner,
  input: Readonly<{
    sessionId: string;
    assertIdleSession: (queue: string) => void;
    recordInboxDeletion: (sessionId: string) => void;
  }>,
): boolean {
  return owner.transactionSync(() => {
    if (readSessionRow(owner, input.sessionId) === undefined) return false;
    input.assertIdleSession(input.sessionId);
    input.recordInboxDeletion(input.sessionId);
    for (const statement of [
      "DELETE FROM session_entries WHERE session_id = ?",
      "DELETE FROM session_command_admissions WHERE session_id = ?",
      // The inbox row references the Session; it goes in the same transaction.
      "DELETE FROM session_attention WHERE session_id = ?",
      "DELETE FROM session_tool_grants WHERE session_id = ?",
      "DELETE FROM session_inbox WHERE session_id = ?",
      "DELETE FROM session_creation_receipts WHERE session_id = ?",
      "DELETE FROM sessions WHERE id = ?",
    ])
      owner.sql.exec(statement, input.sessionId);
    return true;
  });
}

/** The exact Woodstock SQL shape this build reads and writes. */
const currentWoodstockSchemaVersion = 1;
const currentWoodstockProtocol = "shiitake.extension/v0";

function tableExists(owner: SqliteOwner, name: string): boolean {
  const rows = owner.sql
    .exec(
      "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
      name,
    )
    .toArray();
  const raw = rows[0];
  if (raw === undefined) return false;
  const decoded = decodeTableNameRow(raw);
  if (decoded._tag === "Failure")
    throw unavailable("Store catalog row is invalid");
  return true;
}

/** Read only the schema marker. A host that owns a file decides how to preserve it. */
function readWoodstockStoreProtocol(
  owner: SqliteOwner,
): "fresh" | "v0" | "unmarked" {
  if (!tableExists(owner, "woodstock_schema"))
    return tableExists(owner, "sessions") ? "unmarked" : "fresh";
  const columns = owner.sql
    .exec("PRAGMA table_info(woodstock_schema)")
    .toArray();
  const hasProtocol = columns.some((row) => row.name === "protocol");
  if (!hasProtocol) return "unmarked";
  const rows = owner.sql
    .exec("SELECT version, protocol FROM woodstock_schema")
    .toArray();
  if (rows.length !== 1) return "unmarked";
  const decoded = decodeWoodstockSchemaVersionRow(rows[0]);
  return decoded._tag === "Success" &&
    decoded.success.version === currentWoodstockSchemaVersion
    ? "v0"
    : "unmarked";
}

/** Stamp fresh Woodstock stores; incompatible files require host-side custody. */
function ensureWoodstockSchemaVersion(owner: SqliteOwner): void {
  const protocol = readWoodstockStoreProtocol(owner);
  if (protocol === "v0") return;
  if (protocol === "unmarked")
    throw incompatible(
      "Woodstock store has no v0 protocol marker; a hosted store never resets itself — reset the owning Durable Object storage, or remove the old local store file before opening a fresh one",
    );
  owner.sql.exec(
    "CREATE TABLE woodstock_schema (version INTEGER NOT NULL, protocol TEXT NOT NULL)",
  );
  owner.sql.exec(
    "INSERT INTO woodstock_schema (version, protocol) VALUES (?, ?)",
    currentWoodstockSchemaVersion,
    currentWoodstockProtocol,
  );
}

function createSessionEntryTable(owner: SqliteOwner): void {
  owner.sql.exec(`
    CREATE TABLE IF NOT EXISTS session_entries (
      id TEXT PRIMARY KEY,
      session_id TEXT NOT NULL,
      parent_id TEXT,
      sequence INTEGER NOT NULL,
      type TEXT NOT NULL,
      role TEXT,
      state TEXT NOT NULL CHECK (state IN ('complete', 'streaming')),
      request_id TEXT,
      work_id TEXT,
      data_json TEXT NOT NULL,
      timestamp INTEGER NOT NULL,
      completed_at INTEGER,
      UNIQUE (session_id, sequence)
    )
  `);
}

/** Create the retained Session schema and private creation/admission receipts. */
export function prepareSessionStore(owner: SqliteOwner): void {
  owner.transactionSync(() => {
    ensureWoodstockSchemaVersion(owner);
    owner.sql.exec(`
    CREATE TABLE IF NOT EXISTS sessions (
      id TEXT PRIMARY KEY,
      cwd TEXT NOT NULL,
      parent_session TEXT,
      leaf_id TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      metadata_json TEXT
    )
  `);
    createSessionEntryTable(owner);
    prepareSessionDocuments(owner);
    prepareSessionAttention(owner);
    prepareSessionGrants(owner);
    prepareSessionInbox(owner);
    owner.sql.exec(`
    CREATE TABLE IF NOT EXISTS session_creation_receipts (
      request_id TEXT PRIMARY KEY,
      session_id TEXT NOT NULL UNIQUE REFERENCES sessions(id)
    )
  `);
    owner.sql.exec(
      "CREATE INDEX IF NOT EXISTS idx_session_entries_parent ON session_entries(parent_id)",
    );
    owner.sql.exec(
      "CREATE INDEX IF NOT EXISTS idx_session_entries_session ON session_entries(session_id, sequence)",
    );
    owner.sql.exec(
      "CREATE INDEX IF NOT EXISTS idx_session_entries_streaming ON session_entries(session_id, state)",
    );
    owner.sql.exec(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_session_entries_request
       ON session_entries(session_id, request_id)
       WHERE request_id IS NOT NULL`,
    );
    owner.sql.exec(
      "CREATE INDEX IF NOT EXISTS idx_session_entries_work ON session_entries(work_id, sequence)",
    );
    owner.sql
      .exec(`CREATE INDEX IF NOT EXISTS idx_session_entries_successful_tool
      ON session_entries(session_id, work_id, json_extract(data_json, '$.call.id'))
      WHERE type = 'tool_outcome' AND json_extract(data_json, '$.result.isError') = 0`);
    owner.sql.exec(`
    CREATE TABLE IF NOT EXISTS session_command_admissions (
      id TEXT PRIMARY KEY,
      session_id TEXT NOT NULL,
      sequence INTEGER NOT NULL,
      request_id TEXT NOT NULL,
      work_id TEXT NOT NULL,
      data_json TEXT NOT NULL,
      timestamp INTEGER NOT NULL,
      origin_kind TEXT NOT NULL CHECK (origin_kind IN ('user', 'extension')),
      origin_id TEXT,
      origin_correlation TEXT,
      CHECK ((origin_kind = 'user') = (origin_id IS NULL)),
      CHECK (origin_kind = 'extension' OR origin_correlation IS NULL),
      -- This orders durable acceptance facts, not conversation entries.
      UNIQUE (session_id, sequence),
      UNIQUE (session_id, request_id),
      UNIQUE (session_id, work_id)
    )
  `);
  });
  prepareExtensionStore(owner);
}

/** Read the nullable preference at exactly this selected lineage. */
export function readSessionModelPreference(
  owner: SqliteOwner,
  sessionId: string,
): ModelPreference | null {
  return lineageModelPreference(
    owner,
    readSessionLineage(owner, { sessionId }),
  );
}

/**
 * Woodstock order of the newest model choice on the selected lineage, or null.
 * Clients compare it with terminal orders; Woodstock assigns it no meaning.
 */
export function readLastModelChoiceSequence(
  owner: SqliteOwner,
  sessionId: string,
): number | null {
  const leaf = readSessionRow(owner, sessionId)?.leaf_id;
  if (leaf === undefined || leaf === null) return null;
  const raw = owner.sql
    .exec(
      `WITH RECURSIVE branch (id, parent_id, sequence, type, state) AS (
         SELECT id, parent_id, sequence, type, state
           FROM session_entries
          WHERE session_id = ? AND id = ?
         UNION
         SELECT parent.id, parent.parent_id, parent.sequence, parent.type,
                parent.state
           FROM session_entries AS parent
           JOIN branch AS child ON parent.id = child.parent_id
          WHERE parent.session_id = ?
       )
       SELECT MAX(sequence) AS sequence
         FROM branch
        WHERE type = 'model_preference' AND state != 'streaming'`,
      sessionId,
      leaf,
      sessionId,
    )
    .one();
  const decoded = decodeModelChoiceRow(raw);
  if (decoded._tag === "Failure")
    throw unavailable("Stored model choice order is invalid");
  return decoded.success.sequence;
}

/** Append a decoded preference request; no command or scheduler work is minted. */
export function setSessionModelPreference(
  owner: SqliteOwner,
  sessionId: string,
  preference: ModelPreference | null,
): StoredSessionRow {
  return owner.transactionSync(() =>
    appendSessionEntry(owner, {
      sessionId,
      id: sessionIds.fact.generate(),
      entry: { kind: "model_preference", workId: null, preference },
    }),
  );
}

/**
 * Set or clear the user's Session title. Display metadata only: no entry is
 * appended, so history, the selected leaf and recency are unchanged. Setting
 * the same value again is a no-op write.
 */
export function setSessionTitle(
  owner: SqliteOwner,
  sessionId: string,
  title: string | null,
): void {
  owner.transactionSync(() => {
    ensureSession(owner, sessionId);
    owner.sql.exec(
      "UPDATE sessions SET metadata_json = ? WHERE id = ?",
      title === null ? null : encodeSessionMetadata({ title }),
      sessionId,
    );
    touchSessionInbox(owner, {
      sessionId,
      title: inboxTitle(owner, sessionId),
    });
  });
}

/**
 * The retained append word.
 *
 * Complete entries advance the selected leaf. A streaming message placeholder
 * is durable but deliberately leaves the selected branch unchanged.
 */
export function appendSessionEntry(
  owner: SqliteOwner,
  input: AppendSessionEntryInput,
): StoredSessionRow {
  const session = ensureSession(owner, input.sessionId);
  const leaf = validateCompleteSessionLeaf(owner, session);
  const state = input.state ?? "complete";
  if ((state === "streaming") !== (input.entry.kind === "message_start"))
    throw unavailable("A message start must be the streaming entry");
  if (
    state === "streaming" &&
    input.entry.kind === "message_start" &&
    input.id !== input.entry.message.value
  )
    throw unavailable("Message placeholder identity does not match its row");

  const now = Date.now();
  const entrySequence = nextSequence(owner, input.sessionId);
  owner.sql.exec(
    `INSERT INTO session_entries
       (id, session_id, parent_id, sequence, type, role, state,
        request_id, work_id, data_json, timestamp, completed_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    input.id,
    input.sessionId,
    leaf?.id ?? null,
    entrySequence,
    input.entry.kind,
    roleFor(input.entry),
    state,
    requestIdFor(input.entry),
    input.entry.workId,
    encodeStoredSessionEntry(input.entry),
    now,
    state === "complete" ? now : null,
  );

  if (state === "complete") {
    owner.sql.exec(
      "UPDATE sessions SET leaf_id = ?, updated_at = ? WHERE id = ?",
      input.id,
      now,
      input.sessionId,
    );
    // Only an entering prompt can change the first-prompt title.
    touchSessionInbox(
      owner,
      input.entry.kind === "command"
        ? {
            sessionId: input.sessionId,
            activityAt: now,
            title: inboxTitle(owner, input.sessionId),
          }
        : { sessionId: input.sessionId, activityAt: now },
    );
  }

  const appended = readEntryRow(owner, input.sessionId, input.id);
  if (appended === undefined)
    throw unavailable("Appended Session entry is unavailable");
  return appended;
}

/**
 * The retained leaf-finalize word.
 *
 * The private placeholder keeps its message identity. When the complete leaf
 * advanced during streaming, finish after that leaf with a fresh sequence.
 * Otherwise retain its original position. Completed history is never moved.
 */
export function completeSessionLeaf(
  owner: SqliteOwner,
  input: CompleteSessionLeafInput,
): StoredSessionRow {
  const session = ensureSession(owner, input.sessionId);
  const leaf = validateCompleteSessionLeaf(owner, session);
  const existing = readEntryRow(owner, input.sessionId, input.id);
  const now = Date.now();
  // The placeholder is private and has never advanced the selected lineage.
  // Preserve complete preferences/cancellations committed while it streamed.
  const leafAdvanced =
    existing?.state === "streaming" && existing.parentId !== (leaf?.id ?? null);
  const parentId = leafAdvanced
    ? (leaf?.id ?? null)
    : (existing?.parentId ?? leaf?.id ?? null);
  const entrySequence = leafAdvanced
    ? nextSequence(owner, input.sessionId)
    : (existing?.sequence ?? nextSequence(owner, input.sessionId));
  const createdAt = existing?.timestamp ?? now;

  if (input.id !== input.entry.message.value)
    throw unavailable("Completed message identity does not match its row");
  if (existing !== undefined) {
    const previous = existing.entry;
    if (previous.kind !== "message_start" && previous.kind !== "message")
      throw unavailable("Completed Session leaf is not a message entry");
    if (
      previous.workId !== input.entry.workId ||
      !sameCausalIdentity(previous.run, input.entry.run) ||
      !sameCausalIdentity(previous.turn, input.entry.turn) ||
      !sameCausalIdentity(previous.message, input.entry.message) ||
      (previous.kind === "message_start" &&
        previous.role !== input.entry.value.role)
    )
      throw unavailable("Completed message does not match its placeholder");
  }

  owner.sql.exec(
    `INSERT OR REPLACE INTO session_entries
       (id, session_id, parent_id, sequence, type, role, state,
        request_id, work_id, data_json, timestamp, completed_at)
     VALUES (?, ?, ?, ?, 'message', ?, 'complete',
             NULL, ?, ?, ?, ?)`,
    input.id,
    input.sessionId,
    parentId,
    entrySequence,
    roleFor(input.entry),
    input.entry.workId,
    encodeStoredSessionEntry(input.entry),
    createdAt,
    now,
  );
  owner.sql.exec(
    "UPDATE sessions SET leaf_id = ?, updated_at = ? WHERE id = ?",
    input.id,
    now,
    input.sessionId,
  );
  touchSessionInbox(owner, { sessionId: input.sessionId, activityAt: now });

  const completed = readEntryRow(owner, input.sessionId, input.id);
  if (completed === undefined)
    throw unavailable("Completed Session leaf is unavailable");
  return completed;
}

const entryColumns = `id, session_id, parent_id, sequence, type, role, state,
       request_id, work_id, data_json, timestamp, completed_at`;

/** Rows one bounded lineage read decodes at once. */
export const lineageRowLimit = 200;

/**
 * Stored bytes one model context read may decode: the retained storage
 * plane's `LLM_CONTEXT_BYTES`, now a typed refusal rather than a silent
 * truncation, because a truncated context would change the model's input.
 */
export const contextByteLimit = 12 * 1_048_576;

/**
 * Rows one fork copy window decodes. A fork copies the whole lineage, so a
 * wide window keeps the walks few; the byte bound still caps each window.
 */
const forkWindowRows = 4_000;

/**
 * Stored entry bytes one bounded lineage read decodes at once. One row is
 * always read, so a single oversized entry still makes progress.
 */
const lineageByteLimit = 4 * 1_048_576;

/**
 * Root-to-tip walks carry identities only (the 2026-08-15 connection-plane
 * law); entry data is read afterwards for exactly the rows a reader needs.
 * The selected walk drops streaming placeholders after walking; the fork walk
 * stops at them, as a fork copies only complete history.
 */
const lineageWalks = {
  selected: `WITH RECURSIVE branch (id, parent_id, sequence, state) AS (
       SELECT id, parent_id, sequence, state
         FROM session_entries
        WHERE session_id = ? AND id = ?
       UNION
       SELECT parent.id, parent.parent_id, parent.sequence, parent.state
         FROM session_entries AS parent
         JOIN branch AS child ON parent.id = child.parent_id
        WHERE parent.session_id = ?
     )`,
  fork: `WITH RECURSIVE branch (id, parent_id, sequence, state) AS (
       SELECT id, parent_id, sequence, state
         FROM session_entries
        WHERE session_id = ? AND id = ? AND state != 'streaming'
       UNION
       SELECT parent.id, parent.parent_id, parent.sequence, parent.state
         FROM session_entries AS parent
         JOIN branch AS child ON parent.id = child.parent_id
        WHERE parent.session_id = ? AND parent.state != 'streaming'
     )`,
} as const;

const lineageFailures = {
  selected: {
    empty: "Selected Session leaf is unavailable",
    missingParent: "Selected Session path has a missing parent",
    invalid: "Selected Session path is invalid",
  },
  fork: {
    empty: "Fork entry is unavailable",
    missingParent: "Fork path has a missing parent",
    invalid: "Fork path is invalid",
  },
} as const;

const count = Schema.Int.pipe(Schema.check(Schema.isGreaterThanOrEqualTo(0)));
const decodeLineageCheck = Schema.decodeUnknownResult(
  Schema.Struct({
    length: count,
    root_id: Schema.NullOr(boundedId),
    root_parent_id: Schema.NullOr(Schema.String),
    broken: count,
    newest_id: Schema.NullOr(boundedId),
    newest_sequence: Schema.NullOr(sequence),
  }),
  closed,
);
const decodeLineageSize = Schema.decodeUnknownResult(
  Schema.Struct({ sequence, bytes: count }),
  closed,
);
const decodeContextBytes = Schema.decodeUnknownResult(
  Schema.Struct({ bytes: count }),
  closed,
);
const decodeLineageSequence = Schema.decodeUnknownResult(
  Schema.Struct({ sequence }),
  closed,
);
const decodeFirstKept = Schema.decodeUnknownResult(
  Schema.Struct({ first_kept: boundedId }),
  closed,
);
const decodeLineageWorkId = Schema.decodeUnknownResult(
  Schema.Struct({ work_id: boundedId }),
  closed,
);
const decodeLineageDiagnostic = Schema.decodeUnknownResult(
  Schema.Struct({ work_id: boundedId, diagnostic: Schema.String }),
  closed,
);

/**
 * One validated root-to-tip lineage, located in SQL without decoding its
 * rows. Readers take bounded windows of it by sequence.
 */
export type SessionLineage = Readonly<{
  sessionId: string;
  walk: keyof typeof lineageWalks;
  /** The entry the walk starts from: the selected leaf or a fork point. */
  tipId: string;
  /** The oldest row; client cursors carry it as their generation. */
  rootId: string;
  newestId: string;
  newestSequence: number;
}>;

function lineageQuery(
  owner: SqliteOwner,
  lineage: SessionLineage,
  query: string,
  ...bindings: SqliteValue[]
): readonly RawRow[] {
  return owner.sql
    .exec(
      `${lineageWalks[lineage.walk]} ${query}`,
      lineage.sessionId,
      lineage.tipId,
      lineage.sessionId,
      ...bindings,
    )
    .toArray();
}

/** Validate one root-to-tip chain in SQL, the same way decoded rows would be. */
function locateLineage(
  owner: SqliteOwner,
  sessionId: string,
  tipId: string,
  walk: SessionLineage["walk"],
): SessionLineage {
  const failures = lineageFailures[walk];
  const checked = decodeLineageCheck(
    owner.sql
      .exec(
        `${lineageWalks[walk]},
         kept AS MATERIALIZED (
           SELECT id, parent_id, sequence FROM branch
            WHERE state != 'streaming'
         ),
         links AS (
           SELECT parent_id, LAG(id) OVER (ORDER BY sequence) AS previous
             FROM kept
         )
         SELECT
           (SELECT COUNT(*) FROM kept) AS length,
           (SELECT id FROM kept ORDER BY sequence ASC LIMIT 1) AS root_id,
           (SELECT parent_id FROM kept ORDER BY sequence ASC LIMIT 1)
             AS root_parent_id,
           (SELECT COUNT(*) FROM links
             WHERE previous IS NOT NULL AND parent_id IS NOT previous) AS broken,
           (SELECT id FROM kept ORDER BY sequence DESC LIMIT 1) AS newest_id,
           (SELECT sequence FROM kept ORDER BY sequence DESC LIMIT 1)
             AS newest_sequence`,
        sessionId,
        tipId,
        sessionId,
      )
      .one(),
  );
  if (checked._tag === "Failure")
    throw unavailable("Stored Session lineage check is invalid");
  const lineage = checked.success;
  if (
    lineage.length === 0 ||
    lineage.root_id === null ||
    lineage.newest_id === null ||
    lineage.newest_sequence === null
  )
    throw unavailable(failures.empty);
  if (lineage.root_parent_id !== null)
    throw unavailable(failures.missingParent);
  if (lineage.broken !== 0) throw unavailable(failures.invalid);
  return Object.freeze({
    sessionId,
    walk,
    tipId,
    rootId: lineage.root_id,
    newestId: lineage.newest_id,
    newestSequence: lineage.newest_sequence,
  });
}

/** Locate the selected lineage; undefined when the Session has no leaf. */
export function readSessionLineage(
  owner: SqliteOwner,
  input: ReadSelectedPathInput,
): SessionLineage | undefined {
  const tip = input.leafId ?? readSessionRow(owner, input.sessionId)?.leaf_id;
  if (tip === undefined || tip === null) return undefined;
  return locateLineage(owner, input.sessionId, tip, "selected");
}

function readLineageRange(
  owner: SqliteOwner,
  lineage: SessionLineage,
  oldest: number,
  newest: number,
): readonly StoredSessionRow[] {
  return Object.freeze(
    lineageQuery(
      owner,
      lineage,
      `SELECT ${entryColumns}
         FROM session_entries
        WHERE session_id = ? AND state != 'streaming'
          AND sequence BETWEEN ? AND ?
          AND id IN (SELECT id FROM branch)
        ORDER BY sequence ASC`,
      lineage.sessionId,
      oldest,
      newest,
    ).map(parseEntryRow),
  );
}

export type LineageWindowInput = Readonly<{
  /** Exclusive lower sequence bound; omitted starts at the root. */
  after?: number;
  /** Exclusive upper sequence bound; omitted ends at the newest row. */
  before?: number;
  /** `newest` takes rows backward from `before`, `oldest` forward from `after`. */
  from: "oldest" | "newest";
  /** Most rows to take; the stored-byte bound may stop the window earlier. */
  rows: number;
}>;

/** A contiguous run of lineage rows, oldest first, and whether more remain. */
export type LineageWindow = Readonly<{
  rows: readonly StoredSessionRow[];
  more: boolean;
}>;

/**
 * Read one contiguous window of a lineage, bounded by row count and by stored
 * bytes. Row sizes are measured in SQL before any entry is decoded.
 */
export function readLineageWindow(
  owner: SqliteOwner,
  lineage: SessionLineage,
  input: LineageWindowInput,
): LineageWindow {
  const sizes = lineageQuery(
    owner,
    lineage,
    `SELECT sequence, LENGTH(CAST(data_json AS BLOB)) AS bytes
       FROM session_entries
      WHERE session_id = ? AND state != 'streaming'
        AND sequence > ? AND sequence < ?
        AND id IN (SELECT id FROM branch)
      ORDER BY sequence ${input.from === "newest" ? "DESC" : "ASC"}
      LIMIT ?`,
    lineage.sessionId,
    input.after ?? 0,
    input.before ?? Number.MAX_SAFE_INTEGER,
    input.rows + 1,
  ).map((raw) => {
    const decoded = decodeLineageSize(raw);
    if (decoded._tag === "Failure")
      throw unavailable("Stored Session entry size is invalid");
    return decoded.success;
  });
  let taken = 0;
  let bytes = 0;
  for (const size of sizes) {
    if (taken === input.rows) break;
    if (taken > 0 && bytes + size.bytes > lineageByteLimit) break;
    bytes += size.bytes;
    taken += 1;
  }
  const first = sizes[0];
  const last = sizes[taken - 1];
  if (first === undefined || last === undefined)
    return Object.freeze({ rows: Object.freeze([]), more: false });
  return Object.freeze({
    rows: readLineageRange(
      owner,
      lineage,
      Math.min(first.sequence, last.sequence),
      Math.max(first.sequence, last.sequence),
    ),
    more: sizes.length > taken,
  });
}

/** Whether a complete row with this sequence lies on the lineage. */
export function lineageHasSequence(
  owner: SqliteOwner,
  lineage: SessionLineage,
  rowSequence: number,
): boolean {
  return (
    lineageQuery(
      owner,
      lineage,
      `SELECT 1 AS present
         FROM session_entries
        WHERE session_id = ? AND state != 'streaming' AND sequence = ?
          AND id IN (SELECT id FROM branch)`,
      lineage.sessionId,
      rowSequence,
    ).length > 0
  );
}

/** The sequence of one complete row on the lineage, if it is there. */
export function lineageEntrySequence(
  owner: SqliteOwner,
  lineage: SessionLineage,
  id: string,
): number | undefined {
  const raw = lineageQuery(
    owner,
    lineage,
    `SELECT sequence
       FROM session_entries
      WHERE session_id = ? AND state != 'streaming' AND id = ?
        AND id IN (SELECT id FROM branch)`,
    lineage.sessionId,
    id,
  )[0];
  if (raw === undefined) return undefined;
  const decoded = decodeLineageSequence(raw);
  if (decoded._tag === "Failure")
    throw unavailable("Stored Session entry sequence is invalid");
  return decoded.success.sequence;
}

/** The newest lineage row of one entry kind, decoded. */
function readNewestLineageEntry(
  owner: SqliteOwner,
  lineage: SessionLineage,
  kind: StoredSessionEntry["kind"],
): StoredSessionRow | undefined {
  const raw = lineageQuery(
    owner,
    lineage,
    `SELECT ${entryColumns}
       FROM session_entries
      WHERE session_id = ? AND state != 'streaming' AND type = ?
        AND id IN (SELECT id FROM branch)
      ORDER BY sequence DESC
      LIMIT 1`,
    lineage.sessionId,
    kind,
  )[0];
  return raw === undefined ? undefined : parseEntryRow(raw);
}

function lineageModelPreference(
  owner: SqliteOwner,
  lineage: SessionLineage | undefined,
): ModelPreference | null {
  const entry =
    lineage === undefined
      ? undefined
      : readNewestLineageEntry(owner, lineage, "model_preference")?.entry;
  return entry?.kind === "model_preference" ? entry.preference : null;
}

/** Latest document value on this exact lineage, not a global last write. */
function readLineageDocument(
  owner: SqliteOwner,
  lineage: SessionLineage | undefined,
  extensionId: string,
  name: string,
): string | undefined {
  if (lineage === undefined) return undefined;
  const raw = lineageQuery(
    owner,
    lineage,
    `SELECT ${entryColumns}
       FROM session_entries
      WHERE session_id = ? AND state != 'streaming' AND type = 'document'
        AND json_extract(data_json, '$.extensionId') = ?
        AND json_extract(data_json, '$.name') = ?
        AND id IN (SELECT id FROM branch)
      ORDER BY sequence DESC
      LIMIT 1`,
    lineage.sessionId,
    extensionId,
    name,
  )[0];
  const entry = raw === undefined ? undefined : parseEntryRow(raw).entry;
  if (entry === undefined) return undefined;
  if (
    entry.kind !== "document" ||
    entry.extensionId !== extensionId ||
    entry.name !== name
  )
    throw unavailable("Stored Session document disagrees with its row");
  return entry.valueJson;
}

/** What a bounded client page needs to know about the works it shows. */
export type LineageWorkFacts = Readonly<{
  /** Works whose command entry lies on the lineage. */
  commands: ReadonlySet<string>;
  /** Newest failure text each work recorded on the lineage. */
  diagnostics: ReadonlyMap<string, string>;
  /** Host metadata of the cancellation that ended each work. */
  cancellations: ReadonlyMap<string, Json>;
}>;

/**
 * Look up, across the whole lineage, only the facts of the given works. A
 * diagnostic is the newest of an assistant error, a final retry error or a
 * work terminal's diagnostic; only that text is extracted, never the message.
 */
export function readLineageWorkFacts(
  owner: SqliteOwner,
  lineage: SessionLineage,
  workIds: ReadonlySet<string>,
): LineageWorkFacts {
  if (workIds.size === 0)
    return Object.freeze({
      commands: new Set<string>(),
      diagnostics: new Map<string, string>(),
      cancellations: new Map<string, Json>(),
    });
  const works = JSON.stringify([...workIds]);
  const commands = new Set(
    lineageQuery(
      owner,
      lineage,
      `SELECT DISTINCT work_id
         FROM session_entries
        WHERE session_id = ? AND state != 'streaming' AND type = 'command'
          AND work_id IN (SELECT value FROM json_each(?))
          AND id IN (SELECT id FROM branch)`,
      lineage.sessionId,
      works,
    ).map((raw) => {
      const decoded = decodeLineageWorkId(raw);
      if (decoded._tag === "Failure")
        throw unavailable("Stored Session command work is invalid");
      return decoded.success.work_id;
    }),
  );
  const diagnostics = new Map<string, string>();
  for (const raw of lineageQuery(
    owner,
    lineage,
    `SELECT work_id, diagnostic FROM (
       SELECT work_id, sequence,
              CASE
                WHEN type = 'work_terminal'
                  THEN json_extract(data_json, '$.diagnostic')
                WHEN type = 'message'
                  THEN json_extract(data_json, '$.value.errorMessage')
                WHEN json_extract(data_json, '$.event.type') = 'auto_retry_end'
                  THEN json_extract(data_json, '$.event.finalError')
              END AS diagnostic
         FROM session_entries
        WHERE session_id = ? AND state != 'streaming'
          AND work_id IN (SELECT value FROM json_each(?))
          AND (type IN ('work_terminal', 'live_retry')
               OR (type = 'message' AND role = 'assistant'))
          AND id IN (SELECT id FROM branch)
     )
     WHERE diagnostic IS NOT NULL
     ORDER BY sequence ASC`,
    lineage.sessionId,
    works,
  )) {
    const decoded = decodeLineageDiagnostic(raw);
    if (decoded._tag === "Failure")
      throw unavailable("Stored Session diagnostic is invalid");
    diagnostics.set(decoded.success.work_id, decoded.success.diagnostic);
  }
  const cancellations = new Map<string, Json>();
  for (const { entry } of lineageQuery(
    owner,
    lineage,
    `SELECT ${entryColumns}
       FROM session_entries
      WHERE session_id = ? AND state != 'streaming' AND type = 'cancellation'
        AND json_extract(data_json, '$.targetWorkId')
            IN (SELECT value FROM json_each(?))
        AND id IN (SELECT id FROM branch)
      ORDER BY sequence ASC`,
    lineage.sessionId,
    works,
  ).map(parseEntryRow))
    if (entry.kind === "cancellation" && entry.meta !== undefined)
      cancellations.set(entry.targetWorkId, entry.meta);
  return Object.freeze({ commands, diagnostics, cancellations });
}

/**
 * Read one complete selected root-to-leaf branch, decoding every row.
 *
 * Heap grows with the whole lineage, so no production reader uses this:
 * clients page with `readLineageWindow`, runs read `readSessionContextPath`.
 */
export function readSelectedPath(
  owner: SqliteOwner,
  input: ReadSelectedPathInput,
): readonly StoredSessionRow[] {
  const lineage = readSessionLineage(owner, input);
  return lineage === undefined
    ? Object.freeze([])
    : readLineageRange(owner, lineage, 1, lineage.newestSequence);
}

/** One lineage, optionally only its rows after `after` that belong to `work`. */
type ContextSegment = Readonly<{
  lineage: SessionLineage;
  after: number;
  work: string | null;
}>;

function segmentQuery(
  owner: SqliteOwner,
  segment: ContextSegment,
  columns: string,
  clause: string,
  ...bindings: SqliteValue[]
): readonly RawRow[] {
  return lineageQuery(
    owner,
    segment.lineage,
    `SELECT ${columns}
       FROM session_entries
      WHERE session_id = ? AND state != 'streaming'
        AND id IN (SELECT id FROM branch)
        AND sequence > ?
        AND (? IS NULL OR work_id = ?
             OR (type = 'command'
                 AND json_extract(data_json, '$.targetWorkId') = ?))
        ${clause}`,
    segment.lineage.sessionId,
    segment.after,
    segment.work,
    segment.work,
    segment.work,
    ...bindings,
  );
}

/**
 * First sequence the model context reads: the latest compaction's kept
 * entry. Zero when nothing compacted, or when that entry is not on these
 * segments, where `buildSessionContext` also reads the whole path.
 */
function contextStart(
  owner: SqliteOwner,
  segments: readonly ContextSegment[],
): number {
  for (const segment of [...segments].reverse()) {
    const raw = segmentQuery(
      owner,
      segment,
      "json_extract(data_json, '$.firstKeptEntryId') AS first_kept",
      "AND type = 'compaction' ORDER BY sequence DESC LIMIT 1",
    )[0];
    if (raw === undefined) continue;
    const latest = decodeFirstKept(raw);
    if (latest._tag === "Failure")
      throw unavailable("Stored Session compaction boundary is invalid");
    for (const candidate of segments) {
      const kept = segmentQuery(
        owner,
        candidate,
        "sequence",
        "AND id = ?",
        latest.success.first_kept,
      )[0];
      if (kept === undefined) continue;
      const decoded = decodeLineageSequence(kept);
      if (decoded._tag === "Failure")
        throw unavailable("Stored Session entry sequence is invalid");
      return decoded.success.sequence;
    }
    return 0;
  }
  return 0;
}

/**
 * The rows model context and compaction read: the latest compaction's kept
 * window through the tip, preceded only by the older facts that still decide
 * it. Those are cancellations that withdrew a command inside the window, and
 * earlier compactions of the active work, whose recovery runs at most once.
 */
function readContextRows(
  owner: SqliteOwner,
  segments: readonly ContextSegment[],
  workId: string | null,
): readonly StoredSessionRow[] {
  const start = contextStart(owner, segments);
  let stored = 0;
  for (const segment of segments) {
    const measured = decodeContextBytes(
      segmentQuery(
        owner,
        segment,
        "COALESCE(SUM(LENGTH(CAST(data_json AS BLOB))), 0) AS bytes",
        "AND sequence >= ?",
        start,
      )[0],
    );
    if (measured._tag === "Failure")
      throw unavailable("Stored Session context size is invalid");
    stored += measured.success.bytes;
  }
  if (stored > contextByteLimit)
    throw new ShiitakeError(
      "store",
      "context_too_large",
      "This Session's model context is too large to read safely. Branch or fork from an earlier entry to continue.",
    );
  const window = segments.flatMap((segment) =>
    segmentQuery(
      owner,
      segment,
      entryColumns,
      "AND sequence >= ? ORDER BY sequence ASC",
      start,
    ).map(parseEntryRow),
  );
  if (start === 0) return Object.freeze(window);
  const commands = JSON.stringify(
    window.flatMap(({ entry }) =>
      entry.kind === "command" ? [entry.workId] : [],
    ),
  );
  const prefix = segments.flatMap((segment) =>
    segmentQuery(
      owner,
      segment,
      entryColumns,
      `AND sequence < ?
       AND ((type = 'compaction' AND work_id = ?)
            OR (type = 'cancellation'
                AND json_extract(data_json, '$.targetWorkId')
                    IN (SELECT value FROM json_each(?))))
       ORDER BY sequence ASC`,
      start,
      workId,
      commands,
    ).map(parseEntryRow),
  );
  return Object.freeze([...prefix, ...window]);
}

/**
 * Context rows of the current selected lineage for one accepted work, or,
 * without a work, the settled context the next run starts from.
 */
export function readSessionContextPath(
  owner: SqliteOwner,
  input: Readonly<{ sessionId: string; workId?: string }>,
): readonly StoredSessionRow[] {
  const lineage = readSessionLineage(owner, { sessionId: input.sessionId });
  return lineage === undefined
    ? Object.freeze([])
    : readContextRows(
        owner,
        [{ lineage, after: 0, work: null }],
        input.workId ?? null,
      );
}

/** Work cancelled before delivery stays in history but never enters model context. */
export function withdrawnWorkIds(
  path: readonly StoredSessionRow[],
): ReadonlySet<string> {
  // A queued prompt retained after its cancellation is transcript history,
  // not an instruction delivered to Mule. Context paths carry the older
  // cancellations that still withdraw a command inside their window.
  const delivered = new Set<string>();
  const withdrawn = new Set<string>();
  for (const { entry } of path) {
    if (entry.kind === "command") delivered.add(entry.workId);
    if (entry.kind === "cancellation" && !delivered.has(entry.targetWorkId))
      withdrawn.add(entry.targetWorkId);
  }
  return withdrawn;
}

export function buildSessionContext(
  path: readonly StoredSessionRow[],
  activeWorkId?: string,
): readonly Message[] {
  const withdrawn = withdrawnWorkIds(path);
  let compaction: StoredSessionRow | undefined;
  for (let index = path.length - 1; index >= 0; index -= 1) {
    const row = path[index];
    if (row?.entry.kind === "compaction") {
      compaction = row;
      break;
    }
  }
  const compactionEntry =
    compaction?.entry.kind === "compaction" ? compaction.entry : undefined;
  const keptIndex =
    compactionEntry === undefined
      ? -1
      : path.findIndex(({ id }) => id === compactionEntry.firstKeptEntryId);
  const contextPath = keptIndex >= 0 ? path.slice(keptIndex) : path;
  const visible = contextMessageIds(contextPath, activeWorkId);
  const messages: Message[] = [];
  if (compactionEntry !== undefined && compaction !== undefined) {
    messages.push(
      compactionSummaryMessage(compactionEntry.summary, compaction.timestamp),
    );
  }
  for (const row of contextPath) {
    const prompt = promptCommand(row.entry);
    if (prompt !== undefined && !withdrawn.has(prompt.workId))
      messages.push(commandContextMessage(prompt));
    if (row.entry.kind === "operator")
      messages.push(operatorContextMessage(row.entry));
    if (row.entry.kind === "message" && visible.has(row.id))
      messages.push(row.entry.value);
  }
  return Object.freeze(messages);
}

/** Prepare one accepted command against its frozen selected lineage. */
export function prepareSessionRun(
  owner: SqliteOwner,
  input: PrepareSessionRunInput,
): PreparedSessionRun {
  return owner.transactionSync(() => {
    let command = readEntryRow(owner, input.sessionId, input.commandFactId);
    let entryDelivery: PreparedSessionRun["entryDelivery"] = "replay";
    const admission = readSessionCommandAdmission(
      owner,
      input.sessionId,
      input.commandFactId,
    );
    if (command === undefined && admission !== undefined) {
      command = appendSessionEntry(owner, {
        sessionId: admission.sessionId,
        id: admission.id,
        entry: admission.entry,
      });
      entryDelivery = "new";
    }
    if (command?.entry.kind !== "command")
      throw unavailable("Accepted Session command is unavailable");
    if (
      admission !== undefined &&
      encodeStoredSessionEntry(command.entry) !==
        encodeStoredSessionEntry(admission.entry)
    )
      throw unavailable(
        "Accepted Session command disagrees with its admission",
      );
    const through = locateLineage(
      owner,
      input.sessionId,
      command.id,
      "selected",
    );
    if (through.newestId !== command.id)
      throw unavailable("Accepted Session command lineage is unavailable");
    // The current claim owns the command. Empty placeholders from a lost
    // reader carry no recoverable text and must not survive as live progress.
    owner.sql.exec(
      "DELETE FROM session_entries WHERE session_id = ? AND work_id = ? AND state = 'streaming' AND type = 'message_start'",
      input.sessionId,
      command.entry.workId,
    );
    const work = readSessionEntriesForWork(
      owner,
      input.sessionId,
      command.entry.workId,
    );
    const workId = command.entry.workId;
    const current = readSessionLineage(owner, { sessionId: input.sessionId });
    // The command's own lineage, then only this work's later rows.
    const continuedPath = readContextRows(
      owner,
      current === undefined
        ? [{ lineage: through, after: 0, work: null }]
        : [
            { lineage: through, after: 0, work: null },
            { lineage: current, after: command.sequence, work: workId },
          ],
      workId,
    );
    const selectedCommand: PreparedSessionRun["command"] = Object.freeze({
      ...command,
      entry: command.entry,
    });
    return Object.freeze({
      command: selectedCommand,
      modelPreference: lineageModelPreference(owner, through),
      messages: buildSessionContext(continuedPath, command.entry.workId),
      path: continuedPath,
      entryDelivery,
      resume: recoverAssistantTurn(work),
      abnormalTerminal: abnormalTerminal(work),
    });
  });
}

/** One compaction that committed nothing, recorded so clients show the result. */
export type CompactionNoticeInput = Readonly<{
  sessionId: string;
  workId: string;
  trigger: CompactionTrigger;
  result: "nothing_to_compact" | "failed";
  /** Bounded, display-safe reason; only a failure carries one. */
  diagnostic?: string | undefined;
}>;

/** Append one compaction notice on the selected lineage. */
export function appendCompactionNoticeOwned(
  owner: SqliteOwner,
  input: CompactionNoticeInput,
): StoredSessionRow {
  return owner.transactionSync(() => {
    const entry: StoredSessionEntry = {
      kind: "compaction_notice",
      workId: input.workId,
      trigger: input.trigger,
      result: input.result,
    };
    return appendSessionEntry(owner, {
      sessionId: input.sessionId,
      id: sessionIds.fact.generate(),
      entry:
        input.diagnostic === undefined
          ? entry
          : { ...entry, diagnostic: input.diagnostic.slice(0, 200) },
    });
  });
}

/** Atomically append one compaction transition using the selected leaf CAS. */
export function appendCompactionTransitionOwned(
  owner: SqliteOwner,
  input: AppendCompactionInput,
): StoredSessionRow {
  return owner.transactionSync(() => {
    const session = readSessionRow(owner, input.sessionId);
    if (session?.leaf_id !== input.expectedLeafId)
      throw new ShiitakeError(
        "store",
        "conflict",
        "Session leaf changed during compaction",
      );
    const entry: StoredSessionEntry = {
      kind: "compaction",
      resumesRun: input.resumesRun ?? false,
      trigger: input.trigger,
      tokensAfter: input.tokensAfter,
      workId: input.workId,
      firstKeptEntryId: input.firstKeptEntryId,
      summary: input.summary,
      tokensBefore: input.tokensBefore,
      usage: input.usage,
      contextRevision: input.contextRevision,
    };
    return appendSessionEntry(owner, {
      sessionId: input.sessionId,
      id: sessionIds.fact.generate(),
      entry:
        input.details === undefined
          ? entry
          : { ...entry, details: input.details },
    });
  });
}

function isPromptParts(
  content: UserMessage["content"],
): content is Exclude<UserMessage["content"], string> {
  return Array.isArray(content);
}

function promptContent(input: PromptCommand): UserMessage["content"] {
  if (input.images === undefined) return input.text;
  const content: Exclude<UserMessage["content"], string> = [];
  if (input.text.length > 0) content.push({ type: "text", text: input.text });
  content.push(...input.images);
  return content;
}

function samePromptContent(
  left: UserMessage["content"],
  right: UserMessage["content"],
): boolean {
  if (!isPromptParts(left) || !isPromptParts(right)) return left === right;
  return (
    left.length === right.length &&
    left.every((part, index) => {
      const other = right[index];
      return part.type === "text"
        ? other?.type === "text" && part.text === other.text
        : other?.type === "image" &&
            part.data === other.data &&
            part.mimeType === other.mimeType;
    })
  );
}

/** Whether a retried request names exactly the work already admitted. */
function sameAcceptedWork(
  previous: StoredSessionCommandEntry,
  input: AcceptedWork,
): boolean {
  if (input.type === "compact")
    return (
      isCompactCommand(previous) && previous.instructions === input.instructions
    );
  const prompt = promptCommand(previous);
  return (
    prompt?.delivery === input.delivery &&
    samePromptContent(prompt.message.content, promptContent(input))
  );
}

/** The durable command entry for one decoded admission. */
function acceptedEntry(
  input: AcceptedWork,
  commandFactId: string,
  now: number,
  contract: RunContract,
  targetWorkId: string | undefined,
  /** Only a prompt the host admitted as a verified Agent reply carries one. */
  reply: VerifiedAgentReply | undefined,
): StoredSessionCommandEntry {
  const identity = {
    kind: "command",
    requestId: input.requestId,
    receiptId: commandFactId,
    workId: commandFactId,
  } as const;
  if (input.type === "compact") {
    const work = {
      ...identity,
      work: "compact",
      timestamp: now,
      contract,
    } as const;
    return input.instructions === undefined
      ? work
      : { ...work, instructions: input.instructions };
  }
  const prompt = {
    ...identity,
    delivery: input.delivery,
    message: {
      role: "user",
      content: promptContent(input),
      timestamp: now,
    },
    contract,
  } as const;
  const targeted =
    targetWorkId === undefined ? prompt : { ...prompt, targetWorkId };
  return reply === undefined ? targeted : { ...targeted, reply };
}

function sameReply(
  left: VerifiedAgentReply | undefined,
  right: VerifiedAgentReply | undefined,
): boolean {
  return (
    left?.agentId === right?.agentId &&
    left?.sessionId === right?.sessionId &&
    left?.commandId === right?.commandId &&
    left?.outcome === right?.outcome &&
    left?.diagnostic === right?.diagnostic
  );
}

/**
 * Write one decoded admission (a prompt or a compaction request) and its one
 * durable work pointer.
 *
 * Only the accepted-command door (`internal/admission-door.ts`) calls this,
 * inside its owner transaction. A retried request id replays only when the
 * whole canonical command (kind, delivery, text, images, host metadata,
 * verified reply and origin) is unchanged.
 */
export function acceptSessionCommandOwned(
  owner: SqliteOwner,
  sessionId: string,
  input: AcceptedWork,
  meta: Json | undefined,
  origin: AdmissionOrigin,
  contract: RunContract,
  enqueue: EnqueueAcceptedWork | undefined,
  targetWorkId?: string,
  reply?: VerifiedAgentReply,
): AcceptedSessionCommand {
  const previous = readSessionCommandAdmissionByRequest(
    owner,
    sessionId,
    input.requestId,
  );
  if (previous !== undefined) {
    if (
      !sameAcceptedWork(previous.entry, input) ||
      !sameHostMeta(previous.entry.meta, meta) ||
      !sameReply(promptCommand(previous.entry)?.reply, reply) ||
      !equivalentAdmissionOrigin(previous.origin, origin)
    )
      throw new ShiitakeError(
        "contract",
        "conflict",
        "Request id conflicts with an accepted Session command",
      );
    return acceptanceFromAdmission(previous, "replay");
  }

  const commandFactId = sessionIds.fact.generate();
  ensureSession(owner, sessionId);
  const now = Date.now();
  const entry = acceptedEntry(
    input,
    commandFactId,
    now,
    contract,
    targetWorkId,
    reply,
  );
  owner.sql.exec(
    `INSERT INTO session_command_admissions
       (id, session_id, sequence, request_id, work_id, data_json, timestamp,
        origin_kind, origin_id, origin_correlation)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    commandFactId,
    sessionId,
    nextAdmissionSequence(owner, sessionId),
    input.requestId,
    commandFactId,
    encodeStoredSessionEntry(withHostMeta(entry, meta)),
    now,
    origin.kind,
    origin.kind === "user" ? null : origin.extensionId,
    origin.kind === "user" ? null : origin.correlation,
  );
  const command = readSessionCommandAdmission(owner, sessionId, commandFactId);
  if (command === undefined)
    throw unavailable("Accepted Session command admission is unavailable");
  enqueue?.({
    jobId: commandFactId,
    sessionId,
    commandFactId,
  });
  return acceptanceFromAdmission(command, "new");
}

function acceptanceFromAdmission(
  row: StoredCommandAdmission,
  delivery: AcceptedSessionCommand["delivery"],
): AcceptedSessionCommand {
  const accepted: AcceptedCommand = Object.freeze({
    sessionId: row.sessionId,
    requestId: row.entry.requestId,
    commandId: row.id,
  });
  return Object.freeze({ accepted, command: row, delivery });
}

function acceptanceFromCancellation(
  row: StoredSessionRow,
  delivery: AcceptedSessionCancellation["delivery"],
  active: boolean,
): AcceptedSessionCancellation {
  if (row.entry.kind !== "cancellation")
    throw unavailable("Accepted Session cancellation is unavailable");
  return Object.freeze({
    accepted: Object.freeze({
      commandId: row.entry.targetWorkId,
      entryId: row.id,
    }),
    delivery,
    active,
  });
}

/** Cancel the running turn and every waiting admission in one owner transaction. */
export function acceptSessionAbort(
  owner: SqliteOwner,
  sessionId: string,
  readQueueHead: () => SessionQueueHead,
  record: RecordAcceptedCancellation,
  meta?: Json,
): AcceptedSessionCancellation | undefined {
  return owner.transactionSync(() => {
    const head = readQueueHead();
    const activeAdmission =
      head.state === "running"
        ? readSessionCommandAdmissionByWorkId(owner, sessionId, head.jobId)
        : undefined;
    const active =
      activeAdmission !== undefined
        ? acceptSessionCancellationInTransaction(
            owner,
            sessionId,
            activeAdmission.id,
            readQueueHead,
            record,
            false,
            meta,
          )
        : undefined;
    // Page through owner facts, not the client's bounded queue snapshot. Each
    // cancellation removes that admission from the next page, including steers
    // that have no separate Watchdog job yet.
    // The immutable admission sequence bounds this transaction even if a
    // future projection fails to remove an admission from the pending page.
    for (
      let remaining = nextAdmissionSequence(owner, sessionId);
      remaining > 0;
    ) {
      const { commands: pending } = readPendingSessionCommands(
        owner,
        sessionId,
        64,
      );
      if (pending.length === 0) return active;
      remaining -= pending.length;
      for (const admission of pending) {
        const cancellation = acceptSessionCancellationInTransaction(
          owner,
          sessionId,
          admission.id,
          readQueueHead,
          record,
          false,
          meta,
          true,
        );
        if (cancellation === undefined)
          throw unavailable("Pending Session command could not be cancelled");
        // Keep the exact prompt (including images, attribution and host meta)
        // available for deliberate resubmission after it leaves the queue.
        appendSessionEntry(owner, {
          sessionId,
          id: admission.id,
          entry: admission.entry,
        });
      }
    }
    throw unavailable(
      "Session cancellation did not settle its pending admissions",
    );
  });
}

/**
 * Atomically resolve one accepted request and delegate queued work to
 * Watchdog. `undefined` means it is too late (or the id names nothing): the
 * command is unknown, has already settled, or, for an `onlyIfPending` cancel,
 * has already started. Nothing is written in that case.
 */
export function acceptSessionCancellation(
  owner: SqliteOwner,
  sessionId: string,
  commandId: string,
  readQueueHead: () => SessionQueueHead,
  record: RecordAcceptedCancellation,
  onlyIfPending = false,
  meta?: Json,
): AcceptedSessionCancellation | undefined {
  return owner.transactionSync(() =>
    acceptSessionCancellationInTransaction(
      owner,
      sessionId,
      commandId,
      readQueueHead,
      record,
      onlyIfPending,
      meta,
    ),
  );
}

function acceptSessionCancellationInTransaction(
  owner: SqliteOwner,
  sessionId: string,
  commandId: string,
  readQueueHead: () => SessionQueueHead,
  record: RecordAcceptedCancellation,
  onlyIfPending = false,
  meta?: Json,
  settleMissing = false,
): AcceptedSessionCancellation | undefined {
  const queueHead = readQueueHead();
  const command = readSessionCommandAdmission(owner, sessionId, commandId);
  if (command?.entry.kind !== "command") return undefined;
  const workEntries = readSessionEntriesForWork(
    owner,
    sessionId,
    command.entry.workId,
  );
  const recordedCancellation = workEntries.find(
    ({ entry }) => entry.kind === "cancellation",
  );
  if (recordedCancellation?.entry.kind === "cancellation")
    return acceptanceFromCancellation(
      recordedCancellation,
      "replay",
      queueHead.state === "running" && queueHead.jobId === command.entry.workId,
    );
  const active =
    queueHead.state === "running" && queueHead.jobId === command.entry.workId;
  // A terminal can precede a blocked compaction handoff. Watchdog still owns
  // the running job until that handoff settles, so cancellation stays live.
  if (
    !active &&
    workEntries.some(
      ({ entry }) =>
        entry.kind === "run_terminal" || entry.kind === "work_terminal",
    )
  )
    return undefined;
  if (
    onlyIfPending &&
    (active || workEntries.some(({ entry }) => entry.kind === "command"))
  )
    return undefined;
  if (
    promptCommand(command.entry)?.delivery === "steer" &&
    workEntries.some(({ entry }) => entry.kind === "command") &&
    !active
  )
    return undefined;
  const factId = sessionIds.fact.generate();
  const decision = record({
    jobId: command.entry.workId,
    cancellationId: factId,
    onlyIfPending,
  });
  // A steer has no Watchdog row while it waits at the Mule boundary. Its
  // owner fact is still cancellable, with a terminal recorded below. Cancel-all
  // also settles orphan admissions whose scheduler row is missing.
  if (
    decision.status === "missing" &&
    promptCommand(command.entry)?.delivery !== "steer" &&
    !settleMissing
  )
    return undefined;
  // Watchdog refuses an `onlyIfQueued` cancel once the job has been claimed.
  if (decision.status === "conflict" && onlyIfPending) return undefined;
  if (decision.status !== "recorded" && decision.status !== "missing")
    throw new ShiitakeError(
      "store",
      "conflict",
      "Session cancellation identity conflicts",
    );

  const cancellation = appendSessionEntry(owner, {
    sessionId,
    id: factId,
    entry: withHostMeta(
      {
        kind: "cancellation",
        requestId: factId,
        receiptId: factId,
        workId: command.entry.workId,
        targetWorkId: command.entry.workId,
      },
      meta,
    ),
  });
  if (decision.status === "missing")
    appendSessionEntry(owner, {
      sessionId,
      id: sessionIds.fact.generate(),
      entry: {
        kind: "work_terminal",
        workId: command.entry.workId,
        outcome: "cancelled",
      },
    });
  return acceptanceFromCancellation(cancellation, "new", active);
}

/** Move the durable leaf to an existing entry in the same Session. */
export function selectSessionBranch(
  owner: SqliteOwner,
  input: SelectSessionBranchInput,
): void {
  owner.transactionSync(() => {
    input.assertIdleSession?.(input.sessionId);
    const session = readSessionRow(owner, input.sessionId);
    if (session === undefined)
      throw unavailable("Session is unavailable for branch selection");

    validateCompleteSessionLeaf(owner, session);

    const target = readEntryRow(owner, input.sessionId, input.entryId);
    if (target?.state !== "complete")
      throw unavailable("Selected branch entry is unavailable");

    const now = Date.now();
    owner.sql.exec(
      "UPDATE sessions SET leaf_id = ?, updated_at = ? WHERE id = ?",
      target.id,
      now,
      session.id,
    );
    touchSessionInbox(owner, {
      sessionId: session.id,
      activityAt: now,
      title: inboxTitle(owner, session.id),
    });
  });
}

/**
 * Record one operator or party line on an idle Session. The request id is
 * the idempotency key: a replay of the same line is `false`, another line
 * under the same id is a conflict.
 */
export function appendOperatorEntry(
  owner: SqliteOwner,
  input: Readonly<{
    sessionId: string;
    requestId: string;
    author: "operator" | "party";
    meta: Json | undefined;
    text: string;
    assertIdleSession: (queue: string) => void;
  }>,
): boolean {
  return owner.transactionSync(() => {
    if (readSessionRow(owner, input.sessionId) === undefined)
      throw unavailable("Session is unavailable for an operator entry");
    const existing = owner.sql
      .exec(
        "SELECT id FROM session_entries WHERE session_id = ? AND request_id = ?",
        input.sessionId,
        input.requestId,
      )
      .toArray()[0];
    if (existing !== undefined) {
      const decoded = decodeSessionIdRow(existing);
      if (decoded._tag === "Failure")
        throw unavailable("Stored Session entry identity is invalid");
      const row = readEntryRow(owner, input.sessionId, decoded.success.id);
      if (
        row?.entry.kind === "operator" &&
        row.entry.author === input.author &&
        sameHostMeta(row.entry.meta, input.meta) &&
        row.entry.message.content === input.text
      )
        return false;
      throw new ShiitakeError(
        "store",
        "conflict",
        "Request id already names another Session entry",
      );
    }
    input.assertIdleSession(input.sessionId);
    const entry: OperatorSessionEntry = {
      kind: "operator",
      workId: null,
      requestId: input.requestId,
      author: input.author,
      message: { role: "user", content: input.text, timestamp: Date.now() },
    };
    appendSessionEntry(owner, {
      sessionId: input.sessionId,
      id: sessionIds.fact.generate(),
      entry: withHostMeta(entry, input.meta),
    });
    return true;
  });
}

function readSessionRows(
  owner: SqliteOwner,
  sessionId: string,
): readonly StoredSessionRow[] {
  return owner.sql
    .exec(
      `SELECT id, session_id, parent_id, sequence, type, role, state,
              request_id, work_id, data_json, timestamp, completed_at
         FROM session_entries
        WHERE session_id = ? AND state != 'streaming'
        ORDER BY sequence ASC`,
      sessionId,
    )
    .toArray()
    .map(parseEntryRow);
}

/** Link every stored row into a forest, retaining rows with no parent. */
function buildMutableSessionForest(
  rows: readonly StoredSessionRow[],
): readonly MutableSessionTreeNode[] {
  const mutableNodes = new Map<string, MutableSessionTreeNode>();
  for (const row of rows) mutableNodes.set(row.id, { row, children: [] });

  const roots: MutableSessionTreeNode[] = [];
  for (const row of rows) {
    const node = mutableNodes.get(row.id);
    if (node === undefined)
      throw unavailable("Session tree row is unavailable");

    if (
      row.parentId === null ||
      row.parentId === row.id ||
      !mutableNodes.has(row.parentId)
    ) {
      roots.push(node);
      continue;
    }

    const parent = mutableNodes.get(row.parentId);
    if (parent === undefined)
      throw unavailable("Session tree parent is unavailable");
    parent.children.push(node);
  }

  roots.sort((left, right) => left.row.sequence - right.row.sequence);
  return roots;
}

function freezeSessionForest(
  roots: readonly MutableSessionTreeNode[],
): readonly SessionTreeNode[] {
  const frozenNodes = new Map<string, SessionTreeNode>();
  const pending: {
    node: MutableSessionTreeNode;
    expanded: boolean;
  }[] = roots.map((node) => ({ node, expanded: false }));

  while (pending.length > 0) {
    const current = pending.pop();
    if (current === undefined) throw unavailable("Session tree is unavailable");
    if (!current.expanded) {
      pending.push({ node: current.node, expanded: true });
      current.node.children.sort(
        (left, right) => left.row.sequence - right.row.sequence,
      );
      for (
        let index = current.node.children.length - 1;
        index >= 0;
        index -= 1
      ) {
        const child = current.node.children[index];
        if (child !== undefined) pending.push({ node: child, expanded: false });
      }
      continue;
    }

    const children = current.node.children.map((child) => {
      const frozen = frozenNodes.get(child.row.id);
      if (frozen === undefined)
        throw unavailable("Session tree child is unavailable");
      return frozen;
    });
    frozenNodes.set(
      current.node.row.id,
      Object.freeze({
        row: current.node.row,
        children: Object.freeze(children),
      }),
    );
  }

  return Object.freeze(
    roots.map((root) => {
      const frozen = frozenNodes.get(root.row.id);
      if (frozen === undefined)
        throw unavailable("Session tree root is unavailable");
      return frozen;
    }),
  );
}

/** Read the complete retained Session tree, including orphan roots. */
export function readSessionTree(
  owner: SqliteOwner,
  input: SessionTreeInput,
): readonly SessionTreeNode[] {
  if (readSessionRow(owner, input.sessionId) === undefined) return [];

  const rows = readSessionRows(owner, input.sessionId);
  return freezeSessionForest(buildMutableSessionForest(rows));
}

/** Fork exactly one Session root-to-entry lineage with fresh row identities. */
export function forkSessionAt(
  owner: SqliteOwner,
  input: ForkSessionAtInput,
): string {
  return owner.transactionSync(() => {
    input.assertIdleSession?.(input.sessionId);
    if (readSessionRow(owner, input.sessionId) === undefined)
      throw unavailable("Source Session is unavailable");

    const source = locateLineage(owner, input.sessionId, input.entryId, "fork");
    const newSessionId = sessionIds.session.generate();
    const now = Date.now();

    owner.sql.exec(
      `INSERT INTO sessions
         (id, cwd, parent_session, leaf_id, created_at, updated_at, metadata_json)
       VALUES (?, '/', ?, NULL, ?, ?, NULL)`,
      newSessionId,
      input.sessionId,
      now,
      now,
    );
    copySessionGrants(owner, input.sessionId, newSessionId);

    let parentId: string | null = null;
    let leafId: string | null = null;
    let sequence = 1;
    // Only identities accumulate; entry data is copied one bounded window
    // at a time.
    const idMap = new Map<string, string>();
    let copied = 0;
    for (;;) {
      const window = readLineageWindow(owner, source, {
        after: copied,
        from: "oldest",
        rows: forkWindowRows,
      });
      for (const row of window.rows) {
        const id = sessionIds.fact.generate();
        idMap.set(row.id, id);
        const entry =
          row.entry.kind === "compaction"
            ? {
                ...row.entry,
                firstKeptEntryId:
                  idMap.get(row.entry.firstKeptEntryId) ??
                  (() => {
                    throw unavailable(
                      "Fork compaction boundary is unavailable",
                    );
                  })(),
              }
            : row.entry;
        owner.sql.exec(
          `INSERT INTO session_entries
             (id, session_id, parent_id, sequence, type, role, state,
              request_id, work_id, data_json, timestamp, completed_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          id,
          newSessionId,
          parentId,
          sequence,
          entry.kind,
          roleFor(entry),
          row.state,
          requestIdFor(entry),
          entry.workId,
          encodeStoredSessionEntry(entry),
          row.timestamp,
          row.completedAt,
        );
        parentId = id;
        leafId = id;
        sequence += 1;
        copied = row.sequence;
      }
      if (!window.more) break;
    }

    if (leafId === null) throw unavailable("Fork path is empty");
    owner.sql.exec(
      "UPDATE sessions SET leaf_id = ?, updated_at = ? WHERE id = ?",
      leafId,
      now,
      newSessionId,
    );
    const current = readSessionLineage(owner, { sessionId: input.sessionId });
    for (const entry of forkDocumentEntries(owner, (extensionId, name) =>
      readLineageDocument(owner, current, extensionId, name),
    ))
      appendSessionEntry(owner, {
        sessionId: newSessionId,
        id: sessionIds.fact.generate(),
        entry,
      });
    touchSessionInbox(owner, {
      sessionId: newSessionId,
      activityAt: now,
      title: inboxTitle(owner, newSessionId),
    });
    return newSessionId;
  });
}

/** Read document state from the selected transcript branch. */
export function readSessionDocument(
  owner: SqliteOwner,
  sessionId: string,
  extensionId: string,
  name: string,
): string | undefined {
  if (readSessionRow(owner, sessionId) === undefined)
    throw unavailable("Session document owner is unavailable");
  return readLineageDocument(
    owner,
    readSessionLineage(owner, { sessionId }),
    extensionId,
    name,
  );
}
/** Append a document snapshot under the same transaction as its producing hook. */
export function writeSessionDocument(
  owner: SqliteOwner,
  sessionId: string,
  extensionId: string,
  name: string,
  valueJson: string,
): void {
  if (readSessionRow(owner, sessionId) === undefined)
    throw unavailable("Session document owner is unavailable");
  appendSessionEntry(owner, {
    sessionId,
    id: sessionIds.fact.generate(),
    entry: { kind: "document", workId: null, extensionId, name, valueJson },
  });
}
