import { prepareExtensionStore } from "./extension-store.js";
import {
  prepareSessionDocuments,
  forkDocumentEntries,
} from "./session-documents.js";
import type {
  UserMessage,
  Usage,
  MuleResume,
} from "@fungi.computer/mule/effect";
import * as Schema from "effect/Schema";

import type { ModelPreference } from "../model-preference.js";

import type { ModelMessage, PromptCommand } from "../../client.js";
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
import { prepareWorkSenders } from "./work-senders.js";
import { prepareSessionDelegators } from "./session-delegator.js";
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
  damaged: Schema.Literals([0, 1]),
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
const decodeNextSequenceRow = Schema.decodeUnknownResult(
  NextSequenceRowSchema,
  closed,
);
const decodeEntryPresenceRow = Schema.decodeUnknownResult(
  EntryPresenceRowSchema,
  closed,
);

/** The columns of an entry row from `table`, with whether its Session is damaged. */
function entryColumnsOf(table: string): string {
  const columns = [
    "id",
    "session_id",
    "parent_id",
    "sequence",
    "type",
    "role",
    "state",
    "request_id",
    "work_id",
    "data_json",
    "timestamp",
    "completed_at",
  ].map((column) => `${table}.${column}`);
  return `${columns.join(", ")},
    EXISTS (SELECT 1 FROM session_lineage_damage AS damage
             WHERE damage.session_id = ${table}.session_id) AS damaged`;
}

const entryColumns = entryColumnsOf("session_entries");

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
  if (readSessionRow(owner, sessionId) === undefined) return undefined;
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
           FROM branch AS child
           CROSS JOIN session_entries AS parent
          WHERE parent.id = child.parent_id AND parent.session_id = ?
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
  messages: readonly ModelMessage[];
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
  head: string;
  summary: string;
  tokensBefore: number;
  usage: Usage;
  contextRevision: string;
  /** The archive owner's receipt, stored in the entry that commits. */
  receipt?: string;
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
    delivery: "steer" | "followUp";
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

/**
 * Decode one entry row. Every entry read selects its Session's damage with
 * it (`entryColumns`), so a row of a Session the lineage audit refused is
 * refused here, whichever reader asked for it.
 */
function parseEntryRow(owner: SqliteOwner, raw: RawRow): StoredSessionRow {
  const decodedRow = decodeEntryRow(raw);
  if (decodedRow._tag === "Failure")
    throw unavailable("Stored Session entry row is invalid");

  const row = decodedRow.success;
  if (row.damaged === 1) throw brokenSession(owner, row.session_id);
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

const decodeReadableSessionRow = Schema.decodeUnknownResult(
  Schema.Struct({
    ...SessionRowSchema.fields,
    damaged: Schema.Literals([0, 1]),
  }),
  closed,
);

/**
 * The Session row every reader and writer of one Session starts from. A
 * Session the lineage audit found broken is refused here, so no read of it
 * and no write to it gets further.
 */
function readSessionRow(
  owner: SqliteOwner,
  sessionId: string,
): SessionRow | undefined {
  const raw = owner.sql
    .exec(
      `SELECT id, leaf_id,
              EXISTS (SELECT 1 FROM session_lineage_damage
                       WHERE session_id = sessions.id) AS damaged
         FROM sessions WHERE id = ?`,
      sessionId,
    )
    .toArray()[0];
  if (raw === undefined) return undefined;
  const decoded = decodeReadableSessionRow(raw);
  if (decoded._tag === "Failure")
    throw unavailable("Stored Session row is invalid");
  if (decoded.success.damaged === 1) throw brokenSession(owner, sessionId);
  return Object.freeze({
    id: decoded.success.id,
    leaf_id: decoded.success.leaf_id,
  });
}

function readEntryRow(
  owner: SqliteOwner,
  sessionId: string,
  id: string,
): StoredSessionRow | undefined {
  const raw = owner.sql
    .exec(
      `SELECT ${entryColumns}
         FROM session_entries
        WHERE session_id = ? AND id = ?`,
      sessionId,
      id,
    )
    .toArray()[0];
  return raw === undefined ? undefined : parseEntryRow(owner, raw);
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
      `SELECT ${entryColumns}
         FROM session_entries
        WHERE id = ?`,
      id,
    )
    .toArray()[0];
  return raw === undefined ? undefined : parseEntryRow(owner, raw);
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

/** A written run terminal closes the window for attaching another steer. */
export function hasRecordedRunTerminal(
  owner: SqliteOwner,
  sessionId: string,
  workId: string,
): boolean {
  return (
    owner.sql
      .exec(
        "SELECT 1 FROM session_entries WHERE session_id = ? AND work_id = ? AND type = 'run_terminal' LIMIT 1",
        sessionId,
        workId,
      )
      .toArray().length > 0
  );
}

/** Host-owned metadata identity, interpreted by SQLite's JSON decoder only. */
export type AdmissionIdentity = Readonly<{ path: string; value: string }>;

const decodeIdentity = Schema.decodeUnknownSync(
  Schema.Struct({ identity: Schema.NullOr(Schema.String) }),
);

/** Extract the host-selected principal without giving the engine domain grammar. */
export function readAdmissionIdentity(
  owner: SqliteOwner,
  path: string,
  meta: Json | undefined,
): AdmissionIdentity {
  const { identity } = decodeIdentity(
    owner.sql
      .exec(
        "SELECT json_extract(?, ?) AS identity",
        JSON.stringify(meta ?? null),
        path,
      )
      .one(),
  );
  if (identity === null || identity.length === 0)
    throw new ShiitakeError(
      "runtime",
      "rejected",
      "Request author is required",
    );
  return { path, value: identity };
}

/** Immutable admission author check; used before every exact cancellation, including replay. */
export function assertAdmissionIdentity(
  owner: SqliteOwner,
  sessionId: string,
  commandId: string,
  identity: AdmissionIdentity,
): void {
  const admission = readSessionCommandAdmission(owner, sessionId, commandId);
  if (admission === undefined) return;
  const stored = readAdmissionIdentity(
    owner,
    identity.path,
    admission.entry.meta,
  );
  if (stored.value !== identity.value)
    throw new ShiitakeError(
      "runtime",
      "rejected",
      "Message author does not match",
    );
}

/** Read just one newest admission; FIFO execution and oldest-first paging are unchanged. */
export function readNewestPendingSessionCommand(
  owner: SqliteOwner,
  sessionId: string,
  identity?: AdmissionIdentity,
): StoredCommandAdmission | undefined {
  const raw = owner.sql
    .exec(
      `SELECT ${admissionColumns("a.")} ${pendingAdmissions}
       AND json_extract(a.data_json, '$.work') IS NULL
       AND a.origin_kind = 'user'
       AND (? IS NULL OR json_extract(json_extract(a.data_json, '$.meta'), ?) = ?)
     ORDER BY a.sequence DESC LIMIT 1`,
      sessionId,
      identity?.path ?? null,
      identity?.path ?? null,
      identity?.value ?? null,
    )
    .toArray()[0];
  return raw === undefined ? undefined : parseCommandAdmissionRow(raw);
}
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
     ORDER BY (json_extract(a.data_json, '$.delivery') = 'steer') DESC,
              a.sequence ASC LIMIT ?`,
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
  message: ModelMessage;
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
    diagnostic: string | undefined;
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
          delivery: "steer",
        });
        continue;
      }
      // Only a delivered control shares its failed parent's safe reason.
      // Late, unmaterialized work was requeued above and owns its own outcome.
      const diagnostic =
        input.outcome === "failed" || input.outcome === "interrupted"
          ? input.diagnostic
          : undefined;
      let entry: Extract<StoredSessionEntry, { kind: "work_terminal" }> = {
        kind: "work_terminal",
        workId: row.entry.workId,
        outcome: input.outcome,
      };
      if (diagnostic !== undefined) entry = { ...entry, diagnostic };
      appendSessionEntry(owner, {
        sessionId: input.sessionId,
        id: sessionIds.fact.generate(),
        entry,
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

/** Probe the indexed terminal facts without materializing a live work's body. */
export function hasSessionWorkTerminal(
  owner: SqliteOwner,
  sessionId: string,
  workId: string,
): boolean {
  // Keep the readability check even when the work has no terminal rows.
  readSessionRow(owner, sessionId);
  return (
    owner.sql
      .exec(
        `SELECT 1 AS present FROM session_entries
         INDEXED BY idx_session_entries_work_terminal
         WHERE session_id = ? AND work_id = ?
           AND type IN ('run_terminal', 'work_terminal')
         LIMIT 1`,
        sessionId,
        workId,
      )
      .toArray().length > 0
  );
}

/** Read entries correlated to one accepted work identity in Session order. */
export function readSessionEntriesForWork(
  owner: SqliteOwner,
  sessionId: string,
  workId: string,
): readonly StoredSessionRow[] {
  // Refuse the Session before selecting entries: a damaged work may have no
  // rows left, so decoding the result cannot establish that it is readable.
  readSessionRow(owner, sessionId);
  return Object.freeze(
    owner.sql
      .exec(
        `SELECT ${entryColumns}
           FROM session_entries
          WHERE session_id = ? AND work_id = ?
          ORDER BY sequence ASC`,
        sessionId,
        workId,
      )
      .toArray()
      .map((raw) => parseEntryRow(owner, raw)),
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
      `SELECT ${entryColumns}
         FROM session_entries
        WHERE session_id = ? AND work_id = ? AND type = 'live_retry'
        ORDER BY sequence DESC
        LIMIT 1`,
      sessionId,
      workId,
    )
    .toArray()[0];
  if (raw === undefined) return undefined;
  const row = parseEntryRow(owner, raw);
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
      `SELECT ${entryColumnsOf("intent")}
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
  const row = parseEntryRow(owner, raw);
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
  return owner.transactionSync(() =>
    createStoredSessionWithinTransaction(owner, requestId, admitNew),
  );
}

/** Reserve a Session while the caller owns the enclosing SQLite transaction. */
export function createStoredSessionWithinTransaction(
  owner: SqliteOwner,
  requestId: string,
  /** Refuses a new Session under storage pressure; replays skip it. */
  admitNew: () => void,
): string {
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
    // A Session the lineage audit refused can still be deleted.
    const exists =
      owner.sql
        .exec("SELECT id FROM sessions WHERE id = ?", input.sessionId)
        .toArray().length > 0;
    if (!exists) return false;
    input.assertIdleSession(input.sessionId);
    input.recordInboxDeletion(input.sessionId);
    for (const statement of [
      "DELETE FROM session_command_admissions WHERE session_id = ?",
      // The inbox row references the Session; it goes in the same transaction.
      "DELETE FROM session_attention WHERE session_id = ?",
      "DELETE FROM session_work_senders WHERE session_id = ?",
      "DELETE FROM session_tool_grants WHERE session_id = ?",
      "DELETE FROM session_inbox WHERE session_id = ?",
      "DELETE FROM session_creation_receipts WHERE session_id = ?",
      "DELETE FROM sessions WHERE id = ?",
      // Entries go last: only a removed Session's lineage may lose parents.
      "DELETE FROM session_entries WHERE session_id = ?",
      "DELETE FROM session_lineage_damage WHERE session_id = ?",
    ])
      owner.sql.exec(statement, input.sessionId);
    return true;
  });
}

/**
 * Version 3 keeps application tool data outside model messages/results.
 * Older preview stores require explicit host custody; no compatibility reader
 * or automatic rewrite runs when the store opens.
 */
const currentWoodstockSchemaVersion = 3;
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
  if (decoded._tag !== "Success") return "unmarked";
  // The decoder already requires the current protocol literal.
  if (decoded.success.version === currentWoodstockSchemaVersion) return "v0";
  return "unmarked";
}

/** Stamp fresh Woodstock stores; incompatible files require host-side custody. */
function ensureWoodstockSchemaVersion(owner: SqliteOwner): void {
  const protocol = readWoodstockStoreProtocol(owner);
  if (protocol === "v0") return;
  if (protocol === "unmarked")
    throw incompatible(
      "Woodstock store has no current v0 schema marker; incompatible storage requires explicit host custody. To reset, stop the owning host before resetting a local store, then remove its database file and the file's .owner.sqlite sidecar. Reset discards that store's data. For the Hub, a Hub Agent is erased only with its Team; Team erasure is destructive and separately authorized, never a routine single-Agent repair. No migration runs.",
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

const lineageGuards = [
  "session_entries_lineage_insert",
  "session_entries_lineage_update",
  "session_entries_lineage_delete",
  "session_entries_damaged_session",
  "session_entries_lineage_replace",
  "session_entries_lineage_replace_position",
] as const;

const decodeInstalledGuards = Schema.decodeUnknownResult(
  Schema.Struct({
    installed: Schema.Int.pipe(Schema.check(Schema.isGreaterThanOrEqualTo(0))),
  }),
  closed,
);
const decodeDamagedSession = Schema.decodeUnknownResult(
  Schema.Struct({ session_id: Schema.String }),
  closed,
);

/** What a writer to a Session the lineage audit found broken is told. */
const brokenSessionMessage =
  "Woodstock Session holds a broken lineage; a store never repairs itself — delete this Session (deleteSession), or reset the owning store";

const decodeDamageCount = Schema.decodeUnknownResult(
  Schema.Struct({
    damaged: Schema.Int.pipe(Schema.check(Schema.isGreaterThan(0))),
    first: Schema.String,
  }),
  closed,
);

/**
 * The refusal of every read of, and write to, a Session the lineage audit
 * found broken. Work settled for it carries `diagnostic`, a bounded form of
 * the message that still names the Session and its reset.
 */
export class BrokenSessionError extends ShiitakeError {
  constructor(
    readonly store: string,
    readonly sessionId: string,
    message: string,
  ) {
    super("store", "unavailable", message);
  }

  get diagnostic(): string {
    return `${this.store}: Woodstock Session ${this.sessionId} holds a broken lineage; delete it with deleteSession("${this.sessionId}"), or reset the owning store`;
  }
}

/** The refusal of one Session the lineage audit found broken, naming its store. */
function brokenSession(
  owner: SqliteOwner,
  sessionId: string,
): BrokenSessionError {
  const store = owner.storeName ?? "Woodstock store";
  return new BrokenSessionError(
    store,
    sessionId,
    `${store}: ${brokenSessionRefusal(owner, sessionId)}`,
  );
}

/**
 * Whether the lineage audit refused this Session. Settlement asks before it
 * records anything in the Session, which it can no longer write.
 */
export function sessionLineageRefused(
  owner: SqliteOwner,
  sessionId: string,
): boolean {
  return (
    owner.sql
      .exec(
        "SELECT session_id FROM session_lineage_damage WHERE session_id = ?",
        sessionId,
      )
      .toArray().length > 0
  );
}

/**
 * What a reader of a broken Session is told: which Session, how many of this
 * store's Sessions the audit found broken and the first of them, and the
 * steps that clear it.
 */
function brokenSessionRefusal(owner: SqliteOwner, sessionId: string): string {
  const decoded = decodeDamageCount(
    owner.sql
      .exec(
        "SELECT COUNT(*) AS damaged, MIN(session_id) AS first FROM session_lineage_damage",
      )
      .one(),
  );
  if (decoded._tag === "Failure")
    throw unavailable("Stored Session lineage damage is invalid");
  const { damaged, first } = decoded.success;
  return `Woodstock Session ${sessionId} holds a broken lineage (this store has ${String(damaged)} damaged Session${damaged === 1 ? "" : "s"}, first ${first}); a store never repairs itself — delete this Session with deleteSession("${sessionId}"), or reset the owning store: for a Durable Object, erase all of its storage (DurableObjectStorage.deleteAll(); a Hub Agent is erased only with its Team, POST /operator/teams/:teamId/erasure-durable-objects); for a local store, remove its file and the file's .owner.sqlite sidecar`;
}

/**
 * Every lineage stays whole by construction, enforced where it is written
 * rather than re-walked where it is read. Each entry's parent is an earlier
 * complete entry of its own Session, each Session has one complete root, the
 * lineage columns never change, and a parent is never removed from under a
 * child while its Session exists. So every walk from a complete leaf reaches
 * that one root over complete rows of falling sequence, and readers walk only
 * the rows they return.
 *
 * A store written while any guard was missing is audited as they are
 * installed. Each Session that breaks them is recorded in
 * `session_lineage_damage`, in the same pass: its inbox row turns
 * unavailable, every read of it is refused and no entry is written to it,
 * while every other Session opens and works as before. The guards only judge
 * later writes, and each judges one Session, so a damaged Session never
 * blocks installing them or writing elsewhere. Deleting the Session clears it.
 */
function guardSessionLineage(owner: SqliteOwner): void {
  owner.sql.exec(
    "CREATE TABLE IF NOT EXISTS session_lineage_damage (session_id TEXT PRIMARY KEY)",
  );
  const installed = decodeInstalledGuards(
    owner.sql
      .exec(
        `SELECT COUNT(*) AS installed FROM sqlite_master
          WHERE type = 'trigger'
            AND name IN (SELECT value FROM json_each(?))`,
        JSON.stringify(lineageGuards),
      )
      .one(),
  );
  if (installed._tag === "Failure")
    throw unavailable("Stored Session lineage guards are invalid");
  if (installed.success.installed !== lineageGuards.length) {
    const damaged = owner.sql
      .exec(
        `SELECT child.session_id AS session_id
           FROM session_entries AS child
           LEFT JOIN session_entries AS parent ON parent.id = child.parent_id
          WHERE child.parent_id IS NOT NULL
            AND (parent.id IS NULL
                 OR parent.session_id != child.session_id
                 OR parent.state != 'complete'
                 OR parent.sequence >= child.sequence)
         UNION
         SELECT session_id FROM session_entries
          WHERE parent_id IS NULL AND state = 'complete'
          GROUP BY session_id HAVING COUNT(*) > 1`,
      )
      .toArray()
      .map((raw) => {
        const decoded = decodeDamagedSession(raw);
        if (decoded._tag === "Failure")
          throw unavailable("Stored Session lineage audit is invalid");
        return decoded.success.session_id;
      });
    for (const sessionId of damaged) {
      owner.sql.exec(
        "INSERT OR IGNORE INTO session_lineage_damage (session_id) VALUES (?)",
        sessionId,
      );
      touchSessionInbox(owner, { sessionId, title: null });
    }
  }
  // SQLite fires DELETE triggers for the row an INSERT OR REPLACE removes
  // only under PRAGMA recursive_triggers, which is off by default and which
  // Woodstock never turns on. So the delete guard cannot see a REPLACE; this
  // guard refuses a replacing row that would leave the replaced row's
  // children under a streaming, later or foreign parent.
  owner.sql.exec(`
    CREATE TRIGGER IF NOT EXISTS session_entries_lineage_replace
    BEFORE INSERT ON session_entries
    WHEN EXISTS (
      SELECT 1 FROM session_entries AS child
       WHERE child.parent_id = NEW.id
         AND (child.session_id != NEW.session_id
              OR child.sequence <= NEW.sequence
              OR NEW.state != 'complete'))
    BEGIN
      SELECT RAISE(ABORT, 'Session entry would strand its children');
    END
  `);
  // REPLACE also resolves the UNIQUE (session_id, sequence) conflict by
  // deleting whichever row holds that position, under any id. Refuse a
  // replacing row whose position belongs to another row with children.
  owner.sql.exec(`
    CREATE TRIGGER IF NOT EXISTS session_entries_lineage_replace_position
    BEFORE INSERT ON session_entries
    WHEN EXISTS (
      SELECT 1 FROM session_entries AS held
        JOIN session_entries AS child ON child.parent_id = held.id
       WHERE held.session_id = NEW.session_id
         AND held.sequence = NEW.sequence
         AND held.id != NEW.id)
    BEGIN
      SELECT RAISE(ABORT, 'Session entry would strand its children');
    END
  `);
  owner.sql.exec(`
    CREATE TRIGGER IF NOT EXISTS session_entries_damaged_session
    BEFORE INSERT ON session_entries
    WHEN EXISTS (
      SELECT 1 FROM session_lineage_damage WHERE session_id = NEW.session_id)
    BEGIN
      SELECT RAISE(ABORT, '${brokenSessionMessage}');
    END
  `);
  owner.sql.exec(`
    CREATE TRIGGER IF NOT EXISTS session_entries_lineage_insert
    BEFORE INSERT ON session_entries
    BEGIN
      SELECT RAISE(ABORT, 'Session entry parent is not an earlier complete entry of its Session')
       WHERE NEW.parent_id IS NOT NULL AND NOT EXISTS (
         SELECT 1 FROM session_entries AS parent
          WHERE parent.id = NEW.parent_id
            AND parent.session_id = NEW.session_id
            AND parent.state = 'complete'
            AND parent.sequence < NEW.sequence);
      SELECT RAISE(ABORT, 'Session already has a root entry')
       WHERE NEW.parent_id IS NULL AND EXISTS (
         SELECT 1 FROM session_entries AS other
          WHERE other.session_id = NEW.session_id
            AND other.state = 'complete'
            AND other.id != NEW.id);
    END
  `);
  owner.sql.exec(`
    CREATE TRIGGER IF NOT EXISTS session_entries_lineage_update
    BEFORE UPDATE OF id, session_id, parent_id, sequence, state
    ON session_entries
    BEGIN
      SELECT RAISE(ABORT, 'Session entry lineage columns are immutable');
    END
  `);
  owner.sql.exec(`
    CREATE TRIGGER IF NOT EXISTS session_entries_lineage_delete
    BEFORE DELETE ON session_entries
    WHEN EXISTS (SELECT 1 FROM sessions WHERE id = OLD.session_id)
     AND EXISTS (SELECT 1 FROM session_entries WHERE parent_id = OLD.id)
    BEGIN
      SELECT RAISE(ABORT, 'Session entry still has children');
    END
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
    prepareWorkSenders(owner);
    prepareSessionDelegators(owner);
    prepareSessionGrants(owner);
    prepareSessionInbox(owner);
    owner.sql.exec(`
    CREATE TABLE IF NOT EXISTS session_creation_receipts (
      request_id TEXT PRIMARY KEY,
      session_id TEXT NOT NULL UNIQUE REFERENCES sessions(id)
    )
  `);
    // Preview admissions could have mistaken a later worker for the creator.
    owner.sql.exec(`DELETE FROM session_delegators
      WHERE NOT EXISTS (
        SELECT 1 FROM session_creation_receipts AS creation
        WHERE creation.session_id = session_delegators.session_id
          AND creation.request_id = session_delegators.mail_key
      )`);
    owner.sql.exec(
      "CREATE INDEX IF NOT EXISTS idx_session_entries_parent ON session_entries(parent_id)",
    );
    // Replace the preview index, not its payload-length expression: preparing
    // identity lookup must never read every stored message's payload.
    owner.sql.exec("DROP INDEX IF EXISTS idx_session_entries_lineage");
    owner.sql.exec(
      `CREATE INDEX IF NOT EXISTS idx_session_entries_ancestry
       ON session_entries(id, session_id, parent_id, sequence, state, type)`,
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
    owner.sql.exec(
      `CREATE INDEX IF NOT EXISTS idx_session_entries_work_terminal
       ON session_entries(session_id, work_id)
       WHERE type IN ('run_terminal', 'work_terminal')`,
    );
    // Context and preference reads look up these rare older facts directly
    // rather than walking the history behind them.
    owner.sql.exec(
      `CREATE INDEX IF NOT EXISTS idx_session_entries_model_preference
       ON session_entries(session_id, sequence)
       WHERE type = 'model_preference'`,
    );
    owner.sql.exec(
      `CREATE INDEX IF NOT EXISTS idx_session_entries_cancellation_target
       ON session_entries(session_id, json_extract(data_json, '$.targetWorkId'))
       WHERE type = 'cancellation'`,
    );
    owner.sql
      .exec(`CREATE INDEX IF NOT EXISTS idx_session_entries_successful_tool
      ON session_entries(session_id, work_id, json_extract(data_json, '$.call.id'))
      WHERE type = 'tool_outcome' AND json_extract(data_json, '$.result.isError') = 0`);
    guardSessionLineage(owner);
    owner.sql.exec(`
    CREATE TABLE IF NOT EXISTS session_request_withdrawals (
      session_id TEXT NOT NULL,
      request_id TEXT NOT NULL,
      PRIMARY KEY (session_id, request_id)
    )
  `);
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
  const leaf = readSessionRow(owner, sessionId)?.leaf_id;
  return leaf === undefined || leaf === null
    ? null
    : lineageModelPreference(owner, sessionId, leaf);
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
  return newestLineagePreference(owner, sessionId, leaf)?.sequence ?? null;
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
    const metadata = title === null ? null : encodeSessionMetadata({ title });
    if (
      owner.sql
        .exec(
          "SELECT 1 FROM sessions WHERE id = ? AND metadata_json IS ?",
          sessionId,
          metadata,
        )
        .toArray().length > 0
    )
      return;
    owner.sql.exec(
      "UPDATE sessions SET metadata_json = ? WHERE id = ?",
      metadata,
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

/** Rows one bounded lineage read decodes at once. */
export const lineageRowLimit = 200;

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
 * Each walk descends from the tip only while it is above a floor sequence,
 * so a reader of recent rows never visits the history behind them. The
 * selected walk steps only to an older parent, so it ends without
 * deduplicating; the fork walk keeps every link for its whole-lineage check.
 */
// Keep each recursive step child-first: SQLite must look up that child's
// parent, not scan every entry of the Session before joining the walk.
const lineageWalks = {
  selected: `WITH RECURSIVE branch (id, parent_id, sequence, state) AS (
       SELECT id, parent_id, sequence, state
         FROM session_entries
        WHERE session_id = ? AND id = ?
       UNION ALL
       SELECT parent.id, parent.parent_id, parent.sequence, parent.state
         FROM branch AS child
         CROSS JOIN session_entries AS parent
        WHERE parent.id = child.parent_id AND parent.session_id = ?
          AND child.sequence > ?
          AND +parent.sequence < child.sequence
     )`,
  fork: `WITH RECURSIVE branch (id, parent_id, sequence, state) AS (
       SELECT id, parent_id, sequence, state
         FROM session_entries
        WHERE session_id = ? AND id = ? AND state != 'streaming'
       UNION
       SELECT parent.id, parent.parent_id, parent.sequence, parent.state
         FROM branch AS child
         CROSS JOIN session_entries AS parent
        WHERE parent.id = child.parent_id AND parent.session_id = ?
          AND child.sequence > ?
          AND parent.state != 'streaming'
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

const decodeAncestryRow = Schema.decodeUnknownResult(
  Schema.Struct({
    id: boundedId,
    parent_id: Schema.NullOr(Schema.String),
    sequence,
    state: Schema.Literals(["complete", "streaming"]),
  }),
  closed,
);
type AncestryRow = Readonly<{
  id: string;
  parent_id: string | null;
  sequence: number;
  state: "complete" | "streaming";
}>;
type SessionRead = {
  sessionId: string;
  before: number;
  ancestry?: readonly AncestryRow[];
  encoded: Map<string, string>;
};
type AncestryRange = Readonly<{ floor: number; ceiling?: number }>;
const sessionReads = new WeakMap<SqliteOwner, SessionRead>();

/**
 * One synchronous, transaction-consistent Session baseline. Activity, timeline,
 * extension projections and context figures share its identity-only ancestry;
 * asynchronous model description runs after this scope has ended. Nothing is
 * retained between reads, and payload windows keep their row and byte bounds.
 */
export function withSessionRead<A>(
  owner: SqliteOwner,
  sessionId: string,
  read: () => A,
  before = Number.MAX_SAFE_INTEGER,
): A {
  const previous = sessionReads.get(owner);
  sessionReads.set(owner, { sessionId, before, encoded: new Map() });
  try {
    return owner.transactionSync(read);
  } finally {
    if (previous === undefined) sessionReads.delete(owner);
    else sessionReads.set(owner, previous);
  }
}

/** Join the shared Session baseline, or keep a direct projection atomic. */
export function withSessionReadTransaction<A>(
  owner: SqliteOwner,
  sessionId: string,
  read: () => A,
): A {
  return sessionReads.get(owner)?.sessionId === sessionId
    ? read()
    : owner.transactionSync(read);
}

/**
 * Walk identities once to the kept context boundary and the requested page's
 * row lookahead, whichever needs the older floor. Only compactions need their
 * payload to identify that boundary. A historical page's intervening gap is
 * traversed in SQL, not materialized into this request's identity cache.
 *
 * A context's kept entry can reuse the suffix starting at that ancestor. A later
 * tip may share only if it is the seed or an ancestor retained in that suffix;
 * another valid leaf uses its own walk instead of becoming an unavailable read.
 * A query crossing an omitted gap or asking below the cached floor also uses
 * its existing bounded walk. Nothing is retained between requests.
 */
function readAncestry(
  owner: SqliteOwner,
  sessionId: string,
  tipId: string,
  range?: AncestryRange,
): string | undefined {
  const read = sessionReads.get(owner);
  if (read?.sessionId !== sessionId) return undefined;
  read.ancestry ??= owner.sql
    .exec(
      `WITH RECURSIVE read_window (before_sequence, rows) AS (VALUES (?, ?)),
       ancestry (id, parent_id, sequence, state, head, context_closed, taken) AS (
         SELECT id, parent_id, sequence, state,
                ${compactionHead("session_entries")}, 0,
                CASE WHEN state != 'streaming'
                           AND sequence < (SELECT before_sequence FROM read_window)
                     THEN 1 ELSE 0 END
           FROM session_entries INDEXED BY idx_session_entries_ancestry
          WHERE session_id = ? AND id = ?
         UNION ALL
         SELECT parent.id, parent.parent_id, parent.sequence, parent.state,
                COALESCE(child.head, ${compactionHead("parent")}),
                child.context_closed OR
                  (child.head IS child.id AND child.state != 'streaming'),
                child.taken + CASE WHEN parent.state != 'streaming'
                         AND parent.sequence < (SELECT before_sequence FROM read_window)
                     THEN 1 ELSE 0 END
           FROM ancestry AS child
           CROSS JOIN session_entries AS parent INDEXED BY idx_session_entries_ancestry
          WHERE parent.id = child.parent_id AND parent.session_id = ?
            AND +parent.sequence < child.sequence
            AND (child.taken < (SELECT rows FROM read_window)
                 OR (child.context_closed = 0
                     AND (child.head IS NOT child.id OR child.state = 'streaming')))
       )
       SELECT id, parent_id, sequence, state FROM ancestry
        WHERE context_closed = 0
           OR sequence <= (SELECT before_sequence FROM read_window)
        ORDER BY sequence DESC`,
      read.before,
      lineageRowLimit + 1,
      sessionId,
      tipId,
      sessionId,
    )
    .toArray()
    .map((raw) => {
      const decoded = decodeAncestryRow(raw);
      if (decoded._tag === "Failure")
        throw unavailable("Stored Session ancestry is invalid");
      return decoded.success;
    });
  const start = read.ancestry.findIndex(({ id }) => id === tipId);
  if (start === -1) return undefined;
  const rows = read.ancestry.slice(start);
  if (range !== undefined) {
    const deepest = rows.at(-1);
    if (
      deepest !== undefined &&
      deepest.parent_id !== null &&
      deepest.sequence > range.floor + 1
    )
      return undefined;
    const ceiling = range.ceiling ?? Number.MAX_SAFE_INTEGER;
    for (let index = 1; index < rows.length; index += 1) {
      const child = rows[index - 1];
      const parent = rows[index];
      if (
        child !== undefined &&
        parent !== undefined &&
        child.parent_id !== parent.id &&
        range.floor < child.sequence - 1 &&
        ceiling > parent.sequence
      )
        return undefined;
    }
  }
  const encoded = read.encoded.get(tipId);
  if (encoded !== undefined) return encoded;
  const value = JSON.stringify(rows);
  read.encoded.set(tipId, value);
  return value;
}

/** A table over already-walked identities, decoded by SQLite's JSON reader. */
function ancestryTable(name: "ancestors" | "branch"): string {
  return `${name} AS MATERIALIZED (
    SELECT json_extract(value, '$.id') AS id,
           json_extract(value, '$.parent_id') AS parent_id,
           json_extract(value, '$.sequence') AS sequence,
           json_extract(value, '$.state') AS state
      FROM json_each(?)
  )`;
}

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
const decodeLineageSequence = Schema.decodeUnknownResult(
  Schema.Struct({ sequence }),
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
 * One root-to-tip lineage. Its integrity is enforced where entries are
 * written (`guardSessionLineage`), so locating it reads only its tip and its
 * root; readers take bounded windows of it by sequence, walking down from the
 * tip only as far as their floor.
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

/**
 * Run one query over the lineage rows above `floor`: the walk descends from
 * the tip while its rows are above it, so only those rows, and the first at
 * or below it, are visited.
 */
function lineageQuery(
  owner: SqliteOwner,
  lineage: SessionLineage,
  range: AncestryRange,
  query: string,
  ...bindings: SqliteValue[]
): readonly RawRow[] {
  const ancestry =
    lineage.walk === "selected"
      ? readAncestry(owner, lineage.sessionId, lineage.tipId, range)
      : undefined;
  if (ancestry !== undefined)
    return owner.sql
      .exec(`WITH ${ancestryTable("branch")} ${query}`, ancestry, ...bindings)
      .toArray();
  return owner.sql
    .exec(
      `${lineageWalks[lineage.walk]} ${query}`,
      lineage.sessionId,
      lineage.tipId,
      lineage.sessionId,
      range.floor,
      ...bindings,
    )
    .toArray();
}

/**
 * Walk and check one whole root-to-tip chain in SQL, the same way decoded
 * rows would be. A fork copies every row of it, so it checks every row.
 */
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
        0,
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

const decodeLineageEnds = Schema.decodeUnknownResult(
  Schema.Struct({
    state: Schema.Literals(["complete", "streaming"]),
    newest_sequence: sequence,
    root_id: Schema.NullOr(boundedId),
    root_parent_id: Schema.NullOr(Schema.String),
  }),
  closed,
);

/**
 * Locate a selected lineage from its two ends; undefined when the Session
 * has no leaf. The guards make a Session's one complete root the root of
 * every lineage in it, so neither end needs a walk.
 */
export function readSessionLineage(
  owner: SqliteOwner,
  input: ReadSelectedPathInput,
): SessionLineage | undefined {
  // Read the Session row even for an explicit tip: it refuses a broken one.
  const session = readSessionRow(owner, input.sessionId);
  const tip = input.leafId ?? session?.leaf_id;
  if (tip === undefined || tip === null) return undefined;
  const failures = lineageFailures.selected;
  const raw = owner.sql
    .exec(
      `SELECT tip.state AS state, tip.sequence AS newest_sequence,
              root.id AS root_id, root.parent_id AS root_parent_id
         FROM session_entries AS tip
         LEFT JOIN (
           SELECT id, parent_id FROM session_entries
            WHERE session_id = ? AND state = 'complete'
            ORDER BY sequence ASC LIMIT 1
         ) AS root
        WHERE tip.session_id = ? AND tip.id = ?`,
      input.sessionId,
      input.sessionId,
      tip,
    )
    .toArray()[0];
  if (raw === undefined) throw unavailable(failures.empty);
  const ends = decodeLineageEnds(raw);
  if (ends._tag === "Failure")
    throw unavailable("Stored Session lineage ends are invalid");
  if (ends.success.state !== "complete" || ends.success.root_id === null)
    throw unavailable(failures.empty);
  if (ends.success.root_parent_id !== null)
    throw unavailable(failures.missingParent);
  return Object.freeze({
    sessionId: input.sessionId,
    walk: "selected",
    tipId: tip,
    rootId: ends.success.root_id,
    newestId: tip,
    newestSequence: ends.success.newest_sequence,
  });
}

function readLineageRange(
  owner: SqliteOwner,
  lineage: SessionLineage,
  oldest: number,
  newest: number,
): readonly StoredSessionRow[] {
  const rows = Object.freeze(
    lineageQuery(
      owner,
      lineage,
      { floor: oldest - 1, ceiling: newest },
      `SELECT ${entryColumns}
         FROM session_entries
        WHERE session_id = ? AND state != 'streaming'
          AND sequence BETWEEN ? AND ?
          AND id IN (SELECT id FROM branch)
        ORDER BY sequence ASC`,
      lineage.sessionId,
      oldest,
      newest,
    ).map((raw) => parseEntryRow(owner, raw)),
  );
  // The walk stopped at the range or at the root, never at a missing parent.
  const failures = lineageFailures.selected;
  const deepest = rows[0];
  if (
    deepest !== undefined &&
    deepest.parentId !== null &&
    deepest.sequence !== oldest
  )
    throw unavailable(failures.missingParent);
  let previous: StoredSessionRow | undefined;
  for (const row of rows) {
    if (previous !== undefined && row.parentId !== previous.id)
      throw unavailable(failures.invalid);
    previous = row;
  }
  return rows;
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

const decodeWindowSizes = Schema.decodeUnknownResult(
  Schema.Struct({
    sizes: Schema.fromJsonString(Schema.Array(Schema.Tuple([sequence, count]))),
    deepest_parent: Schema.NullOr(Schema.String),
    deepest_sequence: Schema.NullOr(sequence),
  }),
  closed,
);

/**
 * Sizes of up to `rows` lineage rows inside the bounds, newest or oldest
 * first. A newest window's walk counts its rows as it descends and stops once
 * it holds that many; an oldest window's walk stops at `after`. Either way the
 * walk visits the rows from the tip down to the window, never the history
 * below it. The deepest walked row comes back with the sizes, so a walk that
 * ended at a missing parent above where it meant to stop is refused, never
 * read as the end of history. When a read already owns the ancestry, choose
 * the same bounded candidate identities before consulting their record sizes.
 * SQLite's octet_length(column) reads the byte count from record metadata,
 * without loading the text's overflow pages or a payload-length index.
 */
function readWindowSizes(
  owner: SqliteOwner,
  lineage: SessionLineage,
  from: LineageWindowInput["from"],
  after: number,
  before: number,
  rows: number,
): readonly Readonly<{ sequence: number; bytes: number }>[] {
  let ancestry =
    lineage.walk === "selected"
      ? readAncestry(owner, lineage.sessionId, lineage.tipId)
      : undefined;
  if (ancestry !== undefined) {
    const candidates = sessionReads
      .get(owner)
      ?.ancestry?.filter(
        (row) =>
          row.sequence <= lineage.newestSequence &&
          row.state !== "streaming" &&
          row.sequence > after &&
          row.sequence < before,
      );
    const last = from === "newest" ? candidates?.[rows - 1] : undefined;
    ancestry = readAncestry(owner, lineage.sessionId, lineage.tipId, {
      floor: last === undefined ? after : last.sequence - 1,
      ceiling: before - 1,
    });
  }
  const direction = from === "newest" ? "DESC" : "ASC";
  const ends = `SELECT
         (SELECT json_group_array(json_array(sequence, bytes)) FROM (
            SELECT sequence, octet_length(data_json) AS bytes
              FROM session_entries
             WHERE ${from === "newest" ? "" : "session_id = ? AND"}
                   state != 'streaming'
               AND sequence > ? AND sequence < ?
               AND id IN (SELECT id FROM ${ancestry === undefined ? "branch" : "candidates"})
             ORDER BY sequence ${direction}
             LIMIT ?)) AS sizes,
         (SELECT parent_id FROM branch ORDER BY sequence ASC LIMIT 1)
           AS deepest_parent,
         (SELECT sequence FROM branch ORDER BY sequence ASC LIMIT 1)
           AS deepest_sequence`;
  const counts = (row: string) =>
    `CASE WHEN ${row}.state != 'streaming'
               AND ${row}.sequence < ? AND ${row}.sequence > ?
          THEN 1 ELSE 0 END`;
  const raw =
    ancestry !== undefined
      ? owner.sql
          .exec(
            `WITH ${ancestryTable("branch")},
             candidates AS MATERIALIZED (
               SELECT id FROM branch
                WHERE state != 'streaming' AND sequence > ? AND sequence < ?
                ORDER BY sequence ${direction} LIMIT ?
             ) ${ends}`,
            ancestry,
            after,
            before,
            rows,
            ...(from === "newest" ? [] : [lineage.sessionId]),
            after,
            before,
            rows,
          )
          .toArray()
      : from === "newest"
        ? owner.sql
            .exec(
              `WITH RECURSIVE branch (id, parent_id, sequence, state, taken) AS (
               SELECT id, parent_id, sequence, state,
                      ${counts("session_entries")}
                 FROM session_entries
                WHERE session_id = ? AND id = ?
               UNION ALL
               SELECT parent.id, parent.parent_id, parent.sequence, parent.state,
                      child.taken + ${counts("parent")}
                 FROM branch AS child
                 CROSS JOIN session_entries AS parent
                WHERE parent.id = child.parent_id AND parent.session_id = ?
                  AND +parent.sequence < child.sequence
                  AND child.taken < ? AND child.sequence > ?
             )
             ${ends}`,
              before,
              after,
              lineage.sessionId,
              lineage.tipId,
              before,
              after,
              lineage.sessionId,
              rows,
              after,
              after,
              before,
              rows,
            )
            .toArray()
        : lineageQuery(
            owner,
            lineage,
            { floor: after },
            ends,
            lineage.sessionId,
            after,
            before,
            rows,
          );
  const decoded = decodeWindowSizes(raw[0]);
  if (decoded._tag === "Failure")
    throw unavailable("Stored Session entry size is invalid");
  const { sizes, deepest_parent, deepest_sequence } = decoded.success;
  const stopped =
    deepest_parent === null ||
    deepest_sequence === null ||
    deepest_sequence <= after ||
    (from === "newest" && sizes.length === rows);
  if (!stopped) throw unavailable(lineageFailures.selected.missingParent);
  return Object.freeze(
    sizes
      .map(([rowSequence, bytes]) => ({ sequence: rowSequence, bytes }))
      .sort((left, right) =>
        from === "newest"
          ? right.sequence - left.sequence
          : left.sequence - right.sequence,
      ),
  );
}

/**
 * Read one contiguous window of a lineage, bounded by row count and by stored
 * bytes. Row sizes are measured in SQL before any entry is decoded.
 */
export function readLineageWindow(
  owner: SqliteOwner,
  lineage: SessionLineage,
  input: LineageWindowInput,
): LineageWindow {
  const sizes = readWindowSizes(
    owner,
    lineage,
    input.from,
    input.after ?? 0,
    input.before ?? Number.MAX_SAFE_INTEGER,
    input.rows + 1,
  );
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
      { floor: rowSequence - 1, ceiling: rowSequence },
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
  const raw = owner.sql
    .exec(
      `SELECT sequence FROM session_entries
        WHERE session_id = ? AND id = ? AND state != 'streaming'`,
      lineage.sessionId,
      id,
    )
    .toArray()[0];
  if (raw === undefined) return undefined;
  const decoded = decodeLineageSequence(raw);
  if (decoded._tag === "Failure")
    throw unavailable("Stored Session entry sequence is invalid");
  return lineageHasSequence(owner, lineage, decoded.success.sequence)
    ? decoded.success.sequence
    : undefined;
}

/**
 * The newest preference on the lineage ending at `tipId`. The Session's
 * preference rows come from their index; the walk from the tip stops at the
 * first of them it meets, so it never passes the newest preference on this
 * lineage, and a Session without one walks nothing. One set at the root
 * still walks the whole lineage.
 */
function newestLineagePreference(
  owner: SqliteOwner,
  sessionId: string,
  tipId: string,
): StoredSessionRow | undefined {
  const continues = (row: string) =>
    `${row}.id NOT IN (SELECT id FROM targets)
     AND ${row}.sequence > (SELECT MIN(sequence) FROM targets)`;
  const [newest] = readTargetIds(
    owner,
    sessionId,
    tipId,
    `SELECT id, sequence FROM session_entries
            WHERE session_id = ? AND type = 'model_preference'
              AND state != 'streaming'`,
    [sessionId],
    continues,
  );
  return newest === undefined
    ? undefined
    : readEntryRow(owner, sessionId, newest);
}

function lineageModelPreference(
  owner: SqliteOwner,
  sessionId: string,
  tipId: string,
): ModelPreference | null {
  const entry = newestLineagePreference(owner, sessionId, tipId)?.entry;
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
  // No index finds documents yet, so this still walks the whole lineage.
  const raw = lineageQuery(
    owner,
    lineage,
    { floor: 0 },
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
  const entry = raw === undefined ? undefined : parseEntryRow(owner, raw).entry;
  if (entry === undefined) return undefined;
  if (
    entry.kind !== "document" ||
    entry.extensionId !== extensionId ||
    entry.name !== name
  )
    throw unavailable("Stored Session document disagrees with its row");
  return entry.valueJson;
}

const decodeWorkBounds = Schema.decodeUnknownResult(
  Schema.Struct({
    oldest: Schema.NullOr(sequence),
    newest: Schema.NullOr(sequence),
  }),
  closed,
);

/** What a bounded client page needs to know about the works it shows. */
export type LineageWorkFacts = Readonly<{
  /** Works whose command entry lies on the lineage. */
  commands: ReadonlySet<string>;
  /** Newest owner-produced terminal diagnostic on the lineage. */
  diagnostics: ReadonlyMap<string, string>;
  /** Host metadata of the cancellation that ended each work. */
  cancellations: ReadonlyMap<string, Json>;
}>;

/**
 * Look up, on the lineage, only the facts of the given works. Diagnostics come
 * only from owned terminal fields, never provider message or retry text. The works'
 * indexes give their oldest row, and the walks stop just below it, so a page
 * of recent works never visits older history.
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
  const bounds = decodeWorkBounds(
    owner.sql
      .exec(
        `SELECT MIN(oldest) AS oldest, MAX(newest) AS newest FROM (
           SELECT MIN(sequence) AS oldest, MAX(sequence) AS newest
             FROM session_entries INDEXED BY idx_session_entries_work
            WHERE work_id IN (SELECT value FROM json_each(?))
              AND session_id = ?
           UNION ALL
           SELECT MIN(sequence), MAX(sequence) FROM session_entries
                  INDEXED BY idx_session_entries_cancellation_target
            WHERE session_id = ? AND type = 'cancellation'
              AND json_extract(data_json, '$.targetWorkId')
                  IN (SELECT value FROM json_each(?))
         )`,
        works,
        lineage.sessionId,
        lineage.sessionId,
        works,
      )
      .one(),
  );
  if (bounds._tag === "Failure")
    throw unavailable("Stored Session work order is invalid");
  if (bounds.success.oldest === null || bounds.success.newest === null)
    return Object.freeze({
      commands: new Set<string>(),
      diagnostics: new Map<string, string>(),
      cancellations: new Map<string, Json>(),
    });
  const floor = bounds.success.oldest - 1;
  const range = { floor, ceiling: bounds.success.newest };
  const commands = new Set(
    lineageQuery(
      owner,
      lineage,
      range,
      `SELECT DISTINCT work_id
         FROM session_entries
        WHERE session_id = ? AND sequence > ? AND state != 'streaming' AND type = 'command'
          AND work_id IN (SELECT value FROM json_each(?))
          AND id IN (SELECT id FROM branch)`,
      lineage.sessionId,
      floor,
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
    range,
    `SELECT work_id, diagnostic FROM (
       SELECT work_id, sequence,
              json_extract(data_json, '$.diagnostic') AS diagnostic
         FROM session_entries
        WHERE session_id = ? AND sequence > ? AND state != 'streaming'
          AND work_id IN (SELECT value FROM json_each(?))
          AND type IN ('work_terminal', 'run_terminal')
          AND id IN (SELECT id FROM branch)
     )
     WHERE diagnostic IS NOT NULL
     ORDER BY sequence ASC`,
    lineage.sessionId,
    floor,
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
    range,
    `SELECT ${entryColumns}
       FROM session_entries
      WHERE session_id = ? AND sequence > ? AND state != 'streaming' AND type = 'cancellation'
        AND json_extract(data_json, '$.targetWorkId')
            IN (SELECT value FROM json_each(?))
        AND id IN (SELECT id FROM branch)
      ORDER BY sequence ASC`,
    lineage.sessionId,
    floor,
    works,
  ).map((raw) => parseEntryRow(owner, raw)))
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

/**
 * A walk from one row toward the root for the `targets` the statement
 * selects first by index. `continues` decides, per row, whether the walk
 * steps past it; a statement without targets reads only its starting row,
 * so a missing one is still refused.
 */
function targetWalk(
  sessionId: string,
  tipId: string,
  continues: string,
  ancestry: string | undefined,
) {
  if (ancestry !== undefined)
    return {
      sql: `${ancestryTable("ancestors")},
        stopped AS (
          SELECT child.*,
                 COALESCE(SUM(CASE WHEN ${continues} THEN 0 ELSE 1 END)
                   OVER (ORDER BY child.sequence DESC
                     ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS stops
            FROM ancestors AS child
        ),
        walk AS (
          SELECT id, parent_id, sequence, state FROM stopped WHERE stops = 0
        )`,
      bindings: [ancestry],
      recursive: false,
    };
  return {
    sql: `walk (id, parent_id, sequence, state) AS (
       SELECT id, parent_id, sequence, state
         FROM session_entries
        WHERE session_id = ? AND id = ?
       UNION ALL
       SELECT parent.id, parent.parent_id, parent.sequence, parent.state
         FROM walk AS child
         CROSS JOIN session_entries AS parent
        WHERE parent.id = child.parent_id AND parent.session_id = ?
          AND +parent.sequence < child.sequence
          AND ${continues}
     )`,
    bindings: [sessionId, tipId, sessionId],
    recursive: true,
  };
}

/**
 * A shared prefix can answer a target read only if it reaches that read's
 * stopping point without crossing an omitted gap. Otherwise run the original
 * target-bounded walk; a cache horizon is not evidence of damaged history.
 */
function readTargetIds(
  owner: SqliteOwner,
  sessionId: string,
  tipId: string,
  targets: string,
  bindings: readonly SqliteValue[],
  continues: (row: string) => string,
): readonly string[] {
  const ancestry = readAncestry(owner, sessionId, tipId);
  const run = (shared: string | undefined) => {
    const walk = targetWalk(sessionId, tipId, continues("child"), shared);
    return owner.sql
      .exec(
        `WITH ${walk.recursive ? "RECURSIVE " : ""}targets AS MATERIALIZED (
           ${targets}
         ),
         ${walk.sql}
         ${walkSteps(continues("walk"))}`,
        ...bindings,
        ...walk.bindings,
      )
      .toArray();
  };
  let raw = run(ancestry);
  if (ancestry !== undefined) {
    const last = raw.at(-1);
    if (last !== undefined) {
      const deepest = decodeWalkStep(last);
      if (deepest._tag === "Failure")
        throw unavailable("Stored Session walk row is invalid");
      if (
        (deepest.success.continues === 1 &&
          deepest.success.parent_id !== null) ||
        readAncestry(owner, sessionId, tipId, {
          floor: deepest.success.sequence - 1,
        }) === undefined
      )
        raw = run(undefined);
    }
  }
  return walkedTargets(raw);
}

/**
 * The walked targets, newest first, then the deepest walked row, each with
 * whether the walk meant to pass it. Only these rows reach JavaScript.
 */
function walkSteps(continues: string): string {
  return `SELECT id, parent_id, sequence, target, continues FROM (
             SELECT id, parent_id, sequence,
                    id IN (SELECT id FROM targets) AS target,
                    COALESCE((${continues}), 0) AS continues
               FROM walk
           )
           WHERE target = 1 OR sequence = (SELECT MIN(sequence) FROM walk)
           ORDER BY sequence DESC`;
}

const flag = Schema.Literals([0, 1]);
const decodeWalkStep = Schema.decodeUnknownResult(
  Schema.Struct({
    id: boundedId,
    parent_id: Schema.NullOr(Schema.String),
    sequence,
    target: flag,
    continues: flag,
  }),
  closed,
);

/**
 * The targets a bounded walk reached, newest first, once the walk is known to
 * have ended at the root or where it meant to stop, never at a missing or
 * out-of-order parent. Walks step only to an older parent, so they end.
 * A walk that found not even its starting row is refused as an unavailable
 * leaf.
 */
function walkedTargets(raws: readonly RawRow[]): readonly string[] {
  const failures = lineageFailures.selected;
  if (raws.length === 0) throw unavailable(failures.empty);
  const steps = raws.map((raw) => {
    const decoded = decodeWalkStep(raw);
    if (decoded._tag === "Failure")
      throw unavailable("Stored Session walk row is invalid");
    return decoded.success;
  });
  const deepest = steps.at(-1);
  if (deepest?.continues === 1 && deepest.parent_id !== null)
    throw unavailable(failures.missingParent);
  return steps.flatMap((step) => (step.target === 1 ? [step.id] : []));
}

/** The kept head of a complete compaction row, else null. */
function compactionHead(row: string): string {
  return `CASE WHEN ${row}.type = 'compaction' AND ${row}.state != 'streaming'
               THEN json_extract(${row}.data_json, '$.head') END`;
}

/**
 * The walk a settled context reads: from the tip back to the kept entry of
 * the newest complete compaction, which `head` carries once passed, or to the
 * root when nothing compacted or that entry is not on this lineage. It never
 * visits the history a compaction already cut.
 */
const contextWalk = `WITH RECURSIVE walk (id, parent_id, sequence, state, head) AS (
       SELECT id, parent_id, sequence, state,
              ${compactionHead("session_entries")}
         FROM session_entries
        WHERE session_id = ? AND id = ?
       UNION ALL
       SELECT parent.id, parent.parent_id, parent.sequence, parent.state,
              COALESCE(child.head, ${compactionHead("parent")})
         FROM walk AS child
         CROSS JOIN session_entries AS parent
        WHERE parent.id = child.parent_id AND parent.session_id = ?
          AND +parent.sequence < child.sequence
          AND (child.head IS NOT child.id OR child.state = 'streaming')
     )`;

/**
 * The older facts that still decide a kept window: compactions of the active
 * work and cancellations of a command inside the window. Their indexes find
 * them; the walk down from the kept entry stops at the oldest of them, so
 * only facts on this lineage count and a window without any walks nothing.
 */
function readDecidingFacts(
  owner: SqliteOwner,
  sessionId: string,
  kept: StoredSessionRow,
  window: readonly StoredSessionRow[],
  workId: string | null,
): readonly StoredSessionRow[] {
  const commands = JSON.stringify(
    window.flatMap(({ entry }) =>
      entry.kind === "command" ? [entry.workId] : [],
    ),
  );
  const continues = (row: string) =>
    `${row}.sequence > (SELECT MIN(sequence) FROM targets)`;
  const facts = readTargetIds(
    owner,
    sessionId,
    kept.id,
    `SELECT id, sequence FROM session_entries
          WHERE session_id = ? AND type = 'compaction' AND work_id = ?
            AND sequence < ? AND state != 'streaming'
         UNION ALL
         SELECT id, sequence
           FROM session_entries
                INDEXED BY idx_session_entries_cancellation_target
          WHERE session_id = ? AND type = 'cancellation'
            AND json_extract(data_json, '$.targetWorkId')
                IN (SELECT value FROM json_each(?))
            AND sequence < ? AND state != 'streaming'`,
    [sessionId, workId, kept.sequence, sessionId, commands, kept.sequence],
    continues,
  );
  if (facts.length === 0) return Object.freeze([]);
  return Object.freeze(
    owner.sql
      .exec(
        `SELECT ${entryColumns}
           FROM session_entries
          WHERE id IN (SELECT value FROM json_each(?))
          ORDER BY sequence ASC`,
        JSON.stringify(facts),
      )
      .toArray()
      .map((raw) => parseEntryRow(owner, raw)),
  );
}

/**
 * One lineage's settled window: the walk from `tipId` to the kept entry of
 * its newest complete compaction, or to the root. `compacted` says the walk
 * stopped at a kept entry rather than at the root. Only the walked rows are
 * read, and they are checked the way `locateLineage` checks a whole lineage.
 */
function readContextWindow(
  owner: SqliteOwner,
  sessionId: string,
  tipId: string,
): Readonly<{ rows: readonly StoredSessionRow[]; compacted: boolean }> {
  const failures = lineageFailures.selected;
  let ancestry = readAncestry(owner, sessionId, tipId);
  // The seed owns this cache's compaction boundary. An ancestor tip can have an
  // older boundary of its own (including one in an omitted historical gap).
  if (sessionReads.get(owner)?.ancestry?.[0]?.id !== tipId)
    ancestry = undefined;
  const source =
    ancestry === undefined
      ? { sql: contextWalk, bindings: [sessionId, tipId, sessionId] }
      : {
          sql: `WITH ${ancestryTable("ancestors")},
            boundary AS (
              SELECT sequence, json_extract(data_json, '$.head') AS head
                FROM session_entries
               WHERE session_id = ? AND type = 'compaction'
                 AND state != 'streaming'
                 AND id IN (SELECT id FROM ancestors)
               ORDER BY sequence DESC LIMIT 1
            ),
            walk AS (
              SELECT * FROM ancestors
               WHERE sequence >= COALESCE((
                 SELECT ancestors.sequence FROM ancestors, boundary
                  WHERE ancestors.id = boundary.head
                    AND ancestors.state != 'streaming'
                    AND ancestors.sequence <= boundary.sequence
               ), 0)
            )`,
          bindings: [ancestry, sessionId],
        };
  const rows = owner.sql
    .exec(
      `${source.sql}
       SELECT ${entryColumns}
         FROM session_entries
        WHERE id IN (SELECT id FROM walk) AND state != 'streaming'
        ORDER BY sequence ASC`,
      ...source.bindings,
    )
    .toArray()
    .map((raw) => parseEntryRow(owner, raw));
  const kept = rows[0];
  if (kept === undefined) throw unavailable(failures.empty);
  let head: string | undefined;
  let previous: StoredSessionRow | undefined;
  for (const row of rows) {
    if (previous !== undefined && row.parentId !== previous.id)
      throw unavailable(failures.invalid);
    if (row.entry.kind === "compaction") head = row.entry.head;
    previous = row;
  }
  const compacted = head === kept.id;
  if (!compacted && kept.parentId !== null)
    throw unavailable(failures.missingParent);
  return Object.freeze({ rows: Object.freeze(rows), compacted });
}

/**
 * Context rows of the current selected lineage for one accepted work, or,
 * without a work, the settled context the next run starts from: the kept
 * window through the tip, preceded only by the older facts that still decide
 * it. Those are cancellations that withdrew a command inside the window, and
 * earlier compactions of the active work, whose recovery runs at most once.
 * The read is bounded by the kept window and its deciding facts, not by the
 * history a compaction cut.
 */
export function readSessionContextPath(
  owner: SqliteOwner,
  input: Readonly<{ sessionId: string; workId?: string }>,
): readonly StoredSessionRow[] {
  const leaf = readSessionRow(owner, input.sessionId)?.leaf_id;
  if (leaf === undefined || leaf === null) return Object.freeze([]);
  const window = readContextWindow(owner, input.sessionId, leaf);
  const kept = window.rows[0];
  if (!window.compacted || kept === undefined) return window.rows;
  return Object.freeze([
    ...readDecidingFacts(
      owner,
      input.sessionId,
      kept,
      window.rows,
      input.workId ?? null,
    ),
    ...window.rows,
  ]);
}

/** Whether a row is an older fact that decides `commands` for `workId`. */
function decides(
  row: StoredSessionRow,
  workId: string,
  commands: ReadonlySet<string>,
): boolean {
  return (
    (row.entry.kind === "compaction" && row.entry.workId === workId) ||
    (row.entry.kind === "cancellation" && commands.has(row.entry.targetWorkId))
  );
}

/**
 * The context a prepared run reads: its command's own lineage through the
 * command, then only this work's later rows on the current lineage (the
 * tail). The newest compaction of the tail, else of the command's lineage,
 * decides where the window starts, with the same deciding facts before it as
 * a settled context. The command's lineage is walked only from the command
 * down to that start, and the tail only down to the command.
 */
function readRunContextRows(
  owner: SqliteOwner,
  sessionId: string,
  command: StoredSessionRow,
  current: SessionLineage | undefined,
  workId: string,
): readonly StoredSessionRow[] {
  const through = readSessionLineage(owner, { sessionId, leafId: command.id });
  if (through === undefined)
    throw unavailable("Accepted Session command lineage is unavailable");
  const tail =
    current === undefined
      ? []
      : lineageQuery(
          owner,
          current,
          { floor: command.sequence },
          `SELECT ${entryColumns}
             FROM session_entries
            WHERE session_id = ? AND sequence > ? AND state != 'streaming'
              AND id IN (SELECT id FROM branch)
              AND (work_id = ?
                   OR (type = 'command'
                       AND json_extract(data_json, '$.targetWorkId') = ?))
            ORDER BY sequence ASC`,
          sessionId,
          command.sequence,
          workId,
          workId,
        ).map((raw) => parseEntryRow(owner, raw));
  let latest: string | undefined;
  for (const row of tail)
    if (row.entry.kind === "compaction") latest = row.entry.head;
  let start = 0;
  let lineageRows: readonly StoredSessionRow[];
  if (latest === undefined) {
    const window = readContextWindow(owner, sessionId, command.id);
    lineageRows = window.rows;
    if (window.compacted) start = window.rows[0]?.sequence ?? 0;
  } else {
    const head = latest;
    start =
      tail.find(({ id }) => id === head)?.sequence ??
      lineageEntrySequence(owner, through, head) ??
      0;
    lineageRows =
      start > command.sequence
        ? []
        : readLineageRange(
            owner,
            through,
            Math.max(start, 1),
            command.sequence,
          );
  }
  if (start === 0) return Object.freeze([...lineageRows, ...tail]);
  const window = [
    ...lineageRows,
    ...tail.filter(({ sequence }) => sequence >= start),
  ];
  const commands = new Set(
    window.flatMap(({ entry }) =>
      entry.kind === "command" ? [entry.workId] : [],
    ),
  );
  return Object.freeze([
    ...readDecidingFacts(
      owner,
      sessionId,
      lineageRows[0] ?? command,
      window,
      workId,
    ),
    ...tail.filter(
      (row) => row.sequence < start && decides(row, workId, commands),
    ),
    ...window,
  ]);
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
): readonly ModelMessage[] {
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
      : path.findIndex(({ id }) => id === compactionEntry.head);
  const contextPath = keptIndex >= 0 ? path.slice(keptIndex) : path;
  const visible = contextMessageIds(contextPath, activeWorkId);
  const messages: ModelMessage[] = [];
  if (compactionEntry !== undefined) messages.push(...compactionEntry.model);
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
    const continuedPath = readRunContextRows(
      owner,
      input.sessionId,
      command,
      current,
      workId,
    );
    const selectedCommand: PreparedSessionRun["command"] = Object.freeze({
      ...command,
      entry: command.entry,
    });
    return Object.freeze({
      command: selectedCommand,
      modelPreference: lineageModelPreference(
        owner,
        input.sessionId,
        command.id,
      ),
      messages: buildSessionContext(continuedPath, command.entry.workId),
      path: continuedPath,
      entryDelivery,
      resume: recoverAssistantTurn(work),
      abnormalTerminal: abnormalTerminal(work),
    });
  });
}

/**
 * Whether a summary whose first kept entry is `head` still fits the selected
 * lineage (Pi Durable §6). It is stale when `head` is not on the selected
 * lineage or sits before the current context start, where a later compaction
 * already cut; placing it would bring cut history back. One bounded read.
 */
export function compactionHeadState(
  owner: SqliteOwner,
  input: Readonly<{ sessionId: string; head: string }>,
): "current" | "stale" {
  const leaf = readSessionRow(owner, input.sessionId)?.leaf_id;
  if (leaf === undefined || leaf === null) return "stale";
  // The head is current exactly when it lies in the settled window.
  const raw = owner.sql
    .exec(
      `${contextWalk}
       SELECT sequence
         FROM session_entries
        WHERE state != 'streaming' AND id = ? AND id IN (SELECT id FROM walk)`,
      input.sessionId,
      leaf,
      input.sessionId,
      input.head,
    )
    .toArray()[0];
  if (raw === undefined) return "stale";
  const decoded = decodeLineageSequence(raw);
  if (decoded._tag === "Failure")
    throw unavailable("Stored Session entry sequence is invalid");
  return "current";
}

/** One compaction that committed nothing, recorded so clients show the result. */
export type CompactionNoticeInput = Readonly<{
  sessionId: string;
  workId: string;
  trigger: CompactionTrigger;
}> &
  (
    | Readonly<{
        result: "nothing_to_compact";
        /** Bounded, display-safe notice diagnostic. */
        diagnostic?: string | undefined;
      }>
    | Readonly<{
        result: "failed";
        /** Bounded, display-safe reason for the failure. */
        diagnostic?: string | undefined;
        /** Known completed summary spend, even if its handoff failed. */
        usage?: Usage | undefined;
      }>
    | Readonly<{
        /** A finished summary dropped because its head moved. */
        result: "stale";
        /** What the dropped summary cost, so its spend is not lost. */
        usage?: Usage | undefined;
      }>
    | Readonly<{
        /** A finished summary dropped because, placed, it would not fit. */
        result: "insufficient";
        /** What the dropped summary cost, so its spend is not lost. */
        usage?: Usage | undefined;
      }>
  );

/** Append one compaction notice on the selected lineage. */
export function appendCompactionNoticeOwned(
  owner: SqliteOwner,
  input: CompactionNoticeInput,
): StoredSessionRow {
  return owner.transactionSync(() => {
    const common = {
      kind: "compaction_notice",
      workId: input.workId,
      trigger: input.trigger,
    } as const;
    let entry: Extract<StoredSessionEntry, { kind: "compaction_notice" }>;
    if (input.result === "stale" || input.result === "insufficient") {
      const dropped = { ...common, result: input.result };
      entry =
        input.usage === undefined
          ? dropped
          : { ...dropped, usage: input.usage };
    } else if (input.result === "failed") {
      const failed: Extract<
        StoredSessionEntry,
        { kind: "compaction_notice"; result: "failed" }
      > = {
        ...common,
        result: "failed",
      };
      const diagnosed =
        input.diagnostic === undefined
          ? failed
          : { ...failed, diagnostic: input.diagnostic.slice(0, 200) };
      entry =
        input.usage === undefined
          ? diagnosed
          : { ...diagnosed, usage: input.usage };
    } else {
      const nothing: Extract<
        StoredSessionEntry,
        { kind: "compaction_notice"; result: "nothing_to_compact" }
      > = {
        ...common,
        result: "nothing_to_compact",
      };
      entry =
        input.diagnostic === undefined
          ? nothing
          : { ...nothing, diagnostic: input.diagnostic.slice(0, 200) };
    }
    return appendSessionEntry(owner, {
      sessionId: input.sessionId,
      id: sessionIds.fact.generate(),
      entry,
    });
  });
}

/** Atomically append one compaction transition using the selected leaf CAS. */
export function appendCompactionTransitionOwned(
  owner: SqliteOwner,
  input: AppendCompactionInput,
): StoredSessionRow {
  return owner.transactionSync(() => appendCompactionRow(owner, input));
}

/**
 * Place a summary selected before now (a background compaction): the head
 * check of `compactionHeadState` and the leaf CAS in one transaction, so a
 * head that moved after any earlier check still makes it `stale` (Pi
 * Durable §6). A stale summary writes nothing.
 */
export function placeCompactionTransitionOwned(
  owner: SqliteOwner,
  input: AppendCompactionInput,
): StoredSessionRow | "stale" {
  return owner.transactionSync(() =>
    compactionHeadState(owner, {
      sessionId: input.sessionId,
      head: input.head,
    }) === "stale"
      ? "stale"
      : appendCompactionRow(owner, input),
  );
}

/** The leaf CAS and the compaction row; callers own the transaction. */
function appendCompactionRow(
  owner: SqliteOwner,
  input: AppendCompactionInput,
): StoredSessionRow {
  const session = readSessionRow(owner, input.sessionId);
  if (session?.leaf_id !== input.expectedLeafId)
    throw new ShiitakeError(
      "store",
      "conflict",
      "Session leaf changed during compaction",
    );
  const entry: StoredSessionEntry = {
    kind: "compaction",
    workId: input.workId,
    head: input.head,
    model: [compactionSummaryMessage(input.summary, Date.now())],
    data: {
      reason: input.trigger,
      resumesRun: input.resumesRun ?? false,
      tokensBefore: input.tokensBefore,
      tokensAfter: input.tokensAfter,
      usage: input.usage,
      contextRevision: input.contextRevision,
    },
  };
  const withReceipt =
    input.receipt === undefined
      ? entry
      : { ...entry, data: { ...entry.data, receipt: input.receipt } };
  return appendSessionEntry(owner, {
    sessionId: input.sessionId,
    id: sessionIds.fact.generate(),
    entry:
      input.details === undefined
        ? withReceipt
        : {
            ...withReceipt,
            data: { ...withReceipt.data, details: input.details },
          },
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
    left?.outcome === right?.outcome &&
    (left?.outcome === "undeliverable"
      ? right?.outcome === "undeliverable" && left.reason === right.reason
      : right?.outcome !== "undeliverable" &&
        left?.commandId === right?.commandId &&
        left?.diagnostic === right?.diagnostic)
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
    delivery: input.type === "prompt" ? input.delivery : "followUp",
  });
  return acceptanceFromAdmission(command, "new");
}

/** Called only in the admission door's transaction, shared by admission and withdrawal. */
export function assertSessionRequestNotWithdrawn(
  owner: SqliteOwner,
  sessionId: string,
  requestId: string,
): void {
  if (
    owner.sql
      .exec(
        "SELECT 1 FROM session_request_withdrawals WHERE session_id = ? AND request_id = ?",
        sessionId,
        requestId,
      )
      .toArray().length > 0
  )
    throw new ShiitakeError(
      "contract",
      "closed",
      "Request was withdrawn",
      requestId,
    );
}

/** An admitted request wins; otherwise its key becomes a durable admission fence. */
export function withdrawSessionRequestOwned(
  owner: SqliteOwner,
  sessionId: string,
  requestId: string,
): import("../../client.js").WithdrawResult {
  const admitted = readSessionCommandAdmissionByRequest(
    owner,
    sessionId,
    requestId,
  );
  if (admitted !== undefined)
    return { status: "accepted", commandId: admitted.id };
  // A held website reply records an operator line rather than scheduling work.
  // It is already durable and must not be restored as though it were unsent.
  const recorded = owner.sql
    .exec(
      "SELECT id FROM session_entries WHERE session_id = ? AND request_id = ? AND type = 'operator'",
      sessionId,
      requestId,
    )
    .toArray()[0];
  if (recorded !== undefined) {
    const decoded = decodeSessionIdRow(recorded);
    if (decoded._tag === "Failure")
      throw unavailable("Stored operator receipt is invalid");
    return { status: "accepted", commandId: decoded.success.id };
  }
  owner.sql.exec(
    "INSERT OR IGNORE INTO session_request_withdrawals(session_id, request_id) VALUES (?, ?)",
    sessionId,
    requestId,
  );
  return { status: "unknown" };
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
    assertSessionRequestNotWithdrawn(owner, input.sessionId, input.requestId);
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
      `SELECT ${entryColumns}
         FROM session_entries
        WHERE session_id = ? AND state != 'streaming'
        ORDER BY sequence ASC`,
      sessionId,
    )
    .toArray()
    .map((raw) => parseEntryRow(owner, raw));
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
                head:
                  idMap.get(row.entry.head) ??
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
