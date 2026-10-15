import { hasSessionAttention } from "./session-attention.js";
import type {
  WatchdogQueueTransition,
  WatchdogSettledJob,
} from "@fungi.computer/watchdog/effect";
import * as Schema from "effect/Schema";
import * as Struct from "effect/Struct";

import type {
  SessionInboxState,
  SessionSummaryPage,
  SessionSummaryRow,
} from "../../client.js";
import { ShiitakeError } from "../../errors.js";
import type { SqliteOwner } from "../contracts.js";

/**
 * The inbox row is Shiitake's one bulk Session projection. Woodstock writes
 * title and activity with each conversation change; Watchdog's projector
 * writes run state with each queue transition, in the same transaction. A page
 * is one indexed keyset read, never a per-Session join.
 */

type RunOutcome = WatchdogSettledJob["outcome"];

/** Rows returned by one bulk page. This bounds work, never saved Sessions. */
const sessionInboxPageSize = 200;

const closed = { onExcessProperty: "error" } as const;
const encoder = new TextEncoder();
const boundedId = Schema.String.pipe(
  Schema.check(Schema.isMinLength(1)),
  Schema.check(
    Schema.makeFilter((value) => encoder.encode(value).byteLength <= 256),
  ),
);
const timestamp = Schema.Int.pipe(
  Schema.check(Schema.isGreaterThanOrEqualTo(0)),
);
const tierSchema = Schema.Literals([0, 1, 2]);
const stateSchema = Schema.Literals([
  "needs_you",
  "running",
  "errored",
  "done",
]);
const outcomeSchema = Schema.Literals([
  "completed",
  "failed",
  "cancelled",
  "outcome_unknown",
  "interrupted",
]);

const InboxRowSchema = Schema.Struct({
  session_id: boundedId,
  title: Schema.NullOr(Schema.String),
  created_at: timestamp,
  last_activity_at: timestamp,
  settled_at: Schema.NullOr(timestamp),
  state: stateSchema,
  tier: tierSchema,
});
const decodeInboxRow = Schema.decodeUnknownResult(InboxRowSchema, closed);
const decodeInboxChangeRow = Schema.decodeUnknownResult(
  InboxRowSchema.mapFields(Struct.assign({ revision: timestamp })),
  closed,
);
const decodeClockRow = Schema.decodeUnknownResult(
  Schema.Struct({ revision: timestamp }),
  closed,
);
const InboxRunRowSchema = Schema.Struct({
  outcome: Schema.NullOr(outcomeSchema),
});
const decodeInboxRunRow = Schema.decodeUnknownResult(InboxRunRowSchema, closed);

const PageCursorSchema = Schema.fromJsonString(
  Schema.Tuple([tierSchema, timestamp, boundedId]),
);
const decodePageCursor = Schema.decodeUnknownResult(PageCursorSchema);
const encodePageCursor = Schema.encodeSync(PageCursorSchema);
type PageCursor = Schema.Schema.Type<typeof PageCursorSchema>;

const inboxColumns =
  "session_id, title, created_at, last_activity_at, settled_at, state, tier";

function unavailable(message: string): ShiitakeError {
  return new ShiitakeError("store", "unavailable", message);
}

/** Queue order: waiting on a person, then live work, then settled recency. */
function tierOf(state: SessionInboxState): 0 | 1 | 2 {
  switch (state) {
    case "needs_you":
      return 0;
    case "running":
      return 1;
    case "errored":
    case "done":
      return 2;
  }
}

/**
 * The one decision from Watchdog authority to inbox state. Live work is
 * `running` whatever its sub-status; an idle queue reports its latest outcome.
 * Open extension attention takes priority over both live and settled work.
 */
function inboxStateFor(
  head: WatchdogQueueTransition["head"],
  outcome: RunOutcome | null,
  attentionOpen: boolean,
): SessionInboxState {
  if (attentionOpen) return "needs_you";
  if (head.state !== "idle") return "running";
  switch (outcome) {
    case "failed":
    case "interrupted":
    case "outcome_unknown":
      return "errored";
    case "completed":
    case "cancelled":
    case null:
      return "done";
  }
}

/** Create the inbox table, its keyset and revision indexes and the revision clock. */
export function prepareSessionInbox(owner: SqliteOwner): void {
  owner.sql.exec(`
    CREATE TABLE IF NOT EXISTS session_inbox (
      session_id TEXT PRIMARY KEY REFERENCES sessions(id),
      title TEXT,
      created_at INTEGER NOT NULL,
      last_activity_at INTEGER NOT NULL,
      settled_at INTEGER,
      outcome TEXT CHECK (
        outcome IS NULL OR outcome IN (
          'completed', 'failed', 'cancelled', 'outcome_unknown', 'interrupted'
        )
      ),
      state TEXT NOT NULL
        CHECK (state IN ('needs_you', 'running', 'errored', 'done')),
      tier INTEGER NOT NULL CHECK (tier IN (0, 1, 2)),
      revision INTEGER NOT NULL
    )
  `);
  owner.sql.exec(
    `CREATE INDEX IF NOT EXISTS idx_session_inbox_order
       ON session_inbox(tier, last_activity_at DESC, session_id)`,
  );
  owner.sql.exec(
    `CREATE INDEX IF NOT EXISTS idx_session_inbox_revision
       ON session_inbox(revision)`,
  );
  owner.sql.exec(`
    CREATE TABLE IF NOT EXISTS session_inbox_clock (
      singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
      revision INTEGER NOT NULL
    )
  `);
  owner.sql.exec(
    "INSERT OR IGNORE INTO session_inbox_clock (singleton, revision) VALUES (1, 0)",
  );
  owner.sql.exec(`
    CREATE TABLE IF NOT EXISTS session_inbox_tombstones (
      session_id TEXT PRIMARY KEY,
      revision INTEGER NOT NULL
    ) STRICT
  `);
  owner.sql.exec(
    "CREATE INDEX IF NOT EXISTS idx_session_inbox_tombstones_revision ON session_inbox_tombstones(revision)",
  );
}

/** Advance the owner revision stamped on the row this transaction changes. */
function advanceRevision(owner: SqliteOwner): void {
  owner.sql.exec(
    "UPDATE session_inbox_clock SET revision = revision + 1 WHERE singleton = 1",
  );
}

const currentRevision =
  "(SELECT revision FROM session_inbox_clock WHERE singleton = 1)";

/** Conversation facts Woodstock supplies with each Session write. */
export type SessionInboxFacts = Readonly<{
  sessionId: string;
  /** Conversation activity; absent for display-only writes such as a title. */
  activityAt?: number;
  /**
   * First prompt title; null when the stored lineage is unreadable, and
   * undefined when this write cannot change it.
   */
  title?: string | null;
}>;

/**
 * Upsert title and recency in the caller's Session transaction. A new row
 * starts `done`: nothing has run, and Watchdog's projector owns run state.
 */
export function touchSessionInbox(
  owner: SqliteOwner,
  facts: SessionInboxFacts,
): void {
  const initialTitle = facts.title === undefined ? "" : facts.title;
  const replaceTitle = facts.title === undefined ? 0 : 1;
  advanceRevision(owner);
  owner.sql.exec(
    `INSERT INTO session_inbox
       (session_id, title, created_at, last_activity_at, settled_at, outcome,
        state, tier, revision)
     SELECT id, ?, created_at, COALESCE(?, created_at), NULL, NULL, 'done', 2,
            ${currentRevision}
       FROM sessions
      WHERE id = ?
     ON CONFLICT (session_id) DO UPDATE SET
       title = CASE WHEN ? = 1 THEN excluded.title ELSE title END,
       last_activity_at = MAX(last_activity_at, excluded.last_activity_at),
       revision = excluded.revision`,
    initialTitle,
    facts.activityAt ?? null,
    facts.sessionId,
    replaceTitle,
  );
}

/** Record a deletion in the Session owner's transaction and revision stream. */
export function recordSessionInboxDeletion(
  owner: SqliteOwner,
  sessionId: string,
): void {
  advanceRevision(owner);
  owner.sql.exec(
    `INSERT INTO session_inbox_tombstones (session_id, revision)
     VALUES (?, ${currentRevision})
     ON CONFLICT (session_id) DO UPDATE SET revision = excluded.revision`,
    sessionId,
  );
}

/** Drop tombstones the current member subscribers have all acknowledged. */
export function pruneSessionInboxTombstones(
  owner: SqliteOwner,
  through: number,
): void {
  owner.sql.exec(
    "DELETE FROM session_inbox_tombstones WHERE revision <= ?",
    through,
  );
}

/**
 * Watchdog's same-transaction projector. Queues that are not saved Sessions
 * have no row and are left alone.
 */
export function projectSessionRun(
  owner: SqliteOwner,
  transition: WatchdogQueueTransition,
  now: number,
): void {
  const raw = owner.sql
    .exec(
      "SELECT outcome FROM session_inbox WHERE session_id = ?",
      transition.queue,
    )
    .toArray()[0];
  if (raw === undefined) return;
  const prior = decodeInboxRunRow(raw);
  if (prior._tag === "Failure")
    throw unavailable("Stored inbox row is invalid");
  const outcome = transition.settled?.outcome ?? prior.success.outcome;
  const state = inboxStateFor(
    transition.head,
    outcome,
    hasSessionAttention(owner, transition.queue),
  );
  advanceRevision(owner);
  owner.sql.exec(
    `UPDATE session_inbox
        SET outcome = ?, state = ?, tier = ?, revision = ${currentRevision},
            settled_at = CASE WHEN ? = 1 THEN ? ELSE settled_at END,
            last_activity_at = MAX(last_activity_at, ?)
      WHERE session_id = ?`,
    outcome,
    state,
    tierOf(state),
    transition.settled === undefined ? 0 : 1,
    now,
    now,
    transition.queue,
  );
}

/** Refresh attention using the current queue head in the extension transaction. */
export function projectSessionAttention(
  owner: SqliteOwner,
  sessionId: string,
  head: WatchdogQueueTransition["head"],
): void {
  const raw = owner.sql
    .exec("SELECT outcome FROM session_inbox WHERE session_id = ?", sessionId)
    .toArray()[0];
  if (raw === undefined) return;
  const prior = decodeInboxRunRow(raw);
  if (prior._tag === "Failure")
    throw unavailable("Stored inbox row is invalid");
  const state = inboxStateFor(
    head,
    prior.success.outcome,
    hasSessionAttention(owner, sessionId),
  );
  advanceRevision(owner);
  owner.sql.exec(
    `UPDATE session_inbox SET state = ?, tier = ?, revision = ${currentRevision} WHERE session_id = ?`,
    state,
    tierOf(state),
    sessionId,
  );
}

type InboxRow = Schema.Schema.Type<typeof InboxRowSchema>;

function summaryRow(row: InboxRow): SessionSummaryRow {
  if (row.title === null)
    return Object.freeze({ sessionId: row.session_id, state: "unavailable" });
  const base = {
    sessionId: row.session_id,
    title: row.title,
    createdAt: row.created_at,
    lastActivityAt: row.last_activity_at,
    state: row.state,
  };
  return Object.freeze(
    row.settled_at === null ? base : { ...base, settledAt: row.settled_at },
  );
}

/** Revisioned Session inbox changes after one owner revision, oldest first. */
export type SessionInboxChanges = Readonly<{
  rows: readonly (
    SessionSummaryRow | Readonly<{ sessionId: string; deleted: true }>
  )[];
  /** Revision of the last returned row, or `after` when none changed. */
  through: number;
  /** Latest owner revision; `through < latest` means more remain. */
  latest: number;
}>;

/**
 * A bounded strict-after read of changed rows. Each Session appears once, at
 * its latest revision, so pending delivery is bounded by Session count.
 */
export function readSessionInboxChanges(
  owner: SqliteOwner,
  after: number,
  limit: number,
): SessionInboxChanges {
  const clock = decodeClockRow(
    owner.sql
      .exec("SELECT revision FROM session_inbox_clock WHERE singleton = 1")
      .one(),
  );
  if (clock._tag === "Failure") throw unavailable("Inbox revision is invalid");
  const rows = owner.sql
    .exec(
      `SELECT ${inboxColumns}, revision FROM session_inbox
        WHERE revision > ?
        ORDER BY revision ASC
        LIMIT ?`,
      after,
      limit,
    )
    .toArray();
  const deleted = owner.sql
    .exec(
      `SELECT session_id, revision FROM session_inbox_tombstones
        WHERE revision > ? ORDER BY revision ASC LIMIT ?`,
      after,
      limit,
    )
    .toArray()
    .map((raw) => {
      const parsed = Schema.decodeUnknownResult(
        Schema.Struct({ session_id: boundedId, revision: timestamp }),
        closed,
      )(raw);
      if (parsed._tag === "Failure")
        throw unavailable("Stored inbox tombstone is invalid");
      return {
        revision: parsed.success.revision,
        delta: { sessionId: parsed.success.session_id, deleted: true as const },
      };
    });
  const changed = rows.map((raw) => {
    const decoded = decodeInboxChangeRow(raw);
    if (decoded._tag === "Failure")
      throw unavailable("Stored inbox row is invalid");
    const { revision, ...row } = decoded.success;
    return { revision, delta: summaryRow(row) };
  });
  const ordered = [...changed, ...deleted]
    .sort((left, right) => left.revision - right.revision)
    .slice(0, limit);
  let through = after;
  const deltas: SessionInboxChanges["rows"][number][] = [];
  for (const change of ordered) {
    through = change.revision;
    deltas.push(change.delta);
  }
  return { rows: deltas, through, latest: clock.success.revision };
}

function parseCursor(after: string): PageCursor {
  const decoded = decodePageCursor(after);
  if (decoded._tag === "Failure")
    throw new ShiitakeError("contract", "invalid", "Session cursor is invalid");
  return decoded.success;
}

/** Indexed lookup using the same row decoder and availability as the inbox listing. */
export function readSessionInboxSummary(
  owner: SqliteOwner,
  sessionId: string,
): SessionSummaryRow | undefined {
  const raw = owner.sql
    .exec(
      `SELECT ${inboxColumns} FROM session_inbox WHERE session_id = ?`,
      sessionId,
    )
    .toArray()[0];
  if (raw === undefined) return undefined;
  const decoded = decodeInboxRow(raw);
  if (decoded._tag === "Failure")
    throw unavailable("Stored inbox row is invalid");
  return summaryRow(decoded.success);
}

/**
 * One bounded page in tier, then newest-activity order: a single indexed
 * statement regardless of how many Sessions the Agent holds. A row whose
 * stored facts are unreadable is its own `unavailable` row.
 */
export function readSessionInboxPage(
  owner: SqliteOwner,
  after: string | undefined,
): SessionSummaryPage {
  const cursor = after === undefined ? undefined : parseCursor(after);
  const rows =
    cursor === undefined
      ? owner.sql
          .exec(
            `SELECT ${inboxColumns} FROM session_inbox
              ORDER BY tier ASC, last_activity_at DESC, session_id ASC
              LIMIT ?`,
            sessionInboxPageSize,
          )
          .toArray()
      : owner.sql
          .exec(
            `SELECT ${inboxColumns} FROM session_inbox
              WHERE tier > ?
                 OR (tier = ? AND last_activity_at < ?)
                 OR (tier = ? AND last_activity_at = ? AND session_id > ?)
              ORDER BY tier ASC, last_activity_at DESC, session_id ASC
              LIMIT ?`,
            cursor[0],
            cursor[0],
            cursor[1],
            cursor[0],
            cursor[1],
            cursor[2],
            sessionInboxPageSize,
          )
          .toArray();
  const sessions: SessionSummaryRow[] = [];
  let last: PageCursor | undefined;
  for (const raw of rows) {
    const decoded = decodeInboxRow(raw);
    // Without a valid identity and position the row cannot be paged past.
    if (decoded._tag === "Failure")
      throw unavailable("Stored inbox row is invalid");
    const row = decoded.success;
    last = [row.tier, row.last_activity_at, row.session_id];
    sessions.push(summaryRow(row));
  }
  return {
    sessions,
    next:
      rows.length === sessionInboxPageSize && last !== undefined
        ? encodePageCursor(last)
        : undefined,
  };
}
