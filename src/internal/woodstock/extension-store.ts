import * as Schema from "effect/Schema";

import { ShiitakeError } from "../../errors.js";
import type { SqliteOwner, SqliteValue } from "../contracts.js";

/**
 * Core-owned durability for rostered v1 extensions.
 *
 * Every table is namespaced by extension identity, and only core writes SQL.
 * An extension sees typed collections, timers and its own settled cursor
 * through the runtime; it never receives this owner.
 */

const Text = Schema.String.pipe(Schema.check(Schema.isMinLength(1)));
const Instant = Schema.Int.pipe(Schema.check(Schema.isGreaterThanOrEqualTo(0)));
const closed = { onExcessProperty: "error" } as const;

const ValueRowSchema = Schema.Struct({ value_json: Schema.String });
const StateRowSchema = Schema.Struct({
  state_version: Schema.Int.pipe(
    Schema.check(Schema.isGreaterThanOrEqualTo(1)),
  ),
  revision: Text,
});
const StatusRowSchema = Schema.Struct({
  status: Schema.Literals(["running", "stopped"]),
  reason: Schema.NullOr(Schema.String),
  stopped_at: Schema.NullOr(Instant),
  revision: Text,
});
const TimerRowSchema = Schema.Struct({
  extension_id: Text,
  key: Text,
  due_at: Instant,
});
const StoredRecordRowSchema = Schema.Struct({
  collection: Text,
  key: Text,
  value_json: Schema.String,
});
const CursorRowSchema = Schema.Struct({ sequence: Instant });
const WakeRowSchema = Schema.Struct({
  due: Schema.Literals([0, 1]),
  next_due_at: Schema.NullOr(Instant),
});

function unavailable(message: string): ShiitakeError {
  return new ShiitakeError("store", "unavailable", message);
}

function decodeRows<A, I>(
  schema: Schema.Codec<A, I>,
  rows: readonly Record<string, SqliteValue>[],
  message: string,
): readonly A[] {
  const decode = Schema.decodeUnknownResult(schema, closed);
  return rows.map((row) => {
    const decoded = decode(row);
    if (decoded._tag === "Failure") throw unavailable(message);
    return decoded.success;
  });
}

/** Create every extension table. Idempotent `CREATE … IF NOT EXISTS` only. */
export function prepareExtensionStore(owner: SqliteOwner): void {
  owner.transactionSync(() => {
    owner.sql.exec(`CREATE TABLE IF NOT EXISTS shiitake_extension_records (
      extension_id TEXT NOT NULL,
      collection TEXT NOT NULL,
      key TEXT NOT NULL,
      index_value TEXT,
      value_json TEXT NOT NULL,
      PRIMARY KEY (extension_id, collection, key)
    )`);
    owner.sql
      .exec(`CREATE INDEX IF NOT EXISTS idx_shiitake_extension_records_index
      ON shiitake_extension_records(extension_id, collection, index_value, key)`);
    owner.sql.exec(`CREATE TABLE IF NOT EXISTS shiitake_extension_state (
      extension_id TEXT PRIMARY KEY,
      state_version INTEGER NOT NULL CHECK (state_version >= 1),
      revision TEXT NOT NULL
    )`);
    owner.sql.exec(`CREATE TABLE IF NOT EXISTS shiitake_extension_status (
      extension_id TEXT PRIMARY KEY,
      status TEXT NOT NULL CHECK (status IN ('running', 'stopped')),
      reason TEXT,
      stopped_at INTEGER,
      revision TEXT NOT NULL,
      CHECK ((status = 'running') = (reason IS NULL)),
      CHECK ((reason IS NULL) = (stopped_at IS NULL))
    )`);
    owner.sql.exec(`CREATE TABLE IF NOT EXISTS shiitake_extension_timers (
      extension_id TEXT NOT NULL,
      key TEXT NOT NULL,
      due_at INTEGER NOT NULL CHECK (due_at >= 0),
      PRIMARY KEY (extension_id, key)
    )`);
    owner.sql.exec(`CREATE INDEX IF NOT EXISTS idx_shiitake_extension_timers_due
      ON shiitake_extension_timers(due_at, extension_id, key)`);
    owner.sql.exec(`CREATE TABLE IF NOT EXISTS shiitake_settled_cursors (
      consumer TEXT PRIMARY KEY,
      sequence INTEGER NOT NULL CHECK (sequence >= 0)
    )`);
  });
}

// ---------- records ------------------------------------------------------------

/** Read one encoded record value. */
export function readRecord(
  owner: SqliteOwner,
  extensionId: string,
  collection: string,
  key: string,
): string | undefined {
  const rows = owner.sql
    .exec(
      `SELECT value_json FROM shiitake_extension_records
       WHERE extension_id = ? AND collection = ? AND key = ?`,
      extensionId,
      collection,
      key,
    )
    .toArray();
  return decodeRows(
    ValueRowSchema,
    rows,
    "Stored extension record is invalid",
  )[0]?.value_json;
}

/** Read one key-ordered page of encoded record values. */
export function readRecordPage(
  owner: SqliteOwner,
  extensionId: string,
  collection: string,
  query: Readonly<{
    index?: string | undefined;
    after?: string | undefined;
    limit: number;
  }>,
): readonly string[] {
  const rows = owner.sql
    .exec(
      `SELECT value_json FROM shiitake_extension_records
       WHERE extension_id = ? AND collection = ?
         AND (? IS NULL OR index_value = ?)
         AND (? IS NULL OR key > ?)
       ORDER BY key LIMIT ?`,
      extensionId,
      collection,
      query.index ?? null,
      query.index ?? null,
      query.after ?? null,
      query.after ?? null,
      query.limit,
    )
    .toArray();
  return decodeRows(
    ValueRowSchema,
    rows,
    "Stored extension record is invalid",
  ).map(({ value_json }) => value_json);
}

/** Upsert one encoded record. */
export function writeRecord(
  owner: SqliteOwner,
  extensionId: string,
  collection: string,
  key: string,
  index: string | null,
  valueJson: string,
): void {
  owner.sql.exec(
    `INSERT INTO shiitake_extension_records
       (extension_id, collection, key, index_value, value_json)
     VALUES (?, ?, ?, ?, ?)
     ON CONFLICT(extension_id, collection, key) DO UPDATE SET
       index_value = excluded.index_value, value_json = excluded.value_json`,
    extensionId,
    collection,
    key,
    index,
    valueJson,
  );
}

/** Delete one record; absent keys are a no-op. */
export function deleteRecord(
  owner: SqliteOwner,
  extensionId: string,
  collection: string,
  key: string,
): void {
  owner.sql.exec(
    `DELETE FROM shiitake_extension_records
     WHERE extension_id = ? AND collection = ? AND key = ?`,
    extensionId,
    collection,
    key,
  );
}

/** Every stored record of one extension, for activation-time migration. */
export function readAllRecords(
  owner: SqliteOwner,
  extensionId: string,
): readonly Readonly<{ collection: string; key: string; valueJson: string }>[] {
  const rows = owner.sql
    .exec(
      `SELECT collection, key, value_json FROM shiitake_extension_records
       WHERE extension_id = ? ORDER BY collection, key`,
      extensionId,
    )
    .toArray();
  return decodeRows(
    StoredRecordRowSchema,
    rows,
    "Stored extension record is invalid",
  ).map((row) => ({
    collection: row.collection,
    key: row.key,
    valueJson: row.value_json,
  }));
}

/** Remove every record of one extension inside a migration transaction. */
export function deleteAllRecords(
  owner: SqliteOwner,
  extensionId: string,
): void {
  owner.sql.exec(
    "DELETE FROM shiitake_extension_records WHERE extension_id = ?",
    extensionId,
  );
}

// ---------- migration ledger ---------------------------------------------------

/** Stored state version and the revision that last wrote it. */
export function readStateLedger(
  owner: SqliteOwner,
  extensionId: string,
): Readonly<{ version: number; revision: string }> | undefined {
  const rows = owner.sql
    .exec(
      `SELECT state_version, revision FROM shiitake_extension_state
       WHERE extension_id = ?`,
      extensionId,
    )
    .toArray();
  const row = decodeRows(
    StateRowSchema,
    rows,
    "Extension ledger is invalid",
  )[0];
  return row === undefined
    ? undefined
    : { version: row.state_version, revision: row.revision };
}

/** Record the state version an activation migrated to. */
export function writeStateLedger(
  owner: SqliteOwner,
  extensionId: string,
  version: number,
  revision: string,
): void {
  owner.sql.exec(
    `INSERT INTO shiitake_extension_state (extension_id, state_version, revision)
     VALUES (?, ?, ?)
     ON CONFLICT(extension_id) DO UPDATE SET
       state_version = excluded.state_version, revision = excluded.revision`,
    extensionId,
    version,
    revision,
  );
}

// ---------- status -------------------------------------------------------------

/** Durable running/stopped state of one extension. */
export type StoredExtensionStatus = Readonly<{
  status: "running" | "stopped";
  reason: string | null;
  stoppedAt: number | null;
  revision: string;
}>;

/** Read one extension's durable status. */
export function readStatus(
  owner: SqliteOwner,
  extensionId: string,
): StoredExtensionStatus | undefined {
  const rows = owner.sql
    .exec(
      `SELECT status, reason, stopped_at, revision FROM shiitake_extension_status
       WHERE extension_id = ?`,
      extensionId,
    )
    .toArray();
  const row = decodeRows(
    StatusRowSchema,
    rows,
    "Extension status is invalid",
  )[0];
  return row === undefined
    ? undefined
    : {
        status: row.status,
        reason: row.reason,
        stoppedAt: row.stopped_at,
        revision: row.revision,
      };
}

/** Write one extension's durable status. */
export function writeStatus(
  owner: SqliteOwner,
  extensionId: string,
  status: StoredExtensionStatus,
): void {
  owner.sql.exec(
    `INSERT INTO shiitake_extension_status
       (extension_id, status, reason, stopped_at, revision)
     VALUES (?, ?, ?, ?, ?)
     ON CONFLICT(extension_id) DO UPDATE SET status = excluded.status,
       reason = excluded.reason, stopped_at = excluded.stopped_at,
       revision = excluded.revision`,
    extensionId,
    status.status,
    status.reason,
    status.stoppedAt,
    status.revision,
  );
}

// ---------- timers -------------------------------------------------------------

/** Upsert one timer. */
export function writeTimer(
  owner: SqliteOwner,
  extensionId: string,
  key: string,
  dueAt: number,
): void {
  owner.sql.exec(
    `INSERT INTO shiitake_extension_timers (extension_id, key, due_at)
     VALUES (?, ?, ?)
     ON CONFLICT(extension_id, key) DO UPDATE SET due_at = excluded.due_at`,
    extensionId,
    key,
    dueAt,
  );
}

/** Delete one timer; absent keys are a no-op. */
export function deleteTimer(
  owner: SqliteOwner,
  extensionId: string,
  key: string,
): void {
  owner.sql.exec(
    "DELETE FROM shiitake_extension_timers WHERE extension_id = ? AND key = ?",
    extensionId,
    key,
  );
}

/** Read one timer's due instant. */
export function readTimer(
  owner: SqliteOwner,
  extensionId: string,
  key: string,
): number | undefined {
  const rows = owner.sql
    .exec(
      `SELECT extension_id, key, due_at FROM shiitake_extension_timers
       WHERE extension_id = ? AND key = ?`,
      extensionId,
      key,
    )
    .toArray();
  return decodeRows(TimerRowSchema, rows, "Extension timer is invalid")[0]
    ?.due_at;
}

function placeholders(values: readonly string[]): string {
  return values.map(() => "?").join(", ");
}

/** Due timers of the given extensions, oldest first, bounded by `limit`. */
export function readDueTimers(
  owner: SqliteOwner,
  extensionIds: readonly string[],
  now: number,
  limit: number,
): readonly Readonly<{ extensionId: string; key: string; dueAt: number }>[] {
  if (extensionIds.length === 0) return [];
  const rows = owner.sql
    .exec(
      `SELECT extension_id, key, due_at FROM shiitake_extension_timers
       WHERE due_at <= ? AND extension_id IN (${placeholders(extensionIds)})
       ORDER BY due_at, extension_id, key LIMIT ?`,
      now,
      ...extensionIds,
      limit,
    )
    .toArray();
  return decodeRows(TimerRowSchema, rows, "Extension timer is invalid").map(
    (row) => ({
      extensionId: row.extension_id,
      key: row.key,
      dueAt: row.due_at,
    }),
  );
}

/** Whether a timer is due now, and the next future timer instant. */
export type TimerWake = Readonly<{ due: boolean; nextWakeAt: number | null }>;

/** Whether a timer of the given extensions is due, and the next future one. */
export function readTimerWake(
  owner: SqliteOwner,
  extensionIds: readonly string[],
  now: number,
): TimerWake {
  if (extensionIds.length === 0) return { due: false, nextWakeAt: null };
  const ids = placeholders(extensionIds);
  const rows = owner.sql
    .exec(
      `SELECT
         EXISTS (SELECT 1 FROM shiitake_extension_timers
                 WHERE due_at <= ? AND extension_id IN (${ids})) AS due,
         (SELECT MIN(due_at) FROM shiitake_extension_timers
          WHERE due_at > ? AND extension_id IN (${ids})) AS next_due_at`,
      now,
      ...extensionIds,
      now,
      ...extensionIds,
    )
    .toArray();
  const row = decodeRows(WakeRowSchema, rows, "Extension timer is invalid")[0];
  if (row === undefined) throw unavailable("Extension timer is unavailable");
  return { due: row.due === 1, nextWakeAt: row.next_due_at };
}

// ---------- settled cursors ----------------------------------------------------

/**
 * The one durable settled cursor table: one row per consumer (`core` or an
 * extension identity).
 *
 * Watchdog's settled `sequence` is the enqueue order, not the settle order.
 * Watchdog exposes only the settled prefix below the first unfinished serial
 * row. That fence makes this high-water cursor exact across queue skips and
 * concurrent delivery. Stream order is enqueue order, not completion order.
 */
export function readCursor(
  owner: SqliteOwner,
  consumer: string,
): number | undefined {
  const rows = owner.sql
    .exec(
      "SELECT sequence FROM shiitake_settled_cursors WHERE consumer = ?",
      consumer,
    )
    .toArray();
  return decodeRows(CursorRowSchema, rows, "Settled cursor is invalid")[0]
    ?.sequence;
}

/** Advance one consumer's cursor; it never moves backwards. */
export function advanceCursor(
  owner: SqliteOwner,
  consumer: string,
  sequence: number,
): void {
  owner.sql.exec(
    `INSERT INTO shiitake_settled_cursors (consumer, sequence) VALUES (?, ?)
     ON CONFLICT(consumer) DO UPDATE SET sequence = MAX(sequence, excluded.sequence)`,
    consumer,
    sequence,
  );
}
