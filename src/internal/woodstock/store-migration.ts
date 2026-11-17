import * as Schema from "effect/Schema";

import { ShiitakeError } from "../../errors.js";
import type { SqliteOwner, SqliteRow, SqliteValue } from "../contracts.js";
import { extensionTimerLane } from "./extension-store.js";
import { sessionIds } from "./identities.js";
import {
  moveToolDetailsToData,
  rewriteOldCompactionRows,
} from "./legacy-entry-rewrite.js";
import { decodeStoredSessionEntry } from "./session-entry.js";

/**
 * In-place upgrade of a Woodstock store from schema marker 1 to 5 to the
 * current one. Every step keeps the person's Agents, Sessions and history; the
 * whole upgrade runs in the caller's transaction, so a store that cannot be
 * upgraded is left exactly as it was found.
 *
 * Steps are numbered by the marker whose build introduced the change, and a
 * store at marker N runs every step numbered above N, in order:
 * - 2: compactions are stored as `head`, `model` and `data`.
 * - 3: application data travels in `data`; `details` is retired.
 * - 5: a stored `auto_retry_start` fact gained the absolute `dueAt`.
 * - 6: Watchdog jobs gained the `completing` state, `attempt_count`, the
 *   recovery window and `not_before`; extension timers became Watchdog jobs,
 *   so the timer table points at its job instead of holding a deadline; and
 *   two Local tables nothing reads any more are dropped.
 *
 * The structural steps also look at the store's real shape, so they are
 * no-ops where they already hold.
 */

const Text = Schema.String.pipe(Schema.check(Schema.isMinLength(1)));
const Instant = Schema.Int.pipe(Schema.check(Schema.isGreaterThanOrEqualTo(0)));
const ColumnRowSchema = Schema.Struct({ name: Schema.String });
const DefinitionRowSchema = Schema.Struct({ sql: Schema.String });
const LegacyTimerRowSchema = Schema.Struct({
  extension_id: Text,
  key: Text,
  due_at: Instant,
});
const ExtensionIdRowSchema = Schema.Struct({ extension_id: Text });
const SequenceRowSchema = Schema.Struct({ seq: Instant });
const CountRowSchema = Schema.Struct({ count: Instant });
const EntryRowSchema = Schema.Struct({ data_json: Schema.String });

function incompatible(message: string): ShiitakeError {
  return new ShiitakeError("store", "incompatible", message);
}

function rows<A, I>(
  schema: Schema.Codec<A, I>,
  raw: readonly SqliteRow[],
  message: string,
): readonly A[] {
  const decode = Schema.decodeUnknownResult(schema);
  return raw.map((row) => {
    const decoded = decode(row);
    if (decoded._tag === "Failure") throw incompatible(message);
    return decoded.success;
  });
}

function select(
  owner: SqliteOwner,
  statement: string,
  ...bindings: SqliteValue[]
): readonly SqliteRow[] {
  return owner.sql.exec(statement, ...bindings).toArray();
}

/** An absent table has no columns. */
function columnsOf(owner: SqliteOwner, table: string): readonly string[] {
  return rows(
    ColumnRowSchema,
    select(owner, `PRAGMA table_info(${table})`),
    "Store table description is invalid",
  ).map((column) => column.name);
}

function countOf(owner: SqliteOwner, table: string): number {
  const row = rows(
    CountRowSchema,
    select(owner, `SELECT COUNT(*) AS count FROM ${table}`),
    "Store row count is invalid",
  )[0];
  if (row === undefined) throw incompatible("Store row count is missing");
  return row.count;
}

/** Watchdog's jobs table as marker 6 defines it; a migration is a frozen shape. */
const watchdogJobsDefinition = `
  CREATE TABLE watchdog_jobs (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT UNIQUE NOT NULL,
    queue_name TEXT NOT NULL,
    lane TEXT NOT NULL,
    priority INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    not_before INTEGER NOT NULL DEFAULT 0 CHECK (not_before >= 0),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    recovery_max INTEGER,
    recovery_window_ms INTEGER CHECK (recovery_window_ms IS NULL OR recovery_window_ms > 0),
    recovery_window_started_at INTEGER,
    state TEXT NOT NULL CHECK (state IN ('queued', 'running', 'completing', 'settled')),
    outcome TEXT CHECK (
      outcome IS NULL OR outcome IN (
        'completed', 'failed', 'cancelled', 'outcome_unknown', 'interrupted'
      )
    ),
    claim_token TEXT,
    recovery_count INTEGER NOT NULL DEFAULT 0,
    cancellation_id TEXT UNIQUE,
    failure_reason TEXT,
    CHECK (recovery_max IS NULL OR recovery_max >= 0),
    CONSTRAINT watchdog_execution_state CHECK (
      (state = 'queued' AND outcome IS NULL AND claim_token IS NULL) OR
      (state = 'running' AND outcome IS NULL AND claim_token IS NOT NULL) OR
      (state = 'completing' AND outcome IS NOT NULL AND claim_token IS NOT NULL) OR
      (state = 'settled' AND outcome IS NOT NULL)
    )
  )
`;

const watchdogJobColumns = [
  "sequence",
  "job_id",
  "queue_name",
  "lane",
  "priority",
  "payload_json",
  "not_before",
  "attempt_count",
  "recovery_max",
  "recovery_window_ms",
  "recovery_window_started_at",
  "state",
  "outcome",
  "claim_token",
  "recovery_count",
  "cancellation_id",
  "failure_reason",
] as const;

/** What every marker in range already carries, so nothing is invented for it. */
const requiredJobColumns = [
  "sequence",
  "job_id",
  "queue_name",
  "lane",
  "priority",
  "payload_json",
  "recovery_max",
  "state",
  "outcome",
  "claim_token",
  "recovery_count",
  "cancellation_id",
  "failure_reason",
] as const;

const retiredJobs = "watchdog_jobs_before_marker_6";

/** Rebuild the jobs table in place, keeping every row, its order and its sequence. */
function upgradeWatchdogJobs(owner: SqliteOwner): void {
  const old = columnsOf(owner, "watchdog_jobs");
  if (old.length === 0) return;
  const definition = rows(
    DefinitionRowSchema,
    select(
      owner,
      "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 'watchdog_jobs'",
    ),
    "Watchdog jobs definition is invalid",
  )[0];
  if (
    definition?.sql.includes("watchdog_execution_state") === true &&
    watchdogJobColumns.every((column) => old.includes(column))
  )
    return;
  if (!requiredJobColumns.every((column) => old.includes(column)))
    throw incompatible("Watchdog jobs table has an unknown shape");
  const shared = watchdogJobColumns.filter((column) => old.includes(column));
  const before = countOf(owner, "watchdog_jobs");
  owner.sql.exec(`ALTER TABLE watchdog_jobs RENAME TO ${retiredJobs}`);
  owner.sql.exec(watchdogJobsDefinition);
  const list = shared.join(", ");
  owner.sql.exec(
    `INSERT INTO watchdog_jobs (${list}) SELECT ${list} FROM ${retiredJobs} ORDER BY sequence`,
  );
  if (countOf(owner, "watchdog_jobs") !== before)
    throw incompatible("Watchdog jobs were not all carried over");
  // The sequence counter orders queued work; keep it at least as high as before.
  const counter = rows(
    SequenceRowSchema,
    select(
      owner,
      "SELECT seq FROM sqlite_sequence WHERE name = ?",
      retiredJobs,
    ),
    "Watchdog job counter is invalid",
  )[0];
  owner.sql.exec(`DROP TABLE ${retiredJobs}`);
  if (counter !== undefined) {
    owner.sql.exec("DELETE FROM sqlite_sequence WHERE name = 'watchdog_jobs'");
    owner.sql.exec(
      "INSERT INTO sqlite_sequence (name, seq) VALUES ('watchdog_jobs', MAX(?, (SELECT COALESCE(MAX(sequence), 0) FROM watchdog_jobs)))",
      counter.seq,
    );
  }
}

const retiredTimers = "shiitake_extension_timers_before_marker_6";

/**
 * A timer that held its own deadline becomes a queued Watchdog job due then,
 * exactly as registering it now would. A stopped extension's timers keep their
 * registration but their wake is cancelled, as stopping it does.
 */
function upgradeExtensionTimers(owner: SqliteOwner): void {
  const old = columnsOf(owner, "shiitake_extension_timers");
  if (old.length === 0 || old.includes("job_id")) return;
  const legacy = rows(
    LegacyTimerRowSchema,
    select(
      owner,
      "SELECT extension_id, key, due_at FROM shiitake_extension_timers ORDER BY extension_id, key",
    ),
    "Stored extension timer is invalid",
  );
  if (legacy.length > 0 && columnsOf(owner, "watchdog_jobs").length === 0)
    throw incompatible("Extension timers have no Watchdog jobs to move to");
  const stopped = new Set(
    columnsOf(owner, "shiitake_extension_status").length === 0
      ? []
      : rows(
          ExtensionIdRowSchema,
          select(
            owner,
            "SELECT extension_id FROM shiitake_extension_status WHERE status = 'stopped'",
          ),
          "Stored extension status is invalid",
        ).map((row) => row.extension_id),
  );
  owner.sql.exec(
    `ALTER TABLE shiitake_extension_timers RENAME TO ${retiredTimers}`,
  );
  owner.sql.exec(`CREATE TABLE shiitake_extension_timers (
    extension_id TEXT NOT NULL,
    key TEXT NOT NULL,
    job_id TEXT NOT NULL,
    PRIMARY KEY (extension_id, key)
  )`);
  for (const timer of legacy) {
    const jobId = sessionIds.work.generate();
    owner.sql.exec(
      `INSERT INTO watchdog_jobs
         (job_id, queue_name, lane, priority, payload_json, not_before,
          state, outcome, claim_token, recovery_count, cancellation_id)
       VALUES (?, ?, ?, 0, ?, ?, 'queued', NULL, NULL, 0, ?)`,
      jobId,
      jobId,
      extensionTimerLane,
      JSON.stringify({ extensionId: timer.extension_id, key: timer.key }),
      timer.due_at,
      stopped.has(timer.extension_id) ? `${jobId}:cancel` : null,
    );
    owner.sql.exec(
      "INSERT INTO shiitake_extension_timers (extension_id, key, job_id) VALUES (?, ?, ?)",
      timer.extension_id,
      timer.key,
      jobId,
    );
  }
  owner.sql.exec(`DROP TABLE ${retiredTimers}`);
}

/** A retry already waiting when the store was saved is due its delay after it was recorded. */
function upgradeRetryDeadlines(owner: SqliteOwner): void {
  owner.sql.exec(
    `UPDATE session_entries
        SET data_json = json_set(
          data_json, '$.event.dueAt',
          CAST(timestamp + json_extract(data_json, '$.event.delayMs') AS INTEGER)
        )
      WHERE type = 'live_retry'
        AND json_extract(data_json, '$.event.type') = 'auto_retry_start'
        AND json_extract(data_json, '$.event.dueAt') IS NULL`,
  );
  // Whatever this left unreadable is refused loudly, never carried over silently.
  for (const row of rows(
    EntryRowSchema,
    select(
      owner,
      "SELECT data_json FROM session_entries WHERE type = 'live_retry'",
    ),
    "Stored retry fact is invalid",
  ))
    if (decodeStoredSessionEntry(row.data_json)._tag === "Failure")
      throw incompatible("A stored retry fact cannot be read");
}

/** Local tables whose features were removed; nothing reads or writes them now. */
const retiredLocalTables = [
  "private_local_exec_rules",
  "local_birddog_outbox",
] as const;

function dropRetiredLocalTables(owner: SqliteOwner): void {
  for (const table of retiredLocalTables)
    owner.sql.exec(`DROP TABLE IF EXISTS ${table}`);
}

type UpgradeStep = Readonly<{
  /** The marker whose build introduced this change. */
  marker: number;
  apply: (owner: SqliteOwner) => void;
}>;

const upgradeSteps: readonly UpgradeStep[] = [
  { marker: 2, apply: rewriteOldCompactionRows },
  { marker: 3, apply: moveToolDetailsToData },
  { marker: 5, apply: upgradeRetryDeadlines },
  { marker: 6, apply: upgradeWatchdogJobs },
  { marker: 6, apply: upgradeExtensionTimers },
  { marker: 6, apply: dropRetiredLocalTables },
];

const MarkerRowSchema = Schema.Struct({ version: Schema.Int });

/** Upgrade the store's data and shape to the current marker; the caller stamps it. */
export function upgradeWoodstockStoreData(owner: SqliteOwner): void {
  const row = rows(
    MarkerRowSchema,
    select(owner, "SELECT version FROM woodstock_schema"),
    "Woodstock schema marker is invalid",
  )[0];
  if (row === undefined)
    throw incompatible("Woodstock schema marker is missing");
  for (const step of upgradeSteps)
    if (step.marker > row.version) step.apply(owner);
}
