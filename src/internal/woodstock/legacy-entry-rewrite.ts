import type { Usage } from "@earendil-works/pi-ai";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";

import { compactionSummaryMessage } from "../compaction/utils.js";
import type { SqliteOwner } from "../contracts.js";
import { retainedOutput } from "../redaction.js";
import { ShiitakeError } from "../../errors.js";
import {
  decodeStoredSessionEntry,
  encodeStoredSessionEntry,
  usage,
  type CompactionTrigger,
} from "./session-entry.js";

/**
 * The two rewrites of stored history that older Woodstock markers need. Both
 * keep every fact; they only move it to where the current reader looks.
 */

const closed = { onExcessProperty: "error" } as const;

function invalid(message: string): ShiitakeError {
  return new ShiitakeError("store", "incompatible", message);
}

/** The rewritten entry's `data`; the oldest rows leave three facts absent. */
type RewrittenData = {
  resumesRun: boolean;
  tokensBefore: number;
  usage: Usage;
  contextRevision: string;
  reason?: CompactionTrigger;
  tokensAfter?: number;
  details?: Readonly<{
    readFiles: readonly string[];
    modifiedFiles: readonly string[];
  }>;
};

const CompactionRowSchema = Schema.Struct({
  id: Schema.String,
  timestamp: Schema.Number,
  data_json: Schema.String,
});

/** A compaction as marker 1 stored it, before `head`, `model` and `data`. */
const OldCompactionSchema = Schema.fromJsonString(
  Schema.Struct({
    kind: Schema.Literal("compaction"),
    workId: Schema.String,
    resumesRun: Schema.optionalKey(Schema.Boolean),
    trigger: Schema.optionalKey(
      Schema.Literals(["manual", "threshold", "overflow"]),
    ),
    tokensAfter: Schema.optionalKey(Schema.Number),
    firstKeptEntryId: Schema.String,
    summary: Schema.String,
    tokensBefore: Schema.Number,
    usage,
    contextRevision: Schema.String,
    details: Schema.optionalKey(
      Schema.Struct({
        readFiles: Schema.Array(Schema.String),
        modifiedFiles: Schema.Array(Schema.String),
      }),
    ),
  }),
);

const decodeCompactionRow = Schema.decodeUnknownResult(
  CompactionRowSchema,
  closed,
);
const decodeOldCompaction = Schema.decodeUnknownResult(
  OldCompactionSchema,
  closed,
);

/**
 * Marker 2: compactions are stored as `head`, `model` and `data`. A row from
 * before the trigger or the post-compaction token count were recorded keeps
 * them absent; nothing is invented.
 */
export function rewriteOldCompactionRows(owner: SqliteOwner): void {
  const rows = owner.sql
    .exec(
      "SELECT id, timestamp, data_json FROM session_entries WHERE type = 'compaction' AND json_extract(data_json, '$.head') IS NULL",
    )
    .toArray();
  for (const raw of rows) {
    const row = decodeCompactionRow(raw);
    if (Result.isFailure(row))
      throw invalid("Stored compaction row cannot be read");
    const old = decodeOldCompaction(row.success.data_json);
    if (Result.isFailure(old))
      throw invalid("Stored compaction row cannot be rewritten");
    const value = old.success;
    const data: RewrittenData = {
      resumesRun: value.resumesRun ?? false,
      tokensBefore: value.tokensBefore,
      usage: value.usage,
      contextRevision: value.contextRevision,
    };
    if (value.trigger !== undefined) data.reason = value.trigger;
    if (value.tokensAfter !== undefined) data.tokensAfter = value.tokensAfter;
    if (value.details !== undefined) data.details = value.details;
    owner.sql.exec(
      "UPDATE session_entries SET data_json = ? WHERE id = ?",
      // Re-keys a retained row's own summary; it writes no fresh output.
      encodeStoredSessionEntry(
        retainedOutput({
          kind: "compaction",
          workId: value.workId,
          head: value.firstKeptEntryId,
          model: [
            compactionSummaryMessage(value.summary, row.success.timestamp),
          ],
          data,
        }),
      ),
      row.success.id,
    );
  }
}

const DataRowSchema = Schema.Struct({ data_json: Schema.String });
const decodeDataRow = Schema.decodeUnknownResult(DataRowSchema, closed);

/**
 * Marker 3: application data travels in `data` beside the model-only value,
 * and `details` is retired. A tool outcome's `result.details` and a stored
 * tool-result message's `value.details` move to the entry's `data`.
 */
export function moveToolDetailsToData(owner: SqliteOwner): void {
  owner.sql.exec(
    `UPDATE session_entries
        SET data_json = json_set(
          json_remove(data_json, '$.result.details'),
          '$.data', json(json_extract(data_json, '$.result.details'))
        )
      WHERE type = 'tool_outcome'
        AND json_type(data_json, '$.result.details') IS NOT NULL
        AND json_type(data_json, '$.data') IS NULL`,
  );
  owner.sql.exec(
    `UPDATE session_entries
        SET data_json = json_set(
          json_remove(data_json, '$.value.details'),
          '$.data', json(json_extract(data_json, '$.value.details'))
        )
      WHERE type = 'message'
        AND json_type(data_json, '$.value.details') IS NOT NULL
        AND json_type(data_json, '$.data') IS NULL`,
  );
  // A fact this left unreadable is refused loudly, never carried over silently.
  for (const raw of owner.sql
    .exec(
      "SELECT data_json FROM session_entries WHERE type IN ('tool_outcome', 'message')",
    )
    .toArray()) {
    const row = decodeDataRow(raw);
    if (
      Result.isFailure(row) ||
      decodeStoredSessionEntry(row.success.data_json)._tag === "Failure"
    )
      throw invalid("A stored tool result cannot be read");
  }
}
