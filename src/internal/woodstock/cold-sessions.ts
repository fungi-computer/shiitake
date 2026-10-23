import * as Schema from "effect/Schema";

import type {
  ColdSessionPage,
  ColdSessionPageRequest,
  ColdSessionRow,
} from "../../cold-sessions.js";
import { ShiitakeError } from "../../errors.js";
import type { SqliteOwner } from "../contracts.js";

/**
 * The store keeper's read of cold Sessions. It is one indexed keyset page over
 * the inbox, with each Session's bytes summed from its own rows, so no counter
 * can drift and nothing needs reconciling.
 */

const defaultLimit = 50;
const maxLimit = 200;

const closed = { onExcessProperty: "error" } as const;
const encoder = new TextEncoder();
const timestamp = Schema.Int.pipe(
  Schema.check(Schema.isGreaterThanOrEqualTo(0)),
);
const boundedId = Schema.String.pipe(
  Schema.check(Schema.isMinLength(1)),
  Schema.check(
    Schema.makeFilter((value) => encoder.encode(value).byteLength <= 256),
  ),
);

const RequestSchema = Schema.Struct({
  limit: Schema.optionalKey(
    Schema.Int.pipe(
      Schema.check(Schema.isBetween({ minimum: 1, maximum: maxLimit })),
    ),
  ),
  after: Schema.optionalKey(Schema.String),
});
const decodeRequest = Schema.decodeUnknownResult(RequestSchema, closed);

const CursorSchema = Schema.fromJsonString(
  Schema.Tuple([timestamp, boundedId]),
);
const decodeCursor = Schema.decodeUnknownResult(CursorSchema);
const encodeCursor = Schema.encodeSync(CursorSchema);

const RowSchema = Schema.Struct({
  session_id: boundedId,
  last_activity_at: timestamp,
  state: Schema.Literals(["needs_you", "running", "errored", "done"]),
  bytes: Schema.Int.pipe(Schema.check(Schema.isGreaterThanOrEqualTo(0))),
});
const decodeRow = Schema.decodeUnknownResult(RowSchema, closed);

const invalid = (message: string) =>
  new ShiitakeError("contract", "invalid", message);

const payloadBytes = (table: string, column: string) =>
  `(SELECT COALESCE(SUM(LENGTH(CAST(${column} AS BLOB))), 0) FROM ${table} WHERE session_id = p.session_id)`;

/**
 * The coldest-first window is chosen from 'session_inbox' alone; stored bytes
 * are summed only for its first 'limit' rows. The extra lookahead row only
 * proves another page exists and is discarded, so it is never measured.
 */
const selectColdSessions = (where: string) => `
  WITH p AS (
    SELECT i.session_id, i.last_activity_at, i.state,
      ROW_NUMBER() OVER (ORDER BY i.last_activity_at ASC, i.session_id ASC) AS position
    FROM session_inbox i
    ${where}
    ORDER BY i.last_activity_at ASC, i.session_id ASC LIMIT ?
  )
  SELECT p.session_id, p.last_activity_at, p.state,
    CASE WHEN p.position <= ? THEN
      ${payloadBytes("session_entries", "data_json")}
      + ${payloadBytes("session_command_admissions", "data_json")}
      + ${payloadBytes("session_attention", "value_json")}
      + ${payloadBytes("session_tool_grants", "grants_json")}
    ELSE 0 END AS bytes
  FROM p ORDER BY p.position`;

/** Read one bounded page of Sessions with stored bytes, coldest first. */
export function readColdSessionPageOwned(
  owner: SqliteOwner,
  request: ColdSessionPageRequest,
): ColdSessionPage {
  const decodedRequest = decodeRequest(request);
  if (decodedRequest._tag === "Failure")
    throw invalid("Cold Session page request is invalid");
  const limit = decodedRequest.success.limit ?? defaultLimit;
  const after = decodedRequest.success.after;
  let cursor: readonly [number, string] | undefined;
  if (after !== undefined) {
    const decoded = decodeCursor(after);
    if (decoded._tag === "Failure")
      throw invalid("Cold Session cursor is invalid");
    cursor = decoded.success;
  }
  // One extra row tells a full last page from a page with more behind it.
  const raws =
    cursor === undefined
      ? owner.sql.exec(selectColdSessions(""), limit + 1, limit).toArray()
      : owner.sql
          .exec(
            selectColdSessions(`WHERE i.last_activity_at > ?
                 OR (i.last_activity_at = ? AND i.session_id > ?)`),
            cursor[0],
            cursor[0],
            cursor[1],
            limit + 1,
            limit,
          )
          .toArray();
  const sessions: ColdSessionRow[] = [];
  for (const raw of raws.slice(0, limit)) {
    const decoded = decodeRow(raw);
    if (decoded._tag === "Failure")
      throw new ShiitakeError(
        "store",
        "unavailable",
        "Stored Session row is invalid",
      );
    const row = decoded.success;
    sessions.push({
      sessionId: row.session_id,
      bytes: row.bytes,
      lastActivityAt: row.last_activity_at,
      idle: row.state === "done" || row.state === "errored",
    });
  }
  const last = sessions.at(-1);
  return raws.length > limit && last !== undefined
    ? { sessions, next: encodeCursor([last.lastActivityAt, last.sessionId]) }
    : { sessions };
}
