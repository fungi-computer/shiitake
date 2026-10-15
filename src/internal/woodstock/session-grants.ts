import type { Grants } from "@fungi.computer/mycelium";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";

import { ShiitakeError } from "../../errors.js";
import type { SqliteOwner } from "../contracts.js";

/**
 * One Session's tool grants as the host delivered them: module or extension
 * ids mapped to a grant level. Bounded, so a host cannot grow the row.
 */
const SessionGrantsSchema = Schema.Record(
  Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(160)),
  Schema.Literals(["none", "read", "write"]),
).check(Schema.makeFilter((grants) => Object.keys(grants).length <= 256));

/**
 * One versioned delivery of a host decision. The host owns the decision and
 * its version; Shiitake keeps only the newest delivery it has seen.
 */
const GrantsDeliverySchema = Schema.Struct({
  /** The host's own version of this decision; deliveries only move forward. */
  version: Schema.Int.check(
    Schema.isGreaterThanOrEqualTo(0),
    Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
  ),
  grants: SessionGrantsSchema,
});

/** One versioned delivery of a Session's grants. */
export type GrantsDelivery = Readonly<{ version: number; grants: Grants }>;

/** Whether a delivery replaced the stored grants or repeated them. */
export type GrantsDeliveryResult = "applied" | "unchanged";

const decodeDelivery = Schema.decodeUnknownResult(GrantsDeliverySchema, {
  onExcessProperty: "error",
});
const decodeStoredGrants = Schema.decodeUnknownResult(
  Schema.fromJsonString(SessionGrantsSchema),
);
const encodeGrants = Schema.encodeSync(
  Schema.fromJsonString(SessionGrantsSchema),
);
const decodeRow = Schema.decodeUnknownResult(
  Schema.Struct({ version: Schema.Int, grants_json: Schema.String }),
);

/** Woodstock keeps each Session's newest delivered grants beside it. */
export function prepareSessionGrants(owner: SqliteOwner): void {
  owner.sql.exec(`CREATE TABLE IF NOT EXISTS session_tool_grants (
    session_id TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    grants_json TEXT NOT NULL
  )`);
}

/**
 * Store one versioned delivery of a Session's grants. A delivery may arrive
 * before the Session's first prompt. An older version is stale and never
 * writes. The same version with other grants replaces the stored grants: the
 * host recomputed its decision (for example after a deploy) without a new
 * version, and the newest computation always wins over the old grant. Only
 * the in-process host delivers grants.
 */
export function deliverSessionGrants(
  owner: SqliteOwner,
  sessionId: string,
  delivery: GrantsDelivery,
): GrantsDeliveryResult {
  const decoded = decodeDelivery(delivery);
  if (Result.isFailure(decoded))
    throw new ShiitakeError(
      "contract",
      "invalid",
      "Session grants delivery is invalid",
    );
  const grantsJson = encodeGrants(decoded.success.grants);
  const stored = readStoredRow(owner, sessionId);
  if (stored !== undefined) {
    if (decoded.success.version < stored.version)
      throw new ShiitakeError(
        "contract",
        "conflict",
        "Session grants delivery is older than the stored delivery",
      );
    if (
      decoded.success.version === stored.version &&
      stored.grantsJson === grantsJson
    )
      return "unchanged";
  }
  owner.sql.exec(
    `INSERT INTO session_tool_grants (session_id, version, grants_json)
     VALUES (?, ?, ?)
     ON CONFLICT(session_id) DO UPDATE SET
       version = excluded.version, grants_json = excluded.grants_json`,
    sessionId,
    decoded.success.version,
    grantsJson,
  );
  return "applied";
}

function readStoredRow(
  owner: SqliteOwner,
  sessionId: string,
): Readonly<{ version: number; grantsJson: string }> | undefined {
  const [raw] = owner.sql
    .exec(
      "SELECT version, grants_json FROM session_tool_grants WHERE session_id = ?",
      sessionId,
    )
    .toArray();
  if (raw === undefined) return undefined;
  const row = decodeRow(raw);
  if (Result.isFailure(row))
    throw new ShiitakeError(
      "store",
      "unavailable",
      "Stored Session grants are invalid",
    );
  return { version: row.success.version, grantsJson: row.success.grants_json };
}

/** One Session's newest delivery; null when the host delivered none. */
export function readSessionGrants(
  owner: SqliteOwner,
  sessionId: string,
): GrantsDelivery | null {
  const stored = readStoredRow(owner, sessionId);
  if (stored === undefined) return null;
  const grants = decodeStoredGrants(stored.grantsJson);
  if (Result.isFailure(grants))
    throw new ShiitakeError(
      "store",
      "unavailable",
      "Stored Session grants are invalid",
    );
  return { version: stored.version, grants: grants.success };
}

/** A fork keeps its source's delivery, so it never widens to the default. */
export function copySessionGrants(
  owner: SqliteOwner,
  fromSessionId: string,
  toSessionId: string,
): void {
  owner.sql.exec(
    `INSERT INTO session_tool_grants (session_id, version, grants_json)
     SELECT ?, version, grants_json FROM session_tool_grants WHERE session_id = ?`,
    toSessionId,
    fromSessionId,
  );
}
