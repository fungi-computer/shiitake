import type { SqliteOwner, SqliteValue } from "../../src/internal/contracts.js";

const guards = [
  "session_entries_lineage_insert",
  "session_entries_lineage_update",
  "session_entries_lineage_delete",
  "session_entries_lineage_replace",
  "session_entries_lineage_replace_position",
  "session_entries_damaged_session",
] as const;

/**
 * Damage a store the way only a writer outside Woodstock can: drop its
 * lineage guards, then run one raw statement. Laws use it to show what a
 * read, or the next open's audit, does with a store Woodstock never wrote.
 */
export function damageOutsideWoodstock(
  owner: SqliteOwner,
  statement: string,
  ...bindings: SqliteValue[]
): void {
  for (const guard of guards) owner.sql.exec(`DROP TRIGGER IF EXISTS ${guard}`);
  owner.sql.exec(statement, ...bindings);
}
