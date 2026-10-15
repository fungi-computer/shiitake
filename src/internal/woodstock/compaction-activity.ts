import type { SessionActivity } from "../../client.js";
import type { SqliteOwner } from "../contracts.js";
import { publishOwnerObservation } from "../observation-kernel.js";

export type CompactionActivity = NonNullable<SessionActivity["compaction"]>;

const compactionByOwner = new WeakMap<
  SqliteOwner,
  Map<string, CompactionActivity>
>();

function compactionFor(owner: SqliteOwner): Map<string, CompactionActivity> {
  const existing = compactionByOwner.get(owner);
  if (existing !== undefined) return existing;
  const created = new Map<string, CompactionActivity>();
  compactionByOwner.set(owner, created);
  return created;
}

/**
 * Read the current process-local compaction activity for one addressed
 * Session; null while no compaction is running on it.
 */
export function readProcessCompaction(
  owner: SqliteOwner,
  sessionId: string,
): CompactionActivity | null {
  return compactionFor(owner).get(sessionId) ?? null;
}

/**
 * Mark a compaction as running now, cleared by `endCompactionActivity`.
 *
 * Process-local only, like `openProgress`: the running state is never
 * durable, so a restarted process shows no running compaction.
 */
export function beginCompactionActivity(
  owner: SqliteOwner,
  sessionId: string,
  trigger: CompactionActivity["trigger"],
): void {
  compactionFor(owner).set(sessionId, Object.freeze({ trigger }));
  publishOwnerObservation(owner, sessionId, "progress");
}

/** Clear a running compaction's activity fact, however the commit ended. */
export function endCompactionActivity(
  owner: SqliteOwner,
  sessionId: string,
): void {
  if (compactionFor(owner).delete(sessionId))
    publishOwnerObservation(owner, sessionId, "progress");
}
