import type { SqliteOwner } from "../../src/effect.js";
import {
  readSessionCommandAdmission,
  readSessionCommandAdmissionByRequest,
} from "../../src/internal/woodstock/session-store.js";

/** Resolve private work identity from the caller receipt for owner-internal tests. */
export function requireAcceptedWorkId(
  owner: SqliteOwner,
  sessionId: string,
  requestId: string,
): string {
  const command =
    readSessionCommandAdmission(owner, sessionId, requestId) ??
    readSessionCommandAdmissionByRequest(owner, sessionId, requestId);
  if (command === undefined)
    throw new Error("Accepted command work is unavailable");
  return command.entry.workId;
}
