import * as Schema from "effect/Schema";
import {
  AttentionRecipientSchema,
  type AttentionRecipient,
} from "../../session-attention.js";
import type { SqliteOwner } from "../contracts.js";
import { readSessionAttention } from "./session-attention.js";
import { readSessionDelegator } from "./session-delegator.js";

/** The most open items one settlement names as waiting on its sender. */
const maximumWaitingOnYou = 8;

/** One open item of a Session that waits on the Agent that sent its work. */
export type WaitingOnYou = Readonly<{
  itemId: string;
  kind: string;
  summary: string;
}>;

const decodeRow = Schema.decodeUnknownSync(
  Schema.Struct({
    agent_id: Schema.String,
    sender_session_id: Schema.String,
    name: Schema.String,
  }),
);
const decodeRecipient = Schema.decodeUnknownSync(AttentionRecipientSchema);
const decodeWorkRow = Schema.decodeUnknownSync(
  Schema.Struct({ work_id: Schema.String }),
);

/**
 * The Session's parent: the Agent Session whose mail first delegated it, else
 * the Agent that made this one. Later senders never move this relationship.
 */
export function readSessionParent(
  owner: SqliteOwner,
  sessionId: string,
  /** The `createdBy` on this Agent's own record, as its host names it. */
  createdBy?: string,
): AttentionRecipient | undefined {
  const parent = readSessionDelegator(owner, sessionId)?.replyTo[0];
  if (parent === undefined)
    return readSessionMaker(owner, sessionId, createdBy);
  const raw = owner.sql
    .exec(
      "SELECT agent_id, sender_session_id, name FROM session_work_senders WHERE session_id = ? AND agent_id = ? AND sender_session_id = ? LIMIT 1",
      sessionId,
      parent.agentId,
      parent.sessionId,
    )
    .toArray()[0];
  return decodeRecipient({
    agentId: parent.agentId,
    sessionId: parent.sessionId,
    name: raw === undefined ? parent.agentId : decodeRow(raw).name,
  });
}

/**
 * The Agent that made this one, at its Session that last sent this Session
 * work, else at its Session that last sent this Agent any work. Its host
 * delivers to that Agent's newest Session once the named one is gone. A maker
 * that never sent this Agent work names no Session, so nothing waits on it.
 */
export function readSessionMaker(
  owner: SqliteOwner,
  sessionId: string,
  createdBy: string | undefined,
): AttentionRecipient | undefined {
  if (createdBy === undefined) return undefined;
  const raw = owner.sql
    .exec(
      "SELECT agent_id, sender_session_id, name FROM session_work_senders WHERE agent_id = ? ORDER BY session_id = ? DESC, rowid DESC LIMIT 1",
      createdBy,
      sessionId,
    )
    .toArray()[0];
  if (raw === undefined) return undefined;
  const row = decodeRow(raw);
  return decodeRecipient({
    agentId: row.agent_id,
    sessionId: row.sender_session_id,
    name: row.name,
  });
}

/** This Session's works one Agent sent or continued, newest first, bounded. */
export function readWorksSentBy(
  owner: SqliteOwner,
  sessionId: string,
  agentId: string,
  limit: number,
): readonly string[] {
  return owner.sql
    .exec(
      "SELECT work_id FROM session_work_senders WHERE session_id = ? AND agent_id = ? ORDER BY rowid DESC LIMIT ?",
      sessionId,
      agentId,
      limit,
    )
    .toArray()
    .map((row) => decodeWorkRow(row).work_id);
}

/**
 * Woodstock records which Agent Session sent each accepted run, beside the
 * run's admission. Attention that an extension raises during a run waits on
 * this sender first; a run no Agent sent has no row.
 */
export function prepareWorkSenders(owner: SqliteOwner): void {
  owner.sql.exec(`CREATE TABLE IF NOT EXISTS session_work_senders (
    session_id TEXT NOT NULL,
    work_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    sender_session_id TEXT NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (session_id, work_id)
  )`);
}

/** Record the sender of one accepted run; a replay writes the same row. */
export function recordWorkSender(
  owner: SqliteOwner,
  sessionId: string,
  workId: string,
  sender: AttentionRecipient,
): void {
  owner.sql.exec(
    `INSERT OR IGNORE INTO session_work_senders
       (session_id, work_id, agent_id, sender_session_id, name)
     VALUES (?, ?, ?, ?, ?)`,
    sessionId,
    workId,
    sender.agentId,
    sender.sessionId,
    sender.name,
  );
}

/** The Agent Session that sent this run, or undefined when none did. */
export function readWorkSender(
  owner: SqliteOwner,
  sessionId: string,
  workId: string,
): AttentionRecipient | undefined {
  const raw = owner.sql
    .exec(
      "SELECT agent_id, sender_session_id, name FROM session_work_senders WHERE session_id = ? AND work_id = ?",
      sessionId,
      workId,
    )
    .toArray()[0];
  if (raw === undefined) return undefined;
  const row = decodeRow(raw);
  return decodeRecipient({
    agentId: row.agent_id,
    sessionId: row.sender_session_id,
    name: row.name,
  });
}

/**
 * The Session's open attention that waits on the sender of this run, newest
 * first as stored, bounded. Escalated items wait on someone else and are
 * absent; a run no Agent sent names none.
 */
export function readWaitingOnSender(
  owner: SqliteOwner,
  sessionId: string,
  workId: string,
): readonly WaitingOnYou[] {
  const sender = readWorkSender(owner, sessionId, workId);
  return sender === undefined ? [] : readWaitingOn(owner, sessionId, [sender]);
}

/**
 * The Session's open attention that waits on any of these Agent Sessions,
 * newest first as stored, bounded: what a reply or report to them names.
 * Custody is the Agent's, so any of its Sessions is named for its items.
 */
export function readWaitingOn(
  owner: SqliteOwner,
  sessionId: string,
  recipients: readonly Readonly<{ agentId: string; sessionId: string }>[],
): readonly WaitingOnYou[] {
  return readSessionAttention(owner, sessionId)
    .filter((item) =>
      recipients.some(
        (recipient) => item.waitingOn?.agentId === recipient.agentId,
      ),
    )
    .slice(0, maximumWaitingOnYou)
    .map((item) => ({
      itemId: item.id,
      kind: item.kind,
      summary: item.summary,
    }));
}
