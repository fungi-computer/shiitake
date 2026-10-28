import * as Schema from "effect/Schema";
import {
  AttentionRecipientSchema,
  type AttentionRecipient,
} from "../../session-attention.js";
import type { SqliteOwner } from "../contracts.js";
import { readSessionAttention } from "./session-attention.js";

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
 */
export function readWaitingOn(
  owner: SqliteOwner,
  sessionId: string,
  recipients: readonly Readonly<{ agentId: string; sessionId: string }>[],
): readonly WaitingOnYou[] {
  return readSessionAttention(owner, sessionId)
    .filter((item) =>
      recipients.some(
        (recipient) =>
          item.waitingOn?.agentId === recipient.agentId &&
          item.waitingOn.sessionId === recipient.sessionId,
      ),
    )
    .slice(0, maximumWaitingOnYou)
    .map((item) => ({
      itemId: item.id,
      kind: item.kind,
      summary: item.summary,
    }));
}
