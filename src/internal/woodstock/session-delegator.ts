import type { BirdDogDelegator, BirdDogMail } from "@fungi.computer/bird-dog";
import type { WatchdogSqliteOwner } from "@fungi.computer/watchdog";
import { BirdDogAgentReferenceSchema } from "@fungi.computer/bird-dog/effect";
import * as Schema from "effect/Schema";

import type { SqliteOwner } from "../contracts.js";

const decodeRow = Schema.decodeUnknownSync(
  Schema.Struct({
    mail_key: Schema.String,
    agent_id: Schema.String,
    reply_to_json: Schema.fromJsonString(
      Schema.Array(BirdDogAgentReferenceSchema),
    ),
  }),
);
const encodeReplyTo = Schema.encodeSync(
  Schema.fromJsonString(Schema.Array(BirdDogAgentReferenceSchema)),
);

/** Woodstock keeps each Session's delegator of record beside the Session. */
export function prepareSessionDelegators(owner: SqliteOwner): void {
  owner.sql.exec(`CREATE TABLE IF NOT EXISTS session_delegators (
    session_id TEXT PRIMARY KEY,
    mail_key TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    reply_to_json TEXT NOT NULL
  )`);
}

/**
 * Record who sent a Session its work first, in the admitting transaction.
 * Only an original mail that asks for replies names a delegator; the first one
 * stays, so the relationship never moves once it exists.
 */
export function recordSessionDelegator(
  owner: SqliteOwner,
  sessionId: string,
  mail: BirdDogMail | undefined,
): void {
  if (
    mail === undefined ||
    mail.reply !== undefined ||
    mail.from.kind !== "agent" ||
    mail.replyTo.length === 0
  )
    return;
  owner.sql.exec(
    "INSERT OR IGNORE INTO session_delegators (session_id, mail_key, agent_id, reply_to_json) VALUES (?, ?, ?, ?)",
    sessionId,
    mail.key,
    mail.to.agentId,
    encodeReplyTo(mail.replyTo),
  );
}

/** The Session's delegator of record, if any Agent has sent it work. */
export function readSessionDelegator(
  owner: WatchdogSqliteOwner,
  sessionId: string,
): BirdDogDelegator | undefined {
  const [row] = owner.sql
    .exec(
      "SELECT mail_key, agent_id, reply_to_json FROM session_delegators WHERE session_id = ?",
      sessionId,
    )
    .toArray();
  if (row === undefined) return undefined;
  const decoded = decodeRow(row);
  return {
    key: decoded.mail_key,
    agentId: decoded.agent_id,
    replyTo: decoded.reply_to_json,
  };
}
