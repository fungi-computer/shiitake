import * as Schema from "effect/Schema";
import { ShiitakeError } from "../../errors.js";
import {
  AttentionItemSchema,
  maximumSessionAttention,
  maximumExtensionAttention,
  SessionAttentionSchema,
  type AttentionItem,
  type SessionAttention,
} from "../../session-attention.js";
import type { SqliteOwner } from "../contracts.js";

const decodeRow = Schema.decodeUnknownSync(
  Schema.Struct({ extension_id: Schema.String, value_json: Schema.String }),
);
const decodeItem = Schema.decodeUnknownSync(
  Schema.fromJsonString(AttentionItemSchema),
);
const decodePublic = Schema.decodeUnknownSync(SessionAttentionSchema);
const encodeItem = Schema.encodeSync(
  Schema.fromJsonString(AttentionItemSchema),
);

/** Woodstock owns the durable attention rows alongside their Session. */
export function prepareSessionAttention(owner: SqliteOwner): void {
  owner.sql.exec(`CREATE TABLE IF NOT EXISTS session_attention_grants (
    extension_id TEXT PRIMARY KEY
  )`);
  owner.sql.exec(`CREATE TABLE IF NOT EXISTS session_attention (
    session_id TEXT NOT NULL REFERENCES sessions(id),
    extension_id TEXT NOT NULL,
    id TEXT NOT NULL,
    value_json TEXT NOT NULL,
    PRIMARY KEY (session_id, extension_id, id)
  )`);
  owner.sql.exec(
    "CREATE INDEX IF NOT EXISTS idx_session_attention_owner ON session_attention(extension_id, session_id)",
  );
}

/** Read the complete bounded attention projection inside the owner's transaction. */
export function readSessionAttention(
  owner: SqliteOwner,
  sessionId: string,
): readonly SessionAttention[] {
  return owner.sql
    .exec(
      "SELECT a.extension_id, a.value_json FROM session_attention a JOIN shiitake_extension_status s ON s.extension_id = a.extension_id AND s.status = 'running' JOIN session_attention_grants g ON g.extension_id = a.extension_id WHERE a.session_id = ? ORDER BY a.extension_id, a.id LIMIT ?",
      sessionId,
      maximumSessionAttention,
    )
    .toArray()
    .map((raw) => {
      const row = decodeRow(raw);
      return decodePublic({
        ...decodeItem(row.value_json),
        extensionId: row.extension_id,
      });
    });
}

/** Whether any owner still requests attention; no wall-clock expiry heuristic. */
export function hasSessionAttention(
  owner: SqliteOwner,
  sessionId: string,
): boolean {
  return (
    owner.sql
      .exec(
        "SELECT 1 FROM session_attention a JOIN shiitake_extension_status s ON s.extension_id = a.extension_id AND s.status = 'running' JOIN session_attention_grants g ON g.extension_id = a.extension_id WHERE a.session_id = ? LIMIT 1",
        sessionId,
      )
      .toArray().length > 0
  );
}

/** Upsert only the calling extension's item, in the calling transaction. */
export function setSessionAttention(
  owner: SqliteOwner,
  sessionId: string,
  extensionId: string,
  item: AttentionItem,
): void {
  const usage = Schema.decodeUnknownSync(Schema.Struct({ count: Schema.Int }))(
    owner.sql
      .exec(
        "SELECT COUNT(*) AS count FROM session_attention WHERE session_id = ? AND NOT (extension_id = ? AND id = ?)",
        sessionId,
        extensionId,
        item.id,
      )
      .one(),
  );
  if (usage.count >= maximumSessionAttention)
    throw new ShiitakeError(
      "contract",
      "invalid",
      "Session attention capacity reached",
    );
  const extensionUsage = Schema.decodeUnknownSync(
    Schema.Struct({ count: Schema.Int }),
  )(
    owner.sql
      .exec(
        "SELECT COUNT(*) AS count FROM session_attention WHERE extension_id = ? AND NOT (session_id = ? AND id = ?)",
        extensionId,
        sessionId,
        item.id,
      )
      .one(),
  );
  if (extensionUsage.count >= maximumExtensionAttention)
    throw new ShiitakeError(
      "contract",
      "invalid",
      "Extension attention capacity reached",
    );
  owner.sql.exec(
    `INSERT INTO session_attention (session_id, extension_id, id, value_json) VALUES (?, ?, ?, ?)
     ON CONFLICT (session_id, extension_id, id) DO UPDATE SET value_json = excluded.value_json`,
    sessionId,
    extensionId,
    item.id,
    encodeItem(item),
  );
}

/** Clear only the calling extension's item; an absent item is a no-op. */
export function clearSessionAttention(
  owner: SqliteOwner,
  sessionId: string,
  extensionId: string,
  id: string,
): void {
  owner.sql.exec(
    "DELETE FROM session_attention WHERE session_id = ? AND extension_id = ? AND id = ?",
    sessionId,
    extensionId,
    id,
  );
}

/** Bounded keyset traversal for refreshing an owner's attention on stop/resume. */
export function readAttentionSessionPage(
  owner: SqliteOwner,
  extensionId: string,
  after: string,
): readonly string[] {
  const decode = Schema.decodeUnknownSync(
    Schema.Struct({ session_id: Schema.String }),
  );
  return owner.sql
    .exec(
      "SELECT DISTINCT session_id FROM session_attention WHERE extension_id = ? AND session_id > ? ORDER BY session_id LIMIT 64",
      extensionId,
      after,
    )
    .toArray()
    .map((row) => decode(row).session_id);
}

/** Apply current grants and return the bounded roster identities whose visibility changed. */
export function replaceAttentionGrants(
  owner: SqliteOwner,
  ids: readonly string[],
): readonly string[] {
  const decode = Schema.decodeUnknownSync(
    Schema.Struct({ extension_id: Schema.String }),
  );
  const prior = new Set(
    owner.sql
      .exec("SELECT extension_id FROM session_attention_grants LIMIT 64")
      .toArray()
      .map((row) => decode(row).extension_id),
  );
  const next = new Set(ids);
  const changed: string[] = [];
  for (const id of prior)
    if (!next.has(id)) {
      owner.sql.exec(
        "DELETE FROM session_attention_grants WHERE extension_id = ?",
        id,
      );
      changed.push(id);
    }
  for (const id of next)
    if (!prior.has(id)) {
      owner.sql.exec(
        "INSERT INTO session_attention_grants (extension_id) VALUES (?)",
        id,
      );
      changed.push(id);
    }
  return changed;
}
