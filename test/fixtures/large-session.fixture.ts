import type { AssistantMessage } from "@fungi.computer/mule/effect";

import * as Schema from "effect/Schema";

import type { SqliteOwner, SqliteRow } from "../../src/internal/contracts.js";
import { sessionIds } from "../../src/internal/woodstock/identities.js";
import {
  encodeStoredSessionEntry,
  type StoredSessionEntry,
} from "../../src/internal/woodstock/session-entry.js";
import { appendSessionEntry } from "../../src/internal/woodstock/session-store.js";

const contract = { core: "a".repeat(64), extensions: [] } as const;
const usage = {
  input: 4,
  output: 2,
  cacheRead: 1,
  cacheWrite: 0,
  totalTokens: 7,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
};

/** One synthetic turn: the prompt, its exact Pi answer and its terminal. */
export type LargeSessionTurn = Readonly<{
  commandId: string;
  workId: string;
  assistantId: string;
  terminalId: string;
}>;

/** A large Session with realistic Pi entry shapes on one selected lineage. */
export type LargeSession = Readonly<{
  sessionId: string;
  turns: readonly LargeSessionTurn[];
  /** Compaction rows in append order, with the turn index each one kept from. */
  compactions: readonly Readonly<{ id: string; keptTurn: number }>[];
  entries: number;
}>;

export type LargeSessionOptions = Readonly<{
  sessionId: string;
  turns: number;
  /** UTF-8 text bytes in each prompt and each answer. */
  textBytes?: number;
  /** Append a compaction after every N turns, keeping the last `keepTurns`. */
  compactEvery?: number;
  keepTurns?: number;
}>;

function assistant(text: string): AssistantMessage {
  return {
    role: "assistant",
    content: [{ type: "text", text, textSignature: "signed-text" }],
    api: "anthropic-messages",
    provider: "anthropic",
    model: "claude-test",
    responseModel: "claude-test-20260903",
    responseId: "response-1",
    usage,
    stopReason: "stop",
    timestamp: 2,
  };
}

function text(label: string, bytes: number): string {
  return `${label} ${"x".repeat(Math.max(0, bytes - label.length - 1))}`;
}

/** The entry columns Woodstock's append word derives from each kind used here. */
function columnsFor(entry: StoredSessionEntry) {
  if (entry.kind === "command")
    return { role: "user", requestId: entry.requestId } as const;
  if (entry.kind === "message")
    return { role: "assistant", requestId: null } as const;
  return { role: null, requestId: null } as const;
}

const decodeLeaf = Schema.decodeUnknownSync(
  Schema.Struct({ leaf_id: Schema.NullOr(Schema.String) }),
);

/**
 * Append one complete entry with exactly the columns `appendSessionEntry`
 * writes, without its per-append inbox title refresh, so a law can build
 * tens of thousands of entries cheaply. Every read decodes and checks these
 * columns against the data, so a drifted fixture fails loudly.
 */
function append(
  owner: SqliteOwner,
  sessionId: string,
  entry: StoredSessionEntry,
  id: string = sessionIds.fact.generate(),
): string {
  const raw = owner.sql
    .exec("SELECT leaf_id FROM sessions WHERE id = ?", sessionId)
    .toArray()[0];
  if (raw === undefined)
    return appendSessionEntry(owner, { sessionId, id, entry }).id;
  const leaf = decodeLeaf(raw).leaf_id;
  const { role, requestId } = columnsFor(entry);
  owner.sql.exec(
    `INSERT INTO session_entries
       (id, session_id, parent_id, sequence, type, role, state,
        request_id, work_id, data_json, timestamp, completed_at)
     VALUES (?, ?, ?,
             (SELECT COALESCE(MAX(sequence), 0) + 1 FROM session_entries
               WHERE session_id = ?),
             ?, ?, 'complete', ?, ?, ?, 1, 1)`,
    id,
    sessionId,
    leaf,
    sessionId,
    entry.kind,
    role,
    requestId,
    entry.workId,
    encodeStoredSessionEntry(entry),
  );
  owner.sql.exec("UPDATE sessions SET leaf_id = ? WHERE id = ?", id, sessionId);
  return id;
}

/**
 * Write `turns` completed prompt/answer/terminal triples, with optional
 * compactions, so lineage-size laws run against realistic Pi entry shapes.
 */
export function writeLargeSession(
  owner: SqliteOwner,
  options: LargeSessionOptions,
): LargeSession {
  const textBytes = options.textBytes ?? 512;
  const keepTurns = options.keepTurns ?? 4;
  const turns: LargeSessionTurn[] = [];
  const compactions: { id: string; keptTurn: number }[] = [];
  let entries = 0;
  owner.transactionSync(() => {
    for (let index = 0; index < options.turns; index += 1) {
      const workId = sessionIds.work.generate();
      const run = { kind: "run", value: `run-${String(index)}` } as const;
      const turn = {
        kind: "turn",
        value: `turn-${String(index)}`,
        parent: run,
      } as const;
      const commandId = append(owner, options.sessionId, {
        kind: "command",
        workId,
        requestId: `request-${workId}`,
        receiptId: sessionIds.receipt.generate(),
        message: {
          role: "user",
          content: text(`prompt ${String(index)}`, textBytes),
          timestamp: 1,
        },
        contract,
      });
      const assistantId = sessionIds.fact.generate();
      append(
        owner,
        options.sessionId,
        {
          kind: "message",
          workId,
          run,
          turn,
          message: { kind: "message", value: assistantId, parent: turn },
          value: assistant(text(`answer ${String(index)}`, textBytes)),
        },
        assistantId,
      );
      const terminalId = append(owner, options.sessionId, {
        kind: "run_terminal",
        workId,
        run,
        terminal: {
          kind: "terminal",
          value: `terminal-${String(index)}`,
          parent: run,
        },
        outcome: "completed",
      });
      entries += 3;
      turns.push({ commandId, workId, assistantId, terminalId });
      const every = options.compactEvery;
      if (every !== undefined && (index + 1) % every === 0) {
        const keptTurn = Math.max(0, index + 1 - keepTurns);
        const kept = turns[keptTurn];
        if (kept === undefined) throw new Error("kept turn is missing");
        const id = append(owner, options.sessionId, {
          kind: "compaction",
          workId,
          firstKeptEntryId: kept.commandId,
          summary: `summary through turn ${String(keptTurn)}`,
          tokensBefore: 1000,
          usage,
          contextRevision: terminalId,
        });
        entries += 1;
        compactions.push({ id, keptTurn });
      }
    }
  });
  return Object.freeze({
    sessionId: options.sessionId,
    turns: Object.freeze(turns),
    compactions: Object.freeze(compactions),
    entries,
  });
}

/** A SQLite owner that counts what its reads hand to the JavaScript heap. */
export type MaterializedBytes = Readonly<{
  owner: SqliteOwner;
  /** Serialized row characters read since the last take. */
  take: () => number;
}>;

/**
 * Wrap a SQLite owner so a law can count the row bytes each read
 * materializes into the JavaScript heap.
 */
export function countMaterializedBytes(owner: SqliteOwner): MaterializedBytes {
  let bytes = 0;
  const measure = (row: SqliteRow) => {
    bytes += JSON.stringify(row).length;
  };
  return {
    owner: {
      ...owner,
      sql: {
        exec: (statement, ...bindings) => {
          const cursor = owner.sql.exec(statement, ...bindings);
          return {
            toArray: () => {
              const rows = cursor.toArray();
              for (const row of rows) measure(row);
              return rows;
            },
            one: () => {
              const row = cursor.one();
              measure(row);
              return row;
            },
          };
        },
      },
    },
    take: () => {
      const taken = bytes;
      bytes = 0;
      return taken;
    },
  };
}
