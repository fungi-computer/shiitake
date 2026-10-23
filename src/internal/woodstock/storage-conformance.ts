/**
 * Runner-independent conformance cases for one Woodstock Session storage host.
 *
 * Adapted from Pi Durable's `src/testing/storage-conformance.ts`
 * (`earendil-works/pi` at 8efd314f): every host that backs Woodstock with a
 * {@link SqliteOwner} — the local `node:sqlite` owner and the Durable Object's
 * `ctx.storage` — runs the same cases through Woodstock's own writers and
 * readers. Fungi delta: Pi's suite checks a generic record store (ID minting,
 * task and document tables); Woodstock's store is the parent-pointer Session
 * tree, so these cases check atomic admission, request-ID replay, bounded
 * lineage reads, branch, fork, placeholder completion, deletion and the inbox.
 * The cases import no test runner, only `node:assert`, so they run inside a
 * Worker with `nodejs_compat` too.
 *
 * @packageDocumentation
 */
import assert from "node:assert/strict";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import type { SqliteOwner } from "../contracts.js";
import { openAdmissionDoor, type AdmitRequest } from "../admission-door.js";
import { sessionIds } from "./identities.js";
import {
  appendCompactionNotice,
  branchSession,
  createSession,
  deleteSession,
  forkSession,
  prepareWoodstockSchema,
  readSessionInbox,
} from "./index.js";
import type { PromptCommandEntry } from "./session-entry.js";
import {
  readSessionInboxChanges,
  readSessionInboxSummary,
} from "./session-inbox.js";
import {
  appendCompactionTransitionOwned,
  appendSessionEntry,
  buildSessionContext,
  compactionHeadState,
  completeSessionLeaf,
  hasStoredSession,
  lineageRowLimit,
  readPendingSessionCommands,
  readSelectedPath,
  readSessionCommandAdmissionByRequest,
  readSessionTree,
  readStoredSessionSummary,
  setSessionTitle,
  type RunContract,
} from "./session-store.js";
import { admitWithoutCheck } from "./storage-admission.js";

/**
 * Calls `use` exactly once with an empty, writable owner of the host under
 * test, then releases the owner. It resolves after `use` returns.
 */
export type WoodstockStorageProvider = (
  use: (owner: SqliteOwner) => void,
) => Promise<void>;

/** One named case. */
export type WoodstockConformanceCase = Readonly<{
  /** Stable case name, for the runner's test title. */
  name: string;
  /** Open a fresh owner through the provider and run the case on it. */
  run: () => Promise<void>;
}>;

const contract: RunContract = { core: "a".repeat(64), extensions: [] };

function commandEntry(text: string): PromptCommandEntry {
  return {
    kind: "command",
    workId: sessionIds.work.generate(),
    requestId: `request-${sessionIds.fact.generate()}`,
    receiptId: sessionIds.receipt.generate(),
    message: { role: "user", content: text, timestamp: 1 },
    contract,
  };
}

function append(owner: SqliteOwner, sessionId: string, text: string) {
  return appendSessionEntry(owner, {
    sessionId,
    id: sessionIds.fact.generate(),
    entry: commandEntry(text),
  });
}

function door(owner: SqliteOwner, enqueue: () => void = () => undefined) {
  return openAdmissionDoor({
    owner,
    admitNew: admitWithoutCheck.admitNew,
    contract: () => contract,
    enqueue,
    readQueueHead: () => ({ state: "idle" }),
    commit: (mutation) => mutation,
    requestWake: Effect.void,
  });
}

function prompt(
  sessionId: string,
  requestId: string,
  text: string,
): AdmitRequest {
  return {
    sessionId,
    command: { type: "prompt", requestId, text, delivery: "followUp" },
    when: "now",
    origin: { kind: "user" },
  };
}

function admit(owner: SqliteOwner, request: AdmitRequest) {
  return Effect.runSync(door(owner).admit(request));
}

const decodeCount = Schema.decodeUnknownSync(
  Schema.Struct({ count: Schema.Number }),
);

function count(owner: SqliteOwner, table: string): number {
  return decodeCount(
    owner.sql.exec(`SELECT COUNT(*) AS count FROM ${table}`).one(),
  ).count;
}

const idle = () => undefined;

const usage = {
  input: 7,
  output: 3,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 10,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0.5 },
};

/** Compact `sessionId` so its context starts at `head`, from the current leaf. */
function compact(owner: SqliteOwner, sessionId: string, head: string) {
  const leaf = readSelectedPath(owner, { sessionId }).at(-1);
  assert.ok(leaf !== undefined, "The Session has a leaf");
  return appendCompactionTransitionOwned(owner, {
    sessionId,
    expectedLeafId: leaf.id,
    workId: sessionIds.work.generate(),
    trigger: "threshold",
    tokensAfter: 10,
    head,
    summary: "summary",
    tokensBefore: 100,
    usage,
    contextRevision: leaf.id,
  });
}

/** Woodstock's typed refusal for an operation, matched by its codes. */
function refuses(
  operation: () => void,
  refusal: Readonly<{ boundary: string; cause: string }>,
): void {
  assert.throws(operation, { _tag: "ShiitakeError", ...refusal });
}

const sessionTables = [
  "sessions",
  "session_entries",
  "session_command_admissions",
  "session_inbox",
] as const;

/** Create the runner-independent cases for one Woodstock storage host. */
export function createWoodstockStorageConformance(
  withOwner: WoodstockStorageProvider,
): readonly WoodstockConformanceCase[] {
  const prepared =
    (test: (owner: SqliteOwner) => void) => (owner: SqliteOwner) => {
      Effect.runSync(prepareWoodstockSchema(owner));
      test(owner);
    };
  const testCase = (
    name: string,
    test: (owner: SqliteOwner) => void,
  ): WoodstockConformanceCase => ({
    name,
    run: () => withOwner(prepared(test)),
  });

  return Object.freeze([
    testCase(
      "prepares the schema idempotently over stored Sessions",
      (owner) => {
        const root = append(owner, "prepared-twice", "kept");
        Effect.runSync(prepareWoodstockSchema(owner));
        assert.deepStrictEqual(
          readSelectedPath(owner, { sessionId: "prepared-twice" }).map(
            ({ id }) => id,
          ),
          [root.id],
        );
      },
    ),

    testCase(
      "admits a command batch atomically and rolls every table back on failure",
      (owner) => {
        const before = sessionTables.map((table) => count(owner, table));
        let enqueued = 0;
        const failing = door(owner, () => {
          enqueued += 1;
          if (enqueued === 2) throw new Error("enqueue refused");
        });
        const exit = Effect.runSyncExit(
          failing.transact((within) => {
            within(prompt("atomic-first", "atomic-1", "first"));
            within(prompt("atomic-second", "atomic-2", "second"));
          }),
        );
        assert.strictEqual(exit._tag, "Failure");
        assert.strictEqual(enqueued, 2);
        assert.deepStrictEqual(
          sessionTables.map((table) => count(owner, table)),
          before,
        );
        assert.strictEqual(hasStoredSession(owner, "atomic-first"), false);
        assert.strictEqual(
          readSessionCommandAdmissionByRequest(
            owner,
            "atomic-first",
            "atomic-1",
          ),
          undefined,
        );
        // The same batch commits whole once nothing refuses it.
        Effect.runSync(
          door(owner).transact((within) => {
            within(prompt("atomic-first", "atomic-1", "first"));
            within(prompt("atomic-second", "atomic-2", "second"));
          }),
        );
        assert.strictEqual(hasStoredSession(owner, "atomic-first"), true);
        assert.strictEqual(hasStoredSession(owner, "atomic-second"), true);
      },
    ),

    testCase(
      "replays a request ID per Session and refuses a conflicting reuse",
      (owner) => {
        const first = admit(owner, prompt("replay-a", "same-request", "hi"));
        const replay = admit(owner, prompt("replay-a", "same-request", "hi"));
        const elsewhere = admit(
          owner,
          prompt("replay-b", "same-request", "hi"),
        );
        assert.deepStrictEqual(replay, { ...first, delivery: "replay" });
        assert.strictEqual(first.status, "accepted");
        assert.strictEqual(elsewhere.status, "accepted");
        assert.notStrictEqual(
          elsewhere.commandId,
          first.commandId,
          "A request ID is scoped to its Session",
        );
        refuses(
          () => admit(owner, prompt("replay-a", "same-request", "changed")),
          { boundary: "contract", cause: "conflict" },
        );
        assert.strictEqual(
          readPendingSessionCommands(owner, "replay-a", 10).total,
          1,
        );
      },
    ),

    testCase("creates one Session per creation request ID", (owner) => {
      const create = (requestId: string) =>
        Effect.runSync(
          createSession(owner, requestId, admitWithoutCheck.admitNew),
        );
      const created = create("create-1");
      assert.strictEqual(create("create-1"), created);
      assert.ok(create("create-2") !== created, "A new request is a new id");
      assert.strictEqual(count(owner, "sessions"), 2);
    }),

    testCase("keeps string identities and text lossless", (owner) => {
      const text = 'Ünïcödé 🍄 \u0000 \u2028 \ud800 "quoted" \\ end';
      const requestId = "request-🍄-ünï";
      const accepted = admit(owner, prompt("lossless", requestId, text));
      assert.strictEqual(accepted.status, "accepted");
      const stored = readSessionCommandAdmissionByRequest(
        owner,
        "lossless",
        requestId,
      );
      assert.ok(stored, "A Unicode request ID indexes its admission");
      assert.strictEqual(stored.id, accepted.commandId);
      assert.ok("message" in stored.entry, "The admission stores its prompt");
      assert.strictEqual(stored.entry.message.content, text);
      const row = append(owner, "lossless", text);
      assert.deepStrictEqual(
        readSelectedPath(owner, { sessionId: "lossless" }).at(-1)?.entry,
        row.entry,
      );
      // Text columns cannot hold a lone surrogate, so the door refuses such
      // an identity before any write instead of indexing a lossy copy.
      const before = sessionTables.map((table) => count(owner, table));
      refuses(
        () => admit(owner, prompt("lossless", "request-\ud800", "lone")),
        { boundary: "contract", cause: "invalid" },
      );
      refuses(
        () => admit(owner, prompt("lossless-\ud800", "request-lone", "lone")),
        { boundary: "contract", cause: "invalid" },
      );
      assert.deepStrictEqual(
        sessionTables.map((table) => count(owner, table)),
        before,
      );
    }),

    testCase(
      "reads the complete selected lineage past every bounded lineage page",
      (owner) => {
        const sessionId = "deep-lineage";
        // One row past the bounded lineage page is the fewest that crosses it;
        // one transaction keeps a file-backed host from syncing every row.
        const total = lineageRowLimit + 1;
        const ids = owner.transactionSync(() =>
          Array.from(
            { length: total },
            (_, index) => append(owner, sessionId, `turn ${String(index)}`).id,
          ),
        );
        const path = readSelectedPath(owner, { sessionId });
        assert.deepStrictEqual(
          path.map(({ id }) => id),
          ids,
        );
        assert.deepStrictEqual(
          path.map(({ parentId }) => parentId),
          [null, ...ids.slice(0, -1)],
        );
        assert.deepStrictEqual(
          path.map(({ sequence }) => sequence),
          ids.map((_, index) => index + 1),
        );
      },
    ),

    testCase(
      "moves the selected branch in place and keeps the abandoned branch",
      (owner) => {
        const sessionId = "branching";
        const root = append(owner, sessionId, "root");
        const abandoned = append(owner, sessionId, "abandoned");
        Effect.runSync(
          branchSession(owner, {
            sessionId,
            type: "branch",
            entryId: root.id,
            assertIdleSession: idle,
          }),
        );
        const selected = append(owner, sessionId, "selected");
        assert.deepStrictEqual(
          readSelectedPath(owner, { sessionId }).map(({ id }) => id),
          [root.id, selected.id],
        );
        const tree = readSessionTree(owner, { sessionId });
        assert.deepStrictEqual(
          tree.map(({ row }) => row.id),
          [root.id],
        );
        assert.deepStrictEqual(
          tree[0]?.children.map(({ row }) => row.id),
          [abandoned.id, selected.id],
        );
      },
    ),

    testCase(
      "forks a lineage with fresh identities and leaves its source unchanged",
      (owner) => {
        const sessionId = "fork-source";
        const root = append(owner, sessionId, "root");
        const middle = append(owner, sessionId, "middle");
        const tail = append(owner, sessionId, "tail");
        const source = readSelectedPath(owner, { sessionId });
        const forked = Effect.runSync(
          forkSession(owner, {
            sessionId,
            type: "fork",
            entryId: middle.id,
            assertIdleSession: idle,
          }),
        ).sessionId;
        const fork = readSelectedPath(owner, { sessionId: forked });
        assert.strictEqual(fork.length, 2);
        assert.ok(
          fork.every(({ id }) => id !== root.id && id !== middle.id),
          "Forked rows get fresh identities",
        );
        assert.deepStrictEqual(
          fork.map(({ parentId }) => parentId),
          [null, fork[0]?.id],
        );
        assert.deepStrictEqual(
          buildSessionContext(fork),
          buildSessionContext([root, middle]),
        );
        append(owner, forked, "fork only");
        assert.deepStrictEqual(readSelectedPath(owner, { sessionId }), source);
        assert.strictEqual(source.at(-1)?.id, tail.id);
      },
    ),

    testCase(
      "completes a streaming placeholder in place with the exact message",
      (owner) => {
        const sessionId = "placeholder";
        const command = commandEntry("question");
        appendSessionEntry(owner, {
          sessionId,
          id: sessionIds.fact.generate(),
          entry: command,
        });
        const run = { kind: "run", value: "run-1" } as const;
        const turn = { kind: "turn", value: "turn-1", parent: run } as const;
        const message = {
          kind: "message",
          value: sessionIds.fact.generate(),
          parent: turn,
        } as const;
        const placeholder = appendSessionEntry(owner, {
          sessionId,
          id: message.value,
          state: "streaming",
          entry: {
            kind: "message_start",
            workId: command.workId,
            run,
            turn,
            message,
            role: "assistant",
          },
        });
        const value = {
          role: "assistant" as const,
          content: [{ type: "text" as const, text: "answer" }],
          api: "anthropic-messages",
          provider: "anthropic",
          model: "claude-test",
          usage: {
            input: 1,
            output: 1,
            cacheRead: 0,
            cacheWrite: 0,
            totalTokens: 2,
            cost: {
              input: 0,
              output: 0,
              cacheRead: 0,
              cacheWrite: 0,
              total: 0,
            },
          },
          stopReason: "stop" as const,
          timestamp: 2,
        };
        const completed = completeSessionLeaf(owner, {
          sessionId,
          id: message.value,
          entry: {
            kind: "message",
            workId: command.workId,
            run,
            turn,
            message,
            value,
          },
        });
        assert.deepStrictEqual(
          [completed.id, completed.parentId, completed.sequence],
          [placeholder.id, placeholder.parentId, placeholder.sequence],
        );
        assert.strictEqual(completed.state, "complete");
        const leaf = readSelectedPath(owner, { sessionId }).at(-1);
        assert.ok(leaf?.entry.kind === "message", "The leaf is the message");
        assert.strictEqual(leaf.id, placeholder.id);
        assert.deepStrictEqual(leaf.entry.value, value);
      },
    ),

    testCase(
      "deletes one Session completely and leaves the others intact",
      (owner) => {
        admit(owner, prompt("doomed", "doomed-1", "bye"));
        const kept = append(owner, "kept", "stay");
        const deleted = Effect.runSync(
          deleteSession(owner, {
            sessionId: "doomed",
            assertIdleSession: idle,
          }),
        );
        assert.strictEqual(deleted, true);
        assert.strictEqual(hasStoredSession(owner, "doomed"), false);
        assert.deepStrictEqual(
          readSelectedPath(owner, { sessionId: "doomed" }),
          [],
        );
        assert.strictEqual(
          readSessionCommandAdmissionByRequest(owner, "doomed", "doomed-1"),
          undefined,
        );
        assert.strictEqual(readSessionInboxSummary(owner, "doomed"), undefined);
        assert.deepStrictEqual(
          readSelectedPath(owner, { sessionId: "kept" }).map(({ id }) => id),
          [kept.id],
        );
        assert.strictEqual(
          Effect.runSync(
            deleteSession(owner, {
              sessionId: "doomed",
              assertIdleSession: idle,
            }),
          ),
          false,
        );
      },
    ),

    testCase(
      "keeps a live lineage whole where it is written, and still deletes it whole",
      (owner) => {
        const root = append(owner, "guarded", "root");
        const leaf = append(owner, "guarded", "leaf");
        assert.throws(
          () =>
            owner.sql.exec("DELETE FROM session_entries WHERE id = ?", root.id),
          /Session entry still has children/u,
        );
        assert.throws(
          () =>
            owner.sql.exec(
              "UPDATE session_entries SET parent_id = NULL WHERE id = ?",
              leaf.id,
            ),
          /Session entry lineage columns are immutable/u,
        );
        assert.strictEqual(
          Effect.runSync(
            deleteSession(owner, {
              sessionId: "guarded",
              assertIdleSession: idle,
            }),
          ),
          true,
        );
        assert.deepStrictEqual(
          readSelectedPath(owner, { sessionId: "guarded" }),
          [],
        );
      },
    ),

    testCase(
      "refuses a REPLACE that would take a parent's place under another id",
      (owner) => {
        const root = append(owner, "replaced", "root");
        const middle = append(owner, "replaced", "middle");
        const leaf = append(owner, "replaced", "leaf");
        // Same Session and sequence as the middle row, a different id: the
        // UNIQUE (session_id, sequence) conflict would delete the middle row.
        try {
          assert.throws(
            () =>
              owner.sql.exec(
                `INSERT OR REPLACE INTO session_entries
                 (id, session_id, parent_id, sequence, type, role, state,
                  request_id, work_id, data_json, timestamp, completed_at)
               SELECT 'replacement', session_id, parent_id, sequence, type,
                      role, state, 'replacement-request', work_id, data_json,
                      timestamp, completed_at
                 FROM session_entries WHERE id = ?`,
                middle.id,
              ),
            /Session entry would strand its children/u,
          );
        } finally {
          // A missing exception alone understates this regression: the old
          // REPLACE actually deletes middle and orphans leaf.
          assert.deepStrictEqual(
            owner.sql
              .exec(
                `SELECT
                 EXISTS (SELECT 1 FROM session_entries WHERE id = ?) AS parent_exists,
                 (SELECT COUNT(*) FROM session_entries AS child
                    LEFT JOIN session_entries AS parent ON parent.id = child.parent_id
                   WHERE child.id = ? AND parent.id IS NULL) AS orphaned_children`,
                middle.id,
                leaf.id,
              )
              .one(),
            { parent_exists: 1, orphaned_children: 0 },
          );
        }
        assert.deepStrictEqual(
          readSelectedPath(owner, { sessionId: "replaced" }).map(
            ({ id }) => id,
          ),
          [root.id, middle.id, leaf.id],
        );
      },
    ),

    testCase(
      "refuses only the Session a writer outside Woodstock broke, from the next open",
      (owner) => {
        append(owner, "healthy", "root");
        append(owner, "damaged", "root");
        const middle = append(owner, "damaged", "middle");
        append(owner, "damaged", "leaf");
        for (const guard of [
          "session_entries_lineage_insert",
          "session_entries_lineage_update",
          "session_entries_lineage_delete",
          "session_entries_damaged_session",
          "session_entries_lineage_replace",
          "session_entries_lineage_replace_position",
        ])
          owner.sql.exec(`DROP TRIGGER ${guard}`);
        owner.sql.exec("DELETE FROM session_entries WHERE id = ?", middle.id);
        Effect.runSync(prepareWoodstockSchema(owner));
        assert.throws(
          () => readSelectedPath(owner, { sessionId: "damaged" }),
          /holds a broken lineage/u,
        );
        assert.throws(
          () => append(owner, "damaged", "more"),
          /holds a broken lineage/u,
        );
        const more = append(owner, "healthy", "more");
        assert.deepStrictEqual(
          readSelectedPath(owner, { sessionId: "healthy" }).at(-1)?.id,
          more.id,
        );
      },
    ),

    testCase(
      "judges a compaction head against the context start and the selected lineage",
      (owner) => {
        const sessionId = "head-check";
        const first = append(owner, sessionId, "first");
        const second = append(owner, sessionId, "second");
        const third = append(owner, sessionId, "third");
        const state = (head: string) =>
          compactionHeadState(owner, { sessionId, head });
        // Before any compaction every entry on the lineage is current.
        assert.strictEqual(state(first.id), "current");
        compact(owner, sessionId, second.id);
        // Older than the context start: a later compaction already cut it.
        assert.strictEqual(state(first.id), "stale");
        // At or after the context start: still inside the model context.
        assert.strictEqual(state(second.id), "current");
        assert.strictEqual(state(third.id), "current");
        assert.strictEqual(state("unknown-entry"), "stale");
        assert.strictEqual(
          compactionHeadState(owner, { sessionId: "no-such", head: first.id }),
          "stale",
        );
      },
    ),

    testCase(
      "treats a compaction head off the selected lineage as stale",
      (owner) => {
        const sessionId = "head-off-lineage";
        const root = append(owner, sessionId, "root");
        const abandoned = append(owner, sessionId, "abandoned");
        Effect.runSync(
          branchSession(owner, {
            sessionId,
            type: "branch",
            entryId: root.id,
            assertIdleSession: idle,
          }),
        );
        const selected = append(owner, sessionId, "selected");
        assert.strictEqual(
          compactionHeadState(owner, { sessionId, head: abandoned.id }),
          "stale",
        );
        assert.strictEqual(
          compactionHeadState(owner, { sessionId, head: selected.id }),
          "current",
        );
      },
    ),

    testCase(
      "round-trips a stale compaction notice with its usage",
      (owner) => {
        const sessionId = "stale-notice";
        append(owner, sessionId, "first");
        const workId = sessionIds.work.generate();
        const row = appendCompactionNotice(owner, {
          sessionId,
          workId,
          trigger: "threshold",
          result: "stale",
          usage,
        });
        const stored = readSelectedPath(owner, { sessionId }).find(
          ({ id }) => id === row.id,
        );
        assert.deepStrictEqual(stored?.entry, {
          kind: "compaction_notice",
          workId,
          trigger: "threshold",
          result: "stale",
          usage,
        });
        // A notice that spent nothing records no usage.
        const failed = appendCompactionNotice(owner, {
          sessionId,
          workId,
          trigger: "threshold",
          result: "failed",
          diagnostic: "boom",
        });
        assert.ok(!("usage" in failed.entry), "A failed notice has no usage");
      },
    ),

    testCase(
      "keeps accepted work pending until a terminal settles it",
      (owner) => {
        const first = admit(owner, prompt("pending", "pending-1", "one"));
        const second = admit(owner, prompt("pending", "pending-2", "two"));
        const pending = readPendingSessionCommands(owner, "pending", 10);
        assert.strictEqual(pending.total, 2);
        assert.deepStrictEqual(
          pending.commands.map(({ entry }) => entry.requestId),
          ["pending-1", "pending-2"],
        );
        assert.ok(
          first.status === "accepted" && second.status === "accepted",
          "Both prompts are accepted",
        );
      },
    ),

    testCase(
      "keeps explicit title overrides distinct from derived titles and replays clears without inbox changes",
      (owner) => {
        const sessionId = "title-replay";
        append(owner, sessionId, "Fern notes");
        const before = readSessionInboxChanges(owner, 0, 64).latest;

        // Equal-looking display text must still become an explicit override.
        setSessionTitle(owner, sessionId, "Fern notes");
        const overridden = readSessionInboxChanges(owner, before, 64);
        assert.ok(overridden.latest > before);
        assert.equal(
          readStoredSessionSummary(owner, sessionId)?.title,
          "Fern notes",
        );
        setSessionTitle(owner, sessionId, "Fern notes");
        assert.deepStrictEqual(
          readSessionInboxChanges(owner, overridden.latest, 64),
          { rows: [], through: overridden.latest, latest: overridden.latest },
        );

        setSessionTitle(owner, sessionId, "Other notes");
        assert.equal(
          readStoredSessionSummary(owner, sessionId)?.title,
          "Other notes",
        );
        setSessionTitle(owner, sessionId, null);
        assert.equal(
          readStoredSessionSummary(owner, sessionId)?.title,
          "Fern notes",
        );
        const cleared = readSessionInboxChanges(owner, 0, 64).latest;
        setSessionTitle(owner, sessionId, null);
        assert.deepStrictEqual(readSessionInboxChanges(owner, cleared, 64), {
          rows: [],
          through: cleared,
          latest: cleared,
        });
      },
    ),

    testCase(
      "pages the inbox in one stable order past a full page",
      (owner) => {
        // One Session past the 200-row inbox page, written in one transaction.
        const total = 201;
        owner.transactionSync(() => {
          for (let index = 0; index < total; index += 1)
            append(owner, `inbox-${String(index).padStart(3, "0")}`, "hello");
        });
        const seen: string[] = [];
        let after: string | undefined;
        do {
          const page = Effect.runSync(readSessionInbox(owner, after));
          seen.push(...page.sessions.map(({ sessionId }) => sessionId));
          after = page.next;
        } while (after !== undefined);
        assert.strictEqual(seen.length, total);
        assert.strictEqual(new Set(seen).size, total);
      },
    ),
  ]);
}
