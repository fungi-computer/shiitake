// Runtime for every law here: Node 24, `node:sqlite` (Woodstock's Local host).
import * as Effect from "effect/Effect";
import { describe, expect, it } from "vitest";

import type { SqliteOwner } from "../src/internal/contracts.js";
import { sessionIds } from "../src/internal/woodstock/identities.js";
import { prepareWoodstockSchema } from "../src/internal/woodstock/index.js";
import type { StoredSessionEntry } from "../src/internal/woodstock/session-entry.js";
import * as woodstock from "../src/internal/woodstock/session-store.js";
import { compactionEntryFixture } from "./fixtures/compaction-entry.fixture.js";
import { writeLargeSession } from "./fixtures/large-session.fixture.js";
import { damageOutsideWoodstock } from "./fixtures/lineage-damage.fixture.js";
import {
  createNativeSqliteOwnerFixture,
  type NativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

const contract = { core: "a".repeat(64), extensions: [] } as const;
const usage = {
  input: 1,
  output: 1,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 2,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
};
const preference = { providerId: "openai", modelId: "gpt-6-luna" } as const;

function withFixture<A>(run: (fixture: NativeSqliteOwnerFixture) => A): A {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    return run(fixture);
  } finally {
    fixture.close();
  }
}

function append(
  owner: SqliteOwner,
  sessionId: string,
  entry: StoredSessionEntry,
): woodstock.StoredSessionRow {
  return woodstock.appendSessionEntry(owner, {
    sessionId,
    id: sessionIds.fact.generate(),
    entry,
  });
}

function command(workId: string, content: string): StoredSessionEntry {
  return {
    kind: "command",
    workId,
    requestId: `request-${workId}`,
    receiptId: sessionIds.receipt.generate(),
    message: { role: "user", content, timestamp: 1 },
    contract,
  };
}

function cancellation(targetWorkId: string): StoredSessionEntry {
  return {
    kind: "cancellation",
    workId: targetWorkId,
    requestId: `cancel-${targetWorkId}`,
    receiptId: sessionIds.receipt.generate(),
    targetWorkId,
  };
}

function compaction(workId: string, head: string): StoredSessionEntry {
  return compactionEntryFixture({
    workId,
    head,
    summary: `summary kept from ${head}`,
    usage,
    contextRevision: head,
  });
}

/** Owner operations one read takes. */
function counted<A>(fixture: NativeSqliteOwnerFixture, read: () => A) {
  const before = fixture.ownerOperations();
  const value = read();
  return { value, operations: fixture.ownerOperations() - before };
}

/**
 * Delete every row older than `sequence`, as only a writer outside Woodstock
 * could: history a bounded read must not visit.
 */
function cutHistoryBefore(
  owner: SqliteOwner,
  sessionId: string,
  sequence: number,
): void {
  damageOutsideWoodstock(
    owner,
    "DELETE FROM session_entries WHERE session_id = ? AND sequence < ?",
    sessionId,
    sequence,
  );
}

describe("the settled context and footer read the kept window, not history (Node, node:sqlite)", () => {
  function settledRead(turns: number) {
    return withFixture((fixture) => {
      const { owner } = fixture;
      const session = writeLargeSession(owner, {
        sessionId: "settled",
        turns,
        compactEvery: 100,
        keepTurns: 4,
        textBytes: 64,
      });
      const full = woodstock.readSelectedPath(owner, {
        sessionId: session.sessionId,
      });
      const latest = session.compactions.at(-1);
      const keptId = session.turns[latest?.keptTurn ?? -1]?.commandId;
      const keptIndex = full.findIndex(({ id }) => id === keptId);
      const kept = full[keptIndex];
      if (kept === undefined) throw new Error("missing kept entry");
      // The history the latest compaction cut is gone: a read that walks or
      // checks it fails, so passing proves it is never visited.
      cutHistoryBefore(owner, session.sessionId, kept.sequence);
      const context = counted(fixture, () =>
        woodstock.readSessionContextPath(owner, {
          sessionId: session.sessionId,
        }),
      );
      const footer = counted(fixture, () =>
        woodstock.readSessionModelPreference(owner, session.sessionId),
      );
      return {
        context,
        footer,
        expectedIds: full.slice(keptIndex).map(({ id }) => id),
        expectedMessages: woodstock.buildSessionContext(full),
      };
    });
  }

  it("takes the same three statements at 500 and 2,000 turns with the same answer", () => {
    const small = settledRead(500);
    const large = settledRead(2_000);
    for (const read of [small, large]) {
      expect(read.context.value.map(({ id }) => id)).toEqual(read.expectedIds);
      expect(woodstock.buildSessionContext(read.context.value)).toEqual(
        read.expectedMessages,
      );
      // The leaf, one walk from it to the kept entry, then the deciding
      // facts' indexes, which find none here and so walk nothing.
      expect(read.context.operations).toBe(3);
      expect(read.footer.value).toBeNull();
      // The leaf, then the preference index: no preference, no walk.
      expect(read.footer.operations).toBe(2);
    }
    expect(large.context.value).toHaveLength(small.context.value.length);
  });

  it("reads a preference inside the window without the history behind it", () => {
    withFixture(({ owner }) => {
      const session = writeLargeSession(owner, {
        sessionId: "preferred",
        turns: 300,
        compactEvery: 100,
        keepTurns: 4,
        textBytes: 64,
      });
      woodstock.setSessionModelPreference(owner, session.sessionId, {
        providerId: "anthropic",
        modelId: "claude-old",
      });
      const kept = append(
        owner,
        session.sessionId,
        command(sessionIds.work.generate(), "kept"),
      );
      woodstock.setSessionModelPreference(owner, session.sessionId, preference);
      cutHistoryBefore(owner, session.sessionId, kept.sequence);

      expect(
        woodstock.readSessionModelPreference(owner, session.sessionId),
      ).toEqual(preference);
    });
  });

  it("ignores a preference on a branch the selected lineage left", () => {
    withFixture(({ owner }) => {
      const sessionId = "branched-preference";
      const root = append(owner, sessionId, command("work-root", "root"));
      woodstock.setSessionModelPreference(owner, sessionId, preference);
      append(owner, sessionId, command("work-later", "later"));
      woodstock.selectSessionBranch(owner, { sessionId, entryId: root.id });
      append(owner, sessionId, command("work-other", "other"));

      expect(woodstock.readSessionModelPreference(owner, sessionId)).toBeNull();
      woodstock.selectSessionBranch(owner, { sessionId, entryId: root.id });
      woodstock.setSessionModelPreference(owner, sessionId, null);
      expect(woodstock.readSessionModelPreference(owner, sessionId)).toBeNull();
    });
  });

  it("keeps an older cancellation on the lineage and drops one off it", () => {
    withFixture((fixture) => {
      const { owner } = fixture;
      const sessionId = "withdrawn";
      const withdrawn = sessionIds.work.generate();
      const root = append(owner, sessionId, command("work-root", "root"));
      // A cancellation on a branch the lineage left decides nothing here.
      append(owner, sessionId, cancellation("work-elsewhere"));
      woodstock.selectSessionBranch(owner, { sessionId, entryId: root.id });
      const cancelled = append(owner, sessionId, cancellation(withdrawn));
      const kept = append(owner, sessionId, command("work-kept", "kept"));
      append(owner, sessionId, compaction("work-kept", kept.id));
      append(owner, sessionId, command(withdrawn, "withdrawn prompt"));
      append(owner, sessionId, command("work-elsewhere", "elsewhere prompt"));
      const full = woodstock.readSelectedPath(owner, { sessionId });

      const context = counted(fixture, () =>
        woodstock.readSessionContextPath(owner, { sessionId }),
      );

      expect(context.value.map(({ id }) => id)).toEqual([
        cancelled.id,
        ...full
          .slice(full.findIndex(({ id }) => id === kept.id))
          .map(({ id }) => id),
      ]);
      const messages = JSON.stringify(
        woodstock.buildSessionContext(context.value),
      );
      expect(messages).not.toContain("withdrawn prompt");
      expect(messages).toContain("elsewhere prompt");
      expect(woodstock.buildSessionContext(context.value)).toEqual(
        woodstock.buildSessionContext(full),
      );
      // The leaf, the window, the walk to the deciding facts, then their rows.
      expect(context.operations).toBe(4);
    });
  });

  it("reads the whole path when the newest compaction keeps an entry off this lineage", () => {
    withFixture(({ owner }) => {
      const sessionId = "head-elsewhere";
      const root = append(owner, sessionId, command("work-root", "root"));
      const elsewhere = append(owner, sessionId, command("work-gone", "gone"));
      woodstock.selectSessionBranch(owner, { sessionId, entryId: root.id });
      append(owner, sessionId, command("work-here", "here"));
      append(owner, sessionId, compaction("work-here", elsewhere.id));
      const full = woodstock.readSelectedPath(owner, { sessionId });

      const context = woodstock.readSessionContextPath(owner, { sessionId });

      expect(context.map(({ id }) => id)).toEqual(full.map(({ id }) => id));
      expect(context[0]?.id).toBe(root.id);
      expect(woodstock.buildSessionContext(context)).toEqual(
        woodstock.buildSessionContext(full),
      );
    });
  });

  it("ignores a newer compaction on a branch the lineage left", () => {
    withFixture(({ owner }) => {
      const sessionId = "newer-elsewhere";
      append(owner, sessionId, command("work-root", "root"));
      const kept = append(owner, sessionId, command("work-kept", "kept"));
      const boundary = append(
        owner,
        sessionId,
        compaction("work-kept", kept.id),
      );
      const leaf = append(owner, sessionId, command("work-leaf", "leaf"));
      woodstock.selectSessionBranch(owner, {
        sessionId,
        entryId: boundary.id,
      });
      const other = append(owner, sessionId, command("work-other", "other"));
      append(owner, sessionId, compaction("work-other", other.id));
      woodstock.selectSessionBranch(owner, { sessionId, entryId: leaf.id });
      const full = woodstock.readSelectedPath(owner, { sessionId });

      const context = woodstock.readSessionContextPath(owner, { sessionId });

      expect(context.map(({ id }) => id)).toEqual([
        kept.id,
        boundary.id,
        leaf.id,
      ]);
      expect(woodstock.buildSessionContext(context)).toEqual(
        woodstock.buildSessionContext(full),
      );
    });
  });

  it("refuses a missing parent between the kept entry and an older fact", () => {
    withFixture(({ owner }) => {
      const sessionId = "broken-facts";
      const withdrawn = sessionIds.work.generate();
      append(owner, sessionId, command("work-root", "root"));
      append(owner, sessionId, cancellation(withdrawn));
      const gap = append(owner, sessionId, command("work-gap", "gap"));
      const kept = append(owner, sessionId, command("work-kept", "kept"));
      append(owner, sessionId, compaction("work-kept", kept.id));
      append(owner, sessionId, command(withdrawn, "withdrawn prompt"));
      damageOutsideWoodstock(
        owner,
        "DELETE FROM session_entries WHERE session_id = ? AND id = ?",
        sessionId,
        gap.id,
      );

      expect(() =>
        woodstock.readSessionContextPath(owner, { sessionId }),
      ).toThrow("Selected Session path has a missing parent");
    });
  });

  it("refuses a missing parent between the leaf and the newest preference", () => {
    withFixture(({ owner }) => {
      const sessionId = "broken-preference";
      append(owner, sessionId, command("work-root", "root"));
      woodstock.setSessionModelPreference(owner, sessionId, preference);
      const gap = append(owner, sessionId, command("work-gap", "gap"));
      append(owner, sessionId, command("work-leaf", "leaf"));
      damageOutsideWoodstock(
        owner,
        "DELETE FROM session_entries WHERE session_id = ? AND id = ?",
        sessionId,
        gap.id,
      );

      expect(() =>
        woodstock.readSessionModelPreference(owner, sessionId),
      ).toThrow("Selected Session path has a missing parent");
    });
  });

  it("carries an earlier compaction of the active work past a later boundary", () => {
    withFixture(({ owner }) => {
      const sessionId = "recovery";
      const work = sessionIds.work.generate();
      append(owner, sessionId, command("work-before", "before"));
      const running = append(owner, sessionId, command(work, "long run"));
      const recovery = append(owner, sessionId, compaction(work, running.id));
      const steer = append(owner, sessionId, command("work-steer", "steer"));
      append(owner, sessionId, compaction(work, steer.id));

      const ids = woodstock
        .readSessionContextPath(owner, { sessionId, workId: work })
        .map(({ id }) => id);

      expect(ids[0]).toBe(recovery.id);
      expect(ids).not.toContain(running.id);
      expect(
        woodstock
          .readSessionContextPath(owner, { sessionId })
          .map(({ id }) => id),
      ).not.toContain(recovery.id);
    });
  });

  it("refuses a deleted leaf rather than answering no preference", () => {
    withFixture(({ owner }) => {
      const sessionId = "deleted-leaf";
      append(owner, sessionId, command("work-root", "root"));
      woodstock.setSessionModelPreference(owner, sessionId, preference);
      const leaf = append(owner, sessionId, command("work-leaf", "leaf"));
      owner.sql.exec("DELETE FROM session_entries WHERE id = ?", leaf.id);

      // The Session still names the deleted row as its leaf: both reads refuse
      // rather than answer "no preference" or an empty context.
      expect(() =>
        woodstock.readSessionModelPreference(owner, sessionId),
      ).toThrow("Selected Session leaf is unavailable");
      expect(() =>
        woodstock.readSessionContextPath(owner, { sessionId }),
      ).toThrow("Selected Session leaf is unavailable");
    });
  });

  it("reads a fork's settled context as the lineage it copied", () => {
    withFixture(({ owner }) => {
      const session = writeLargeSession(owner, {
        sessionId: "fork-parity",
        turns: 250,
        compactEvery: 100,
        keepTurns: 4,
        textBytes: 64,
      });
      const leaf = session.turns.at(-1)?.terminalId;
      if (leaf === undefined) throw new Error("missing leaf");
      const forked = woodstock.forkSessionAt(owner, {
        sessionId: session.sessionId,
        entryId: leaf,
      });

      expect(
        woodstock.buildSessionContext(
          woodstock.readSessionContextPath(owner, { sessionId: forked }),
        ),
      ).toEqual(
        woodstock.buildSessionContext(
          woodstock.readSessionContextPath(owner, {
            sessionId: session.sessionId,
          }),
        ),
      );
    });
  });

  it("ignores a streaming placeholder after a compaction", () => {
    withFixture(({ owner }) => {
      const sessionId = "streaming-after-compaction";
      append(owner, sessionId, command("work-root", "root"));
      const kept = append(owner, sessionId, command("work-kept", "kept"));
      append(owner, sessionId, compaction("work-kept", kept.id));
      const before = woodstock.readSessionContextPath(owner, { sessionId });
      const run = { kind: "run", value: "run-streaming" } as const;
      const turn = {
        kind: "turn",
        value: "turn-streaming",
        parent: run,
      } as const;
      const message = {
        kind: "message",
        value: sessionIds.fact.generate(),
        parent: turn,
      } as const;
      woodstock.appendSessionEntry(owner, {
        sessionId,
        id: message.value,
        state: "streaming",
        entry: {
          kind: "message_start",
          workId: "work-kept",
          run,
          turn,
          message,
          role: "assistant",
        },
      });
      const full = woodstock.readSelectedPath(owner, { sessionId });

      const after = woodstock.readSessionContextPath(owner, { sessionId });

      expect(after).toEqual(before);
      expect(woodstock.buildSessionContext(after)).toEqual(
        woodstock.buildSessionContext(full),
      );
    });
  });

  it("still refuses a broken window", () => {
    withFixture(({ owner }) => {
      const sessionId = "broken";
      append(owner, sessionId, command("work-one", "one"));
      const two = append(owner, sessionId, command("work-two", "two"));
      append(owner, sessionId, command("work-three", "three"));
      damageOutsideWoodstock(
        owner,
        "DELETE FROM session_entries WHERE session_id = ? AND id = ?",
        sessionId,
        two.id,
      );

      expect(() =>
        woodstock.readSessionContextPath(owner, { sessionId }),
      ).toThrow("Selected Session path has a missing parent");
    });
  });
});
