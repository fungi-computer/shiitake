// Runtime for every law here: Node 24, `node:sqlite` (Woodstock's Local host).
import * as Effect from "effect/Effect";
import { describe, expect, it } from "vitest";

import type { SqliteOwner } from "../src/internal/contracts.js";
import type { ObservedActivity } from "../src/internal/session-status.js";
import { readClientSnapshot } from "../src/internal/woodstock/client-projection.js";
import { sessionIds } from "../src/internal/woodstock/identities.js";
import { readSessionInboxSummary } from "../src/internal/woodstock/session-inbox.js";
import { prepareWoodstockSchema } from "../src/internal/woodstock/index.js";
import type { StoredSessionEntry } from "../src/internal/woodstock/session-entry.js";
import * as woodstock from "../src/internal/woodstock/session-store.js";
import { idleActivity } from "./fixtures/client.fixture.js";
import { writeLargeSession } from "./fixtures/large-session.fixture.js";
import { damageOutsideWoodstock } from "./fixtures/lineage-damage.fixture.js";
import {
  createNativeSqliteOwnerFixture,
  type NativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

const activity = {
  facts: idleActivity,
  progress: null,
  toolOutput: [],
} satisfies ObservedActivity;
const contract = { core: "a".repeat(64), extensions: [] } as const;
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

function append(owner: SqliteOwner, sessionId: string, content: string) {
  return woodstock.appendSessionEntry(owner, {
    sessionId,
    id: sessionIds.fact.generate(),
    entry: command(sessionIds.work.generate(), content),
  });
}

/** Owner operations one read takes. */
function counted<A>(fixture: NativeSqliteOwnerFixture, read: () => A) {
  const before = fixture.ownerOperations();
  const value = read();
  return { value, operations: fixture.ownerOperations() - before };
}

describe("opening a Session reads its newest rows, not its history (Node, node:sqlite)", () => {
  function opened(turns: number) {
    return withFixture((fixture) => {
      const { owner } = fixture;
      const session = writeLargeSession(owner, {
        sessionId: "opened",
        turns,
        compactEvery: 100,
        keepTurns: 4,
        textBytes: 64,
      });
      woodstock.setSessionModelPreference(owner, session.sessionId, preference);
      const read = () => ({
        latest: readClientSnapshot(
          owner,
          session.sessionId,
          { limit: 50 },
          activity,
        ),
        choice: woodstock.readLastModelChoiceSequence(owner, session.sessionId),
      });
      const before = read();
      const older = before.latest.oldestCursor;
      if (older === null || older === undefined)
        throw new Error("missing older cursor");
      const olderPage = readClientSnapshot(
        owner,
        session.sessionId,
        { limit: 50, before: older },
        activity,
      );
      const newest = woodstock.readSessionLineage(owner, {
        sessionId: session.sessionId,
      });
      if (newest === undefined) throw new Error("missing lineage");
      // Everything but the root and the newest 600 rows is gone: a read that
      // walks or checks that history fails, so passing proves it never does.
      damageOutsideWoodstock(
        owner,
        "DELETE FROM session_entries WHERE session_id = ? AND sequence BETWEEN 2 AND ?",
        session.sessionId,
        newest.newestSequence - 600,
      );
      const after = counted(fixture, read);
      return {
        before,
        after,
        olderPage,
        olderAfter: readClientSnapshot(
          owner,
          session.sessionId,
          { limit: 50, before: older },
          activity,
        ),
      };
    });
  }

  it("takes the same statements at 300 and 1,200 turns with the same answers", () => {
    const small = opened(300);
    const large = opened(1_200);
    for (const read of [small, large]) {
      expect(read.after.value).toEqual(read.before);
      expect(read.olderAfter).toEqual(read.olderPage);
      expect(read.after.value.choice).not.toBeNull();
    }
    expect(large.after.operations).toBe(small.after.operations);
  });
});

describe("a run and a compaction head check read the kept window (Node, node:sqlite)", () => {
  function prepared(turns: number) {
    return withFixture((fixture) => {
      const { owner } = fixture;
      const session = writeLargeSession(owner, {
        sessionId: "prepared",
        turns,
        compactEvery: 100,
        keepTurns: 4,
        textBytes: 64,
      });
      const next = append(owner, session.sessionId, "next");
      const latest = session.compactions.at(-1);
      const earliest = session.compactions[0];
      const keptHead = session.turns[latest?.keptTurn ?? -1]?.commandId;
      const cutHead = session.turns[earliest?.keptTurn ?? -1]?.commandId;
      if (keptHead === undefined || cutHead === undefined)
        throw new Error("missing compaction heads");
      const read = () => {
        const run = woodstock.prepareSessionRun(owner, {
          sessionId: session.sessionId,
          commandFactId: next.id,
        });
        return {
          messages: run.messages,
          path: run.path.map(({ id }) => id),
          kept: woodstock.compactionHeadState(owner, {
            sessionId: session.sessionId,
            head: keptHead,
          }),
          cut: woodstock.compactionHeadState(owner, {
            sessionId: session.sessionId,
            head: cutHead,
          }),
        };
      };
      const before = read();
      const lineage = woodstock.readSessionLineage(owner, {
        sessionId: session.sessionId,
      });
      if (lineage === undefined) throw new Error("missing lineage");
      damageOutsideWoodstock(
        owner,
        "DELETE FROM session_entries WHERE session_id = ? AND sequence BETWEEN 2 AND ?",
        session.sessionId,
        lineage.newestSequence - 600,
      );
      return { before, after: counted(fixture, read) };
    });
  }

  it("takes the same statements at 300 and 1,200 turns with the same answers", () => {
    const small = prepared(300);
    const large = prepared(1_200);
    for (const read of [small, large]) {
      expect(read.after.value).toEqual(read.before);
      expect(read.before.kept).toBe("current");
      expect(read.before.cut).toBe("stale");
    }
    expect(large.after.operations).toBe(small.after.operations);
  });
});

describe("Woodstock keeps every lineage whole where it is written (Node, node:sqlite)", () => {
  it("refuses removing or re-pointing a parent, a missing parent and a second root", () => {
    withFixture(({ owner }) => {
      const sessionId = "guarded";
      const root = append(owner, sessionId, "root");
      const child = append(owner, sessionId, "child");
      append(owner, sessionId, "leaf");

      expect(() =>
        owner.sql.exec("DELETE FROM session_entries WHERE id = ?", child.id),
      ).toThrow("Session entry still has children");
      expect(() =>
        owner.sql.exec(
          "UPDATE session_entries SET parent_id = ? WHERE id = ?",
          root.id,
          child.id,
        ),
      ).toThrow("Session entry lineage columns are immutable");
      for (const parent of ["missing", null])
        expect(() =>
          owner.sql.exec(
            `INSERT INTO session_entries
               (id, session_id, parent_id, sequence, type, role, state,
                request_id, work_id, data_json, timestamp, completed_at)
             SELECT ?, session_id, ?, 99, type, role, state, 'other-request',
                    work_id, data_json, timestamp, completed_at
               FROM session_entries WHERE id = ?`,
            sessionIds.fact.generate(),
            parent,
            child.id,
          ),
        ).toThrow(
          parent === null
            ? "Session already has a root entry"
            : "Session entry parent is not an earlier complete entry of its Session",
        );
      expect(
        woodstock.readSelectedPath(owner, { sessionId }).map(({ id }) => id),
      ).toHaveLength(3);
    });
  });

  it("still deletes a whole Session", () => {
    withFixture(({ owner }) => {
      const sessionId = "deleted";
      append(owner, sessionId, "root");
      append(owner, sessionId, "leaf");

      expect(
        woodstock.deleteStoredSession(owner, {
          sessionId,
          assertIdleSession: () => undefined,
          recordInboxDeletion: () => undefined,
        }),
      ).toBe(true);
      expect(woodstock.readSelectedPath(owner, { sessionId })).toEqual([]);
    });
  });

  it("refuses a window or range whose walk ends at a missing parent", () => {
    withFixture(({ owner }) => {
      const sessionId = "short-page";
      append(owner, sessionId, "root");
      const middle = append(owner, sessionId, "middle");
      for (const text of ["three", "four", "five"])
        append(owner, sessionId, text);
      damageOutsideWoodstock(
        owner,
        "DELETE FROM session_entries WHERE id = ?",
        middle.id,
      );

      const lineage = woodstock.readSessionLineage(owner, { sessionId });
      if (lineage === undefined) throw new Error("missing lineage");
      const window = (from: "newest" | "oldest") =>
        woodstock.readLineageWindow(owner, lineage, { from, rows: 200 });
      // Both window walks and the range read refuse rather than end early.
      for (const read of [
        () => readClientSnapshot(owner, sessionId, { limit: 50 }, activity),
        () => window("newest"),
        () => window("oldest"),
        () => woodstock.readSelectedPath(owner, { sessionId }),
      ])
        expect(read).toThrow("Selected Session path has a missing parent");
    });
  });

  it("refuses a REPLACE that would strand the replaced row's children", () => {
    withFixture(({ owner }) => {
      const sessionId = "replaced";
      append(owner, sessionId, "root");
      const middle = append(owner, sessionId, "middle");
      append(owner, sessionId, "leaf");

      expect(() =>
        owner.sql.exec(
          `INSERT OR REPLACE INTO session_entries
             (id, session_id, parent_id, sequence, type, role, state,
              request_id, work_id, data_json, timestamp, completed_at)
           SELECT id, session_id, parent_id, 99, type, role, state,
                  request_id, work_id, data_json, timestamp, completed_at
             FROM session_entries WHERE id = ?`,
          middle.id,
        ),
      ).toThrow("Session entry would strand its children");
    });
  });

  it("preserves a parent and its child when REPLACE conflicts on position under a different id", () => {
    withFixture(({ owner }) => {
      const sessionId = "position-replaced";
      const root = append(owner, sessionId, "root");
      const middle = append(owner, sessionId, "middle");
      const leaf = append(owner, sessionId, "leaf");
      try {
        expect(() =>
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
        ).toThrow("Session entry would strand its children");
      } finally {
        // Inspect the stored rows before reading the lineage: on the broken
        // revision REPLACE succeeds, deletes middle and leaves leaf orphaned.
        expect(
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
        ).toEqual({ parent_exists: 1, orphaned_children: 0 });
      }
      expect(
        woodstock.readSelectedPath(owner, { sessionId }).map(({ id }) => id),
      ).toEqual([root.id, middle.id, leaf.id]);
    });
  });

  it("refuses a cycle a writer outside Woodstock left behind the leaf", () => {
    withFixture(({ owner }) => {
      const sessionId = "cycled";
      append(owner, sessionId, "root");
      woodstock.setSessionModelPreference(owner, sessionId, preference);
      const middle = append(owner, sessionId, "middle");
      const leaf = append(owner, sessionId, "leaf");
      // The middle row now points at its own child: a loop no walk may follow.
      damageOutsideWoodstock(
        owner,
        "UPDATE session_entries SET parent_id = ? WHERE id = ?",
        leaf.id,
        middle.id,
      );

      for (const read of [
        () => woodstock.readSessionModelPreference(owner, sessionId),
        () => woodstock.readSessionContextPath(owner, { sessionId }),
      ])
        expect(read).toThrow("Selected Session path has a missing parent");
    });
  });

  it("refuses only the Session whose lineage was broken before its guards", () => {
    withFixture(({ owner }) => {
      const healthy = "healthy";
      const healthyRoot = append(owner, healthy, "root");
      append(owner, healthy, "leaf");
      const damaged = "damaged";
      const root = append(owner, damaged, "root");
      const middle = append(owner, damaged, "middle");
      append(owner, damaged, "leaf");
      damageOutsideWoodstock(
        owner,
        "DELETE FROM session_entries WHERE id = ?",
        middle.id,
      );
      const broken = /holds a broken lineage/u;
      const reads = (sessionId: string, rootId: string) => [
        () => readClientSnapshot(owner, sessionId, { limit: 50 }, activity),
        () => woodstock.readSessionContextPath(owner, { sessionId }),
        () => woodstock.readSessionModelPreference(owner, sessionId),
        () => woodstock.readLastModelChoiceSequence(owner, sessionId),
        () => woodstock.readSelectedPath(owner, { sessionId }),
        () => woodstock.readStoredSessionSummary(owner, sessionId),
        () => woodstock.hasStoredSession(owner, sessionId),
        () => woodstock.compactionHeadState(owner, { sessionId, head: rootId }),
        () => woodstock.forkSessionAt(owner, { sessionId, entryId: rootId }),
        () => append(owner, sessionId, "more"),
      ];

      for (let open = 0; open < 2; open += 1) {
        Effect.runSync(prepareWoodstockSchema(owner));
        for (const read of reads(damaged, root.id))
          expect(read).toThrow(broken);
        // A read names the Session, the store's damage and the step that clears it.
        expect(() =>
          woodstock.readSessionContextPath(owner, { sessionId: damaged }),
        ).toThrow(
          'Woodstock store: Woodstock Session damaged holds a broken lineage (this store has 1 damaged Session, first damaged); a store never repairs itself — delete this Session with deleteSession("damaged"), or reset the owning store: for a Durable Object, erase all of its storage (DurableObjectStorage.deleteAll(); a Hub Agent is erased only with its Team, POST /operator/teams/:teamId/erasure-durable-objects); for a local store, remove its file and the file\'s .owner.sqlite sidecar',
        );
        expect(() =>
          owner.sql.exec(
            `INSERT INTO session_entries
               (id, session_id, parent_id, sequence, type, role, state,
                request_id, work_id, data_json, timestamp, completed_at)
             SELECT ?, session_id, id, 99, type, role, state, 'raw-request',
                    work_id, data_json, timestamp, completed_at
               FROM session_entries WHERE id = ?`,
            sessionIds.fact.generate(),
            root.id,
          ),
        ).toThrow(broken);
        expect(readSessionInboxSummary(owner, damaged)?.state).toBe(
          "unavailable",
        );
        for (const read of reads(healthy, healthyRoot.id))
          expect(read).not.toThrow();
        expect(
          woodstock.readSelectedPath(owner, { sessionId: healthy }).length,
        ).toBe(3 + open);
      }
      expect(
        owner.sql
          .exec(
            "SELECT name FROM sqlite_master WHERE type = 'trigger' ORDER BY name",
          )
          .toArray()
          .map(({ name }) => name),
      ).toEqual([
        "session_entries_damaged_session",
        "session_entries_lineage_delete",
        "session_entries_lineage_insert",
        "session_entries_lineage_replace",
        "session_entries_lineage_replace_position",
        "session_entries_lineage_update",
      ]);

      // Deleting the damaged Session clears it; its identity is usable again.
      expect(
        woodstock.deleteStoredSession(owner, {
          sessionId: damaged,
          assertIdleSession: () => undefined,
          recordInboxDeletion: () => undefined,
        }),
      ).toBe(true);
      append(owner, damaged, "fresh");
      expect(
        woodstock.readSelectedPath(owner, { sessionId: damaged }),
      ).toHaveLength(1);
    });
  });

  it("audits when only one guard is missing", () => {
    withFixture(({ owner }) => {
      const sessionId = "one-guard";
      const root = append(owner, sessionId, "root");
      append(owner, sessionId, "middle");
      const leaf = append(owner, sessionId, "leaf");
      // Only the update guard is dropped, and only it could let this through.
      owner.sql.exec("DROP TRIGGER session_entries_lineage_update");
      owner.sql.exec(
        "UPDATE session_entries SET parent_id = ? WHERE id = ?",
        leaf.id,
        root.id,
      );
      Effect.runSync(prepareWoodstockSchema(owner));

      expect(() =>
        woodstock.readSessionContextPath(owner, { sessionId }),
      ).toThrow("Woodstock Session one-guard holds a broken lineage");
    });
  });

  it("puts dropped guards back on the next open, after auditing", () => {
    withFixture(({ owner }) => {
      const sessionId = "reopened";
      const root = append(owner, sessionId, "root");
      append(owner, sessionId, "leaf");
      damageOutsideWoodstock(owner, "SELECT 1");
      Effect.runSync(prepareWoodstockSchema(owner));

      expect(() =>
        owner.sql.exec("DELETE FROM session_entries WHERE id = ?", root.id),
      ).toThrow("Session entry still has children");
    });
  });
});
