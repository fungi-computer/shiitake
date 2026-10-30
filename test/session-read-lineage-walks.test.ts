// Runtime: Node 24 and the same native SQLite owner used by Local.
import * as Effect from "effect/Effect";
import { expect, it } from "vitest";

import type { SqliteOwner } from "../src/index.js";
import { memoryHost } from "../src/testing.js";
import { prepareWoodstockSchema } from "../src/internal/woodstock/index.js";
import * as woodstock from "../src/internal/woodstock/session-store.js";
import { encodeWoodstockCursor } from "../src/internal/woodstock/cursor.js";
import {
  countMaterializedBytes,
  writeLargeSession,
} from "./fixtures/large-session.fixture.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

it("an explicit leaf on another branch remains readable after the shared ancestry is seeded", () => {
  const fixture = createNativeSqliteOwnerFixture();
  const { owner } = fixture;
  try {
    Effect.runSync(prepareWoodstockSchema(owner));
    const history = writeLargeSession(owner, {
      sessionId: "explicit-lineage",
      turns: 3,
    });
    const root = history.turns[0];
    const original = history.turns.at(-1);
    if (root === undefined || original === undefined)
      throw new Error("Missing branch fixture");
    woodstock.selectSessionBranch(owner, {
      sessionId: history.sessionId,
      entryId: root.terminalId,
    });
    const branch = writeLargeSession(owner, {
      sessionId: history.sessionId,
      turns: 1,
    });
    const ids = (turns: typeof history.turns) =>
      turns.flatMap(({ commandId, assistantId, terminalId }) => [
        commandId,
        assistantId,
        terminalId,
      ]);
    woodstock.withSessionRead(owner, history.sessionId, () => {
      expect(
        woodstock
          .readSelectedPath(owner, {
            sessionId: history.sessionId,
          })
          .map(({ id }) => id),
      ).toEqual([...ids([root]), ...ids(branch.turns)]);
      expect(
        woodstock
          .readSelectedPath(owner, {
            sessionId: history.sessionId,
            leafId: original.terminalId,
          })
          .map(({ id }) => id),
      ).toEqual(ids(history.turns));
      expect(
        woodstock
          .readSelectedPath(owner, {
            sessionId: history.sessionId,
            leafId: root.terminalId,
          })
          .map(({ id }) => id),
      ).toEqual(ids([root]));
    });
  } finally {
    fixture.close();
  }
});

it("a fixed public page and compacted context do not materialize the Session's growing ancestry", async () => {
  const bytes: number[] = [];
  const historicalBytes: number[] = [];
  for (const turns of [300, 3_000]) {
    const fixture = createNativeSqliteOwnerFixture();
    const measured = countMaterializedBytes(fixture.owner);
    try {
      Effect.runSync(prepareWoodstockSchema(measured.owner));
      const history = writeLargeSession(measured.owner, {
        sessionId: "bounded-shared-ancestry",
        turns,
        compactEvery: 100,
        keepTurns: 4,
        textBytes: 64,
      });
      const host = await memoryHost({ owner: measured.owner });
      try {
        measured.take();
        const page = await host.agent.session(history.sessionId).read({
          limit: 20,
        });
        bytes.push(measured.take());
        expect(page.messages).toHaveLength(20);
        expect(page.messages.at(-1)?.id).toBe(
          history.turns.at(-1)?.assistantId,
        );
        expect(page.context?.tokens).toBeGreaterThan(0);
        const point = history.turns[99];
        const root = history.turns[0];
        if (point === undefined || root === undefined)
          throw new Error("Missing historical turns");
        const anchor = woodstock.readSessionEntryById(
          measured.owner,
          point.terminalId,
        );
        if (anchor === undefined) throw new Error("Missing historical anchor");
        const before = encodeWoodstockCursor({
          generation: root.commandId,
          sequence: anchor.sequence,
        });
        if (before === undefined) throw new Error("Invalid historical anchor");
        measured.take();
        const historical = await host.agent.session(history.sessionId).read({
          limit: 20,
          before,
        });
        historicalBytes.push(measured.take());
        expect(historical.messages).toHaveLength(20);
        expect(historical.messages.at(-1)?.id).toBe(point.assistantId);
        expect(historical.context?.tokens).toBeGreaterThan(0);
      } finally {
        await host.close();
      }
    } finally {
      fixture.close();
    }
  }
  const [small, large] = bytes;
  if (small === undefined || large === undefined)
    throw new Error("Missing materialization measurements");
  // The page, kept context and identity window are the same size. Allow for
  // longer sequence encodings, not the extra 8,100 historical entry identities.
  expect(large).toBeLessThanOrEqual(small + 16_384);
  const [smallHistorical, largeHistorical] = historicalBytes;
  if (smallHistorical === undefined || largeHistorical === undefined)
    throw new Error("Missing historical materialization measurements");
  expect(largeHistorical).toBeLessThanOrEqual(smallHistorical + 16_384);
});

it("an older model preference stays readable below the shared compaction and page floor", () => {
  const fixture = createNativeSqliteOwnerFixture();
  const { owner } = fixture;
  try {
    Effect.runSync(prepareWoodstockSchema(owner));
    writeLargeSession(owner, { sessionId: "old-preference", turns: 1 });
    const preference = { providerId: "openai", modelId: "gpt-6-luna" };
    woodstock.setSessionModelPreference(owner, "old-preference", preference);
    writeLargeSession(owner, {
      sessionId: "old-preference",
      turns: 300,
      compactEvery: 100,
      keepTurns: 4,
    });
    woodstock.withSessionRead(owner, "old-preference", () => {
      const context = woodstock.readSessionContextPath(owner, {
        sessionId: "old-preference",
      });
      expect(context).toHaveLength(13);
      expect(
        woodstock.readSessionModelPreference(owner, "old-preference"),
      ).toEqual(preference);
    });
  } finally {
    fixture.close();
  }
});

it("a public Session read traverses lineage at most once across cold, paged, branched and forked history", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  let walks = 0;
  let reads = 0;
  const owner: SqliteOwner = {
    transactionSync: (operation) => fixture.owner.transactionSync(operation),
    sql: {
      exec: (statement, ...bindings) => {
        if (/\bWITH\s+RECURSIVE\b/iu.test(statement)) walks += 1;
        if (/^\s*(?:SELECT|WITH)\b/iu.test(statement)) reads += 1;
        return fixture.owner.sql.exec(statement, ...bindings);
      },
    },
  };
  try {
    Effect.runSync(prepareWoodstockSchema(owner));
    const history = writeLargeSession(owner, {
      sessionId: "lineage-read",
      turns: 120,
      compactEvery: 30,
      keepTurns: 4,
    });
    const host = await memoryHost({ owner });
    try {
      const session = host.agent.session(history.sessionId);
      const measuredRead = async (
        target: typeof session,
        phase: string,
        input: Parameters<typeof session.read>[0],
      ) => {
        walks = 0;
        reads = 0;
        const snapshot = await target.read(input);
        const cost = { walks, reads };
        expect
          .soft(cost.walks, `${phase} Session read: ${JSON.stringify(cost)}`)
          .toBeLessThanOrEqual(1);
        return snapshot;
      };
      let before: string | null = null;
      let latestIds: readonly string[] = [];
      for (const phase of ["cold", "unchanged"] as const) {
        const snapshot = await measuredRead(session, phase, { limit: 50 });
        expect(snapshot.messages).toHaveLength(50);
        expect(snapshot.messages.at(-1)?.id).toBe(
          history.turns.at(-1)?.assistantId,
        );
        expect(snapshot.hasMore).toBe(true);
        expect(snapshot.context?.tokens).toBeGreaterThan(0);
        before = snapshot.oldestCursor ?? null;
        latestIds = snapshot.messages.map(({ id }) => id);
      }
      if (before === null) throw new Error("Missing history cursor");
      const older = await measuredRead(session, "historical", {
        before,
        limit: 50,
      });
      expect(older.messages).toHaveLength(50);
      expect(older.messages.some(({ id }) => latestIds.includes(id))).toBe(
        false,
      );
      expect(older.context?.tokens).toBeGreaterThan(0);

      const point = history.turns[39];
      if (point === undefined) throw new Error("Missing branch point");
      await session.command({ type: "branch", entryId: point.terminalId });
      const branch = await measuredRead(session, "branched", { limit: 20 });
      expect(branch.messages).toHaveLength(20);
      expect(branch.messages.at(-1)?.id).toBe(point.assistantId);
      expect(branch.context?.tokens).toBeGreaterThan(0);

      const forkPoint = history.turns[19];
      if (forkPoint === undefined) throw new Error("Missing fork point");
      const fork = await session.command({
        type: "fork",
        entryId: forkPoint.terminalId,
      });
      const forked = await measuredRead(
        host.agent.session(fork.sessionId),
        "forked",
        { limit: 20 },
      );
      expect(forked.messages).toHaveLength(20);
      expect(JSON.stringify(forked.messages.at(-1))).toContain("answer 19 ");
      expect(forked.context?.tokens).toBeGreaterThan(0);
    } finally {
      await host.close();
    }
  } finally {
    fixture.close();
  }
});
