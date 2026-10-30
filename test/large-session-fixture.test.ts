import * as Effect from "effect/Effect";
import { expect, it } from "vitest";

import type { SqliteOwner } from "../src/index.js";
import { prepareWoodstockSchema } from "../src/internal/woodstock/index.js";
import {
  readSelectedPath,
  selectSessionBranch,
} from "../src/internal/woodstock/session-store.js";
import { writeLargeSession } from "./fixtures/large-session.fixture.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

it("seeds larger histories without rereading or publishing the append position for every row", () => {
  const costs = [10, 100].map((turns) => {
    const fixture = createNativeSqliteOwnerFixture();
    let reads = 0;
    let updates = 0;
    const owner: SqliteOwner = {
      transactionSync: (operation) => fixture.owner.transactionSync(operation),
      sql: {
        exec: (statement, ...bindings) => {
          if (/^\s*(?:SELECT|WITH)\b/iu.test(statement)) reads++;
          if (/^\s*UPDATE sessions SET leaf_id\b/iu.test(statement)) updates++;
          return fixture.owner.sql.exec(statement, ...bindings);
        },
      },
    };
    try {
      Effect.runSync(prepareWoodstockSchema(owner));
      const history = writeLargeSession(owner, { sessionId: "seed", turns });
      const cost = { reads, updates };
      const rows = readSelectedPath(owner, { sessionId: history.sessionId });
      expect(rows.map(({ id }) => id)).toEqual(
        history.turns.flatMap(({ commandId, assistantId, terminalId }) => [
          commandId,
          assistantId,
          terminalId,
        ]),
      );
      expect(rows.map(({ sequence }) => sequence)).toEqual(
        Array.from({ length: turns * 3 }, (_, index) => index + 1),
      );
      return cost;
    } finally {
      fixture.close();
    }
  });
  expect(costs[1]).toEqual(costs[0]);
});

it("resumes at the selected leaf but allocates sequences beyond every existing branch", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const original = writeLargeSession(fixture.owner, {
      sessionId: "branch",
      turns: 3,
    });
    const root = original.turns[0];
    if (root === undefined) throw new Error("Missing branch root");
    selectSessionBranch(fixture.owner, {
      sessionId: original.sessionId,
      entryId: root.terminalId,
    });
    const branch = writeLargeSession(fixture.owner, {
      sessionId: original.sessionId,
      turns: 2,
    });
    const rows = readSelectedPath(fixture.owner, {
      sessionId: original.sessionId,
    });
    expect(rows.map(({ id }) => id)).toEqual(
      [root, ...branch.turns].flatMap(
        ({ commandId, assistantId, terminalId }) => [
          commandId,
          assistantId,
          terminalId,
        ],
      ),
    );
    expect(rows.map(({ sequence }) => sequence)).toEqual([
      1, 2, 3, 10, 11, 12, 13, 14, 15,
    ]);
  } finally {
    fixture.close();
  }
});
