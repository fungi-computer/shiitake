import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import { expect, it } from "vitest";

import { sessionIds } from "../src/internal/woodstock/identities.js";
import { prepareWoodstockSchema } from "../src/internal/woodstock/index.js";
import { decodeStoredSessionEntry } from "../src/internal/woodstock/session-entry.js";
import * as woodstock from "../src/internal/woodstock/session-store.js";
import { compactionEntryFixture } from "./fixtures/compaction-entry.fixture.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

const usage = {
  input: 1,
  output: 1,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 2,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
};

it("stores compaction model/data/head separately and derives context from model only", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const sessionId = "entry-shape";
    const kept = woodstock.appendSessionEntry(fixture.owner, {
      sessionId,
      id: sessionIds.fact.generate(),
      entry: {
        kind: "command",
        workId: sessionIds.work.generate(),
        requestId: "request-kept",
        receiptId: sessionIds.receipt.generate(),
        message: { role: "user", content: "kept", timestamp: 1 },
        contract: { core: "a".repeat(64), extensions: [] },
      },
    });
    const row = woodstock.appendCompactionTransitionOwned(fixture.owner, {
      sessionId,
      expectedLeafId: kept.id,
      workId: sessionIds.work.generate(),
      trigger: "overflow",
      resumesRun: true,
      tokensAfter: 40,
      head: kept.id,
      summary: "the summary",
      tokensBefore: 100,
      usage,
      contextRevision: kept.id,
    });
    if (row.entry.kind !== "compaction") throw new Error("Expected compaction");
    expect(Object.keys(row.entry).sort()).toEqual([
      "data",
      "head",
      "kind",
      "model",
      "workId",
    ]);
    expect(row.entry).toMatchObject({
      head: kept.id,
      model: [{ role: "user" }],
      data: {
        reason: "overflow",
        resumesRun: true,
        tokensBefore: 100,
        tokensAfter: 40,
      },
    });
    const context = woodstock.buildSessionContext(
      woodstock.readSelectedPath(fixture.owner, { sessionId }),
    );
    expect(context).toEqual([
      ...row.entry.model,
      { role: "user", content: "kept", timestamp: 1 },
    ]);
    expect(context[0]).not.toHaveProperty("data");
    expect(context[0]).not.toHaveProperty("meta");
  } finally {
    fixture.close();
  }
});

it.each([0, 1, 2, 4])(
  "version-refuses obsolete or unknown store %s without rewriting its entries",
  (version) => {
    const fixture = createNativeSqliteOwnerFixture();
    try {
      Effect.runSync(prepareWoodstockSchema(fixture.owner));
      const sessionId = "refused-store";
      woodstock.appendSessionEntry(fixture.owner, {
        sessionId,
        id: sessionIds.fact.generate(),
        entry: {
          kind: "operator",
          workId: null,
          requestId: "retained",
          author: "operator",
          message: { role: "user", content: "unchanged", timestamp: 1 },
        },
      });
      const before = fixture.owner.sql
        .exec("SELECT id, data_json FROM session_entries ORDER BY id")
        .toArray();
      fixture.owner.sql.exec(
        "UPDATE woodstock_schema SET version = ?",
        version,
      );
      expect(
        Effect.runSync(Effect.result(prepareWoodstockSchema(fixture.owner))),
      ).toMatchObject({
        _tag: "Failure",
        failure: { boundary: "store", cause: "incompatible" },
      });
      expect(
        fixture.owner.sql.exec("SELECT version FROM woodstock_schema").one()
          .version,
      ).toBe(version);
      expect(
        fixture.owner.sql
          .exec("SELECT id, data_json FROM session_entries ORDER BY id")
          .toArray(),
      ).toEqual(before);
    } finally {
      fixture.close();
    }
  },
);

it("tells the person how to reset a refused v2 store without migrating it", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    fixture.owner.sql.exec(
      "CREATE TABLE woodstock_schema (version INTEGER NOT NULL, protocol TEXT NOT NULL)",
    );
    fixture.owner.sql.exec(
      "INSERT INTO woodstock_schema (version, protocol) VALUES (?, ?)",
      2,
      "shiitake.extension/v0",
    );
    const writes = fixture.ownerWrites();
    const result = Effect.runSync(
      Effect.result(prepareWoodstockSchema(fixture.owner)),
    );
    expect(result).toMatchObject({
      _tag: "Failure",
      failure: {
        _tag: "ShiitakeError",
        boundary: "store",
        cause: "incompatible",
      },
    });
    if (result._tag !== "Failure") throw new Error("Expected store refusal");
    expect(result.failure.message).toContain("To reset, stop the owning host");
    expect(result.failure.message).toContain(
      "remove its database file and the file's .owner.sqlite sidecar",
    );
    expect(result.failure.message).toContain(
      "a Hub Agent is erased only with its Team",
    );
    expect(result.failure.message).toContain(
      "Team erasure is destructive and separately authorized",
    );
    expect(result.failure.message).toContain(
      "never a routine single-Agent repair",
    );
    expect(result.failure.message).not.toContain("POST /operator/");
    expect(result.failure.message).not.toContain(
      "DurableObjectStorage.deleteAll()",
    );
    expect(result.failure.message).toContain(
      "Reset discards that store's data",
    );
    expect(
      fixture.owner.sql.exec("SELECT version FROM woodstock_schema").one()
        .version,
    ).toBe(2);
    expect(fixture.ownerWrites()).toBe(writes);
  } finally {
    fixture.close();
  }
});

it("accepts the retained compaction entry, not the superseded summary/head shape", () => {
  const entry = compactionEntryFixture({
    workId: "work",
    head: "kept",
    summary: "summary",
    usage,
    contextRevision: "kept",
  });
  expect(
    Result.isSuccess(decodeStoredSessionEntry(JSON.stringify(entry))),
  ).toBe(true);
  expect(
    Result.isFailure(
      decodeStoredSessionEntry(
        JSON.stringify({
          kind: "compaction",
          workId: "work",
          firstKeptEntryId: "kept",
          summary: "summary",
          tokensBefore: 100,
          usage,
          contextRevision: "kept",
        }),
      ),
    ),
  ).toBe(true);
});

it("allows known usage on failed notices but refuses it when nothing was compacted", () => {
  const notice = {
    kind: "compaction_notice",
    workId: "work",
    trigger: "threshold",
  };
  expect(
    Result.isFailure(
      decodeStoredSessionEntry(
        JSON.stringify({ ...notice, result: "nothing_to_compact", usage }),
      ),
    ),
  ).toBe(true);
  expect(
    decodeStoredSessionEntry(
      JSON.stringify({
        ...notice,
        result: "failed",
        diagnostic: "The handoff failed",
        usage,
      }),
    ),
  ).toMatchObject({
    _tag: "Success",
    success: { ...notice, result: "failed", usage },
  });
  for (const result of ["stale", "insufficient"]) {
    expect(
      Result.isSuccess(
        decodeStoredSessionEntry(JSON.stringify({ ...notice, result, usage })),
      ),
    ).toBe(true);
    expect(
      Result.isFailure(
        decodeStoredSessionEntry(
          JSON.stringify({ ...notice, result, diagnostic: "x" }),
        ),
      ),
    ).toBe(true);
  }
});

it("retains a failed notice's known usage and diagnostic after SQLite reopen", () => {
  const directory = mkdtempSync(join(tmpdir(), "shiitake-failed-usage-"));
  const databasePath = join(directory, "session.sqlite");
  let fixture = createNativeSqliteOwnerFixture(databasePath);
  const notice = {
    kind: "compaction_notice",
    workId: "work",
    trigger: "threshold",
    result: "failed",
    diagnostic: "The handoff failed",
    usage,
  } as const;
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    woodstock.appendCompactionNoticeOwned(fixture.owner, {
      sessionId: "failed-summary-usage",
      ...notice,
    });
    fixture.close();
    fixture = createNativeSqliteOwnerFixture(databasePath);
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    expect(
      woodstock.readSelectedPath(fixture.owner, {
        sessionId: "failed-summary-usage",
      }),
    ).toMatchObject([{ entry: notice }]);
  } finally {
    fixture.close();
    rmSync(directory, { recursive: true, force: true });
  }
});
