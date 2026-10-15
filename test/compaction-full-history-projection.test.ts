import { idleActivity as idleFacts } from "./fixtures/client.fixture.js";
import * as Effect from "effect/Effect";
import { expect, it } from "vitest";

import { prepareWoodstockSchema } from "../src/internal/woodstock/index.js";
import {
  readClientReplayPage,
  readClientSnapshot,
} from "../src/internal/woodstock/client-projection.js";
import { sessionIds } from "../src/internal/woodstock/identities.js";
import * as woodstock from "../src/internal/woodstock/session-store.js";
import {
  promptCommand,
  type StoredSessionEntry,
} from "../src/internal/woodstock/session-entry.js";
import type { SqliteOwner } from "../src/internal/contracts.js";
import type { ClientReadOptions } from "../src/internal/client-boundary.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

const digest = "a".repeat(64);
const contract = { core: digest, extensions: [] } as const;
const idleActivity = {
  facts: idleFacts,
  progress: null,
};

const usage = {
  input: 1,
  output: 1,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 2,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
};

function commandEntry(
  content: string,
): Extract<StoredSessionEntry, { kind: "command" }> {
  return {
    kind: "command",
    workId: sessionIds.work.generate(),
    requestId: `request-${content}`,
    receiptId: sessionIds.receipt.generate(),
    message: { role: "user", content, timestamp: 1 },
    contract,
  };
}

function appendCommand(owner: SqliteOwner, sessionId: string, content: string) {
  const row = woodstock.appendSessionEntry(owner, {
    sessionId,
    id: sessionIds.fact.generate(),
    entry: commandEntry(content),
  });
  const entry = promptCommand(row.entry);
  if (entry === undefined) throw new Error("expected a prompt command");
  return { ...row, entry };
}

/** Build read options without an explicit `undefined` `before` property. */
function readOptions(
  limit: number,
  before: string | null | undefined,
): ClientReadOptions {
  return before === null || before === undefined
    ? { limit }
    : { limit, before };
}

function appendCompaction(
  owner: SqliteOwner,
  sessionId: string,
  expectedLeafId: string,
  firstKeptEntryId: string,
  summary: string,
) {
  return woodstock.appendSessionEntry(owner, {
    sessionId,
    id: sessionIds.fact.generate(),
    entry: {
      kind: "compaction",
      workId: sessionIds.work.generate(),
      trigger: "threshold",
      firstKeptEntryId,
      summary,
      tokensBefore: 100,
      tokensAfter: 40,
      usage,
      contextRevision: expectedLeafId,
    },
  });
}

/** The token and trigger facts every appended compaction above records. */
const markerFacts = {
  tokensBefore: 100,
  tokensAfter: 40,
  trigger: "threshold",
} as const;

it("projects full history with one compaction marker while the model keeps its bounded context", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const sessionId = "full-history-one-compaction";
    const m1 = appendCommand(fixture.owner, sessionId, "m1");
    const m2 = appendCommand(fixture.owner, sessionId, "m2");
    const m3 = appendCommand(fixture.owner, sessionId, "m3");
    const compaction = appendCompaction(
      fixture.owner,
      sessionId,
      m3.id,
      m3.id,
      "summary of m1 and m2",
    );
    const m4 = appendCommand(fixture.owner, sessionId, "m4");

    const snapshot = readClientSnapshot(
      fixture.owner,
      sessionId,
      { limit: 200 },
      idleActivity,
    );

    // The client sees every durable message, not just the post-compaction tail.
    expect(snapshot.messages.map(({ value }) => value)).toEqual([
      m1.entry.message,
      m2.entry.message,
      m3.entry.message,
      m4.entry.message,
    ]);
    // One minimal marker, positioned to precede the entry the compaction kept.
    expect(snapshot.compactions).toEqual([
      {
        ...markerFacts,
        id: compaction.id,
        sequence: m3.sequence,
        at: compaction.timestamp,
      },
    ]);
    expect(snapshot.hasMore).toBe(false);
    expect(snapshot.oldestCursor).toBeNull();

    // The model's context is unaffected: it still resumes at m3, never m1/m2.
    const path = woodstock.readSelectedPath(fixture.owner, { sessionId });
    const context = woodstock.buildSessionContext(path);
    expect(context.map((candidate) => candidate)).toMatchObject([
      { role: "user" },
      m3.entry.message,
      m4.entry.message,
    ]);
  } finally {
    fixture.close();
  }
});

it("projects full history across two compactions, each with its own marker", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const sessionId = "full-history-two-compactions";
    const m1 = appendCommand(fixture.owner, sessionId, "m1");
    const m2 = appendCommand(fixture.owner, sessionId, "m2");
    const m3 = appendCommand(fixture.owner, sessionId, "m3");
    const compaction1 = appendCompaction(
      fixture.owner,
      sessionId,
      m3.id,
      m3.id,
      "summary one",
    );
    const m4 = appendCommand(fixture.owner, sessionId, "m4");
    const m5 = appendCommand(fixture.owner, sessionId, "m5");
    const compaction2 = appendCompaction(
      fixture.owner,
      sessionId,
      m5.id,
      m5.id,
      "summary two",
    );
    const m6 = appendCommand(fixture.owner, sessionId, "m6");

    const snapshot = readClientSnapshot(
      fixture.owner,
      sessionId,
      { limit: 200 },
      idleActivity,
    );

    expect(snapshot.messages.map(({ value }) => value)).toEqual([
      m1.entry.message,
      m2.entry.message,
      m3.entry.message,
      m4.entry.message,
      m5.entry.message,
      m6.entry.message,
    ]);
    expect(snapshot.compactions).toEqual([
      {
        ...markerFacts,
        id: compaction1.id,
        sequence: m3.sequence,
        at: compaction1.timestamp,
      },
      {
        ...markerFacts,
        id: compaction2.id,
        sequence: m5.sequence,
        at: compaction2.timestamp,
      },
    ]);

    // Only the latest compaction bounds the model's context.
    const path = woodstock.readSelectedPath(fixture.owner, { sessionId });
    const context = woodstock.buildSessionContext(path);
    expect(context.map((candidate) => candidate)).toMatchObject([
      { role: "user" },
      m5.entry.message,
      m6.entry.message,
    ]);
  } finally {
    fixture.close();
  }
});

it("pages backward through full history, across a compaction, to the very first message", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const sessionId = "full-history-backward-paging";
    const m1 = appendCommand(fixture.owner, sessionId, "m1");
    const m2 = appendCommand(fixture.owner, sessionId, "m2");
    const m3 = appendCommand(fixture.owner, sessionId, "m3");
    const compaction1 = appendCompaction(
      fixture.owner,
      sessionId,
      m3.id,
      m3.id,
      "summary one",
    );
    const m4 = appendCommand(fixture.owner, sessionId, "m4");
    const m5 = appendCommand(fixture.owner, sessionId, "m5");
    const compaction2 = appendCompaction(
      fixture.owner,
      sessionId,
      m5.id,
      m5.id,
      "summary two",
    );
    const m6 = appendCommand(fixture.owner, sessionId, "m6");

    const page1 = readClientSnapshot(
      fixture.owner,
      sessionId,
      { limit: 2 },
      idleActivity,
    );
    expect(page1.messages.map(({ value }) => value)).toEqual([
      m5.entry.message,
      m6.entry.message,
    ]);
    expect(page1.hasMore).toBe(true);
    expect(page1.oldestCursor).not.toBeNull();
    expect(page1.compactions).toEqual([
      {
        ...markerFacts,
        id: compaction2.id,
        sequence: m5.sequence,
        at: compaction2.timestamp,
      },
    ]);

    const page2 = readClientSnapshot(
      fixture.owner,
      sessionId,
      readOptions(2, page1.oldestCursor),
      idleActivity,
    );
    expect(page2.messages.map(({ value }) => value)).toEqual([
      m3.entry.message,
      m4.entry.message,
    ]);
    expect(page2.hasMore).toBe(true);
    expect(page2.oldestCursor).not.toBeNull();
    expect(page2.compactions).toEqual([
      {
        ...markerFacts,
        id: compaction1.id,
        sequence: m3.sequence,
        at: compaction1.timestamp,
      },
    ]);

    const page3 = readClientSnapshot(
      fixture.owner,
      sessionId,
      readOptions(2, page2.oldestCursor),
      idleActivity,
    );
    expect(page3.messages.map(({ value }) => value)).toEqual([
      m1.entry.message,
      m2.entry.message,
    ]);
    // Reaching the session's very first message ends backward paging.
    expect(page3.hasMore).toBe(false);
    expect(page3.oldestCursor).toBeNull();
    expect(page3.compactions).toEqual([]);

    // Concatenated oldest-to-newest, the three pages reconstruct full history.
    expect(
      [...page3.messages, ...page2.messages, ...page1.messages].map(
        ({ value }) => value,
      ),
    ).toEqual([
      m1.entry.message,
      m2.entry.message,
      m3.entry.message,
      m4.entry.message,
      m5.entry.message,
      m6.entry.message,
    ]);
  } finally {
    fixture.close();
  }
});

it("keeps a stable generation across compaction so a pre-compaction replay cursor never resets", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const sessionId = "full-history-stable-generation";
    const m1 = appendCommand(fixture.owner, sessionId, "m1");
    const before = readClientSnapshot(
      fixture.owner,
      sessionId,
      { limit: 200 },
      idleActivity,
    );
    appendCompaction(fixture.owner, sessionId, m1.id, m1.id, "summary");
    appendCommand(fixture.owner, sessionId, "m2");

    const replay = readClientReplayPage(
      fixture.owner,
      sessionId,
      before.cursor ?? undefined,
      "replay",
      idleActivity,
    );
    expect(replay.kind).toBe("page");
  } finally {
    fixture.close();
  }
});
