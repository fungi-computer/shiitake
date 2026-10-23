// Runtime for every law here: Node 24, `node:sqlite` (Woodstock's Local host).
import * as Effect from "effect/Effect";
import { describe, expect, it } from "vitest";

import type { SessionSnapshot } from "../src/client.js";
import type { SqliteOwner } from "../src/internal/contracts.js";
import {
  readClientReplayPage,
  readClientSnapshot,
} from "../src/internal/woodstock/client-projection.js";
import { sessionIds } from "../src/internal/woodstock/identities.js";
import { prepareWoodstockSchema } from "../src/internal/woodstock/index.js";
import {
  promptCommand,
  type StoredSessionEntry,
} from "../src/internal/woodstock/session-entry.js";
import * as woodstock from "../src/internal/woodstock/session-store.js";
import { idleActivity as idleFacts } from "./fixtures/client.fixture.js";
import {
  countMaterializedBytes,
  writeLargeSession,
  type LargeSession,
} from "./fixtures/large-session.fixture.js";
import { compactionEntryFixture } from "./fixtures/compaction-entry.fixture.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

const activity = { facts: idleFacts, toolOutput: [], progress: null };
const contract = { core: "a".repeat(64), extensions: [] } as const;
const usage = {
  input: 1,
  output: 1,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 2,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
};

function withStore<A>(run: (owner: SqliteOwner) => A): A {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    return run(fixture.owner);
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

function command(
  workId: string,
  content: string,
  targetWorkId?: string,
): StoredSessionEntry {
  const entry = {
    kind: "command" as const,
    workId,
    requestId: `request-${workId}`,
    receiptId: sessionIds.receipt.generate(),
    message: { role: "user" as const, content, timestamp: 1 },
    contract,
  };
  return targetWorkId === undefined ? entry : { ...entry, targetWorkId };
}

function compaction(
  workId: string,
  firstKeptEntryId: string,
  contextRevision: string,
  resumesRun = false,
): StoredSessionEntry {
  return compactionEntryFixture({
    workId,
    head: firstKeptEntryId,
    summary: `summary kept from ${firstKeptEntryId}`,
    resumesRun,
    usage,
    contextRevision,
  });
}

/** Every message id the lineage holds, oldest first: each prompt and answer. */
function expectedMessageIds(session: LargeSession): readonly string[] {
  return session.turns.flatMap(({ commandId, assistantId }) => [
    commandId,
    assistantId,
  ]);
}

/** Every page a reader saw, newest first, and their messages oldest first. */
type PagedHistory = Readonly<{
  pages: readonly SessionSnapshot[];
  messageIds: readonly string[];
}>;

/** Page newest-first with `before` until the saved history is exhausted. */
function pageBack(
  owner: SqliteOwner,
  sessionId: string,
  limit: number,
): PagedHistory {
  const pages: SessionSnapshot[] = [];
  let before: string | undefined;
  for (;;) {
    const page = readClientSnapshot(
      owner,
      sessionId,
      before === undefined ? { limit } : { limit, before },
      activity,
    );
    pages.push(page);
    if (!page.hasMore) break;
    if (page.oldestCursor === null) throw new Error("hasMore without cursor");
    before = page.oldestCursor;
  }
  return {
    pages,
    messageIds: [...pages]
      .reverse()
      .flatMap(({ messages }) => messages.map(({ id }) => id)),
  };
}

/**
 * The pre-fix run context: the whole command lineage, then the work's later
 * rows on the current lineage, built from the retained full read.
 */
function fullLineageRunContext(
  owner: SqliteOwner,
  sessionId: string,
  commandRow: woodstock.StoredSessionRow,
) {
  const workId = commandRow.entry.workId;
  if (workId === null) throw new Error("command has no work");
  const path = [
    ...woodstock.readSelectedPath(owner, {
      sessionId,
      leafId: commandRow.id,
    }),
    ...woodstock
      .readSelectedPath(owner, { sessionId })
      .filter(
        ({ sequence, entry }) =>
          sequence > commandRow.sequence &&
          (entry.workId === workId ||
            promptCommand(entry)?.targetWorkId === workId),
      ),
  ];
  return { path, messages: woodstock.buildSessionContext(path, workId) };
}

describe("bounded Session reads keep the full saved timeline (Node, node:sqlite)", () => {
  it("pages back through every message, including pre-compaction history", () => {
    withStore((owner) => {
      const session = writeLargeSession(owner, {
        sessionId: "scroll-back",
        turns: 400,
        compactEvery: 100,
        keepTurns: 10,
      });
      const { pages, messageIds } = pageBack(owner, session.sessionId, 200);

      expect(messageIds).toHaveLength(800);
      expect(messageIds).toEqual(expectedMessageIds(session));
      expect(pages.length).toBeGreaterThan(4);
      // The oldest page reaches the very first prompt, long summarized away.
      expect(pages.at(-1)?.messages[0]?.id).toBe(session.turns[0]?.commandId);
      // Every compaction contributes exactly one marker at its kept prompt.
      const markers = [...pages]
        .reverse()
        .flatMap(({ compactions }) => compactions ?? []);
      expect(markers.map(({ id }) => id)).toEqual(
        session.compactions.map(({ id }) => id),
      );
      const path = woodstock.readSelectedPath(owner, {
        sessionId: session.sessionId,
      });
      for (const { id, keptTurn } of session.compactions)
        expect(markers.find((marker) => marker.id === id)?.sequence).toBe(
          path.find((row) => row.id === session.turns[keptTurn]?.commandId)
            ?.sequence,
        );
    });
  });

  it("pages with a small message limit and replays forward to the same history", () => {
    withStore((owner) => {
      const session = writeLargeSession(owner, {
        sessionId: "small-pages",
        turns: 130,
        compactEvery: 40,
        keepTurns: 5,
      });
      const expected = expectedMessageIds(session);
      expect(pageBack(owner, session.sessionId, 7).messageIds).toEqual(
        expected,
      );

      const forward: string[] = [];
      let after: string | undefined;
      for (;;) {
        const page = readClientReplayPage(
          owner,
          session.sessionId,
          after,
          "replay",
          activity,
        );
        if (page.kind !== "page") throw new Error("replay reset");
        for (const frame of page.frames)
          if (frame.kind === "update") {
            forward.push(...frame.messages.map(({ id }) => id));
            after = frame.cursor ?? after;
          }
        if (!page.hasMore) break;
      }
      expect(forward).toEqual(expected);
    });
  });

  it("refuses a cursor from another lineage instead of guessing", () => {
    withStore((owner) => {
      const session = writeLargeSession(owner, {
        sessionId: "cursor-lineage",
        turns: 150,
      });
      const latest = readClientSnapshot(
        owner,
        session.sessionId,
        { limit: 200 },
        activity,
      );
      if (latest.oldestCursor === null) throw new Error("expected more");
      // Select an older branch: the newest page's cursor is no longer on it.
      const branchPoint = session.turns[20];
      if (branchPoint === undefined) throw new Error("missing turn");
      woodstock.selectSessionBranch(owner, {
        sessionId: session.sessionId,
        entryId: branchPoint.terminalId,
      });
      expect(() =>
        readClientSnapshot(
          owner,
          session.sessionId,
          { limit: 200, before: latest.oldestCursor ?? "" },
          activity,
        ),
      ).toThrow("Session read cursor is unavailable");
      // The selected branch pages back to its own first prompt.
      expect(pageBack(owner, session.sessionId, 200).messageIds).toEqual(
        expectedMessageIds(session).slice(0, 42),
      );
    });
  });
});

describe("bounded run context is the same model input (Node, node:sqlite)", () => {
  it("prepares the same messages, leaf and preference as the full lineage", () => {
    withStore((owner) => {
      const sessionId = "run-context";
      writeLargeSession(owner, { sessionId, turns: 30 });
      woodstock.setSessionModelPreference(owner, sessionId, {
        providerId: "openai",
        modelId: "gpt-6-luna",
      });
      const session = writeLargeSession(owner, {
        sessionId,
        turns: 300,
        compactEvery: 100,
        keepTurns: 20,
      });
      const last = session.turns.at(-1);
      if (last === undefined) throw new Error("missing turn");
      const commandRow = woodstock.readSessionEntryById(owner, last.commandId);
      if (commandRow === undefined) throw new Error("missing command");
      const before = fullLineageRunContext(owner, sessionId, commandRow);

      const prepared = woodstock.prepareSessionRun(owner, {
        sessionId,
        commandFactId: last.commandId,
      });

      expect(prepared.messages).toEqual(before.messages);
      expect(prepared.path.at(-1)?.id).toBe(before.path.at(-1)?.id);
      expect(prepared.modelPreference).toEqual({
        providerId: "openai",
        modelId: "gpt-6-luna",
      });
      // Only the kept window is decoded, not the 990-row lineage.
      expect(before.path.length).toBeGreaterThan(900);
      expect(prepared.path.length).toBeLessThan(100);
    });
  });

  it("keeps a command withdrawn by a cancellation older than the boundary", () => {
    withStore((owner) => {
      const sessionId = "withdrawn-across-boundary";
      writeLargeSession(owner, { sessionId, turns: 5 });
      const withdrawn = sessionIds.work.generate();
      append(owner, sessionId, {
        kind: "cancellation",
        workId: withdrawn,
        requestId: `cancel-${withdrawn}`,
        receiptId: sessionIds.receipt.generate(),
        targetWorkId: withdrawn,
      });
      const kept = append(
        owner,
        sessionId,
        command(sessionIds.work.generate(), "kept"),
      );
      append(
        owner,
        sessionId,
        compaction(kept.entry.workId ?? "", kept.id, kept.id),
      );
      append(owner, sessionId, command(withdrawn, "withdrawn prompt"));
      const next = append(
        owner,
        sessionId,
        command(sessionIds.work.generate(), "next"),
      );
      const before = fullLineageRunContext(owner, sessionId, next);

      const prepared = woodstock.prepareSessionRun(owner, {
        sessionId,
        commandFactId: next.id,
      });

      expect(prepared.messages).toEqual(before.messages);
      expect(JSON.stringify(prepared.messages)).not.toContain(
        "withdrawn prompt",
      );
      expect(JSON.stringify(prepared.messages)).toContain("kept");
    });
  });

  it("carries an earlier recovery compaction of the active work past a later boundary", () => {
    withStore((owner) => {
      const sessionId = "one-shot-recovery";
      writeLargeSession(owner, { sessionId, turns: 3 });
      const work = sessionIds.work.generate();
      const running = append(owner, sessionId, command(work, "long run"));
      const recovered = append(
        owner,
        sessionId,
        compaction(work, running.id, running.id, true),
      );
      // Steering commands targeting the work continue its context.
      const steer = append(
        owner,
        sessionId,
        command(sessionIds.work.generate(), "steer", work),
      );
      // A later compaction keeps only rows after the recovery compaction.
      const boundary = append(
        owner,
        sessionId,
        compaction(work, steer.id, steer.id),
      );
      append(
        owner,
        sessionId,
        command(sessionIds.work.generate(), "steer again", work),
      );

      const prepared = woodstock.prepareSessionRun(owner, {
        sessionId,
        commandFactId: running.id,
      });
      const before = fullLineageRunContext(owner, sessionId, running);

      expect(prepared.messages).toEqual(before.messages);
      const ids = prepared.path.map(({ id }) => id);
      expect(ids).toContain(recovered.id);
      expect(ids).toContain(boundary.id);
      // The boundary is real: the running prompt itself is outside the window.
      expect(ids).not.toContain(running.id);
      expect(prepared.path.at(-1)?.id).toBe(before.path.at(-1)?.id);
    });
  });

  it("reads compaction's current context window, not the lineage", () => {
    withStore((owner) => {
      const session = writeLargeSession(owner, {
        sessionId: "compaction-window",
        turns: 250,
        compactEvery: 100,
        keepTurns: 15,
      });
      const last = session.turns.at(-1);
      if (last === undefined) throw new Error("missing turn");
      const full = woodstock.readSelectedPath(owner, {
        sessionId: session.sessionId,
      });
      const window = woodstock.readSessionContextPath(owner, {
        sessionId: session.sessionId,
        workId: last.workId,
      });

      expect(woodstock.buildSessionContext(window, last.workId)).toEqual(
        woodstock.buildSessionContext(full, last.workId),
      );
      expect(window.at(-1)?.id).toBe(full.at(-1)?.id);
      // The window starts at the latest compaction's kept prompt.
      const latest = session.compactions.at(-1);
      expect(window[0]?.id).toBe(
        session.turns[latest?.keptTurn ?? -1]?.commandId,
      );
      // 65 of 250 turns since the boundary, plus the boundary itself.
      expect(window).toHaveLength(65 * 3 + 1);
      expect(full.length).toBeGreaterThan(750);
    });
  });
});

describe("fork and branch stay exact (Node, node:sqlite)", () => {
  it("forks a lineage larger than one copy window with compactions remapped", () => {
    withStore((owner) => {
      const session = writeLargeSession(owner, {
        sessionId: "fork-source",
        turns: 180,
        textBytes: 16 * 1_024,
        compactEvery: 150,
        keepTurns: 30,
      });
      const at = session.turns[169];
      if (at === undefined) throw new Error("missing turn");
      const source = woodstock.readSelectedPath(owner, {
        sessionId: session.sessionId,
        leafId: at.terminalId,
      });
      const lineage = woodstock.readSessionLineage(owner, {
        sessionId: session.sessionId,
        leafId: at.terminalId,
      });
      if (lineage === undefined) throw new Error("missing fork lineage");
      const firstWindow = woodstock.readLineageWindow(owner, lineage, {
        from: "oldest",
        rows: 4_000,
      });
      const firstWindowIds = new Set(firstWindow.rows.map(({ id }) => id));
      // Prove the smaller fixture still crosses the actual copy boundary,
      // with a compaction in a later window referring back to the first.
      expect(firstWindow.more).toBe(true);
      expect(
        source.some(
          ({ id, entry }) =>
            entry.kind === "compaction" &&
            !firstWindowIds.has(id) &&
            firstWindowIds.has(entry.head),
        ),
      ).toBe(true);
      const forkedId = woodstock.forkSessionAt(owner, {
        sessionId: session.sessionId,
        entryId: at.terminalId,
      });
      const forked = woodstock.readSelectedPath(owner, { sessionId: forkedId });

      expect(source.length).toBeLessThan(4_000);
      expect(forked).toHaveLength(source.length);
      const idMap = new Map(
        source.map((row, index) => [row.id, forked[index]?.id]),
      );
      expect(forked.map(({ sequence }) => sequence)).toEqual(
        source.map((_, index) => index + 1),
      );
      expect(forked.map(({ entry }) => entry)).toEqual(
        source.map(({ entry }) =>
          entry.kind === "compaction"
            ? { ...entry, head: idMap.get(entry.head) }
            : entry,
        ),
      );
      // The fork's model context resumes at its own remapped boundary.
      expect(woodstock.buildSessionContext(forked)).toEqual(
        woodstock.buildSessionContext(source),
      );
    });
  });
});

describe("bounded reads materialize a constant amount (Node, node:sqlite)", () => {
  function materialized(turns: number) {
    const fixture = createNativeSqliteOwnerFixture();
    try {
      Effect.runSync(prepareWoodstockSchema(fixture.owner));
      const session = writeLargeSession(fixture.owner, {
        sessionId: "bounded",
        turns,
        compactEvery: 500,
        keepTurns: 50,
      });
      woodstock.writeSessionDocument(
        fixture.owner,
        session.sessionId,
        "extension",
        "notes",
        '"latest"',
      );
      const counted = countMaterializedBytes(fixture.owner);
      const owner = counted.owner;
      const sessionId = session.sessionId;
      const last = session.turns.at(-1);
      if (last === undefined) throw new Error("missing turn");
      counted.take();
      const measure = (read: () => void) => {
        read();
        return counted.take();
      };
      const latest = readClientSnapshot(
        owner,
        sessionId,
        { limit: 200 },
        activity,
      );
      counted.take();
      return {
        entries: session.entries,
        full: measure(() => woodstock.readSelectedPath(owner, { sessionId })),
        snapshot: measure(() =>
          readClientSnapshot(owner, sessionId, { limit: 200 }, activity),
        ),
        olderPage: measure(() =>
          readClientSnapshot(
            owner,
            sessionId,
            { limit: 200, before: latest.oldestCursor ?? "" },
            activity,
          ),
        ),
        replay: measure(() =>
          readClientReplayPage(owner, sessionId, undefined, "replay", activity),
        ),
        model: measure(() =>
          woodstock.readSessionModelPreference(owner, sessionId),
        ),
        document: measure(() =>
          woodstock.readSessionDocument(owner, sessionId, "extension", "notes"),
        ),
        run: measure(() =>
          woodstock.prepareSessionRun(owner, {
            sessionId,
            commandFactId: last.commandId,
          }),
        ),
      };
    } finally {
      fixture.close();
    }
  }

  it("reads the same bytes from a 2k-entry and a 20k-entry Session", () => {
    const small = materialized(667);
    const large = materialized(6_667);
    expect(large.entries).toBeGreaterThan(20_000);
    // The full read is the pre-fix cost every reader paid: it grows tenfold.
    expect(large.full).toBeGreaterThan(9 * small.full);
    for (const reader of [
      "snapshot",
      "olderPage",
      "replay",
      "model",
      "document",
      "run",
    ] as const) {
      expect(large[reader], reader).toBeLessThan(small[reader] * 1.05 + 1_024);
      expect(large[reader], reader).toBeLessThan(large.full / 20);
    }
    expect(large.snapshot).toBeLessThan(512 * 1_024);
  }, 120_000);
});
