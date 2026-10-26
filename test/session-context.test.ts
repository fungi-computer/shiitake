import { expect, it } from "vitest";
import * as Schema from "effect/Schema";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { fauxModel } from "@fungi.computer/mule/testing";
import type {
  ModelPreference,
  ModelSource,
  SessionFrame,
  SqliteOwner,
} from "../src/index.js";
import { memoryHost } from "../src/testing.js";
import {
  defineExtension,
  ExtensionId,
  type CompactionHandoffResultV0,
} from "../src/extensions.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

it("uses the changed Agent default's resolved window for the footer and compaction", async () => {
  const large = fauxModel([fauxAssistantMessage("x".repeat(132_000))], {
    models: [{ id: "large", contextWindow: 200_000 }],
    tokenSize: { min: 1_000, max: 1_000 },
  });
  const small = fauxModel(
    [fauxAssistantMessage("Reply under the smaller model.")],
    { models: [{ id: "small", contextWindow: 30_000 }] },
  );
  let selected = large;
  const acquired: string[] = [];
  let compactionAttempts = 0;
  const handoffId = ExtensionId.make("context-window-witness");
  const handoff = defineExtension({
    id: handoffId,
    version: "1.0.0",
  }).define({
    compaction: () => ({
      consume: () => {
        compactionAttempts += 1;
        return Promise.resolve<CompactionHandoffResultV0>({
          type: "discarded_by_policy",
        });
      },
    }),
  });
  const host = await memoryHost({
    model: {
      acquire: () => {
        acquired.push(selected.model.id);
        return Promise.resolve({
          model: selected.model,
          retryable: () => false,
          stream: selected.stream,
          release: () => Promise.resolve(),
        });
      },
      describe: () =>
        Promise.resolve({ contextWindow: selected.model.contextWindow }),
    },
    compaction: {
      reserveTokens: 1_000,
      keepRecentTokens: 1_000,
      backgroundTokens: 0,
    },
    extensions: {
      roster: [handoff],
      grants: [{ id: handoffId, capabilities: ["compaction_handoff"] }],
    },
  });
  try {
    const session = host.agent.session("default-model-window");
    await session.send("Build context.", { requestId: "large-turn" });
    const first = await host.settle("default-model-window");
    expect(first.context?.window).toBe(200_000);
    expect(first.context?.tokens).toBeGreaterThan(30_000);
    expect(compactionAttempts).toBe(0);

    // Local's actual /model contract changes its Agent default, then clears
    // the Session override. Both describe and acquire select that new default.
    selected = small;
    await session.modelPreference.set(null);
    expect(await session.modelPreference.read()).toBeNull();
    const idle = await session.read();
    expect(acquired).toEqual(["large"]);
    await session.send("Continue.", { requestId: "small-turn" });
    const second = await host.settle("default-model-window");

    // Actual compaction must have used the smaller acquired model, not the
    // old large window. Policy discards the summary; this proves threshold
    // entry, not successful transcript compaction.
    expect(acquired).toEqual(["large", "small"]);
    expect(compactionAttempts).toBeGreaterThan(0);
    expect(idle.context?.window).toBe(30_000);
    expect(second.context?.window).toBe(30_000);
  } finally {
    await host.close();
  }
});

it("displays a failed assistant but excludes it from the next prompt's model context and footer", async () => {
  const failed = fauxAssistantMessage("partial reply", {
    stopReason: "error",
    errorMessage: "provider failed",
  });
  const seen: string[][] = [];
  const faux = fauxModel([
    failed,
    (context) => {
      seen.push(
        context.messages
          .filter((message) => message.role === "assistant")
          .flatMap((message) =>
            message.content
              .filter((part) => part.type === "text")
              .map((part) => part.text),
          ),
      );
      return fauxAssistantMessage("recovered");
    },
  ]);
  const host = await memoryHost({
    model: {
      acquire: () =>
        Promise.resolve({
          model: faux.model,
          stream: faux.stream,
          retryable: () => false,
          release: () => Promise.resolve(),
        }),
    },
  });
  try {
    const session = host.agent.session("failed-follow-up");
    await session.send("First", { requestId: "failure" });
    const first = await host.settle("failed-follow-up");
    expect(
      first.messages.some(
        (message) =>
          message.value.role === "assistant" &&
          message.value.content.some(
            (part) => part.type === "text" && part.text === "partial reply",
          ),
      ),
    ).toBe(true);
    await session.send("Try again", { requestId: "recovery" });
    const second = await host.settle("failed-follow-up");
    expect(seen).toEqual([[]]);
    expect(second.terminals.at(-1)?.outcome).toBe("completed");
  } finally {
    await host.close();
  }
});

const wide = { providerId: "faux", modelId: "wide" } as const;

it("does not pin the tip figure to a cursor read from scrollback before branching", async () => {
  const model = fauxModel(
    Array.from({ length: 4 }, () => fauxAssistantMessage("x".repeat(8_000))),
    {
      models: [{ id: "scrollback", contextWindow: 200_000 }],
      tokenSize: { min: 1_000, max: 1_000 },
    },
  );
  const host = await memoryHost({
    model: {
      acquire: () =>
        Promise.resolve({
          model: model.model,
          retryable: () => false,
          stream: model.stream,
          release: () => Promise.resolve(),
        }),
      describe: () => Promise.resolve({ contextWindow: 200_000 }),
    },
  });
  try {
    const session = host.agent.session("scrollback-branch");
    for (let turn = 0; turn < 4; turn += 1) {
      const requestId = `turn-${String(turn)}`;
      await session.send(`Turn ${String(turn)}`, { requestId });
      await host.settle("scrollback-branch");
    }
    const tip = await session.read({ limit: 1 });
    if (tip.oldestCursor === null || tip.oldestCursor === undefined)
      throw new Error("missing tip cursor");
    const scrollback = await session.read({
      limit: 1,
      before: tip.oldestCursor,
    });
    const branchPoint = scrollback.messages.at(-1);
    if (branchPoint === undefined) throw new Error("missing scrollback entry");
    expect(scrollback.context?.tokens).toBeGreaterThan(0);
    await session.command({ type: "branch", entryId: branchPoint.id });

    const branched = await session.read();
    expect(branched.context?.tokens).toBeLessThan(
      scrollback.context?.tokens ?? Number.POSITIVE_INFINITY,
    );
  } finally {
    await host.close();
  }
});

/** Faux replies; the window follows the saved preference, as a host's would. */
function describedSource(described: (ModelPreference | null)[]): ModelSource {
  const ordinary = fauxModel([fauxAssistantMessage("First reply.")], {
    models: [{ id: "ordinary", contextWindow: 200_000 }],
  });
  const roomy = fauxModel(
    [fauxAssistantMessage("A second, longer reply about the forage notes.")],
    { models: [{ id: "wide", contextWindow: 1_000_000 }] },
  );
  const select = (preference: ModelPreference | null) =>
    preference?.modelId === "wide" ? roomy : ordinary;
  return {
    acquire: ({ preference, signal }) => {
      signal.throwIfAborted();
      const faux = select(preference);
      return Promise.resolve({
        model: faux.model,
        retryable: () => false,
        stream: faux.stream,
        release: () => Promise.resolve(),
      });
    },
    describe: ({ preference, signal }) => {
      signal.throwIfAborted();
      described.push(preference);
      return Promise.resolve({
        contextWindow: select(preference).model.contextWindow,
      });
    },
  };
}

it("reports compaction's context use against the described window, after a turn and after /model", async () => {
  const described: (ModelPreference | null)[] = [];
  const host = await memoryHost({ model: describedSource(described) });
  const session = host.agent.session("context");
  try {
    // No context yet: nothing to report, and no host description is needed.
    expect((await session.read()).context).toBeUndefined();
    expect(described).toEqual([]);

    await session.send("Hello", { requestId: "first" });
    const first = (await host.settle("context")).context;
    expect(first?.window).toBe(200_000);
    expect(first?.tokens).toBeGreaterThan(0);

    // A saved preference is described at once, while the Session is idle.
    await session.modelPreference.set(wide);
    const idle = await session.read();
    expect(idle.activity.queue.state).toBe("idle");
    expect(idle.context).toEqual({ tokens: first?.tokens, window: 1_000_000 });
    expect(described.at(-1)).toEqual(wide);

    // The next turn's update carries the larger context it settles on.
    const watch = await session.watch();
    await session.send("Tell me more", { requestId: "second" });
    await host.settle("context");
    const updates: SessionFrame[] = [];
    for await (const frame of watch.changes) {
      updates.push(frame);
      if (frame.kind === "update" && frame.activity.queue.state === "idle")
        break;
    }
    await watch.close();
    const latest = updates
      .flatMap((frame) => (frame.kind === "update" ? [frame.context] : []))
      .at(-1);
    expect(latest?.window).toBe(1_000_000);
    expect(latest?.tokens).toBeGreaterThan(first?.tokens ?? 0);
    expect((await session.read()).context).toEqual(latest);
    expect(described.at(-1)).toEqual(wide);
  } finally {
    await host.close();
  }
});

it("does not let a held old-cursor describe replace a newer cache entry", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  let contextFigureReads = 0;
  const owner: SqliteOwner = {
    transactionSync: (operation) => fixture.owner.transactionSync(operation),
    sql: {
      exec: (statement, ...values) => {
        if (
          statement.includes("SELECT id FROM walk") &&
          statement.includes("state != 'streaming'") &&
          !(new Error().stack ?? "").includes("readRunContextRows")
        )
          contextFigureReads += 1;
        return fixture.owner.sql.exec(statement, ...values);
      },
    },
  };
  let releaseDescribe!: () => void;
  const heldDescribe = new Promise<void>((resolve) => {
    releaseDescribe = resolve;
  });
  let markDescribeEntered!: () => void;
  const describeEntered = new Promise<void>((resolve) => {
    markDescribeEntered = resolve;
  });
  let holdNextDescribe = false;
  const model = fauxModel([
    fauxAssistantMessage("first"),
    fauxAssistantMessage("second"),
  ]);
  const host = await memoryHost({
    owner,
    model: {
      acquire: () =>
        Promise.resolve({
          model: model.model,
          stream: model.stream,
          retryable: () => false,
          release: () => Promise.resolve(),
        }),
      describe: async () => {
        if (holdNextDescribe) {
          holdNextDescribe = false;
          markDescribeEntered();
          await heldDescribe;
        }
        return { contextWindow: model.model.contextWindow };
      },
    },
  });
  const session = host.agent.session("late-context-description");
  try {
    await session.send("first", { requestId: "first" });
    holdNextDescribe = true;
    const firstSettle = host.settle("late-context-description");
    await describeEntered;
    const readsAtFirstCursor = contextFigureReads;

    await session.send("second", { requestId: "second" });
    await host.settle("late-context-description");
    const readsAtNewCursor = contextFigureReads;
    expect(readsAtNewCursor).toBe(readsAtFirstCursor + 1);

    releaseDescribe();
    await firstSettle;
    const readsBeforeCurrentCursorRead = contextFigureReads;
    await session.read();
    expect(contextFigureReads).toBe(readsBeforeCurrentCursorRead);
  } finally {
    releaseDescribe();
    await host.close();
    fixture.close();
  }
});

it("reads a Session's context use only for updates that move its history", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const contextReads: {
    sessionId: unknown;
    key: string;
    caller: "run-context" | "context-figure";
  }[] = [];
  const owner: SqliteOwner = {
    transactionSync: (operation) => fixture.owner.transactionSync(operation),
    sql: {
      exec: (statement, ...values) => {
        if (
          statement.includes("SELECT id FROM walk") &&
          statement.includes("state != 'streaming'")
        ) {
          const key = JSON.stringify([values[0], values[1]]);
          const stack = new Error().stack ?? "";
          contextReads.push({
            sessionId: values[0],
            key,
            caller: stack.includes("readRunContextRows")
              ? "run-context"
              : "context-figure",
          });
        }
        return fixture.owner.sql.exec(statement, ...values);
      },
    },
  };
  const described: (ModelPreference | null)[] = [];
  const host = await memoryHost({ owner, model: describedSource(described) });
  const watched = host.agent.session("watched");
  const busy = host.agent.session("busy");
  try {
    await watched.send("Hello", { requestId: "watched-first" });
    const settled = (await host.settle("watched")).context;
    expect(settled?.tokens).toBeGreaterThan(0);
    const initialReads = contextReads.slice();
    expect(initialReads.map(({ caller }) => caller)).toEqual([
      "run-context",
      "context-figure",
    ]);
    expect(new Set(initialReads.map(({ key }) => key)).size).toBe(
      initialReads.length,
    );

    // Another Session's run does not wake idle observers for this Session.
    // Its cached context figure remains untouched.
    const watches = await Promise.all([
      watched.watch(),
      watched.watch(),
      watched.watch(),
    ]);
    expect(watches.map((watch) => watch.snapshot.context)).toEqual([
      settled,
      settled,
      settled,
    ]);
    expect(contextReads).toEqual(initialReads);
    const observed = watches.map(async (watch) => {
      const watcherUpdates: SessionFrame[] = [];
      for await (const frame of watch.changes)
        if (frame.kind === "update") watcherUpdates.push(frame);
      return watcherUpdates;
    });
    await busy.send("Hello", { requestId: "busy-first" });
    await host.settle("busy");
    const busyReads = contextReads.slice(initialReads.length);
    // Preparing the model prompt reads its own run context. That is distinct
    // from the context figure; only figure reads are shared per Session cursor.
    expect(busyReads.map(({ caller }) => caller)).toEqual([
      "run-context",
      "context-figure",
    ]);
    expect(busyReads.every(({ sessionId }) => sessionId === "busy")).toBe(true);
    const busyFigureReads = busyReads.filter(
      ({ caller }) => caller === "context-figure",
    );
    expect(new Set(busyFigureReads.map(({ key }) => key)).size).toBe(
      busyFigureReads.length,
    );
    const readsBeforeWatchedChange = contextReads.length;
    await watched.send("Again", { requestId: "watched-second" });
    const updated = await host.settle("watched");
    const watchedReads = contextReads.slice(readsBeforeWatchedChange);
    expect(
      watchedReads.filter(({ caller }) => caller === "run-context"),
    ).toHaveLength(1);
    const watchedFigureReads = watchedReads.filter(
      ({ caller }) => caller === "context-figure",
    );
    expect(watchedReads.every(({ sessionId }) => sessionId === "watched")).toBe(
      true,
    );
    expect(new Set(watchedFigureReads.map(({ key }) => key)).size).toBe(
      watchedFigureReads.length,
    );
    for (const watch of watches) await watch.close();
    const watcherUpdates = await Promise.all(observed);
    for (const updates of watcherUpdates) {
      expect(updates.length).toBeGreaterThan(0);
      const historyUpdates = updates.filter(
        (frame): frame is Extract<SessionFrame, { kind: "update" }> =>
          frame.kind === "update" && frame.messages.length > 0,
      );
      expect(historyUpdates.at(-1)?.context).toEqual(updated.context);
    }
  } finally {
    await host.close();
    fixture.close();
  }
});

it("does not replay-read idle Sessions when a sibling job starts or ends", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const replayReads = new Map<string, number>();
  const owner: SqliteOwner = {
    transactionSync: (operation) => fixture.owner.transactionSync(operation),
    sql: {
      exec: (statement, ...values) => {
        if (
          statement.includes(
            "SELECT tip.state AS state, tip.sequence AS newest_sequence",
          )
        ) {
          const sessionId = Schema.decodeUnknownSync(Schema.String)(values[0]);
          replayReads.set(sessionId, (replayReads.get(sessionId) ?? 0) + 1);
        }
        return fixture.owner.sql.exec(statement, ...values);
      },
    },
  };
  const model = fauxModel([
    fauxAssistantMessage("quiet reply"),
    fauxAssistantMessage("busy reply"),
  ]);
  const host = await memoryHost({
    owner,
    model: {
      acquire: () =>
        Promise.resolve({
          model: model.model,
          stream: model.stream,
          retryable: () => false,
          release: () => Promise.resolve(),
        }),
    },
  });
  const quiet = host.agent.session("quiet-observer");
  const busy = host.agent.session("busy-writer");
  try {
    await quiet.send("quiet", { requestId: "quiet" });
    await host.settle("quiet-observer");
    const watch = await quiet.watch();
    const observation = (async () => {
      for await (const frame of watch.changes) {
        void frame;
      }
    })();
    try {
      const before = replayReads.get("quiet-observer") ?? 0;
      await busy.send("busy", { requestId: "busy" });
      await host.settle("busy-writer");
      expect((replayReads.get("quiet-observer") ?? 0) - before).toBe(0);
    } finally {
      await watch.close();
      await observation;
    }
  } finally {
    await host.close();
    fixture.close();
  }
});
