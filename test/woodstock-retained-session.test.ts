import { parseSessionSnapshot, type PromptCommand } from "../src/client.js";
import {
  idleActivity,
  queuedActivity,
  runningActivity,
} from "./fixtures/client.fixture.js";
import type {
  AssistantMessage,
  ToolResultMessage,
} from "@fungi.computer/mule/effect";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import * as Effect from "effect/Effect";
import { prepareWoodstockSchema } from "../src/internal/woodstock/index.js";

import {
  readClientReplayPage,
  readClientSnapshot,
} from "../src/internal/woodstock/client-projection.js";
import { sessionIds } from "../src/internal/woodstock/identities.js";
import * as woodstock from "../src/internal/woodstock/session-store.js";
import { admitWithoutBudget } from "../src/internal/woodstock/storage-admission.js";
import { readSessionInboxPage } from "../src/internal/woodstock/session-inbox.js";
import {
  promptCommand,
  type PromptCommandEntry,
} from "../src/internal/woodstock/session-entry.js";
import { admitStoredCommand } from "./fixtures/admission-door.fixture.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

const digest = "a".repeat(64);
const contract = {
  core: digest,
  extensions: [],
} as const;

it("replays Session creation by request ID after the SQLite owner reopens", () => {
  const directory = mkdtempSync(join(tmpdir(), "shiitake-session-create-"));
  const databasePath = join(directory, "session.sqlite");
  let fixture = createNativeSqliteOwnerFixture(databasePath);
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const created = woodstock.createStoredSession(
      fixture.owner,
      "model-create-session-1",
      admitWithoutBudget.admitNew,
    );
    const distinct = woodstock.createStoredSession(
      fixture.owner,
      "model-create-session-2",
      admitWithoutBudget.admitNew,
    );
    expect(distinct).not.toBe(created);
    fixture.close();

    fixture = createNativeSqliteOwnerFixture(databasePath);
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    expect(
      woodstock.createStoredSession(
        fixture.owner,
        "model-create-session-1",
        admitWithoutBudget.admitNew,
      ),
    ).toBe(created);
    expect(
      fixture.owner.sql.exec("SELECT COUNT(*) AS count FROM sessions").one()
        .count,
    ).toBe(2);
  } finally {
    fixture.close();
    rmSync(directory, { recursive: true, force: true });
  }
});

it("pages saved conversations past empty summaries without writing to the store", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    for (let index = 0; index <= 200; index += 1) {
      const sessionId = `conversation-${String(index).padStart(3, "0")}`;
      if (index === 0 || index === 200) {
        woodstock.appendSessionEntry(fixture.owner, {
          sessionId,
          id: sessionIds.fact.generate(),
          entry: commandEntry(`Question ${String(index)}`),
        });
      } else {
        woodstock.setSessionModelPreference(fixture.owner, sessionId, null);
      }
    }
    const changes = () =>
      fixture.owner.sql.exec("SELECT total_changes() AS count").one().count;
    const before = changes();
    const first = woodstock.readStoredSessionFactPage(fixture.owner);
    expect(first.sessions.map(({ sessionId }) => sessionId)).toEqual([
      "conversation-000",
    ]);
    expect(first.sessions[0]).toMatchObject({ kind: "session" });
    expect(first.next).toBe("conversation-199");
    const second = woodstock.readStoredSessionFactPage(
      fixture.owner,
      first.next,
    );
    expect(second.sessions).toEqual([
      expect.objectContaining({
        kind: "session",
        sessionId: "conversation-200",
        title: "Question 200",
      }),
    ]);
    expect(second.next).toBeUndefined();
    expect(changes()).toBe(before);
  } finally {
    fixture.close();
  }
});

it("accepts active abort by work identity without decoding unrelated lineage", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const sessionId = "active-abort-indexed-lookup";
    const input = (requestId: string, text: string) => ({
      type: "prompt" as const,
      requestId,
      text,
      delivery: "followUp" as const,
    });
    const first = admitStoredCommand(
      fixture.owner,
      sessionId,
      input("first-abort-lookup", "first"),
      { contract },
    );
    woodstock.appendSessionEntry(fixture.owner, {
      sessionId,
      id: first.accepted.commandId,
      entry: first.command.entry,
    });
    const second = admitStoredCommand(
      fixture.owner,
      sessionId,
      input("second-abort-lookup", "second"),
      { contract },
    );
    woodstock.appendSessionEntry(fixture.owner, {
      sessionId,
      id: second.accepted.commandId,
      entry: second.command.entry,
    });

    // The selected lineage contains an unrelated malformed ancestor. Abort
    // must use the command admission/work index and remain independent of it.
    fixture.owner.sql.exec(
      "UPDATE session_entries SET data_json = ? WHERE id = ?",
      "not-json",
      first.accepted.commandId,
    );
    const head = () => ({
      state: "running" as const,
      jobId: second.accepted.commandId,
      cancellationRequested: false,
    });
    const accepted = woodstock.acceptSessionAbort(
      fixture.owner,
      sessionId,
      head,
      () => ({ status: "recorded" as const }),
    );
    expect(accepted).toMatchObject({
      delivery: "new",
      active: true,
      accepted: { commandId: second.accepted.commandId },
    });

    const replay = woodstock.acceptSessionAbort(
      fixture.owner,
      sessionId,
      head,
      () => {
        throw new Error("replayed abort must not record again");
      },
    );
    expect(replay).toEqual({
      ...accepted,
      delivery: "replay",
    });
  } finally {
    fixture.close();
  }
});

const run = { kind: "run", value: "run-1" } as const;
const turn = { kind: "turn", value: "turn-1", parent: run } as const;
const message = {
  kind: "message",
  value: "message-1",
  parent: turn,
} as const;

function commandEntry(content: string): PromptCommandEntry {
  return {
    kind: "command",
    workId: sessionIds.work.generate(),
    requestId: `request-${content}`,
    receiptId: sessionIds.receipt.generate(),
    message: { role: "user", content, timestamp: 1 },
    contract,
  };
}

function assistant(
  text: string,
  stopReason: AssistantMessage["stopReason"] = "stop",
  content: AssistantMessage["content"] = [
    { type: "text", text, textSignature: "signed-text" },
  ],
): AssistantMessage {
  return {
    role: "assistant",
    content,
    api: "anthropic-messages",
    provider: "anthropic",
    model: "claude-test",
    responseModel: "claude-test-20260903",
    responseId: "response-1",
    usage: {
      input: 4,
      output: 2,
      cacheRead: 1,
      cacheWrite: 0,
      totalTokens: 7,
      cost: {
        input: 0.01,
        output: 0.02,
        cacheRead: 0.001,
        cacheWrite: 0,
        total: 0.031,
      },
    },
    stopReason,
    timestamp: 2,
  };
}

function toolResult(): ToolResultMessage {
  return {
    role: "toolResult",
    toolCallId: "tool-call-1",
    toolName: "lookup",
    content: [{ type: "text", text: "tool result" }],
    isError: false,
    timestamp: 3,
  };
}

it("replaces one streaming placeholder with the exact completed Pi message", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const sessionId = "retained-streaming";
    const commandEntryValue = commandEntry("first question");
    const command = woodstock.appendSessionEntry(fixture.owner, {
      sessionId,
      id: sessionIds.fact.generate(),
      entry: commandEntryValue,
    });
    const placeholder = woodstock.appendSessionEntry(fixture.owner, {
      sessionId,
      id: message.value,
      state: "streaming",
      entry: {
        kind: "message_start",
        workId: commandEntryValue.workId,
        run,
        turn,
        message,
        role: "assistant",
      },
    });

    expect(woodstock.readSelectedPath(fixture.owner, { sessionId })).toEqual([
      command,
    ]);
    // A streaming placeholder is stored history, never run status.
    expect(
      readSessionInboxPage(fixture.owner, undefined).sessions.find(
        ({ sessionId: id }) => id === sessionId,
      ),
    ).toMatchObject({ state: "done" });
    const completedMessage = assistant("first answer");
    const completed = woodstock.completeSessionLeaf(fixture.owner, {
      sessionId,
      id: message.value,
      entry: {
        kind: "message",
        workId: commandEntryValue.workId,
        run,
        turn,
        message,
        value: completedMessage,
      },
    });

    expect(completed).toMatchObject({
      id: placeholder.id,
      parentId: placeholder.parentId,
      sequence: placeholder.sequence,
      state: "complete",
    });
    expect(completed.entry).toMatchObject({
      kind: "message",
      value: completedMessage,
    });
    expect(
      readSessionInboxPage(fixture.owner, undefined).sessions.find(
        ({ sessionId: id }) => id === sessionId,
      ),
    ).toMatchObject({ state: "done" });
  } finally {
    fixture.close();
  }
});

it("projects the selected lineage with the exact completed Pi message", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const sessionId = "retained-client-projection";
    const commandEntryValue = commandEntry("first question");
    const command = woodstock.appendSessionEntry(fixture.owner, {
      sessionId,
      id: sessionIds.fact.generate(),
      entry: commandEntryValue,
    });
    const completedMessage = assistant("first answer");
    const completed = woodstock.completeSessionLeaf(fixture.owner, {
      sessionId,
      id: message.value,
      entry: {
        kind: "message",
        workId: commandEntryValue.workId,
        run,
        turn,
        message,
        value: completedMessage,
      },
    });

    const snapshot = readClientSnapshot(
      fixture.owner,
      sessionId,
      { limit: 200 },
      { facts: idleActivity, progress: null },
    );

    expect(snapshot.cursor).toEqual(expect.any(String));
    expect(snapshot).toEqual({
      extensions: {},
      attention: [],
      pending: { total: 0, prompts: [], hasMore: false },
      sessionId,
      cursor: snapshot.cursor,
      hasMore: false,
      oldestCursor: null,
      messages: [
        {
          id: command.id,
          sequence: command.sequence,
          commandId: commandEntryValue.workId,
          value: commandEntryValue.message,
        },
        {
          id: message.value,
          sequence: completed.sequence,
          commandId: commandEntryValue.workId,
          value: completedMessage,
        },
      ],
      terminals: [],
      compactions: [],
      compactionNotices: [],
      activity: idleActivity,
      progress: null,
    });
    expect(
      readClientReplayPage(fixture.owner, sessionId, undefined, "replay", {
        facts: idleActivity,
        progress: null,
      }),
    ).toMatchObject({
      kind: "page",
      hasMore: false,
      frames: [{ kind: "update", messages: snapshot.messages }],
    });
  } finally {
    fixture.close();
  }
});

it("projects the durable extension origin without attributing human text to an extension", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const sessionId = "extension-origin";
    const text = "Goal turn · Keep the menus tidy";
    for (const [requestId, origin] of [
      [
        "extension-turn",
        {
          kind: "extension",
          extensionId: "goal",
          correlation: "goal-revision",
        },
      ],
      ["human-turn", { kind: "user" }],
    ] as const) {
      const admission = admitStoredCommand(
        fixture.owner,
        sessionId,
        { type: "prompt", requestId, text, delivery: "followUp" },
        { origin },
      );
      const pending = readClientSnapshot(
        fixture.owner,
        sessionId,
        { limit: 200 },
        { facts: idleActivity, progress: null },
      );
      if (origin.kind === "extension") {
        expect(pending.pending.prompts[0]).toHaveProperty("origin", {
          extensionId: "goal",
          correlation: "goal-revision",
        });
        expect(
          parseSessionSnapshot(JSON.stringify(pending)).pending.prompts[0],
        ).toHaveProperty("origin", {
          extensionId: "goal",
          correlation: "goal-revision",
        });
      } else expect(pending.pending.prompts[0]).not.toHaveProperty("origin");
      woodstock.appendSessionEntry(fixture.owner, {
        sessionId,
        id: admission.accepted.commandId,
        entry: admission.command.entry,
      });
    }
    const snapshot = readClientSnapshot(
      fixture.owner,
      sessionId,
      { limit: 200 },
      { facts: idleActivity, progress: null },
    );
    const expected = { extensionId: "goal", correlation: "goal-revision" };
    expect(snapshot.messages[0]).toHaveProperty("origin", expected);
    expect(snapshot.messages[1]).not.toHaveProperty("origin");
    expect(
      parseSessionSnapshot(JSON.stringify(snapshot)).messages[0],
    ).toHaveProperty("origin", expected);
    expect(
      readClientReplayPage(fixture.owner, sessionId, undefined, "replay", {
        facts: idleActivity,
        progress: null,
      }),
    ).toMatchObject({
      kind: "page",
      frames: [{ messages: snapshot.messages }],
    });
  } finally {
    fixture.close();
  }
});

it("projects work terminals after intermediate tool messages without settling later work", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const sessionId = "retained-terminal-projection";
    const firstCommand = commandEntry("first question");
    woodstock.appendSessionEntry(fixture.owner, {
      sessionId,
      id: sessionIds.fact.generate(),
      entry: firstCommand,
    });
    const secondCommand = commandEntry("second question");
    woodstock.appendSessionEntry(fixture.owner, {
      sessionId,
      id: sessionIds.fact.generate(),
      entry: secondCommand,
    });

    const firstTurn = {
      kind: "turn",
      value: "turn-terminal-1",
      parent: run,
    } as const;
    const appendMessage = (
      workId: string,
      value: AssistantMessage | ToolResultMessage,
      messageValue: string,
    ) =>
      woodstock.appendSessionEntry(fixture.owner, {
        sessionId,
        id: sessionIds.fact.generate(),
        state: "complete",
        entry: {
          kind: "message",
          workId,
          run,
          turn: firstTurn,
          message: {
            kind: "message",
            value: messageValue,
            parent: firstTurn,
          },
          value,
        },
      });
    appendMessage(
      firstCommand.workId,
      // SAFETY: The fixture's tool-call content is the retained AssistantMessage content union.
      assistant("tool call", "toolUse", [
        {
          type: "toolCall",
          id: "tool-call-1",
          name: "lookup",
          arguments: {},
        },
      ] as AssistantMessage["content"]),
      "message-terminal-tool-use",
    );
    appendMessage(
      firstCommand.workId,
      toolResult(),
      "message-terminal-tool-result",
    );
    const finalAssistant = assistant("final answer");
    appendMessage(
      firstCommand.workId,
      finalAssistant,
      "message-terminal-final",
    );
    woodstock.appendSessionEntry(fixture.owner, {
      sessionId,
      id: sessionIds.fact.generate(),
      state: "complete",
      entry: {
        kind: "run_terminal",
        workId: firstCommand.workId,
        run,
        terminal: { kind: "terminal", value: "terminal-1", parent: run },
        outcome: "completed",
      },
    });

    const snapshot = readClientSnapshot(
      fixture.owner,
      sessionId,
      { limit: 200 },
      {
        facts: queuedActivity("queued-command"),
        progress: null,
      },
    );
    expect(snapshot.messages.map(({ commandId }) => commandId)).toEqual([
      firstCommand.workId,
      secondCommand.workId,
      firstCommand.workId,
      firstCommand.workId,
      firstCommand.workId,
    ]);
    expect(snapshot.messages.at(-1)?.value).toEqual(finalAssistant);
    expect(snapshot.terminals).toMatchObject([
      { commandId: firstCommand.workId, outcome: "completed" },
    ]);
    expect(snapshot.messages[1]?.commandId).toBe(secondCommand.workId);
    expect(
      readClientReplayPage(fixture.owner, sessionId, undefined, "replay", {
        facts: queuedActivity("queued-command"),
        progress: null,
      }),
    ).toMatchObject({
      kind: "page",
      frames: [{ terminals: snapshot.terminals }],
    });
  } finally {
    fixture.close();
  }
});

it("reconstructs only the selected lineage and forks it with fresh row identities", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const sessionId = "retained-tree-source";
    const root = woodstock.appendSessionEntry(fixture.owner, {
      sessionId,
      id: sessionIds.fact.generate(),
      entry: commandEntry("root"),
    });
    const abandoned = woodstock.appendSessionEntry(fixture.owner, {
      sessionId,
      id: sessionIds.fact.generate(),
      entry: commandEntry("abandoned"),
    });
    woodstock.selectSessionBranch(fixture.owner, {
      sessionId,
      entryId: root.id,
    });
    const selected = woodstock.appendSessionEntry(fixture.owner, {
      sessionId,
      id: sessionIds.fact.generate(),
      entry: commandEntry("selected"),
    });
    const selectedPrompt = promptCommand(selected.entry);
    if (selectedPrompt === undefined) throw new Error("Expected a prompt");
    const preCompaction = readClientSnapshot(
      fixture.owner,
      sessionId,
      { limit: 200 },
      { facts: idleActivity, progress: null },
    );
    const compaction = woodstock.appendSessionEntry(fixture.owner, {
      sessionId,
      id: sessionIds.fact.generate(),
      entry: {
        kind: "compaction",
        workId: selectedPrompt.workId,
        firstKeptEntryId: selected.id,
        summary: "retained summary",
        tokensBefore: 42,
        usage: assistant("usage").usage,
        contextRevision: selected.id,
      },
    });

    expect(
      woodstock
        .readSelectedPath(fixture.owner, { sessionId })
        .map(({ id }) => id),
    ).toEqual([root.id, selected.id, compaction.id]);
    // The full-history projection keeps one stable root/generation across a
    // compaction, so a cursor issued before it still replays forward (no
    // client reset) straight to the new compaction marker.
    const replay = readClientReplayPage(
      fixture.owner,
      sessionId,
      preCompaction.cursor ?? undefined,
      "replay",
      { facts: idleActivity, progress: null },
    );
    expect(replay.kind).toBe("page");
    if (replay.kind !== "page") throw new Error("expected page");
    expect(replay.hasMore).toBe(false);
    expect(replay.frames).toHaveLength(1);
    expect(replay.frames[0]).toMatchObject({
      kind: "update",
      messages: [],
      terminals: [],
      compactions: [
        {
          id: compaction.id,
          sequence: selected.sequence,
          at: compaction.timestamp,
        },
      ],
    });
    const postCompaction = readClientSnapshot(
      fixture.owner,
      sessionId,
      { limit: 200 },
      { facts: idleActivity, progress: null },
    );
    expect(postCompaction.messages.map(({ value }) => value)).toEqual([
      { role: "user", content: "root", timestamp: 1 },
      selectedPrompt.message,
    ]);
    // A row written before compactions recorded trigger and tokensAfter
    // still projects, with those facts unknown.
    expect(postCompaction.compactions).toEqual([
      {
        id: compaction.id,
        sequence: selected.sequence,
        at: compaction.timestamp,
        tokensBefore: 42,
        tokensAfter: null,
        trigger: null,
      },
    ]);
    expect(
      woodstock
        .readSessionTree(fixture.owner, { sessionId })[0]
        ?.children.map(({ row }) => row.id),
    ).toEqual([abandoned.id, selected.id]);

    const forkedSessionId = woodstock.forkSessionAt(fixture.owner, {
      sessionId,
      entryId: compaction.id,
    });
    const forkedPath = woodstock.readSelectedPath(fixture.owner, {
      sessionId: forkedSessionId,
    });
    expect(forkedPath.map(({ id }) => id)).not.toEqual([root.id, selected.id]);
    expect(forkedPath.map(({ parentId }) => parentId)).toEqual([
      null,
      forkedPath[0]?.id,
      forkedPath[1]?.id,
    ]);
    expect(forkedPath.some(({ id }) => id === abandoned.id)).toBe(false);
    const forkedCompaction = forkedPath.at(-1);
    expect(forkedCompaction?.entry).toMatchObject({
      kind: "compaction",
      firstKeptEntryId: forkedPath[1]?.id,
    });
    expect(woodstock.buildSessionContext(forkedPath)).toEqual(
      woodstock.buildSessionContext([root, selected, compaction]),
    );
  } finally {
    fixture.close();
  }
});

it("reads an ordered immutable Session forest with orphan roots", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const sessionId = "retained-tree-order-and-orphans";
    expect(woodstock.readSessionTree(fixture.owner, { sessionId })).toEqual([]);
    const root = woodstock.appendSessionEntry(fixture.owner, {
      sessionId,
      id: sessionIds.fact.generate(),
      entry: commandEntry("root"),
    });
    const firstChild = woodstock.appendSessionEntry(fixture.owner, {
      sessionId,
      id: sessionIds.fact.generate(),
      entry: commandEntry("first child"),
    });
    woodstock.selectSessionBranch(fixture.owner, {
      sessionId,
      entryId: root.id,
    });
    const secondChild = woodstock.appendSessionEntry(fixture.owner, {
      sessionId,
      id: sessionIds.fact.generate(),
      entry: commandEntry("second child"),
    });
    const orphan = woodstock.appendSessionEntry(fixture.owner, {
      sessionId,
      id: sessionIds.fact.generate(),
      entry: commandEntry("orphan"),
    });
    const selfParent = woodstock.appendSessionEntry(fixture.owner, {
      sessionId,
      id: sessionIds.fact.generate(),
      entry: commandEntry("self parent"),
    });
    fixture.owner.sql.exec(
      "UPDATE session_entries SET parent_id = ? WHERE id = ?",
      "missing-parent",
      orphan.id,
    );
    fixture.owner.sql.exec(
      "UPDATE session_entries SET parent_id = id WHERE id = ?",
      selfParent.id,
    );

    const tree = woodstock.readSessionTree(fixture.owner, { sessionId });

    expect(tree.map(({ row }) => row.id)).toEqual([
      root.id,
      orphan.id,
      selfParent.id,
    ]);
    expect(tree[0]?.children.map(({ row }) => row.id)).toEqual([
      firstChild.id,
      secondChild.id,
    ]);
    expect(tree[1]?.row.parentId).toBe("missing-parent");
    expect(tree[2]?.row.parentId).toBe(selfParent.id);
    expect(Object.isFrozen(tree)).toBe(true);
    for (const node of tree) {
      expect(Object.isFrozen(node)).toBe(true);
      expect(Object.isFrozen(node.children)).toBe(true);
      for (const child of node.children) {
        expect(Object.isFrozen(child)).toBe(true);
        expect(Object.isFrozen(child.children)).toBe(true);
      }
    }
  } finally {
    fixture.close();
  }
});

it("prepares a cold run from the exact accepted command lineage", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const sessionId = "retained-cold-two-turn";
    const firstEntry = commandEntry("first question");
    const first = woodstock.appendSessionEntry(fixture.owner, {
      sessionId,
      id: sessionIds.fact.generate(),
      entry: firstEntry,
    });
    const firstAnswer = assistant("first answer");
    woodstock.completeSessionLeaf(fixture.owner, {
      sessionId,
      id: message.value,
      entry: {
        kind: "message",
        workId: firstEntry.workId,
        run,
        turn,
        message,
        value: firstAnswer,
      },
    });
    woodstock.appendSessionEntry(fixture.owner, {
      sessionId,
      id: sessionIds.fact.generate(),
      entry: {
        kind: "run_terminal",
        workId: firstEntry.workId,
        run,
        terminal: { kind: "terminal", value: "terminal-1", parent: run },
        outcome: "completed",
      },
    });
    const secondEntry = commandEntry("second question");
    const second = woodstock.appendSessionEntry(fixture.owner, {
      sessionId,
      id: sessionIds.fact.generate(),
      entry: secondEntry,
    });
    woodstock.appendSessionEntry(fixture.owner, {
      sessionId,
      id: sessionIds.fact.generate(),
      entry: commandEntry("later sibling work"),
    });

    const prepared = woodstock.prepareSessionRun(fixture.owner, {
      sessionId,
      commandFactId: second.id,
    });
    expect(prepared.command.id).toBe(second.id);
    expect(prepared.messages).toEqual([
      firstEntry.message,
      firstAnswer,
      secondEntry.message,
    ]);
    expect(first.parentId).toBeNull();
  } finally {
    fixture.close();
  }
});

it("accepts one command entry as the exact replay and work identity", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const sessionId = "retained-command-acceptance";
    const input = {
      requestId: "same-request",
      delivery: "followUp",

      type: "prompt",
      text: "one command",
    } as const;
    const enqueued: {
      jobId: string;
      sessionId: string;
      commandFactId: string;
    }[] = [];
    let first!: woodstock.AcceptedSessionCommand;

    expect(() => {
      first = admitStoredCommand(fixture.owner, sessionId, input, {
        contract,
        enqueue: (work) => enqueued.push(work),
      });
    }).not.toThrow();

    const writesAfterFirst = fixture.ownerWrites();
    const replay = admitStoredCommand(fixture.owner, sessionId, input, {
      contract,
      enqueue: (work) => enqueued.push(work),
    });

    expect(first.delivery).toBe("new");
    expect(replay).toEqual({ ...first, delivery: "replay" });
    expect(fixture.ownerWrites()).toBe(writesAfterFirst);
    expect(enqueued).toEqual([
      {
        jobId: first.command.id,
        sessionId,
        commandFactId: first.command.id,
      },
    ]);
    expect(first.accepted).toEqual({
      sessionId,
      requestId: input.requestId,
      commandId: first.command.id,
    });

    expect(() =>
      admitStoredCommand(
        fixture.owner,
        sessionId,
        { ...input, text: "changed command" },
        { contract, enqueue: (work) => enqueued.push(work) },
      ),
    ).toThrow(/conflicts/u);
    expect(fixture.ownerWrites()).toBe(writesAfterFirst);
    expect(enqueued).toHaveLength(1);
  } finally {
    fixture.close();
  }
});

it("bounds the waiting view without limiting command acceptance", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const accepted = [];
    for (let index = 0; index < 201; index += 1) {
      accepted.push(
        admitStoredCommand(
          fixture.owner,
          "bounded-waiting",
          {
            delivery: "followUp",

            type: "prompt",
            requestId: `waiting-${String(index)}`,
            text: `Task ${String(index)}`,
          },
          {
            contract,
            enqueue: () => undefined,
          },
        ),
      );
    }
    const read = () =>
      readClientSnapshot(
        fixture.owner,
        "bounded-waiting",
        { limit: 200 },
        {
          facts: queuedActivity("queued-command"),
          progress: null,
        },
      );
    const first = read();
    expect(first.messages).toEqual([]);
    expect(first.cursor).toBeNull();
    expect(first.pending.prompts).toHaveLength(200);
    expect(first.pending.total).toBe(201);
    expect(first.pending.hasMore).toBe(true);
    const command = accepted[0];
    if (!command) throw new Error("Accepted command missing");
    woodstock.prepareSessionRun(fixture.owner, {
      sessionId: "bounded-waiting",
      commandFactId: command.command.id,
    });
    const next = read();
    expect(next.pending.prompts).toHaveLength(200);
    expect(next.pending.total).toBe(200);
    expect(next.pending.prompts[0]?.commandId).toBe(accepted[1]?.command.id);
    expect(next.pending.prompts.at(-1)?.commandId).toBe(
      accepted[200]?.command.id,
    );
    expect(next.pending.hasMore).toBe(false);
    expect(next.messages[0]?.commandId).toBe(accepted[0]?.command.id);
    const replay = readClientReplayPage(
      fixture.owner,
      "bounded-waiting",
      next.cursor ?? undefined,
      "replay",
      {
        facts: queuedActivity("queued-command"),
        progress: null,
      },
    );
    expect(replay).toMatchObject({
      kind: "page",
      hasMore: false,
      frames: [
        {
          kind: "update",
          cursor: next.cursor,
          messages: [],
          pending: next.pending,
        },
      ],
    });
  } finally {
    fixture.close();
  }
});

it("withdraws cancelled follow-ups before their scheduler terminal is written", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const sessionId = "withdrawn-follow-ups";
    const commands = ["withdraw", "keep"].map((requestId) =>
      admitStoredCommand(fixture.owner, sessionId, {
        type: "prompt",
        delivery: "followUp",
        requestId,
        text: requestId,
      }),
    );
    const withdrawn = commands[0];
    const kept = commands[1];
    if (!withdrawn || !kept) throw new Error("Expected both admissions");
    expect(
      woodstock.acceptSessionCancellation(
        fixture.owner,
        sessionId,
        withdrawn.command.id,
        () => ({ state: "idle" }),
        () => ({ status: "recorded" }),
        true,
      ),
    ).toMatchObject({ delivery: "new", active: false });
    const snapshot = readClientSnapshot(
      fixture.owner,
      sessionId,
      { limit: 200 },
      { facts: queuedActivity(kept.command.id), progress: null },
    );
    expect(snapshot.pending.total).toBe(1);
    expect(snapshot.pending.prompts.map(({ commandId }) => commandId)).toEqual([
      kept.command.id,
    ]);
    expect(snapshot.terminals).toEqual([]);
  } finally {
    fixture.close();
  }
});

it("keeps large waiting prompt projection within the existing byte bound", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    for (let index = 0; index < 9; index += 1) {
      admitStoredCommand(
        fixture.owner,
        "large-waiting",
        {
          delivery: "followUp",

          type: "prompt",
          requestId: `large-${String(index)}`,
          text: "x".repeat(256 * 1024),
        },
        {
          contract,
          enqueue: () => undefined,
        },
      );
    }
    const snapshot = readClientSnapshot(
      fixture.owner,
      "large-waiting",
      { limit: 200 },
      {
        facts: queuedActivity("queued-command"),
        progress: null,
      },
    );
    expect(snapshot.pending.total).toBe(9);
    expect(snapshot.pending.prompts.length).toBeGreaterThan(0);
    expect(snapshot.pending.prompts.length).toBeLessThan(9);
    expect(snapshot.pending.hasMore).toBe(true);
    expect(
      new TextEncoder().encode(JSON.stringify(snapshot.pending)).length,
    ).toBeLessThan(2_097_152);
  } finally {
    fixture.close();
  }
});

it("keeps a preference changed during streaming on the completed message lineage", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const sessionId = "preference-during-stream";
    const entry = commandEntry("keep this model choice");
    const command = woodstock.appendSessionEntry(fixture.owner, {
      sessionId,
      id: sessionIds.fact.generate(),
      entry,
    });
    const placeholder = woodstock.appendSessionEntry(fixture.owner, {
      sessionId,
      id: message.value,
      state: "streaming",
      entry: {
        kind: "message_start",
        workId: entry.workId,
        run,
        turn,
        message,
        role: "assistant",
      },
    });
    const preference = { providerId: "faux", modelId: "next-model" };
    const selected = woodstock.setSessionModelPreference(
      fixture.owner,
      sessionId,
      preference,
    );
    const beforeCompletion = readClientSnapshot(
      fixture.owner,
      sessionId,
      { limit: 200 },
      {
        facts: runningActivity,
        progress: null,
      },
    );
    const value = assistant("finished on the original model");
    const completed = woodstock.completeSessionLeaf(fixture.owner, {
      sessionId,
      id: placeholder.id,
      entry: {
        kind: "message",
        workId: entry.workId,
        run,
        turn,
        message,
        value,
      },
    });
    const replay = readClientReplayPage(
      fixture.owner,
      sessionId,
      beforeCompletion.cursor ?? undefined,
      "live",
      {
        facts: idleActivity,
        progress: null,
      },
    );
    expect(replay).toMatchObject({
      kind: "page",
      frames: [{ kind: "update", messages: [{ id: message.value, value }] }],
    });
    expect(completed.id).toBe(placeholder.id);
    expect(completed.parentId).toBe(selected.id);
    expect(completed.sequence).toBeGreaterThan(selected.sequence);
    expect(completed.entry).toMatchObject({ kind: "message", value });
    expect(
      woodstock
        .readSelectedPath(fixture.owner, { sessionId })
        .map((row) => row.id),
    ).toEqual([command.id, selected.id, completed.id]);
    expect(
      woodstock.readSessionModelPreference(fixture.owner, sessionId),
    ).toEqual(preference);
  } finally {
    fixture.close();
  }
});

it("durably admits image prompts and includes images in retry identity and pending reads", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const prompt = {
      type: "prompt",
      delivery: "followUp",
      requestId: "image-retry",
      text: "",
      images: [{ type: "image", data: "aGk=", mimeType: "image/png" }],
    } as const;
    const first = admitStoredCommand(fixture.owner, "images", prompt);
    expect(promptCommand(first.command.entry)?.message.content).toEqual(
      prompt.images,
    );
    expect(admitStoredCommand(fixture.owner, "images", prompt).delivery).toBe(
      "replay",
    );
    expect(() =>
      admitStoredCommand(fixture.owner, "images", {
        ...prompt,
        images: [{ type: "image", data: "Ynl0ZXM=", mimeType: "image/png" }],
      }),
    ).toThrow(/conflicts/u);
    const snapshot = readClientSnapshot(
      fixture.owner,
      "images",
      { limit: 200 },
      { facts: idleActivity, progress: null },
    );
    expect(snapshot.pending.prompts[0]?.images).toEqual(prompt.images);
    const prepared = woodstock.prepareSessionRun(fixture.owner, {
      sessionId: "images",
      commandFactId: first.command.id,
    });
    expect(prepared.messages[0]).toMatchObject({
      role: "user",
      content: prompt.images,
    });
    for (const invalid of [
      {
        ...prompt,
        images: [{ type: "image", data: "not base64!", mimeType: "image/png" }],
      },
      {
        ...prompt,
        images: [{ type: "image", data: "aGk=", mimeType: "text/plain" }],
      },
      { ...prompt, images: [] },
      {
        ...prompt,
        images: [
          {
            type: "image",
            data: "a".repeat(1024 * 1024 + 4),
            mimeType: "image/png",
          },
        ],
      },
    ] satisfies readonly PromptCommand[]) {
      expect(() =>
        admitStoredCommand(fixture.owner, "images", invalid),
      ).toThrow(/invalid/u);
    }
  } finally {
    fixture.close();
  }
});

it("stores host metadata through reopen and refuses changed metadata on replay", () => {
  const directory = mkdtempSync(join(tmpdir(), "shiitake-key-author-"));
  const databasePath = join(directory, "session.sqlite");
  let fixture = createNativeSqliteOwnerFixture(databasePath);
  const meta = { author: { id: "team-key:key-a", name: "Zed local project" } };
  const prompt: PromptCommand = {
    type: "prompt",
    requestId: "key-command",
    text: "Inspect the project",
    images: [{ type: "image", data: "aGk=", mimeType: "image/png" }],
    delivery: "followUp",
  };
  try {
    prepareWoodstockSchema(fixture.owner).pipe(Effect.runSync);
    const accepted = admitStoredCommand(fixture.owner, "key-session", prompt, {
      meta,
    });
    expect(accepted.command.entry.meta).toEqual(meta);
    fixture.close();
    fixture = createNativeSqliteOwnerFixture(databasePath);
    prepareWoodstockSchema(fixture.owner).pipe(Effect.runSync);
    const replay = admitStoredCommand(fixture.owner, "key-session", prompt, {
      meta,
    });
    expect(replay.delivery).toBe("replay");
    expect(replay.command.entry.meta).toEqual(meta);
    expect(promptCommand(replay.command.entry)?.message.content).toEqual([
      { type: "text", text: prompt.text },
      ...(prompt.images ?? []),
    ]);
    expect(() =>
      admitStoredCommand(
        fixture.owner,
        "key-session",
        {
          ...prompt,
          images: [{ type: "image", data: "Ynl0ZXM=", mimeType: "image/png" }],
        },
        { meta },
      ),
    ).toThrow(/conflicts/u);
    expect(() =>
      admitStoredCommand(fixture.owner, "key-session", prompt, {
        meta: { author: { id: "team-key:key-b", name: "Other" } },
      }),
    ).toThrow();
    const ordinary = admitStoredCommand(fixture.owner, "key-session", {
      ...prompt,
      requestId: "ordinary-command",
    });
    expect(ordinary.command.entry.meta).toBeUndefined();
  } finally {
    fixture.close();
    rmSync(directory, { recursive: true, force: true });
  }
});

it("settles an orphan admission without blocking the rest of cancel-all", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const sessionId = "orphan-cancel";
    const admissions = ["orphan", "queued", "steer"].map((text) =>
      admitStoredCommand(
        fixture.owner,
        sessionId,
        {
          type: "prompt",
          requestId: text,
          text,
          delivery: text === "steer" ? "steer" : "followUp",
        },
        { contract },
      ),
    );
    const orphan = admissions[0]?.command.entry.workId;
    const recorded: string[] = [];
    woodstock.acceptSessionAbort(
      fixture.owner,
      sessionId,
      () => ({ state: "idle" }),
      ({ jobId }) => {
        recorded.push(jobId);
        return { status: jobId === orphan ? "missing" : "recorded" };
      },
    );
    expect(recorded).toHaveLength(3);
    expect(
      woodstock.readPendingSessionCommands(fixture.owner, sessionId, 64),
    ).toEqual({ total: 0, commands: [] });
    const snapshot = readClientSnapshot(
      fixture.owner,
      sessionId,
      { limit: 200 },
      { facts: idleActivity, progress: null },
    );
    expect(snapshot.messages.map(({ value }) => value.content)).toEqual([
      "orphan",
      "queued",
      "steer",
    ]);
    expect(snapshot.terminals).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          commandId: admissions[0]?.accepted.commandId,
          outcome: "cancelled",
        }),
      ]),
    );
  } finally {
    fixture.close();
  }
});
