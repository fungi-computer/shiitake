import { fauxModel } from "@fungi.computer/mule/testing";
import {
  contentText,
  createAssistantMessageEventStream,
  type AssistantMessage,
} from "@earendil-works/pi-ai";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import * as Effect from "effect/Effect";
import { expect, it } from "vitest";
import { requestMessages } from "./fixtures/model-context.js";

import {
  borrowedView,
  compact,
  prepareCompaction,
  type CompactionSourceEntry,
} from "../src/internal/compaction/compaction.js";
import {
  compactionSummaryMessage,
  DEFAULT_COMPACTION_SETTINGS,
} from "../src/internal/compaction/utils.js";

const user = (id: string, text: string): CompactionSourceEntry => ({
  id,
  kind: "message",
  message: { role: "user", content: text, timestamp: Number(id) },
});

it("keeps Pi's recent tail and carries the previous summary boundary", () => {
  const entries: CompactionSourceEntry[] = [
    {
      id: "0",
      kind: "compaction",
      head: "1",
      model: compactionSummaryMessage("previous summary", 0),
      usage: fauxAssistantMessage("usage").usage,
    },
    user("1", "old request"),
    user("2", "old detail"),
    user("3", "recent request"),
  ];
  const preparation = prepareCompaction(entries, {
    ...DEFAULT_COMPACTION_SETTINGS,
    keepRecentTokens: 4,
  });

  expect(preparation?.firstKeptEntryId).toBe("3");
  expect(preparation?.messagesToSummarize).toHaveLength(2);
  expect(preparation?.turnPrefixMessages).toHaveLength(0);
  expect(preparation?.previousSummary).toBe("previous summary");
});

it("keeps split-turn prefix and suffix as separate preparation arrays", async () => {
  const entries: CompactionSourceEntry[] = [
    user("0", "prior turn"),
    user("1", "turn request"),
    {
      id: "2",
      kind: "message",
      message: fauxAssistantMessage("tool work"),
    },
    {
      id: "3",
      kind: "message",
      message: {
        role: "toolResult",
        toolCallId: "call-1",
        toolName: "read",
        content: [{ type: "text", text: "tool output" }],
        isError: false,
        timestamp: 3,
      },
    },
    user("4", "tail"),
  ];
  // pi-ai's estimate: "tail" 2 + "tool output" 4 + "tool work" 3 reaches 7
  // at the assistant, so the cut splits the turn after its request.
  const preparation = prepareCompaction(entries, {
    ...DEFAULT_COMPACTION_SETTINGS,
    keepRecentTokens: 7,
  });

  expect(preparation?.isSplitTurn).toBe(true);
  expect(preparation?.messagesToSummarize).not.toBe(
    preparation?.turnPrefixMessages,
  );
  expect(preparation?.turnPrefixMessages).toHaveLength(1);
  expect(preparation?.turnPrefixMessages[0]?.role).toBe("user");
  if (preparation === undefined) throw new Error("expected preparation");
  const faux = fauxModel([]);
  const prompts: string[] = [];
  const started: string[] = [];
  // Pi runs the two summaries in order: the prefix call starts only after
  // the history call has finished (never two concurrent calls, Pi #5536).
  const events: string[] = [];
  const historyUsage: AssistantMessage["usage"] = {
    input: 11,
    output: 2,
    cacheRead: 3,
    cacheWrite: 4,
    totalTokens: 20,
    cost: { input: 1, output: 2, cacheRead: 3, cacheWrite: 4, total: 10 },
  };
  const prefixUsage: AssistantMessage["usage"] = {
    input: 17,
    output: 5,
    cacheRead: 7,
    cacheWrite: 11,
    totalTokens: 40,
    cost: { input: 5, output: 6, cacheRead: 7, cacheWrite: 8, total: 26 },
  };
  const stream = (...args: Parameters<typeof faux.stream>) => {
    const content = requestMessages(args[1])[0]?.content;
    const prompt = contentText(content ?? "", "");
    const kind = prompt.includes("[User]: prior turn") ? "history" : "prefix";
    started.push(kind);
    prompts.push(prompt);
    const response = {
      ...fauxAssistantMessage(
        kind === "history" ? "history summary" : "prefix summary",
      ),
      usage: kind === "history" ? historyUsage : prefixUsage,
    };
    events.push(`start:${kind}`);
    const resultStream = createAssistantMessageEventStream();
    queueMicrotask(() => {
      events.push(`end:${kind}`);
      resultStream.end(response);
    });
    return resultStream;
  };
  const result = await Effect.runPromise(
    compact(preparation, faux.model, stream),
  );
  expect(started).toEqual(["history", "prefix"]);
  expect(events).toEqual([
    "start:history",
    "end:history",
    "start:prefix",
    "end:prefix",
  ]);
  expect(prompts).toHaveLength(2);
  expect(prompts[0]).toContain("[User]: prior turn");
  expect(prompts[1]).toContain("[User]: turn request");
  expect(prompts[0]).not.toBe(prompts[1]);
  expect(result.summary).toContain(
    "history summary\n\n---\n\n**Turn Context (split turn):**\n\nprefix summary",
  );
  expect(result.usage).toEqual({
    input: 28,
    output: 7,
    cacheRead: 10,
    cacheWrite: 15,
    totalTokens: 60,
    cost: {
      input: 6,
      output: 8,
      cacheRead: 10,
      cacheWrite: 12,
      total: 36,
    },
  });
});

it("uses the prefix summary when a split turn has no older history", async () => {
  const preparation = prepareCompaction(
    [
      user("1", "turn request"),
      { id: "2", kind: "message", message: fauxAssistantMessage("tool work") },
      {
        id: "3",
        kind: "message",
        message: {
          role: "toolResult",
          toolCallId: "call-1",
          toolName: "read",
          content: [{ type: "text", text: "tool output" }],
          isError: false,
          timestamp: 3,
        },
      },
      user("4", "tail"),
    ],
    { ...DEFAULT_COMPACTION_SETTINGS, keepRecentTokens: 7 },
  );
  if (preparation === undefined) throw new Error("expected preparation");
  expect(preparation.isSplitTurn).toBe(true);
  expect(preparation.messagesToSummarize).toHaveLength(0);
  const faux = fauxModel([]);
  faux.setResponses([fauxAssistantMessage("prefix summary")]);
  const result = await Effect.runPromise(
    compact(preparation, faux.model, faux.stream),
  );
  expect(result.summary).toContain("No prior history.");
  expect(result.summary).toContain("prefix summary");
  expect(faux.state.callCount).toBe(1);
});

it("borrows Pi preparation arrays and result without copying", () => {
  const preparation = prepareCompaction(
    [user("1", "old request"), user("2", "recent request")],
    { ...DEFAULT_COMPACTION_SETTINGS, keepRecentTokens: 4 },
  );
  if (preparation === undefined) throw new Error("expected preparation");
  const result = {
    summary: "summary",
    firstKeptEntryId: preparation.firstKeptEntryId,
    tokensBefore: preparation.tokensBefore,
    usage: {
      input: 1,
      output: 1,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 2,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
    details: { readFiles: [], modifiedFiles: [] },
  } as const;
  const view = borrowedView(preparation, result, "session", "revision");
  expect(view.sessionId).toBe("session");
  expect(
    Object.is(view.messagesToSummarize, preparation.messagesToSummarize),
  ).toBe(true);
  expect(
    Object.is(view.turnPrefixMessages, preparation.turnPrefixMessages),
  ).toBe(true);
  expect(Object.is(view.result, result)).toBe(true);
});

it("uses the initial prompt for an empty previous summary", async () => {
  const preparation = prepareCompaction(
    [user("1", "old request"), user("2", "recent request")],
    { ...DEFAULT_COMPACTION_SETTINGS, keepRecentTokens: 4 },
  );
  if (preparation === undefined) throw new Error("expected preparation");
  const faux = fauxModel([]);
  const prompts: string[] = [];
  faux.setResponses([
    (context) => {
      const content = requestMessages(context)[0]?.content;
      prompts.push(contentText(content ?? "", ""));
      return fauxAssistantMessage("summary");
    },
  ]);
  const result = await Effect.runPromise(
    compact({ ...preparation, previousSummary: "" }, faux.model, faux.stream),
  );

  expect(result.summary).toContain("summary");
  expect(prompts[0]).toContain("## Goal");
  expect(prompts[0]).not.toContain("NEW conversation messages");
});
