// Pi Durable spec §8.7 "Range selection" (earendil-works/pi at 8efd314f),
// one test per rule. Sizes use pi-ai's estimate: 350 characters = 100 tokens.
import type { Message } from "@earendil-works/pi-ai";
import {
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { expect, it } from "vitest";

import {
  prepareCompaction,
  type CompactionSourceEntry,
} from "../src/internal/compaction/compaction.js";
import { DEFAULT_COMPACTION_SETTINGS } from "../src/internal/compaction/utils.js";

const hundredTokens = "x".repeat(350);

const entry = (id: string, message: Message): CompactionSourceEntry => ({
  id,
  kind: "message",
  message,
});
const user = (id: string, text = hundredTokens) =>
  entry(id, { role: "user", content: text, timestamp: Number(id) });
const assistant = (id: string, text = hundredTokens) =>
  entry(id, fauxAssistantMessage(text, { timestamp: Number(id) }));
const callingAssistant = (id: string, callId: string) =>
  entry(
    id,
    fauxAssistantMessage([fauxToolCall("read", {}, { id: callId })], {
      stopReason: "toolUse",
      timestamp: Number(id),
    }),
  );
const result = (id: string, callId: string, text: string) =>
  entry(id, {
    role: "toolResult",
    toolCallId: callId,
    toolName: "read",
    content: [{ type: "text", text }],
    isError: false,
    timestamp: Number(id),
  });

function firstKept(
  entries: readonly CompactionSourceEntry[],
  keepRecentTokens: number,
) {
  return prepareCompaction(entries, {
    ...DEFAULT_COMPACTION_SETTINGS,
    keepRecentTokens,
  })?.firstKeptEntryId;
}

it("never cuts at a system message", () => {
  const entries = [
    user("0"),
    assistant("1"),
    entry("2", { role: "system", content: hundredTokens, timestamp: 2 }),
    user("3"),
  ];
  // The budget is reached at the system message; the cut moves to the next
  // candidate instead.
  expect(firstKept(entries, 150)).toBe("3");
});

it("never cuts at a tool result, so a kept assistant keeps its results", () => {
  const entries = [
    user("0"),
    callingAssistant("1", "call-1"),
    result("2", "call-1", "y".repeat(1_050)),
  ];
  // The result alone reaches the budget and nothing follows it: the newest
  // candidate, its calling assistant, is kept with it.
  expect(firstKept(entries, 200)).toBe("1");
});

it("does not cut at a user message between a tool call and its result", () => {
  const entries = [
    user("0"),
    callingAssistant("1", "call-1"),
    user("2"),
    result("3", "call-1", "done"),
    assistant("4"),
  ];
  // The budget is reached at the steering user message, but call-1's result
  // still follows it, so the first candidate at or after it is entry 4.
  expect(firstKept(entries, 150)).toBe("4");
});

it("cuts at a user message once the calls before it have their results", () => {
  const entries = [
    user("0"),
    callingAssistant("1", "call-1"),
    result("2", "call-1", "done"),
    user("3"),
    assistant("4"),
  ];
  expect(firstKept(entries, 150)).toBe("3");
});

it("compacts nothing while the context never reaches the kept budget", () => {
  expect(firstKept([user("0"), assistant("1")], 10_000)).toBeUndefined();
});
