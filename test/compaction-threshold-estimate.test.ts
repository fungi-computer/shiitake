// `estimateThresholdContextTokens` is what the TUI/browser footer and the
// compaction threshold check both read. Pi's own usage-plus-trailing
// shortcut (packages/shiitake/src/internal/compaction/utils.ts, a retained
// port) is correct only while nothing has reshaped the context since the
// last reported usage. Once a compaction lands, every kept message's usage
// figure prices the pre-compaction history, so the owner falls back to a
// fresh chars/4 estimate of the whole kept context -- never to the trailing
// count alone, which undercounts (even to a wrong zero) once the stale
// usage message is itself the newest thing in view.
import type { Message } from "@earendil-works/pi-ai";
import { expect, it } from "vitest";

import { estimateThresholdContextTokens } from "../src/internal/compaction/session-compaction.js";
import {
  compactionSummaryMessage,
  estimateTokens,
} from "../src/internal/compaction/utils.js";
import type { StoredSessionRow } from "../src/internal/woodstock/index.js";

const zeroUsage = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 0,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
};

function compactionRow(timestamp: number): StoredSessionRow {
  return {
    id: "compaction-1",
    sessionId: "session-1",
    parentId: "kept-1",
    sequence: 2,
    state: "complete",
    timestamp,
    completedAt: timestamp,
    entry: {
      kind: "compaction",
      workId: "work-1",
      firstKeptEntryId: "kept-1",
      summary: "Earlier work summarized.",
      tokensBefore: 10_000,
      tokensAfter: 1_234,
      trigger: "manual",
      usage: zeroUsage,
      contextRevision: "rev-1",
    },
  };
}

function keptReply(timestamp: number): Message {
  return {
    role: "assistant",
    api: "pi",
    provider: "camel",
    model: "auto",
    content: [{ type: "text", text: "y".repeat(4_000) }],
    stopReason: "stop",
    timestamp,
    usage: {
      input: 1_200,
      output: 50,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 1_250,
      cost: {
        input: 0.01,
        output: 0.01,
        cacheRead: 0,
        cacheWrite: 0,
        total: 0.02,
      },
    },
  };
}

it("estimates the whole kept context fresh when the last usage predates the compaction", () => {
  const boundaryAt = 2_000;
  const path = [compactionRow(boundaryAt)];
  const summary = compactionSummaryMessage(
    "Earlier work summarized.",
    boundaryAt,
  );
  const reply = keptReply(boundaryAt - 100);
  const messages = [summary, reply];

  // The buggy prior behavior returned the chars/4 cost of only what
  // followed the stale usage message -- nothing, since it is the newest
  // message in view -- wrongly reporting 0 for a real, non-empty context.
  expect(estimateThresholdContextTokens(path, messages)).toBe(
    estimateTokens(summary) + estimateTokens(reply),
  );
  expect(estimateThresholdContextTokens(path, messages)).toBeGreaterThan(0);
});

it("still trusts a usage report newer than the compaction", () => {
  const boundaryAt = 2_000;
  const path = [compactionRow(boundaryAt)];
  const summary = compactionSummaryMessage(
    "Earlier work summarized.",
    boundaryAt,
  );
  // This reply's own usage was measured after the compaction landed, so it
  // correctly prices the post-compaction context already; trust it.
  const reply = keptReply(boundaryAt + 100);
  const messages = [summary, reply];

  expect(estimateThresholdContextTokens(path, messages)).toBe(
    reply.role === "assistant" ? reply.usage.totalTokens : 0,
  );
});

it("estimates every message fresh before any compaction has run", () => {
  const reply = keptReply(1_000);
  expect(estimateThresholdContextTokens([], [reply])).toBe(
    reply.role === "assistant" ? reply.usage.totalTokens : 0,
  );
});
