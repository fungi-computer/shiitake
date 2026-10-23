// The footer and the compaction threshold both read pi-ai's own context
// estimate. A provider's usage figure measures the context it read, so once a
// compaction lands, a kept reply's older usage prices the pre-compaction
// history. pi-ai ignores usage older than a newer prefix message (the summary
// carries the compaction's timestamp) and estimates the whole kept context
// fresh -- never the trailing count alone, which would undercount to zero once
// that stale reply is the newest message in view.
import type { Message } from "@earendil-works/pi-ai";
import {
  estimateContextTokens,
  estimateMessageTokens,
} from "@earendil-works/pi-ai/utils/estimate";
import { fauxProvider } from "@earendil-works/pi-ai/providers/faux";
import { describe, expect, it } from "vitest";

import { openCompactionSettings } from "../src/internal/compaction/settings.js";
import {
  compactionSummaryMessage,
  shouldCompact,
  shouldCompactInBackground,
} from "../src/internal/compaction/utils.js";

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
  const summary = compactionSummaryMessage(
    "Earlier work summarized.",
    boundaryAt,
  );
  const reply = keptReply(boundaryAt - 100);
  const messages = [summary, reply];

  // Counting only what followed the stale usage message would report 0
  // for a real, non-empty context.
  expect(estimateContextTokens(messages).tokens).toBe(
    estimateMessageTokens(summary) + estimateMessageTokens(reply),
  );
  expect(estimateContextTokens(messages).tokens).toBeGreaterThan(0);
});

it("still trusts a usage report newer than the compaction", () => {
  const boundaryAt = 2_000;
  const summary = compactionSummaryMessage(
    "Earlier work summarized.",
    boundaryAt,
  );
  // This reply's own usage was measured after the compaction landed, so it
  // correctly prices the post-compaction context already; trust it.
  const reply = keptReply(boundaryAt + 100);
  const messages = [summary, reply];

  expect(estimateContextTokens(messages).tokens).toBe(
    reply.role === "assistant" ? reply.usage.totalTokens : 0,
  );
});

it("estimates every message fresh before any compaction has run", () => {
  const reply = keptReply(1_000);
  expect(estimateContextTokens([reply]).tokens).toBe(
    reply.role === "assistant" ? reply.usage.totalTokens : 0,
  );
});

describe("background compaction threshold (tokens only)", () => {
  const model = fauxProvider({ models: [{ id: "bg-1" }] }).getModel();
  const window = 100_000;
  const options = { reserveTokens: 10_000, backgroundTokens: 20_000 };
  const settings = openCompactionSettings(options)(model);
  // Background starts above 100000 - 10000 - 20000 = 70000; blocking above 90000.

  it("starts one token above the background threshold, not at it", () => {
    expect(shouldCompactInBackground(70_000, window, settings)).toBe(false);
    expect(shouldCompactInBackground(70_001, window, settings)).toBe(true);
  });

  it("blocks one token above the blocking threshold, not in the background", () => {
    expect(shouldCompactInBackground(90_000, window, settings)).toBe(true);
    expect(shouldCompact(90_000, window, settings)).toBe(false);
    expect(shouldCompact(90_001, window, settings)).toBe(true);
    expect(shouldCompactInBackground(90_001, window, settings)).toBe(false);
  });

  it("is off at zero and when compaction is disabled", () => {
    const off = openCompactionSettings({ ...options, backgroundTokens: 0 })(
      model,
    );
    expect(shouldCompactInBackground(89_000, window, off)).toBe(false);
    const disabled = openCompactionSettings({ ...options, enabled: false })(
      model,
    );
    expect(shouldCompactInBackground(89_000, window, disabled)).toBe(false);
  });

  it("defaults to 32768 and lets an exact provider/modelId override win", () => {
    expect(openCompactionSettings(undefined)(model).backgroundTokens).toBe(
      32_768,
    );
    const resolved = openCompactionSettings({
      ...options,
      modelOverrides: {
        [`${model.provider}/${model.id}`]: { backgroundTokens: 5_000 },
        "other/model": { backgroundTokens: 1 },
      },
    })(model);
    expect(resolved.backgroundTokens).toBe(5_000);
    expect(shouldCompactInBackground(70_001, window, resolved)).toBe(false);
    expect(shouldCompactInBackground(85_001, window, resolved)).toBe(true);
  });

  it("rejects a negative value", () => {
    expect(() => openCompactionSettings({ backgroundTokens: -1 })).toThrow(
      /non-negative/u,
    );
    expect(() =>
      openCompactionSettings({
        modelOverrides: { "a/b": { backgroundTokens: -1 } },
      }),
    ).toThrow(/non-negative/u);
  });
});
