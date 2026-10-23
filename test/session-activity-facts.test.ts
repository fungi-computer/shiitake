import * as Effect from "effect/Effect";
import { expect, it } from "vitest";

import { readSessionActivity } from "../src/internal/session-status.js";
import { sessionIds } from "../src/internal/woodstock/identities.js";
import { prepareWoodstockSchema } from "../src/internal/woodstock/index.js";
import * as woodstock from "../src/internal/woodstock/session-store.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

const run = { kind: "run", value: "run-1" } as const;
const turn = { kind: "turn", value: "turn-1", parent: run } as const;
const digest = "a".repeat(64);

const idle = {
  queue: { state: "idle" },
  cancellationRequested: false,
  retry: null,
  activeTool: null,
  lastModelChoiceSequence: null,
  compaction: null,
} as const;

it("publishes raw activity facts and decides no readout", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const sessionId = "activity-facts";
    const workId = sessionIds.work.generate();
    const append = (
      entry: Parameters<typeof woodstock.appendSessionEntry>[1]["entry"],
    ) =>
      woodstock.appendSessionEntry(fixture.owner, {
        sessionId,
        id: sessionIds.fact.generate(),
        entry,
      });
    append({
      kind: "command",
      workId,
      requestId: "request-1",
      receiptId: sessionIds.receipt.generate(),
      message: { role: "user", content: "go", timestamp: 1 },
      contract: { core: digest, extensions: [] },
    });

    expect(
      readSessionActivity(fixture.owner, sessionId, { state: "idle" }, null)
        .facts,
    ).toEqual(idle);
    expect(
      readSessionActivity(
        fixture.owner,
        sessionId,
        {
          state: "queued",
          jobId: workId,
          cancellationRequested: true,
        },
        null,
      ),
    ).toEqual({
      facts: {
        ...idle,
        queue: { state: "queued", commandId: workId, runningWork: null },
        cancellationRequested: true,
      },
      toolOutput: [],
      progress: null,
    });

    const running = {
      state: "running",
      jobId: workId,
      cancellationRequested: false,
    } as const;
    expect(
      readSessionActivity(fixture.owner, sessionId, running, null).facts,
    ).toEqual({
      ...idle,
      queue: { state: "running", commandId: workId },
    });

    const toolCall = {
      kind: "tool_call",
      value: "call-1",
      parent: turn,
    } as const;
    const call = {
      type: "toolCall",
      id: "call-1",
      name: "lookup",
      arguments: {},
    } as const;
    append({
      kind: "tool_intent",
      replay: "unsafe",
      contract: { core: "0".repeat(64), extensions: [] },
      workId,
      run,
      turn,
      toolCall,
      call,
    });
    expect(
      readSessionActivity(fixture.owner, sessionId, running, null).facts
        .activeTool,
    ).toBe("lookup");

    append({
      kind: "live_retry",
      workId,
      event: {
        type: "auto_retry_start",
        run,
        attempt: 1,
        maxAttempts: 3,
        delayMs: 2_000,
        errorMessage: "503 Service Unavailable\n  try again",
      },
    });
    const retry = readSessionActivity(fixture.owner, sessionId, running, null)
      .facts.retry;
    // The error text is exactly as stored: no whitespace collapse or bound.
    expect(retry).toMatchObject({
      attempt: 1,
      maxAttempts: 3,
      delayMs: 2_000,
      errorMessage: "503 Service Unavailable\n  try again",
    });
    expect(retry?.scheduledAt).toEqual(expect.any(Number));

    append({
      kind: "live_retry",
      workId,
      event: { type: "auto_retry_end", run, attempt: 1, success: true },
    });
    expect(
      readSessionActivity(fixture.owner, sessionId, running, null).facts.retry,
    ).toBeNull();

    const choice = woodstock.setSessionModelPreference(
      fixture.owner,
      sessionId,
      null,
    );
    expect(
      readSessionActivity(fixture.owner, sessionId, { state: "idle" }, null)
        .facts.lastModelChoiceSequence,
    ).toBe(choice.sequence);
  } finally {
    fixture.close();
  }
});
