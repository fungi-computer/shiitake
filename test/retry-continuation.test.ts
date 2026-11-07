import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { expect, it } from "vitest";
import {
  abnormalTerminal,
  recoverAssistantTurn,
} from "../src/internal/woodstock/run-recovery.js";
import type { StoredSessionEntry } from "../src/internal/woodstock/session-entry.js";
import type { StoredSessionRow } from "../src/internal/woodstock/session-store.js";

const run = { kind: "run" as const, value: "run" };
const turn = { kind: "turn" as const, value: "turn", parent: run };
const failed = fauxAssistantMessage("", {
  stopReason: "error",
  errorMessage: "503",
});
const message: StoredSessionEntry = {
  kind: "message",
  workId: "work",
  run,
  turn,
  message: { kind: "message", value: "message", parent: turn },
  value: failed,
};
function rows(...entries: StoredSessionEntry[]): StoredSessionRow[] {
  return entries.map((entry, index) => ({
    id: `entry-${String(index)}`,
    sessionId: "session",
    parentId: index === 0 ? null : `entry-${String(index - 1)}`,
    sequence: index + 1,
    state: "complete",
    timestamp: 1_000 + index,
    completedAt: 1_000 + index,
    entry,
  }));
}
const scheduled: StoredSessionEntry = {
  kind: "live_retry",
  workId: "work",
  event: {
    type: "auto_retry_start",
    run,
    attempt: 3,
    maxAttempts: 3,
    delayMs: 8_000,
    dueAt: 9_000,
    errorMessage: "Model unavailable",
  },
};

it("restores the existing scheduled attempt and absolute due instant across a new run boundary", () => {
  expect(
    recoverAssistantTurn(
      rows(message, scheduled, {
        kind: "turn_started",
        workId: "work",
        run,
        turn,
      }),
    ),
  ).toMatchObject({
    kind: "retry",
    phase: "waiting",
    attempt: 3,
    maxAttempts: 3,
    dueAt: 9_000,
  });
});
it("counts a persisted retry dispatch as consumed even before another message", () => {
  expect(
    recoverAssistantTurn(
      rows(message, scheduled, {
        kind: "live_retry",
        workId: "work",
        event: { type: "auto_retry_attempt", run, attempt: 3 },
      }),
    ),
  ).toMatchObject({
    kind: "retry",
    phase: "dispatched",
    attempt: 3,
    maxAttempts: 3,
  });
});
it("uses the real model dispatch fact to count an interrupted initial request", () => {
  expect(
    recoverAssistantTurn(
      rows({
        kind: "model_prompt",
        workId: "work",
        run,
        turn,
        attempt: 1,
        systemPrompt: "instructions",
        contract: { core: "a".repeat(64), extensions: [] },
      }),
    ),
  ).toEqual({ kind: "retry", phase: "dispatched", attempt: 0 });
});
it("keeps completed assistant recovery after retry success", () => {
  const completed = { ...message, value: fauxAssistantMessage("complete") };
  expect(
    recoverAssistantTurn(
      rows(message, scheduled, completed, {
        kind: "live_retry",
        workId: "work",
        event: { type: "auto_retry_end", run, attempt: 3, success: true },
      }),
    ),
  ).toMatchObject({
    kind: "assistant",
    message: { stopReason: "stop" },
    tools: [],
  });
});

it("charges a newer dispatched turn after an earlier retry succeeded", () => {
  expect(
    recoverAssistantTurn(
      rows(
        {
          ...message,
          value: fauxAssistantMessage("tool answer", { stopReason: "toolUse" }),
        },
        {
          kind: "live_retry",
          workId: "work",
          event: { type: "auto_retry_end", run, attempt: 1, success: true },
        },
        { kind: "turn_started", workId: "work", run, turn },
        {
          kind: "model_prompt",
          workId: "work",
          run,
          turn,
          attempt: 1,
          systemPrompt: "Answer.",
          contract: { core: "a".repeat(64), extensions: [] },
        },
      ),
    ),
  ).toEqual({ kind: "retry", phase: "dispatched", attempt: 0 });
});

it.each([true, false])(
  "retains exhausted retry failure across another reopen (previous assistant: %s)",
  (withMessage) => {
    const exhausted = rows(
      ...(withMessage ? [message] : []),
      {
        kind: "model_prompt",
        workId: "work",
        run,
        turn,
        attempt: 1,
        systemPrompt: "instructions",
        contract: { core: "a".repeat(64), extensions: [] },
      },
      { kind: "turn_started", workId: "work", run, turn },
      {
        kind: "live_retry",
        workId: "work",
        event: { type: "auto_retry_end", run, attempt: 0, success: false },
      },
    );
    if (withMessage) {
      expect(recoverAssistantTurn(exhausted)).toMatchObject({
        kind: "assistant",
        message: failed,
      });
      expect(abnormalTerminal(exhausted)).toBeUndefined();
    } else {
      expect(abnormalTerminal(exhausted)).toBe("failed");
    }
    const anotherOpen = rows(...exhausted.map(({ entry }) => entry), {
      kind: "turn_started",
      workId: "work",
      run,
      turn,
    });
    if (withMessage)
      expect(recoverAssistantTurn(anotherOpen)).toMatchObject({
        kind: "assistant",
        message: failed,
      });
    else expect(abnormalTerminal(anotherOpen)).toBe("failed");
    const dispatched = rows(...exhausted.map(({ entry }) => entry), {
      kind: "model_prompt",
      workId: "work",
      run,
      turn,
      attempt: 1,
      systemPrompt: "next turn",
      contract: { core: "a".repeat(64), extensions: [] },
    });
    expect(abnormalTerminal(dispatched)).toBeUndefined();
    expect(recoverAssistantTurn(dispatched)).toMatchObject({
      kind: "retry",
      phase: "dispatched",
      attempt: 0,
    });
  },
);

it("preserves an aborted assistant instead of turning its unsuccessful retry end into failure", () => {
  const aborted = fauxAssistantMessage("", { stopReason: "aborted" });
  const path = rows(
    { ...message, value: aborted },
    {
      kind: "live_retry",
      workId: "work",
      event: { type: "auto_retry_end", run, attempt: 1, success: false },
    },
  );
  expect(abnormalTerminal(path)).toBeUndefined();
  expect(recoverAssistantTurn(path)).toMatchObject({
    kind: "assistant",
    message: aborted,
  });
});
