import { authorMeta } from "@fungi.computer/agar/agents";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { expect, it, vi } from "vitest";

import {
  parseSessionSnapshot,
  participantSnapshot,
  visitorSnapshot,
  type SessionSnapshot,
  type SessionWatch,
} from "../src/client.js";
import { visitorStream } from "../src/server-visitor-stream.js";

function snapshot(): SessionSnapshot {
  return parseSessionSnapshot(
    JSON.stringify({
      sessionId: "private-session",
      cursor: "private-cursor",
      hasMore: false,
      messages: [
        {
          id: "message-from-visitor",
          sequence: 1,
          commandId: "own-command",
          meta: {
            ...authorMeta({ id: "public-visitor", name: "Visitor" }),
            sourceRequestId: "original-request",
          },
          value: {
            role: "user",
            content: "Visible question",
            timestamp: 1,
          },
        },
        {
          id: "unreleased-reply",
          sequence: 2,
          commandId: "own-command",
          value: fauxAssistantMessage("Private reply"),
        },
      ],
      terminals: [],
      pending: { total: 0, prompts: [], hasMore: false },
      activity: {
        queue: { state: "running", commandId: "own-command" },
        cancellationRequested: false,
        activeTool: "private-tool",
        retry: null,
        lastModelChoiceSequence: 2,
        compaction: null,
      },
      toolOutput: [],
      progress: null,
      extensions: {},
      attention: [],
    }),
  );
}

it("projects only the authorized visitor's own work through the host's reply policy", () => {
  const owner = {
    turnIsCurrent: () => false,
    deliverAgentReply: () => false,
  };
  const visible = visitorSnapshot(
    participantSnapshot(snapshot(), owner),
    "public-visitor",
  );
  const encoded = JSON.stringify(visible);
  expect(visible.sessionId).toBe("public-visitor");
  expect(visible.messages).toHaveLength(1);
  expect(encoded).toContain("Visible question");
  expect(encoded).not.toContain("Private reply");
  expect(encoded).not.toContain("private-session");
  expect(encoded).not.toContain("private-tool");
  expect(visible.activity.queue.state).toBe("idle");
});

it("re-authorizes every 30-second heartbeat and closes a revoked visitor's watch exactly once", async () => {
  vi.useFakeTimers();
  const baseline = snapshot();
  let finishChanges: () => void = () => undefined;
  const changesDone = new Promise<void>((resolve) => {
    finishChanges = resolve;
  });
  const watch: SessionWatch = {
    snapshot: baseline,
    changes: {
      [Symbol.asyncIterator]: () => ({
        next: async () => {
          await changesDone;
          return { done: true as const, value: undefined };
        },
      }),
    },
    close: vi.fn(() => {
      finishChanges();
      return Promise.resolve();
    }),
  };
  const release = vi.fn();
  let admitted = true;
  const canRead = vi.fn(() => Promise.resolve(admitted));
  const reader = visitorStream({
    watch,
    visitorId: "public-visitor",
    filter: (value) =>
      participantSnapshot(value, {
        turnIsCurrent: () => false,
        deliverAgentReply: () => false,
      }),
    expiresAt: Date.now() + 90_000,
    now: Date.now,
    release,
    canRead,
    read: () => Promise.resolve(baseline),
    onError: (event, cause) => {
      throw new Error(event, { cause });
    },
  }).getReader();
  try {
    const first = new TextDecoder().decode((await reader.read()).value);
    expect(first).toContain("event: session");
    expect(first).not.toContain("Private reply");
    expect(canRead).toHaveBeenCalledTimes(1);
    admitted = false;
    await vi.advanceTimersByTimeAsync(30_000);
    expect(canRead).toHaveBeenCalledTimes(2);
    expect(release).toHaveBeenCalledTimes(1);
    expect(watch.close).toHaveBeenCalledTimes(1);
    expect((await reader.read()).done).toBe(true);
  } finally {
    await reader.cancel();
    vi.useRealTimers();
  }
});
