import { idleActivity, runningActivity } from "./fixtures/client.fixture.js";
import type { AssistantMessage, MuleEvent } from "@fungi.computer/mule/effect";
import * as Effect from "effect/Effect";
import { expect, it } from "vitest";

import { readClientSnapshot } from "../src/internal/woodstock/client-projection.js";
import {
  openMuleRecorder,
  loadAcceptedRun,
  prepareWoodstockSchema,
} from "../src/internal/woodstock/index.js";
import { readProcessProgress } from "../src/internal/woodstock/progress.js";
import { readSelectedPath } from "../src/internal/woodstock/session-store.js";
import { admitStoredCommand } from "./fixtures/admission-door.fixture.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

const digest = "a".repeat(64);
const contract = {
  core: digest,
  extensions: [],
} as const;
const run = { kind: "run", value: "recorder-run" } as const;
const turn = { kind: "turn", value: "recorder-turn", parent: run } as const;
const message = {
  kind: "message",
  value: "recorder-message",
  parent: turn,
} as const;

function assistant(text: string, stopReason: "pending" | "stop") {
  return {
    role: "assistant",
    content: [{ type: "text", text, textSignature: "signed-text" }],
    api: "anthropic-messages",
    provider: "anthropic",
    model: "claude-test",
    responseId: "response-recorder",
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
  } satisfies AssistantMessage;
}

it("records one Mule run as exact durable messages plus bounded live progress", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const sessionId = "retained-mule-recorder";
    const accepted = admitStoredCommand(
      fixture.owner,
      sessionId,
      {
        requestId: "recorder-request",
        delivery: "followUp",

        type: "prompt",
        text: "first question",
      },
      {
        contract,
        enqueue: () => undefined,
      },
    ).accepted;
    const partial = assistant("partial answer", "pending");
    const completed = assistant("first answer", "stop");

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          yield* loadAcceptedRun(fixture.owner, {
            sessionId,
            commandFactId: accepted.commandId,
          });
          const record = yield* openMuleRecorder(
            fixture.owner,
            accepted,
            contract,
          );
          const events: readonly MuleEvent[] = [
            { type: "run_start", run },
            { type: "turn_start", run, turn },
            {
              type: "message_start",
              run,
              turn,
              message,
              value: partial,
            },
            {
              type: "message_update",
              run,
              turn,
              message,
              value: partial,
              event: {
                type: "text_delta",
                contentIndex: 0,
                delta: "partial answer",
                partial,
              },
            },
          ];
          yield* Effect.forEach(events, record, { discard: true });

          expect(readProcessProgress(fixture.owner, sessionId)?.text).toBe(
            "partial answer",
          );
          const liveProgress = readProcessProgress(fixture.owner, sessionId);
          expect(
            readClientSnapshot(
              fixture.owner,
              sessionId,
              { limit: 200 },
              { facts: runningActivity, progress: liveProgress },
            ),
          ).toMatchObject({
            messages: [{ value: { role: "user", content: "first question" } }],
            activity: { queue: { state: "running" } },
          });

          yield* record({
            type: "message_end",
            run,
            turn,
            message,
            value: completed,
          });
          yield* record({
            type: "run_end",
            run,
            terminal: {
              kind: "terminal",
              value: "recorder-terminal",
              parent: run,
            },
            outcome: "completed",
          });

          const snapshot = readClientSnapshot(
            fixture.owner,
            sessionId,
            { limit: 200 },
            { facts: idleActivity, progress: null },
          );
          expect(snapshot.progress).toBeNull();
          expect(snapshot.activity.queue.state).toBe("idle");
          const messages = snapshot.messages.map(({ value }) => value);
          expect(messages).toHaveLength(2);
          expect(messages[0]).toMatchObject({
            role: "user",
            content: "first question",
          });
          expect(messages[0]?.timestamp).toEqual(expect.any(Number));
          expect(messages[1]).toEqual(completed);
          expect(
            readSelectedPath(fixture.owner, { sessionId }).map(
              ({ entry }) => entry.kind,
            ),
          ).toEqual([
            "command",
            "run_started",
            "turn_started",
            "message",
            "run_terminal",
          ]);
        }),
      ),
    );
  } finally {
    fixture.close();
  }
});
