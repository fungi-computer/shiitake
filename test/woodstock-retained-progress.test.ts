import type { AssistantMessage, MuleEvent } from "@fungi.computer/mule/effect";
import * as Effect from "effect/Effect";
import { expect, it } from "vitest";

import type { AcceptedCommand } from "../src/internal/woodstock/session-store.js";
import { openObservationKernel } from "../src/internal/observation-kernel.js";
import {
  openProgress,
  readProcessProgress,
  readProcessProgressDelta,
  progressPosition,
} from "../src/internal/woodstock/progress.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

const accepted: AcceptedCommand = {
  sessionId: "retained-progress-session",
  requestId: "retained-progress-request",
  commandId: "retained-progress-work",
};
const run = { kind: "run", value: "retained-progress-run" } as const;
const turn = {
  kind: "turn",
  value: "retained-progress-turn",
  parent: run,
} as const;
const message = {
  kind: "message",
  value: "retained-progress-message",
  parent: turn,
} as const;
const partial: AssistantMessage = {
  role: "assistant",
  content: [{ type: "text", text: "partial answer" }],
  api: "anthropic-messages",
  provider: "anthropic",
  model: "claude-test",
  usage: {
    input: 0,
    output: 0,
    cacheRead: 0,
    cacheWrite: 0,
    totalTokens: 0,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
  },
  stopReason: "pending",
  timestamp: 1,
};
const started: Extract<MuleEvent, { type: "message_start" }> = {
  type: "message_start",
  run,
  turn,
  message,
  value: partial,
};
const updated: Extract<MuleEvent, { type: "message_update" }> = {
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
};

it("keeps model progress process-local without per-update SQLite writes", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const kernel = openObservationKernel(fixture.owner);
  const observation = await Effect.runPromise(
    kernel.register(accepted.sessionId),
  );
  try {
    const writesBefore = fixture.ownerWrites();
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const progress = yield* openProgress(
            fixture.owner,
            accepted,
            () => Effect.void,
          );
          yield* progress.started(started);
          yield* progress.update(updated);

          expect(
            readProcessProgress(fixture.owner, accepted.sessionId),
          ).toMatchObject({
            revision: 1,
            text: "partial answer",
          });
          expect(yield* observation.pollSignal).toBe("progress");
          expect(fixture.ownerWrites()).toBe(writesBefore);
          const initial = readProcessProgressDelta(
            fixture.owner,
            accepted.sessionId,
            accepted.commandId,
            null,
          );
          expect(initial).toMatchObject({
            text: { offset: 0, append: "partial answer" },
            thinking: { offset: 0, append: "" },
          });
          const position = progressPosition(
            readProcessProgress(fixture.owner, accepted.sessionId),
          );
          expect(
            readProcessProgressDelta(
              fixture.owner,
              accepted.sessionId,
              accepted.commandId,
              position,
            ),
          ).toBeNull();

          // Pi already supplies the append. Its cumulative partial is not the
          // progress source (reading it on every token is quadratic).
          yield* progress.update({
            ...updated,
            event: {
              ...updated.event,
              type: "text_delta",
              contentIndex: 0,
              delta: "!",
              partial,
            },
          });
          expect(
            readProcessProgress(fixture.owner, accepted.sessionId)?.text,
          ).toBe("partial answer!");
          expect(
            readProcessProgressDelta(
              fixture.owner,
              accepted.sessionId,
              accepted.commandId,
              position,
            ),
          ).toMatchObject({ text: { offset: 14, append: "!" } });
          expect(yield* observation.pollSignal).toBeUndefined();
          // A provider may pause before filling the next wire chunk. The
          // small append must still become visible without another token.
          yield* Effect.promise(
            () => new Promise<void>((resolve) => setTimeout(resolve, 150)),
          );
          expect(yield* observation.pollSignal).toBe("progress");
          for (let index = 0; index < 40; index++)
            yield* progress.update({
              ...updated,
              event: {
                type: "text_delta",
                contentIndex: 0,
                delta: "🌱".repeat(1024),
                partial,
              },
            });
          const tail = readProcessProgress(fixture.owner, accepted.sessionId);
          expect(tail?.textTruncated).toBe(true);
          expect(
            new TextEncoder().encode(tail?.text).byteLength,
          ).toBeLessThanOrEqual(65_536);
          expect(tail?.textOffset).toBeGreaterThan(0);
          expect(
            readProcessProgressDelta(
              fixture.owner,
              accepted.sessionId,
              accepted.commandId,
              position,
            ),
          ).toBeUndefined();
          expect(fixture.ownerWrites()).toBe(writesBefore);
        }),
      ),
    );
  } finally {
    observation.close();
    await Effect.runPromise(kernel.close);
    fixture.close();
  }
});
