import type { AssistantMessage, MuleEvent } from "@fungi.computer/mule/effect";
import * as Effect from "effect/Effect";
import { expect, it } from "vitest";

import type { AcceptedCommand } from "../src/internal/woodstock/session-store.js";
import { openObservationKernel } from "../src/internal/observation-kernel.js";
import {
  openProgress,
  readProcessProgress,
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
        }),
      ),
    );
  } finally {
    observation.close();
    await Effect.runPromise(kernel.close);
    fixture.close();
  }
});
