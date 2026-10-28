import { fauxModel } from "@fungi.computer/mule/testing";
import { createAssistantMessageEventStream } from "@earendil-works/pi-ai";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { expect, it } from "vitest";

import { make as makeShiitake } from "../src/effect.js";
import type { RuntimeHostEffect, ShiitakeError } from "../src/effect.js";
import {
  createNativeSqliteOwnerFixture,
  noExtensions,
} from "./fixtures/native-sqlite-owner.fixture.js";

// Mule owns the live stream deadlines and honors a tighter host
// `modelStreamIdleTimeoutMs` override. This proves the override and Session
// continuation through Shiitake; Mule tests the four deadline modes directly.
it("bounds four attempts then continues the same Session without replaying failed assistant turns", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const faux = fauxModel([fauxAssistantMessage("recovered")]);
  let attempts = 0;
  const followUpAssistantTurns: unknown[] = [];
  let reconcile: Effect.Effect<void, ShiitakeError> | undefined;
  const runtime: RuntimeHostEffect = {
    bind: (input) =>
      Effect.sync(() => {
        reconcile = input.reconcile;
        return Object.freeze({
          commit: (mutation) => mutation,
          requestWake: Effect.void,
          close: Effect.void,
        });
      }),
  };
  try {
    const startedAt = Date.now();
    const [failed, recovered] = await Effect.runPromise(
      Effect.gen(function* () {
        const agent = yield* makeShiitake({
          store: fixture.owner,
          extensions: noExtensions,
          runtime,
          modelStreamIdleTimeoutMs: 20,
          retry: {
            enabled: true,
            maxRetries: 3,
            baseDelayMs: 1,
            maxAgentDelayMs: 1,
          },
          instructions: {
            load: () => Effect.succeed({ revision: "v1", text: "Answer." }),
          },
          model: {
            acquire: () =>
              Effect.succeed({
                model: faux.model,
                retryable: () => false,
                stream: (model, context, options) => {
                  attempts++;
                  if (attempts > 4) {
                    followUpAssistantTurns.push(
                      ...context.messages.filter(
                        (message) => message.role === "assistant",
                      ),
                    );
                    return faux.stream(model, context, options);
                  }
                  // Never emits or ends: Mule's first-event deadline settles it.
                  return createAssistantMessageEventStream();
                },
                release: Effect.void,
              }),
          },
        });
        const session = agent.session("stream-idle-override-session");
        yield* session.command({
          requestId: "stream-idle-override-command",
          delivery: "followUp",
          type: "prompt",
          text: "Hang forever.",
        });
        const operation = reconcile;
        if (operation === undefined) return yield* Effect.die("no reconciler");
        const fiber = yield* Effect.forkChild(operation);
        yield* Fiber.join(fiber);
        const first = yield* session.read();
        yield* session.command({
          requestId: "stream-idle-override-follow-up",
          delivery: "followUp",
          type: "prompt",
          text: "Continue.",
        });
        const followUp = yield* Effect.forkChild(operation);
        yield* Fiber.join(followUp);
        const second = yield* session.read();
        yield* agent.close;
        return [first, second] as const;
      }),
    );

    // The override (20 ms) settles four attempts well before Mule's default
    // first-event deadline (60 s), proving the host's override reached Mule.
    expect(Date.now() - startedAt).toBeLessThan(8_000);
    expect(attempts).toBe(5);
    expect(failed.activity.queue).toEqual({ state: "idle" });
    expect(failed.terminals.at(-1)?.outcome).toBe("failed");
    expect(failed.terminals.at(-1)?.diagnostic).toMatch(
      /failed 4 times on this conversation: Model first event timed out after \d+ ms/u,
    );
    expect(followUpAssistantTurns).toEqual([]);
    expect(recovered.terminals.at(-1)?.outcome).toBe("completed");
    expect(
      recovered.messages.some(
        (message) =>
          message.value.role === "assistant" &&
          message.value.content.some(
            (part) => part.type === "text" && part.text === "recovered",
          ),
      ),
    ).toBe(true);
  } finally {
    fixture.close();
  }
}, 10_000);

it("does not turn a typed usage limit after a transient retry into a failed-N-times summary", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const faux = fauxModel([]);
  const reason =
    "Anthropic usage limit reached on your Claude subscription; resets at 16:00.";
  const limited = fauxAssistantMessage("", {
    stopReason: "error",
    errorMessage: reason,
  });
  let attempts = 0;
  let reconcile: Effect.Effect<void, ShiitakeError> | undefined;
  const runtime: RuntimeHostEffect = {
    bind: (input) =>
      Effect.sync(() => {
        reconcile = input.reconcile;
        return {
          commit: (mutation) => mutation,
          requestWake: Effect.void,
          close: Effect.void,
        };
      }),
  };
  try {
    const snapshot = await Effect.runPromise(
      Effect.gen(function* () {
        const agent = yield* makeShiitake({
          store: fixture.owner,
          extensions: noExtensions,
          runtime,
          retry: { enabled: true, maxRetries: 3, baseDelayMs: 0 },
          instructions: {
            load: () => Effect.succeed({ revision: "v1", text: "Answer." }),
          },
          model: {
            acquire: () =>
              Effect.succeed({
                model: faux.model,
                retryable: (message) => message !== limited,
                rejection: (message) =>
                  message === limited
                    ? { code: "usage-limit-reached" as const }
                    : undefined,
                failureReason: (message) =>
                  message === limited ? reason : undefined,
                stream: () => {
                  attempts++;
                  const message =
                    attempts === 1
                      ? fauxAssistantMessage("", {
                          stopReason: "error",
                          errorMessage: "Anthropic is unavailable.",
                        })
                      : limited;
                  const stream = createAssistantMessageEventStream();
                  stream.push({
                    type: "error",
                    reason: "error",
                    error: message,
                  });
                  stream.end(message);
                  return stream;
                },
                release: Effect.void,
              }),
          },
        });
        const session = agent.session("usage-limit-summary");
        yield* session.command({
          type: "prompt",
          requestId: "usage-limit-command",
          delivery: "followUp",
          text: "Hello.",
        });
        if (reconcile === undefined) return yield* Effect.die("no reconciler");
        yield* reconcile;
        const result = yield* session.read();
        yield* agent.close;
        return result;
      }),
    );
    expect(attempts).toBe(2);
    expect(snapshot.terminals.at(-1)?.diagnostic).toBe(reason);
  } finally {
    fixture.close();
  }
});
