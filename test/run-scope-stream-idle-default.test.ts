import { createAssistantMessageEventStream } from "@earendil-works/pi-ai";
import { fauxProvider } from "@earendil-works/pi-ai/providers/faux";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { expect, it } from "vitest";

import { make as makeShiitake } from "../src/effect.js";
import type { RuntimeHostEffect, ShiitakeError } from "../src/effect.js";
import {
  createNativeSqliteOwnerFixture,
  noExtensions,
} from "./fixtures/native-sqlite-owner.fixture.js";

// `run-scope.ts` defaults the live stream idle timeout to Mule's
// `piHttpIdleTimeoutMs` (300 000 ms, pinned by `packages/mule/test/
// retry-policy.test.ts`) when the host does not configure
// `modelStreamIdleTimeoutMs`, and honors an explicit override. This proves
// the override path directly with a short real deadline; the mechanism
// itself (`withModelStreamDeadline`) is Mule's, already covered in
// `packages/mule/test/live-controls.test.ts`.
it("honors an explicit modelStreamIdleTimeoutMs host override", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const faux = fauxProvider();
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
    const snapshot = await Effect.runPromise(
      Effect.gen(function* () {
        const agent = yield* makeShiitake({
          store: fixture.owner,
          extensions: noExtensions,
          runtime,
          modelStreamIdleTimeoutMs: 20,
          instructions: {
            load: () => Effect.succeed({ revision: "v1", text: "Answer." }),
          },
          model: {
            acquire: () =>
              Effect.succeed({
                model: faux.getModel(),
                retryable: () => false,
                // Never emits and never ends: only the idle deadline settles it.
                stream: () => createAssistantMessageEventStream(),
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
        const result = yield* session.read();
        yield* agent.close;
        return result;
      }),
    );

    // The override (20 ms) settles the run almost immediately; Mule's
    // undefaulted idle timeout would otherwise never fire on this stream,
    // so the short wall-clock bound itself proves the override reached
    // Mule, not the default.
    expect(Date.now() - startedAt).toBeLessThan(5_000);
    expect(snapshot.activity.queue).toEqual({ state: "idle" });
    expect(snapshot.terminals.at(-1)?.outcome).toMatch(
      /^(failed|interrupted)$/u,
    );
  } finally {
    fixture.close();
  }
}, 10_000);
