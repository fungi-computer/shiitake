import { createAssistantMessageEventStream } from "@earendil-works/pi-ai/utils/event-stream";
import { fauxModel } from "@fungi.computer/mule/testing";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { afterEach, expect, it, vi } from "vitest";
import { make } from "../src/effect.js";
import { Shiitake } from "../src/index.js";
import type { RuntimeHostEffect, ShiitakeError } from "../src/effect.js";
import {
  createNativeSqliteOwnerFixture,
  createCompatibilityRuntimeHost,
  noExtensions,
} from "./fixtures/native-sqlite-owner.fixture.js";

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

it.each([false, true])(
  "preserves a stopped retry and records whether cleanup drained: held=%s",
  async (held) => {
    vi.spyOn(Math, "random").mockReturnValue(0);
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
    const fixture = createNativeSqliteOwnerFixture();
    const faux = fauxModel([
      fauxAssistantMessage("", { stopReason: "error", errorMessage: "503" }),
      fauxAssistantMessage("recovered"),
    ]);
    let calls = 0;
    let acquisitions = 0;
    let operation: Effect.Effect<void, ShiitakeError> = Effect.void;
    const runtime: RuntimeHostEffect = {
      bind: (input) =>
        Effect.sync(() => {
          operation = input.reconcile;
          return {
            commit: (mutation) => mutation,
            requestWake: Effect.void,
            close: Effect.void,
          };
        }),
    };
    const open = () =>
      Effect.runPromise(
        make({
          store: fixture.owner,
          runtime,
          extensions: noExtensions,
          instructions: {
            load: () => Effect.succeed({ revision: "stop", text: "Answer." }),
          },
          model: {
            acquire: () => {
              acquisitions++;
              return Effect.succeed({
                model: faux.model,
                retryable: () => true,
                stream: (model, context, options) => {
                  calls++;
                  return faux.stream(model, context, options);
                },
                release:
                  held && acquisitions === 1 ? Effect.never : Effect.void,
              });
            },
          },
        }),
      );
    let agent = await open();
    const sessionId = "stopped-retry";
    const accepted = await Effect.runPromise(
      agent.session(sessionId).command({
        requestId: "command",
        delivery: "followUp",
        type: "prompt",
        text: "Retry.",
      }),
    );
    const row = () =>
      fixture.owner.sql
        .exec(
          "SELECT state, recovery_count FROM watchdog_jobs WHERE job_id = ?",
          accepted.commandId,
        )
        .one();
    try {
      for (let index = 0; index < (held ? 1 : 3); index++) {
        const running = Effect.runFork(operation);
        await vi.waitFor(async () => {
          expect(
            (await Effect.runPromise(agent.session(sessionId).read())).activity
              .retry,
          ).not.toBeNull();
          expect(acquisitions).toBe(index + 1);
        });
        const closing = Effect.runFork(agent.close);
        if (held) {
          await vi.advanceTimersByTimeAsync(1_000);
          await expect(
            Effect.runPromise(Fiber.join(closing)),
          ).rejects.toMatchObject({
            message: "Runtime execution did not drain",
          });
          await expect(
            Effect.runPromise(Fiber.join(running)),
          ).rejects.toBeDefined();
        } else {
          await Effect.runPromise(Fiber.join(closing));
          await Effect.runPromise(Fiber.join(running));
        }
        expect(row()).toEqual({
          state: held ? "running" : "queued",
          recovery_count: 0,
        });
        expect(
          fixture.owner.sql
            .exec(
              "SELECT COUNT(*) AS count FROM session_entries WHERE work_id = ? AND type = 'run_terminal'",
              accepted.commandId,
            )
            .one().count,
        ).toBe(0);
        agent = await open();
      }
      const resumed = Effect.runFork(operation);
      await vi.advanceTimersByTimeAsync(2_000);
      await Effect.runPromise(Fiber.join(resumed));
      expect(row()).toEqual({ state: "settled", recovery_count: held ? 1 : 0 });
      expect(calls).toBe(2);
      expect(acquisitions).toBe(held ? 2 : 4);
    } finally {
      await Effect.runPromise(agent.close);
      fixture.close();
    }
  },
);

it.each([false, true])(
  "waits for the actual model producer on close: cooperative=%s",
  async (cooperative) => {
    const fixture = createNativeSqliteOwnerFixture();
    const faux = fauxModel([]);
    const events = createAssistantMessageEventStream();
    const aborted = fauxAssistantMessage("", { stopReason: "aborted" });
    let markStarted: () => void = () => undefined;
    const started = new Promise<void>((resolve) => {
      markStarted = resolve;
    });
    let operation: Effect.Effect<void, ShiitakeError> = Effect.void;
    const agent = await Effect.runPromise(
      make({
        store: fixture.owner,
        extensions: noExtensions,
        runtime: {
          bind: (input) =>
            Effect.sync(() => {
              operation = input.reconcile;
              return {
                commit: (mutation) => mutation,
                requestWake: Effect.void,
                close: Effect.void,
              };
            }),
        },
        instructions: {
          load: () => Effect.succeed({ revision: "stop", text: "Answer." }),
        },
        model: {
          acquire: () =>
            Effect.succeed({
              model: faux.model,
              retryable: () => true,
              release: Effect.void,
              stream: (_model, _context, options) => {
                options?.signal?.addEventListener(
                  "abort",
                  () => {
                    if (cooperative) events.end(aborted);
                  },
                  { once: true },
                );
                markStarted();
                return events;
              },
            }),
        },
      }),
    );
    const accepted = await Effect.runPromise(
      agent.session("producer").command({
        requestId: "producer",
        delivery: "followUp",
        type: "prompt",
        text: "Wait",
      }),
    );
    const running = Effect.runFork(operation);
    try {
      await started;
      const closing = Effect.runFork(agent.close);
      if (cooperative) {
        await Effect.runPromise(Fiber.join(closing));
        await Effect.runPromise(Fiber.join(running));
      } else {
        await expect(
          Effect.runPromise(Fiber.join(closing)),
        ).rejects.toMatchObject({ message: "Runtime execution did not drain" });
        await expect(
          Effect.runPromise(Fiber.join(running)),
        ).rejects.toBeDefined();
      }
      expect(
        fixture.owner.sql
          .exec(
            "SELECT state, recovery_count FROM watchdog_jobs WHERE job_id = ?",
            accepted.commandId,
          )
          .one(),
      ).toEqual({
        state: cooperative ? "queued" : "running",
        recovery_count: 0,
      });
    } finally {
      events.end(aborted);
      fixture.close();
    }
  },
);

it("drains when a foreign instruction load settles after the stop, releasing its late snapshot", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const faux = fauxModel([]);
  let markStarted: () => void = () => undefined;
  const started = new Promise<void>((resolve) => {
    markStarted = resolve;
  });
  let finishLoad: () => void = () => undefined;
  const loading = new Promise<void>((resolve) => {
    finishLoad = resolve;
  });
  let released = 0;
  const agent = await Shiitake.make({
    store: fixture.owner,
    runtime: runtime.runtime,
    extensions: noExtensions,
    instructions: {
      load: async () => {
        markStarted();
        await loading;
        return {
          revision: "late",
          text: "Late instructions",
          release: () => {
            released++;
          },
        };
      },
    },
    model: {
      acquire: () =>
        Promise.resolve({
          model: faux.model,
          stream: faux.stream,
          retryable: () => false,
          release: () => Promise.resolve(),
        }),
    },
  });
  try {
    const accepted = await agent
      .session("instructions")
      .send("Wait", { requestId: "instructions" });
    const result = runtime.runNext().then(
      () => undefined,
      (error: unknown) => error,
    );
    await started;
    const closing = agent.close();
    // The host's load answers shortly after the stop, well inside the grace.
    setTimeout(finishLoad, 20);
    await expect(closing).resolves.toBeUndefined();
    expect(await result).toBeUndefined();
    expect(released).toBe(1);
    expect(
      fixture.owner.sql
        .exec(
          "SELECT state, recovery_count FROM watchdog_jobs WHERE job_id = ?",
          accepted.commandId,
        )
        .one(),
    ).toEqual({ state: "queued", recovery_count: 0 });
    expect(faux.state.callCount).toBe(0);
  } finally {
    finishLoad();
    fixture.close();
  }
});

it("does not claim drain while foreign instruction acquisition is still pending", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const faux = fauxModel([]);
  let markStarted: () => void = () => undefined;
  const started = new Promise<void>((resolve) => {
    markStarted = resolve;
  });
  let release: () => void = () => undefined;
  const held = new Promise<void>((resolve) => {
    release = resolve;
  });
  const agent = await Shiitake.make({
    store: fixture.owner,
    runtime: runtime.runtime,
    extensions: noExtensions,
    instructions: {
      load: async () => {
        markStarted();
        await held;
        return { revision: "late", text: "Late instructions" };
      },
    },
    model: {
      acquire: () =>
        Promise.resolve({
          model: faux.model,
          stream: faux.stream,
          retryable: () => false,
          release: () => Promise.resolve(),
        }),
    },
  });
  try {
    const accepted = await agent
      .session("instructions")
      .send("Wait", { requestId: "instructions" });
    const result = runtime.runNext().then(
      () => undefined,
      (error: unknown) => error,
    );
    await started;
    await expect(agent.close()).rejects.toMatchObject({
      message: "Runtime execution did not drain",
    });
    expect(await result).toMatchObject({
      message: "Runtime reconciliation failed",
    });
    expect(
      fixture.owner.sql
        .exec(
          "SELECT state, recovery_count FROM watchdog_jobs WHERE job_id = ?",
          accepted.commandId,
        )
        .one(),
    ).toEqual({ state: "running", recovery_count: 0 });
    expect(faux.state.callCount).toBe(0);
  } finally {
    release();
    fixture.close();
  }
});
