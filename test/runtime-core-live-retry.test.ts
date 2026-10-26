import { fauxModel } from "@fungi.computer/mule/testing";
import type { Context } from "@earendil-works/pi-ai";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { inspect } from "node:util";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { afterEach, beforeEach, expect, it, vi } from "vitest";

beforeEach(() =>
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] }),
);
afterEach(() => vi.useRealTimers());

import { requestMessages } from "./fixtures/model-context.js";

import { make as makeShiitake } from "../src/effect.js";
import type {
  RuntimeHostEffect,
  ShiitakeError,
  SqliteCursor,
  SqliteOwner,
  SqliteValue,
} from "../src/effect.js";
import { readSelectedPath } from "../src/internal/woodstock/session-store.js";
import {
  createNativeSqliteOwnerFixture,
  noExtensions,
} from "./fixtures/native-sqlite-owner.fixture.js";
import { requireAcceptedWorkId } from "./fixtures/accepted-work.fixture.js";

// A run's own single terminal write (not one per retry attempt: the retry
// loop now lives inside Mule, inside one run) is what this fails.
function failFirstRunTerminal(source: SqliteOwner): SqliteOwner {
  let terminalCount = 0;
  return {
    sql: {
      exec: (statement: string, ...bindings: SqliteValue[]): SqliteCursor => {
        if (
          statement.includes("INSERT INTO session_entries") &&
          bindings[4] === "run_terminal"
        ) {
          terminalCount += 1;
          if (terminalCount === 1)
            throw new Error("Injected first-run terminal failure");
        }
        return source.sql.exec(statement, ...bindings);
      },
    },
    transactionSync: <A>(operation: () => A): A =>
      source.transactionSync(operation),
  };
}

it("keeps retained Pi retry inside one Watchdog execution and records it", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const faux = fauxModel([]);
  const requests: Context[] = [];
  const privateText = "503 Service Unavailable; sk-live-private";
  faux.setResponses([
    fauxAssistantMessage("", {
      stopReason: "error",
      errorMessage: privateText,
    }),
    fauxAssistantMessage("recovered answer", {
      responseId: "recovered-provider-response",
    }),
  ]);
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
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agent = yield* makeShiitake({
          store: fixture.owner,
          extensions: noExtensions,
          runtime,
          instructions: {
            load: () =>
              Effect.succeed({
                revision: "retained-live-retry-v1",
                text: "Answer concisely.",
              }),
          },
          model: {
            acquire: () =>
              Effect.succeed({
                model: faux.model,
                retryable: () => true,
                stream: (model, context, options) => {
                  requests.push(structuredClone(context));
                  return faux.stream(model, context, options);
                },
                release: Effect.void,
              }),
          },
        });
        const session = agent.session("retained-live-retry-session");
        const accepted = yield* session.command({
          requestId: "retained-live-retry-command",
          delivery: "followUp",

          type: "prompt",
          text: "Recover this answer.",
        });
        const operation = reconcile;
        if (operation === undefined)
          return yield* Effect.die("Runtime did not bind its reconciler");
        const fiber = yield* Effect.forkChild(operation);
        yield* Effect.promise(() =>
          vi.waitFor(async () => {
            expect(
              (await Effect.runPromise(session.read())).activity.retry,
            ).not.toBeNull();
          }),
        );
        const retrySnapshot = yield* session.read();
        yield* Effect.promise(() => vi.advanceTimersByTimeAsync(2_000));
        yield* Fiber.join(fiber);
        const snapshot = yield* session.read();
        yield* agent.close;
        return { accepted, retrySnapshot, snapshot };
      }),
    );

    expect(requests).toHaveLength(2);
    expect(result.retrySnapshot.activity).toMatchObject({
      queue: { state: "running" },
      retry: {
        attempt: 1,
        maxAttempts: 3,
        delayMs: 2_000,
      },
    });
    expect(result.retrySnapshot.activity.retry?.errorMessage).toContain(
      "reason_unavailable: run=",
    );
    expect(result.retrySnapshot.activity.retry?.errorMessage).toContain(
      "Model request failed. Retry the request.",
    );
    expect(result.retrySnapshot.activity.retry?.errorMessage).toContain(
      "provider=faux; model=" + faux.model.id,
    );
    expect(requestMessages(requests[1])).toEqual([
      expect.objectContaining({
        role: "user",
        content: "Recover this answer.",
      }),
    ]);
    expect(result.snapshot.messages.map(({ value }) => value.role)).toEqual([
      "user",
      "assistant",
      "assistant",
    ]);
    expect(result.snapshot.activity.queue).toEqual({ state: "idle" });
    expect(result.snapshot.activity.retry).toBeNull();
    const retainedEntries = readSelectedPath(fixture.owner, {
      sessionId: "retained-live-retry-session",
    });
    const workId = requireAcceptedWorkId(
      fixture.owner,
      "retained-live-retry-session",
      result.accepted.commandId,
    );
    // Both attempts now run inside one Mule run/turn: Pi's retry loop is
    // around each assistant call, not around a whole durable command.
    expect(retainedEntries.map(({ entry }) => entry.kind)).toEqual([
      "command",
      "run_started",
      "turn_started",
      "model_prompt",
      "message",
      "live_retry",
      "live_retry",
      "model_prompt",
      "message",
      "live_retry",
      "run_terminal",
    ]);
    expect(
      retainedEntries
        .filter(({ entry }) => entry.kind === "model_prompt")
        .map(({ entry }) => entry),
    ).toMatchObject([
      { attempt: 1, systemPrompt: "Answer concisely." },
      { attempt: 2, systemPrompt: "Answer concisely." },
    ]);
    const retryEntries = retainedEntries.filter(
      ({ entry }) => entry.kind === "live_retry",
    );
    expect(retryEntries).toHaveLength(3);
    expect(result.retrySnapshot.activity.retry?.scheduledAt).toBe(
      retryEntries[0]?.timestamp,
    );
    expect(retryEntries[0]?.entry).toMatchObject({
      workId,
      event: {
        type: "auto_retry_start",
        attempt: 1,
        maxAttempts: 3,
        delayMs: 2_000,
      },
    });
    expect(JSON.stringify(retryEntries[0]?.entry)).toContain(
      "reason_unavailable: run=",
    );
    expect(
      result.snapshot.messages.find(({ value }) => value.role === "assistant")
        ?.value,
    ).toMatchObject({
      errorMessage: privateText,
    });
    expect(JSON.stringify(retryEntries)).not.toContain("sk-live-private");
    expect(JSON.stringify(result.retrySnapshot.activity)).not.toContain(
      "sk-live-private",
    );
    expect(retryEntries[1]?.entry).toMatchObject({
      workId,
      event: { type: "auto_retry_attempt", attempt: 1 },
    });
    expect(retryEntries[2]?.entry).toMatchObject({
      workId,
      event: { type: "auto_retry_end", attempt: 1, success: true },
    });
    expect(
      fixture.owner.sql
        .exec(
          "SELECT state, outcome, recovery_count FROM watchdog_jobs WHERE job_id = ?",
          workId,
        )
        .one(),
    ).toEqual({ state: "settled", outcome: "completed", recovery_count: 0 });
  } finally {
    fixture.close();
  }
}, 20_000);

it("settles exhausted provider retries as a typed stopped cause", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const faux = fauxModel([]);
  const privateText = "503 Service Unavailable; sk-live-private-exhausted";
  const logs = vi.spyOn(console, "log").mockImplementation(() => undefined);
  // Pi coding-agent's default policy (`piCodingAgentRetryDefaults`): 3
  // retries, 4 attempts total.
  faux.setResponses(
    Array.from({ length: 4 }, () =>
      fauxAssistantMessage("", {
        stopReason: "error",
        errorMessage: privateText,
      }),
    ),
  );
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
    const snapshot = await Effect.runPromise(
      Effect.gen(function* () {
        const agent = yield* makeShiitake({
          store: fixture.owner,
          extensions: noExtensions,
          runtime,
          instructions: {
            load: () =>
              Effect.succeed({
                revision: "exhausted-retry-v1",
                text: "Answer.",
              }),
          },
          model: {
            acquire: () =>
              Effect.succeed({
                model: faux.model,
                retryable: () => true,
                stream: faux.stream,
                release: Effect.void,
              }),
          },
        });
        const session = agent.session("exhausted-retry-session");
        yield* session.command({
          requestId: "exhausted-retry-command",
          delivery: "followUp",
          type: "prompt",
          text: "Try once.",
        });
        const operation = reconcile;
        if (operation === undefined)
          return yield* Effect.die("Runtime did not bind its reconciler");
        const fiber = yield* Effect.forkChild(operation);
        for (let attempt = 1; attempt <= 3; attempt += 1) {
          yield* Effect.promise(() =>
            vi.waitFor(async () => {
              expect(
                (await Effect.runPromise(session.read())).activity.retry
                  ?.attempt,
              ).toBe(attempt);
            }),
          );
          yield* Effect.promise(() =>
            vi.advanceTimersByTimeAsync(2_000 * 2 ** (attempt - 1)),
          );
        }
        yield* Fiber.join(fiber);
        const result = yield* session.read();
        yield* agent.close;
        return result;
      }),
    );
    expect(snapshot.activity.queue).toEqual({ state: "idle" });
    // Pi reports provider prose verbatim. Retry progress is not authority to
    // copy that potentially private text into a durable failure diagnostic.
    expect(snapshot.terminals).toHaveLength(1);
    expect(snapshot.terminals[0]).toMatchObject({
      outcome: "failed",
    });
    expect(snapshot.terminals[0]?.diagnostic).toContain(
      "reason_unavailable: run=mule_run_",
    );
    expect(snapshot.terminals[0]?.diagnostic).toContain("provider=faux");
    expect(snapshot.terminals[0]?.diagnostic).toContain(
      `model=${faux.model.id}`,
    );
    expect(snapshot.terminals[0]?.diagnostic).not.toContain(privateText);
    expect(snapshot.terminals[0]?.diagnostic?.length).toBeLessThanOrEqual(200);
    expect(
      snapshot.messages.filter(({ value }) => value.role === "assistant"),
    ).toHaveLength(4);
    expect(
      snapshot.messages.filter(({ value }) => value.role === "assistant").at(-1)
        ?.value,
    ).toMatchObject({ errorMessage: privateText });
    expect(JSON.stringify(snapshot.activity)).not.toContain(privateText);
    expect(JSON.stringify(logs.mock.calls)).not.toContain(privateText);
    expect(inspect(logs.mock.calls, { depth: null })).not.toContain(
      privateText,
    );
    const entries = readSelectedPath(fixture.owner, {
      sessionId: "exhausted-retry-session",
    });
    expect(
      JSON.stringify(
        entries.filter(({ entry }) => entry.kind === "live_retry"),
      ),
    ).not.toContain(privateText);
  } finally {
    logs.mockRestore();
    fixture.close();
  }
}, 30_000);

it("finishes the owned typed retry before the cancelled run settles", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const faux = fauxModel([]);
  faux.setResponses([
    fauxAssistantMessage("", {
      stopReason: "error",
      errorMessage: "503 Service Unavailable",
    }),
    fauxAssistantMessage("must not run"),
  ]);
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
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agent = yield* makeShiitake({
          store: fixture.owner,
          extensions: noExtensions,
          runtime,
          instructions: {
            load: () =>
              Effect.succeed({
                revision: "retained-live-retry-cancel-v1",
                text: "Answer concisely.",
              }),
          },
          model: {
            acquire: () =>
              Effect.succeed({
                model: faux.model,
                retryable: () => true,
                stream: faux.stream,
                release: Effect.void,
              }),
          },
        });
        const session = agent.session("retained-live-retry-cancel-session");
        const accepted = yield* session.command({
          requestId: "retained-live-retry-cancel-command",
          delivery: "followUp",

          type: "prompt",
          text: "Begin, then wait.",
        });
        const operation = reconcile;
        if (operation === undefined)
          return yield* Effect.die("Runtime did not bind its reconciler");
        const reconcileFiber = yield* Effect.forkChild(operation);
        yield* Effect.promise(() =>
          vi.waitFor(async () => {
            expect(
              (await Effect.runPromise(session.read())).activity.retry,
            ).not.toBeNull();
          }),
        );
        yield* session.abort;
        yield* Fiber.join(reconcileFiber);
        const snapshot = yield* session.read();
        yield* agent.close;
        return { accepted, snapshot };
      }),
    );

    expect(faux.state.callCount).toBe(1);
    expect(result.snapshot.activity.queue).toEqual({ state: "idle" });
    const retainedEntries = readSelectedPath(fixture.owner, {
      sessionId: "retained-live-retry-cancel-session",
    });
    const workId = requireAcceptedWorkId(
      fixture.owner,
      "retained-live-retry-cancel-session",
      result.accepted.commandId,
    );
    // Mule owns the interruptible backoff. Its interruption finalizer records
    // retry completion before the enclosing run can record its terminal.
    expect(retainedEntries.map(({ entry }) => entry.kind)).toEqual([
      "command",
      "run_started",
      "turn_started",
      "model_prompt",
      "message",
      "live_retry",
      "cancellation",
      "live_retry",
      "run_terminal",
    ]);
    const retryEntries = retainedEntries.filter(
      ({ entry }) => entry.kind === "live_retry",
    );
    expect(retryEntries[1]?.entry).toMatchObject({
      kind: "live_retry",
      workId,
      event: {
        type: "auto_retry_end",
        attempt: 1,
        success: false,
        // The owned retry reports the failed attempt that opened its backoff,
        // not a cancellation phrase or a fabricated second provider attempt.
      },
    });
    expect(JSON.stringify(retryEntries.at(-1)?.entry)).toContain(
      "reason_unavailable: run=",
    );
    expect(
      fixture.owner.sql
        .exec(
          "SELECT state, outcome FROM watchdog_jobs WHERE job_id = ?",
          workId,
        )
        .one(),
    ).toEqual({ state: "settled", outcome: "cancelled" });
  } finally {
    fixture.close();
  }
});

it("does not admit a retried message whose own run terminal failed", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const owner = failFirstRunTerminal(fixture.owner);
  const faux = fauxModel([]);
  const requests: Context[] = [];
  faux.setResponses([
    fauxAssistantMessage("", {
      stopReason: "error",
      errorMessage: "503 Service Unavailable",
    }),
    fauxAssistantMessage("uncommitted retried answer"),
    (context) => {
      requests.push(structuredClone(context));
      return fauxAssistantMessage("replacement answer");
    },
  ]);
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
    await Effect.runPromise(
      Effect.gen(function* () {
        const agent = yield* makeShiitake({
          store: owner,
          extensions: noExtensions,
          runtime,
          instructions: {
            load: () =>
              Effect.succeed({
                revision: "retry-terminal-order-v1",
                text: "Answer concisely.",
              }),
          },
          model: {
            acquire: () =>
              Effect.succeed({
                model: faux.model,
                retryable: () => true,
                stream: faux.stream,
                release: Effect.void,
              }),
          },
        });
        const session = agent.session("retry-terminal-order-session");
        yield* session.command({
          requestId: "retry-terminal-order-first",
          delivery: "followUp",

          type: "prompt",
          text: "First prompt.",
        });
        const operation = reconcile;
        if (operation === undefined)
          return yield* Effect.die("Runtime did not bind its reconciler");
        const firstRun = yield* Effect.forkChild(operation);
        yield* Effect.promise(() =>
          vi.waitFor(async () => {
            expect(
              (await Effect.runPromise(session.read())).activity.retry,
            ).not.toBeNull();
          }),
        );
        yield* Effect.promise(() => vi.advanceTimersByTimeAsync(2_000));
        yield* Fiber.join(firstRun);

        yield* session.command({
          requestId: "retry-terminal-order-second",
          delivery: "followUp",

          type: "prompt",
          text: "Second prompt.",
        });
        yield* operation;
        yield* agent.close;
      }),
    );

    expect(requests).toHaveLength(1);
    expect(requests[0]?.messages).not.toContainEqual(
      expect.objectContaining({
        role: "assistant",
        content: [{ type: "text", text: "uncommitted retried answer" }],
      }),
    );
  } finally {
    fixture.close();
  }
}, 20_000);

/** One run whose post-turn steering read fails, and what it left behind. */
async function runWithSteeringFailure() {
  const fixture = createNativeSqliteOwnerFixture();
  const sessionId = "steering-failure-session";
  let steeringReads = 0;
  // Fail only the post-turn steering read: the first read precedes run_start,
  // where Mule owes no terminal.
  const owner: SqliteOwner = {
    transactionSync: (operation) => fixture.owner.transactionSync(operation),
    sql: {
      exec: (statement, ...bindings) => {
        if (
          statement.includes("json_extract(a.data_json, '$.delivery') = ?") &&
          bindings[1] === "steer" &&
          (steeringReads += 1) >= 2
        )
          throw new Error("Injected steering delivery failure");
        return fixture.owner.sql.exec(statement, ...bindings);
      },
    },
  };
  const faux = fauxModel([]);
  faux.setResponses([fauxAssistantMessage("done")]);
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
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agent = yield* makeShiitake({
          store: owner,
          extensions: noExtensions,
          runtime,
          instructions: {
            load: () =>
              Effect.succeed({
                revision: "steering-failure-v1",
                text: "Answer.",
              }),
          },
          model: {
            acquire: () =>
              Effect.succeed({
                model: faux.model,
                retryable: () => false,
                stream: faux.stream,
                release: Effect.void,
              }),
          },
        });
        const session = agent.session(sessionId);
        yield* session.command({
          requestId: "steering-failure-command",
          delivery: "followUp",
          type: "prompt",
          text: "Run once.",
        });
        const operation = reconcile;
        if (operation === undefined)
          return yield* Effect.die("Runtime did not bind its reconciler");
        yield* Effect.exit(operation);
        yield* agent.close;
        const workId = requireAcceptedWorkId(
          fixture.owner,
          sessionId,
          "steering-failure-command",
        );
        return {
          terminals: readSelectedPath(fixture.owner, { sessionId }).flatMap(
            ({ entry }) =>
              entry.workId === workId && entry.kind === "run_terminal"
                ? [entry.outcome]
                : [],
          ),
          job: fixture.owner.sql
            .exec("SELECT * FROM watchdog_jobs WHERE job_id = ?", workId)
            .one(),
        };
      }),
    );
    expect(steeringReads).toBeGreaterThanOrEqual(2);
    return result;
  } finally {
    fixture.close();
  }
}

it("ends a run with one failed terminal when steering delivery fails mid-run", async () => {
  expect((await runWithSteeringFailure()).terminals).toEqual(["failed"]);
});

it("records why a failed run failed on its durable Watchdog job", async () => {
  expect((await runWithSteeringFailure()).job).toMatchObject({
    outcome: "failed",
    failure_reason: "runtime:rejected",
  });
});
