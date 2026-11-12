import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { defineExtension } from "../../src/extensions.js";
import {
  completingRunOptions,
  manualExtensionRuntime,
} from "./extension-host.fixture.js";
import { grantAll } from "./fixture-extensions.js";
import assert from "node:assert/strict";
import { fauxModel } from "@fungi.computer/mule/testing";
import { appendFileSync, writeFileSync } from "node:fs";
import { readSelectedPath } from "../../src/internal/woodstock/session-store.js";
import {
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { requestMessages } from "./model-context.js";
import { Shiitake } from "../../src/index.js";
import { make as makeEffect } from "../../src/effect.js";
import { openPromiseAgent } from "../../src/internal/promise-facade.js";
import {
  compatibilityRunDependencies,
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./native-sqlite-owner.fixture.js";

const [databasePath, mode] = process.argv.slice(2);
if (databasePath === undefined) throw new Error("Database path required");
const fixture = createNativeSqliteOwnerFixture(databasePath);
if (
  mode === "crash-timer-arm" ||
  mode === "crash-timer-recover" ||
  mode === "finish-timer"
) {
  let armed = false;
  const owner = {
    ...fixture.owner,
    transactionSync: <A>(operation: () => A): A => {
      const value = fixture.owner.transactionSync(operation);
      if (
        armed &&
        mode !== "finish-timer" &&
        fixture.owner.sql
          .exec(
            "SELECT job_id FROM watchdog_jobs WHERE lane='extension_timer' AND state='running'",
          )
          .toArray().length > 0
      ) {
        // The claim committed; the extension's consumption transaction has not begun.
        process.send?.("timer-claimed");
        process.kill(process.pid, "SIGSTOP");
      }
      return value;
    },
  };
  const extension = defineExtension("restart-timer").define({
    commands: {
      specs: {
        arm: {
          scope: "agent",
          input: Schema.Struct({}),
          output: Schema.Struct({}),
        },
      },
      handlers: (ctx) => ({
        arm: () => ctx.timers.set("once", 0).pipe(Effect.as({})),
      }),
    },
    hooks: {
      onTimer: () =>
        Effect.sync(() => {
          fixture.owner.sql.exec("UPDATE timer_effect SET fires=fires+1");
        }),
    },
  });
  const host = manualExtensionRuntime();
  const agent = await Effect.runPromise(
    makeEffect(
      completingRunOptions(owner, host.runtime, grantAll([extension])),
    ),
  );
  fixture.owner.sql.exec(
    "CREATE TABLE IF NOT EXISTS timer_effect(fires INTEGER NOT NULL)",
  );
  if (mode === "crash-timer-arm") {
    fixture.owner.sql.exec("INSERT INTO timer_effect VALUES (0)");
    await Effect.runPromise(agent.extensions.call("restart-timer", "arm", {}));
  }
  armed = true;
  await host.drain();
  const facts = {
    fires: fixture.owner.sql.exec("SELECT fires FROM timer_effect").toArray()[0]
      ?.fires,
    pointers: fixture.owner.sql
      .exec("SELECT count(*) AS count FROM shiitake_extension_timers")
      .toArray()[0]?.count,
    jobs: fixture.owner.sql
      .exec(
        "SELECT state,outcome,recovery_count,attempt_count FROM watchdog_jobs WHERE lane='extension_timer'",
      )
      .toArray(),
    pending: await host.pendingWork(),
  };
  await Effect.runPromise(agent.close);
  fixture.close();
  process.send?.(facts);
  process.exit(0);
}
const exhaustZero =
  mode === "crash-exhausted-initial" ||
  mode === "crash-exhausted-decision-zero";
const exhaustOne =
  mode === "crash-exhausted-after-retry" ||
  mode === "crash-exhausted-decision-one";
const stopAtExhaustion =
  mode === "crash-exhausted-decision-zero" ||
  mode === "crash-exhausted-decision-one";
let interceptRetryCommit = false;
// Stop right after a finished summary is pinned, before its placement.
const stopAtPinnedSummary = mode === "crash-pinned";
const store =
  mode === "crash-retry-decision" || stopAtExhaustion || stopAtPinnedSummary
    ? {
        ...fixture.owner,
        transactionSync: <A>(operation: () => A): A => {
          const result = fixture.owner.transactionSync(operation);
          if (stopAtPinnedSummary) {
            const tail = readSelectedPath(fixture.owner, {
              sessionId: "restart",
            }).at(-1)?.entry;
            if (tail?.kind === "compaction_checkpoint") {
              process.send?.("summary-pinned");
              process.kill(process.pid, "SIGSTOP");
            }
            return result;
          }
          if (!interceptRetryCommit) return result;
          const path = readSelectedPath(fixture.owner, {
            sessionId: "restart",
          });
          const retry = path.at(-1)?.entry;
          if (stopAtExhaustion) {
            if (
              retry?.kind !== "live_retry" ||
              retry.event.type !== "auto_retry_end" ||
              retry.event.success
            )
              return result;
            process.send?.("retry-exhausted");
            process.kill(process.pid, "SIGSTOP");
            return result;
          }
          if (
            !path.some(
              ({ entry }) =>
                entry.kind === "message" &&
                entry.value.role === "assistant" &&
                entry.value.stopReason === "error",
            )
          )
            return result;
          writeFileSync(
            databasePath + ".retry-due",
            String(
              retry?.kind === "live_retry" &&
                retry.event.type === "auto_retry_start"
                ? retry.event.dueAt
                : 0,
            ),
          );
          // Stop precisely after the completed error's transaction, before its
          // listener returns or onRetry/backoff can execute. The parent SIGKILLs us.
          process.send?.("retry-decided");
          process.kill(process.pid, "SIGSTOP");
          return result;
        },
      }
    : fixture.owner;
if (mode === "finish-completion")
  fixture.owner.sql.exec("DROP TRIGGER refuse_completion");
const runtime = createCompatibilityRuntimeHost();
const dependencies = compatibilityRunDependencies();
const faux = fauxModel(
  [],
  mode === "crash-compaction" ||
    mode === "crash-summary" ||
    mode === "crash-summary-resume" ||
    mode === "finish-summary" ||
    mode === "crash-pinned" ||
    mode === "finish-pinned" ||
    mode === "compact-recovered"
    ? {
        models: [{ id: "faux-1", contextWindow: 30_000, maxTokens: 100_000 }],
        tokenSize: { min: 100, max: 100 },
      }
    : undefined,
);
let markStarted: () => void = () => undefined;
let releaseFirst: () => void = () => undefined;
const started = new Promise<void>((resolve) => {
  markStarted = resolve;
});
const held = new Promise<void>((resolve) => {
  releaseFirst = resolve;
});
let requests = 0;
faux.setResponses(
  mode === "crash-tool"
    ? [
        fauxAssistantMessage(
          fauxToolCall(
            "execute",
            { code: "return await send();" },
            { id: "send-call" },
          ),
          { stopReason: "toolUse" },
        ),
      ]
    : [fauxAssistantMessage("complete")],
);
if (
  mode === "crash-retry" ||
  mode === "crash-exhausted-after-retry" ||
  mode === "crash-retry-abnormal" ||
  mode === "crash-backoff" ||
  mode === "crash-retry-decision"
)
  faux.setResponses([
    fauxAssistantMessage("failed attempt", {
      stopReason: "error",
      errorMessage: "503 Service Unavailable",
    }),
  ]);
if (
  mode === "crash-compaction" ||
  mode === "crash-summary" ||
  mode === "crash-pinned"
)
  faux.setResponses([
    fauxAssistantMessage("x".repeat(100_000), { stopReason: "length" }),
    fauxAssistantMessage("restart summary"),
  ]);
if (mode === "crash-steer")
  faux.setResponses([
    async () => {
      markStarted();
      await held;
      return fauxAssistantMessage("before steering");
    },
  ]);
if (
  mode === "finish-steer" ||
  mode === "finish-retry" ||
  mode === "finish-compaction"
)
  faux.setResponses([
    (context) => {
      const messages = requestMessages(context);
      if (
        mode === "finish-steer" &&
        !messages.some(
          (m) => m.role === "user" && m.content === "change direction",
        )
      )
        throw new Error("Delivered steering was lost");
      if (
        mode === "finish-retry" &&
        messages.some((m) => m.role === "assistant" && m.stopReason === "error")
      )
        throw new Error("Failed retry attempt leaked into context");
      if (
        mode === "finish-compaction" &&
        !JSON.stringify(messages).includes("restart summary")
      )
        throw new Error("Compaction summary was lost");
      return fauxAssistantMessage("verified recovered context");
    },
  ]);
// After a pinned summary the restart must place it: exactly one request, the
// answer, with the pinned summary in its context and no second summary call.
if (mode === "finish-pinned")
  faux.setResponses([
    (context) => {
      if (!JSON.stringify(requestMessages(context)).includes("restart summary"))
        throw new Error("The pinned summary was not placed");
      return fauxAssistantMessage("verified recovered context");
    },
  ]);
if (mode === "finish-summary")
  faux.setResponses([
    fauxAssistantMessage("restart summary"),
    fauxAssistantMessage("verified recovered context"),
  ]);
if (mode === "fail-context")
  faux.setResponses([
    fauxAssistantMessage("failed after tool result", {
      stopReason: "error",
      errorMessage: "400 Bad Request",
    }),
  ]);
if (mode === "finish-history")
  faux.setResponses([
    (context) => {
      const messages = requestMessages(context);
      if (
        !messages.some(
          (m) =>
            m.role === "assistant" &&
            m.content.some(
              (c) => c.type === "toolCall" && c.id === "send-call",
            ),
        )
      )
        throw new Error("Lost the assistant call after a failed recovery");
      if (
        !messages.some(
          (m) => m.role === "toolResult" && m.toolCallId === "send-call",
        )
      )
        throw new Error("Lost the recovered tool result");
      return fauxAssistantMessage("verified recovered context");
    },
  ]);
if (mode === "compact-recovered")
  faux.setResponses([
    fauxAssistantMessage("x".repeat(100_000)),
    (context) => {
      const serialized = JSON.stringify(requestMessages(context));
      if (
        !serialized.includes("return await send();") ||
        !serialized.includes("may or may not have completed")
      )
        throw new Error("Summarizer lost the recovered assistant/tool pair");
      appendFileSync(databasePath + ".summary", "paired\n");
      return fauxAssistantMessage("paired history summary");
    },
  ]);
// Retry policy is an Effect host option. Use its existing facade adapter for
// these two policy fixtures; the Promise product API intentionally has no knob.
const makeAgent: typeof Shiitake.make =
  exhaustZero || exhaustOne
    ? (options) =>
        openPromiseAgent(options, (effectOptions) =>
          makeEffect({
            ...effectOptions,
            retry: {
              enabled: true,
              maxRetries: exhaustZero ? 0 : 1,
              baseDelayMs: 0,
            },
          }),
        )
    : Shiitake.make;
const agent = await makeAgent({
  ...dependencies,
  store,
  runtime: runtime.runtime,
  tools: {
    ...dependencies.tools,
    sandbox: {
      execute: () => {
        process.send?.("tool-started");
        return new Promise<never>(() => undefined);
      },
    },
  },
  model: {
    acquire: async () => {
      if (mode === "paused")
        throw new Error("Host model acquisition is paused");
      const acquired = await dependencies.model.acquire();
      return {
        ...acquired,
        model: faux.model,
        retryable: () =>
          mode === "crash-exhausted-after-retry" ||
          mode === "crash-retry" ||
          mode === "crash-retry-abnormal" ||
          mode === "crash-backoff" ||
          mode === "crash-retry-decision",
        retryAfterMs: () =>
          mode === "crash-backoff" || mode === "crash-retry-decision"
            ? 10_000
            : undefined,
        release:
          mode === "crash-abnormal" || mode === "crash-retry-abnormal"
            ? () => {
                process.send?.("terminal-saved");
                return new Promise<never>(() => undefined);
              }
            : acquired.release,
        stream: (model, context, options) => {
          requests += 1;
          if (
            exhaustZero ||
            exhaustOne ||
            mode === "finish-exhausted" ||
            mode === "crash-backoff" ||
            mode === "crash-retry-decision" ||
            mode === "crash-retry" ||
            mode === "finish-retry"
          ) {
            const prompt = [
              ...readSelectedPath(fixture.owner, { sessionId: "restart" }),
            ]
              .reverse()
              .find(({ entry }) => entry.kind === "model_prompt");
            if (prompt?.entry.kind !== "model_prompt")
              throw new Error("Missing dispatch fact");
            appendFileSync(
              databasePath + ".retry-requests",
              JSON.stringify({
                at: Date.now(),
                attempt: prompt.entry.attempt,
              }) + "\n",
            );
          }
          if (
            mode === "crash-completion" ||
            mode === "finish-completion" ||
            mode === "finish-pinned"
          )
            appendFileSync(databasePath + ".requests", "request\n");
          if (
            mode === "crash-abnormal" ||
            (mode === "crash-retry-abnormal" && requests === 2)
          )
            throw new Error("transport gone");
          if (mode === "crash-cancel") {
            markStarted();
            return new Promise<never>(() => undefined);
          }
          if (
            mode === "crash-model" ||
            mode === "crash-exhausted-initial" ||
            (mode === "crash-exhausted-after-retry" && requests === 2) ||
            mode === "crash-summary-resume" ||
            ((mode === "crash-steer" || mode === "crash-retry") &&
              requests === 2) ||
            (mode === "crash-compaction" && requests === 3) ||
            (mode === "crash-summary" && requests === 2)
          ) {
            process.send?.("model-started");
            return new Promise<never>(() => undefined);
          }
          return faux.stream(model, context, options);
        },
      };
    },
  },
});
if (mode === "crash-completion")
  fixture.owner.sql.exec(
    "CREATE TRIGGER refuse_completion BEFORE UPDATE OF state ON watchdog_jobs WHEN NEW.state = 'settled' BEGIN SELECT RAISE(ABORT, 'projection unavailable'); END",
  );
if (mode?.startsWith("crash-"))
  await agent.session("restart").command({
    type: "prompt",
    delivery: "followUp",
    requestId: "restart-command",
    text: "Finish after restart",
  });
if (mode === "finish-history")
  await agent.session("restart").command({
    type: "prompt",
    delivery: "followUp",
    requestId: "next-command",
    text: "Continue with the failed history",
  });
if (mode === "cancel") {
  const read = await agent.session("restart").read();
  const commandId = read.messages[0]?.commandId;
  if (commandId === undefined || commandId === null)
    throw new Error("Missing command");
  await agent.session("restart").cancel({ commandId });
}
interceptRetryCommit = mode === "crash-retry-decision" || stopAtExhaustion;
const running = runtime.runNext();
if (mode === "crash-backoff") {
  for (;;) {
    const retry = [...readSelectedPath(fixture.owner, { sessionId: "restart" })]
      .reverse()
      .find(({ entry }) => entry.kind === "live_retry");
    if (
      retry?.entry.kind === "live_retry" &&
      retry.entry.event.type === "auto_retry_start"
    ) {
      writeFileSync(
        databasePath + ".retry-due",
        String(retry.entry.event.dueAt),
      );
      process.send?.("retry-waiting");
      await new Promise<never>(() => undefined);
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}
if (mode === "crash-steer") {
  await started;
  await agent.session("restart").command({
    type: "prompt",
    delivery: "steer",
    requestId: "steer-command",
    text: "change direction",
  });
  releaseFirst();
}
if (mode === "crash-cancel") {
  await started;
  await agent.session("restart").abort();
  process.send?.("cancelled");
  await new Promise<never>(() => undefined);
}
if (mode === "crash-completion") {
  await assert.rejects(running, {
    _tag: "ShiitakeError",
    boundary: "runtime",
    cause: "rejected",
    message: "Runtime reconciliation failed",
  });
  const row = fixture.owner.sql
    .exec("SELECT state, outcome FROM watchdog_jobs")
    .one();
  if (row.state !== "completing" || row.outcome !== "completed")
    throw new Error("Execution decision was lost");
  process.send?.("completion-held");
  await new Promise<never>(() => undefined);
}
await running;
process.send?.(await agent.session("restart").read());
await agent.close();
fixture.close();
process.disconnect();
