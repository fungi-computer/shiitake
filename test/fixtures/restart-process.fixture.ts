import { fauxModel } from "@fungi.computer/mule/testing";
import { appendFileSync } from "node:fs";
import {
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { requestMessages } from "./model-context.js";
import { Shiitake } from "../../src/index.js";
import {
  compatibilityRunDependencies,
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./native-sqlite-owner.fixture.js";

const [databasePath, mode] = process.argv.slice(2);
if (databasePath === undefined) throw new Error("Database path required");
const fixture = createNativeSqliteOwnerFixture(databasePath);
const runtime = createCompatibilityRuntimeHost();
const dependencies = compatibilityRunDependencies();
const faux = fauxModel(
  [],
  mode === "crash-compaction" ||
    mode === "crash-summary" ||
    mode === "crash-summary-resume" ||
    mode === "finish-summary" ||
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
if (mode === "crash-retry" || mode === "crash-retry-abnormal")
  faux.setResponses([
    fauxAssistantMessage("failed attempt", {
      stopReason: "error",
      errorMessage: "503 Service Unavailable",
    }),
  ]);
if (mode === "crash-compaction" || mode === "crash-summary")
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
const agent = await Shiitake.make({
  ...dependencies,
  store: fixture.owner,
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
const running = runtime.runNext();
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
await running;
process.send?.(await agent.session("restart").read());
await agent.close();
fixture.close();
process.disconnect();
