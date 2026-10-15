import { fauxModel } from "@fungi.computer/mule/testing";
import { appendFileSync } from "node:fs";
import {
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { muleEffect, Type } from "@fungi.computer/mule/effect";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import {
  openMuleRecorder,
  loadAcceptedRun,
  prepareWoodstockSchema,
} from "../../src/internal/woodstock/index.js";
import { readClientSnapshot } from "../../src/internal/woodstock/client-projection.js";
import { requestMessages } from "./model-context.js";
import { idleActivity } from "./client.fixture.js";
import { admitStoredCommand } from "./admission-door.fixture.js";
import { createNativeSqliteOwnerFixture } from "./native-sqlite-owner.fixture.js";

const [databasePath, mode] = process.argv.slice(2);
if (databasePath === undefined) throw new Error("Missing database path");
const fixture = createNativeSqliteOwnerFixture(databasePath);
Effect.runSync(prepareWoodstockSchema(fixture.owner));
const accepted = admitStoredCommand(fixture.owner, "restart", {
  type: "prompt",
  delivery: "followUp",
  requestId: "read-command",
  text: "Read the file",
}).accepted;
const prepared = await Effect.runPromise(
  loadAcceptedRun(fixture.owner, {
    sessionId: "restart",
    commandFactId: accepted.commandId,
  }),
);
// The recorder fixture selects a new execution fingerprint only for the
// cross-revision safe-tool recovery proof; the accepted contract stays intact.
const revision =
  mode === "crash-safe-upgrade" || mode === "crash-unsafe-upgrade"
    ? "a"
    : mode === "finish-upgrade"
      ? "b"
      : undefined;
const executionContract =
  revision === undefined
    ? prepared.command.entry.contract
    : {
        core: prepared.command.entry.contract.core,
        extensions: [["test-tools", `sha256:${revision.repeat(64)}`]] as const,
      };
const faux = fauxModel([]);
faux.setResponses(
  mode === "crash-safe" ||
    mode === "crash-safe-upgrade" ||
    mode === "crash-unsafe-upgrade" ||
    mode === "crash-outcome" ||
    mode === "crash-terminate"
    ? [
        fauxAssistantMessage(fauxToolCall("read", {}, { id: "read-call" }), {
          stopReason: "toolUse",
        }),
      ]
    : [
        (context) => {
          if (mode === "finish-terminate")
            throw new Error("Terminated tool dispatched another model request");
          const result = requestMessages(context).find(
            (message) =>
              message.role === "toolResult" &&
              message.toolCallId === "read-call",
          );
          if (result === undefined)
            throw new Error("Model did not receive the recovered tool result");
          return fauxAssistantMessage("finished from saved result");
        },
      ],
);
await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const record = yield* openMuleRecorder(
        fixture.owner,
        accepted,
        executionContract,
      );
      yield* muleEffect.run(
        {
          model: faux.model,
          stream:
            mode === "crash-after-result"
              ? () => {
                  process.send?.("result-saved");
                  return new Promise<never>(() => undefined);
                }
              : faux.stream,
          systemPrompt: "Read the file and finish",
          messages: prepared.messages,
          resume: prepared.resume,
          tools: [
            {
              descriptor: {
                name: "read",
                description: "Read only",
                parameters: Type.Object({}),
              },
              replay:
                mode === "downgraded" ||
                mode === "crash-outcome" ||
                mode === "crash-unsafe-upgrade"
                  ? "unsafe"
                  : "safe",
              prepare: () =>
                Effect.succeed({
                  execute: () =>
                    Stream.fromEffect(
                      Effect.gen(function* () {
                        appendFileSync(databasePath + ".calls", "read\n");
                        if (
                          mode === "crash-safe" ||
                          mode === "crash-safe-upgrade" ||
                          mode === "crash-unsafe-upgrade"
                        ) {
                          process.send?.(
                            mode === "crash-unsafe-upgrade"
                              ? "unsafe-started"
                              : "safe-started",
                          );
                          yield* Effect.never;
                        }
                        return {
                          type: "result" as const,
                          result: {
                            content: [
                              {
                                type: "text" as const,
                                text: "saved file contents",
                              },
                            ],
                            isError: false,
                            terminate: mode === "crash-terminate",
                          },
                        };
                      }),
                    ),
                }),
            },
          ],
        },
        (event) =>
          record(event).pipe(
            Effect.andThen(
              (mode === "crash-outcome" &&
                event.type === "tool_execution_end") ||
                (mode === "crash-terminate" &&
                  event.type === "message_end" &&
                  event.value.role === "toolResult")
                ? Effect.sync(() => process.send?.("outcome-saved")).pipe(
                    Effect.andThen(Effect.never),
                  )
                : Effect.void,
            ),
          ),
      );
    }),
  ),
);
process.send?.(
  readClientSnapshot(
    fixture.owner,
    "restart",
    { limit: 200 },
    { facts: idleActivity, progress: null },
  ),
);
fixture.close();
process.disconnect();
