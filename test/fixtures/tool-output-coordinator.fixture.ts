import { Type } from "@earendil-works/pi-ai";
import {
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { run, type MuleToolExecutionPart } from "@fungi.computer/mule/effect";
import { fauxModel } from "@fungi.computer/mule/testing";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Stream from "effect/Stream";
import { expect } from "vitest";
import type { SessionSnapshot } from "../../src/client.js";
import { openCoordinator } from "../../src/internal/coordinator.js";
import { toPromiseOwner } from "../../src/internal/effect-client.js";
import type { ShiitakeEffectOptions } from "../../src/internal/effect-contracts.js";
import { openRuntimeCoordinator } from "../../src/internal/runtime-coordinator.js";
import {
  loadAcceptedRun,
  openMuleRecorder,
  prepareWoodstockSchema,
} from "../../src/internal/woodstock/index.js";
import { createShiitakeHttpRoutes } from "../../src/http-routes.js";
import { createNativeSqliteOwnerFixture } from "./native-sqlite-owner.fixture.js";

export async function proveToolOutput(
  observe: (live: SessionSnapshot, settled: SessionSnapshot) => void,
) {
  const fixture = createNativeSqliteOwnerFixture();
  const faux = fauxModel([]);
  faux.setResponses([
    fauxAssistantMessage(
      { ...fauxToolCall("build", {}), id: "provider-build-call" },
      { stopReason: "toolUse" },
    ),
    fauxAssistantMessage("Finished"),
  ]);
  const options: ShiitakeEffectOptions = {
    store: fixture.owner,
    extensions: { roster: [], grants: [] },
    runtime: {
      bind: () =>
        Effect.succeed({
          commit: (mutation) => mutation,
          requestWake: Effect.void,
          close: Effect.void,
        }),
    },
    instructions: {
      load: () => Effect.succeed({ revision: "test", text: "Build" }),
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
  };
  const contract = { core: "a".repeat(64), extensions: [] };
  await Effect.runPromise(prepareWoodstockSchema(fixture.owner));
  const runtime = await Effect.runPromise(
    openRuntimeCoordinator(options, contract.core, []),
  );
  let workId = "";
  let executionId = "";
  // The scheduler seam is held running while the real Mule executes the scripted tool.
  const owner = toPromiseOwner(
    openCoordinator(options, {
      ...runtime,
      readQueueHead: () =>
        Effect.succeed({
          state: "running",
          jobId: workId,
          cancellationRequested: false,
        }),
    }),
  );
  const session = owner.session("scripted-output");
  const produced = Effect.runSync(Deferred.make<undefined>());
  const finish = Effect.runSync(Deferred.make<undefined>());
  const paused = Effect.runSync(
    Deferred.make<Awaited<ReturnType<typeof session.watch>>>(),
  );
  let execution: Promise<void> | undefined;
  try {
    const accepted = await session.send("Build");
    workId = accepted.commandId;
    execution = Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          yield* loadAcceptedRun(fixture.owner, {
            sessionId: "scripted-output",
            commandFactId: workId,
          });
          const recorder = yield* openMuleRecorder(
            fixture.owner,
            {
              sessionId: "scripted-output",
              requestId: "script",
              commandId: workId,
            },
            contract,
          );
          yield* run(
            {
              model: faux.model,
              retryable: () => false,
              stream: faux.stream,
              systemPrompt: "Build",
              messages: [{ role: "user", content: "Build", timestamp: 1 }],
              tools: [
                {
                  descriptor: {
                    name: "build",
                    description: "Scripted output",
                    parameters: Type.Object({}),
                  },
                  prepare: () =>
                    Effect.succeed({
                      execute: () =>
                        Stream.fromAsyncIterable(
                          (async function* () {
                            const watch = await session.watch();
                            Effect.runSync(Deferred.succeed(paused, watch));
                            const writes = fixture.ownerWrites();
                            for (let i = 1; i <= 100; i++) {
                              yield {
                                type: "update",
                                result: {
                                  isError: false,
                                  content: [
                                    { type: "text", text: `step ${String(i)}` },
                                  ],
                                  data: {
                                    droppedBytes: 0,
                                    totalBytes: i * 8,
                                  },
                                },
                              } satisfies MuleToolExecutionPart;
                            }
                            expect(fixture.ownerWrites()).toBe(writes);
                            Effect.runSync(
                              Deferred.succeed(produced, undefined),
                            );
                            await Effect.runPromise(Deferred.await(finish));
                            yield {
                              type: "result",
                              result: {
                                isError: false,
                                content: [{ type: "text", text: "complete" }],
                              },
                            } satisfies MuleToolExecutionPart;
                          })(),
                          (cause) => {
                            throw cause;
                          },
                        ),
                    }),
                },
              ],
            },
            (event) => {
              if (event.type === "tool_execution_start") {
                executionId = event.toolCall.value;
                expect(executionId).not.toBe(event.call.id);
                expect(event.call.id).toBe("provider-build-call");
              }
              return recorder(event);
            },
          );
        }),
      ),
    );
    await Effect.runPromise(Deferred.await(produced));
    const snapshot = await session.read();
    expect(snapshot.toolOutput).toMatchObject([
      { toolCallId: "provider-build-call", tail: "step 100", revision: 100 },
    ]);
    const watch = await Effect.runPromise(Deferred.await(paused));
    const changes = watch.changes[Symbol.asyncIterator]();
    try {
      // All 100 updates happened while this client was paused: one latest snapshot.
      expect((await changes.next()).value).toMatchObject({
        kind: "tool_output",
        toolOutput: snapshot.toolOutput,
      });
      const replayStream = session.subscribe(
        snapshot.cursor === null ? {} : { after: snapshot.cursor },
      );
      const replay = replayStream[Symbol.asyncIterator]();
      try {
        expect((await replay.next()).value).toMatchObject({ kind: "update" });
        expect((await replay.next()).value).toMatchObject({
          kind: "tool_output",
          toolOutput: snapshot.toolOutput,
        });
      } finally {
        await replay.return?.();
      }
      const routes = createShiitakeHttpRoutes(
        () => session,
        owner.listSessions,
        owner.createSession,
        owner.deleteSession,
        () => owner.extensions,
      );
      const response = await routes.request(
        "/sessions/scripted-output/events",
        {
          headers: { "Last-Event-ID": snapshot.cursor ?? "" },
        },
      );
      const reader = response.body
        ?.pipeThrough(new TextDecoderStream())
        .getReader();
      if (reader === undefined) throw new Error("Missing SSE body");
      try {
        let text = "";
        while (!text.includes('"kind":"tool_output"')) {
          const next = await reader.read();
          if (next.done) throw new Error("SSE closed before output");
          text += next.value;
        }
        const outputEvent = text
          .split("\n\n")
          .find((event) => event.includes('"kind":"tool_output"'));
        expect(outputEvent).toContain("step 100");
        expect(outputEvent).not.toMatch(/^id:/m);
      } finally {
        await reader.cancel();
      }
      Effect.runSync(Deferred.succeed(finish, undefined));
      await execution;
      const settled = await session.read();
      const result = settled.messages.find(
        (message) => message.value.role === "toolResult",
      );
      expect(result?.value).toMatchObject({
        toolCallId: "provider-build-call",
      });
      expect(executionId).not.toBe("provider-build-call");
      observe(snapshot, settled);
      expect((await session.read()).toolOutput).toEqual([]);
      expect(
        (await session.read()).messages.some(
          (message) => message.value.role === "toolResult",
        ),
      ).toBe(true);
    } finally {
      await changes.return?.();
      await watch.close();
    }
  } finally {
    Effect.runSync(Deferred.succeed(finish, undefined));
    await execution;
    await owner.close();
    await Effect.runPromise(runtime.close);
    fixture.close();
  }
}
