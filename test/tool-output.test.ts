import * as Effect from "effect/Effect";
import { expect, it } from "vitest";

import { readSessionActivity } from "../src/internal/session-status.js";
import { readClientSnapshot } from "../src/internal/woodstock/client-projection.js";
import {
  openMuleRecorder,
  loadAcceptedRun,
  prepareWoodstockSchema,
} from "../src/internal/woodstock/index.js";
import { admitStoredCommand } from "./fixtures/admission-door.fixture.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

it("exposes the current tool tail without persisting updates, then clears after outcome", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const sessionId = "live-tool-output";
  const contract = { core: "a".repeat(64), extensions: [] };
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const accepted = admitStoredCommand(
      fixture.owner,
      sessionId,
      {
        requestId: "output-request",
        delivery: "followUp",
        type: "prompt",
        text: "Build",
      },
      { contract, enqueue: () => undefined },
    ).accepted;
    const read = () =>
      readClientSnapshot(
        fixture.owner,
        sessionId,
        { limit: 200 },
        readSessionActivity(
          fixture.owner,
          sessionId,
          {
            state: "running",
            jobId: accepted.commandId,
            cancellationRequested: false,
          },
          null,
        ),
      );
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          yield* loadAcceptedRun(fixture.owner, {
            sessionId,
            commandFactId: accepted.commandId,
          });
          const record = yield* openMuleRecorder(
            fixture.owner,
            accepted,
            contract,
          );
          const run = { kind: "run", value: "run" } as const;
          const turn = { kind: "turn", value: "turn", parent: run } as const;
          const toolCall = {
            kind: "tool_call",
            value: "execution-call",
            parent: turn,
          } as const;
          const call = {
            type: "toolCall",
            id: "call",
            name: "execute",
            arguments: {},
          } as const;
          yield* record({ type: "run_start", run });
          yield* record({ type: "turn_start", run, turn });
          yield* record({
            type: "tool_execution_start",
            run,
            turn,
            toolCall,
            call,
            replay: "unsafe",
          });
          const writes = fixture.ownerWrites();
          for (let i = 1; i <= 100; i++) {
            yield* record({
              type: "tool_execution_update",
              run,
              turn,
              toolCall,
              call,
              result: {
                isError: false,
                content: [{ type: "text", text: `step ${String(i)}` }],
                data: { droppedBytes: 0, totalBytes: i * 8 },
              },
            });
          }
          expect(read()).toMatchObject({
            toolOutput: [
              {
                toolCallId: "call",
                revision: 100,
                tail: "step 100",
                droppedBytes: 0,
                totalBytes: 800,
              },
            ],
          });
          expect(fixture.ownerWrites()).toBe(writes);
          for (let i = 2; i <= 5; i++) {
            const extra = { ...toolCall, value: `call-${String(i)}` };
            const extraCall = { ...call, id: extra.value };
            yield* record({
              type: "tool_execution_start",
              run,
              turn,
              toolCall: extra,
              call: extraCall,
              replay: "unsafe",
            });
            yield* record({
              type: "tool_execution_update",
              run,
              turn,
              toolCall: extra,
              call: extraCall,
              result: {
                isError: false,
                content: [{ type: "text", text: "working" }],
                data: { droppedBytes: 0, totalBytes: 7 },
              },
            });
          }
          expect(read().toolOutput).toHaveLength(4);
          expect(
            read().toolOutput.map((slot) => slot.toolCallId),
          ).not.toContain("call-5");
          for (let i = 2; i <= 5; i++) {
            const extra = { ...toolCall, value: `call-${String(i)}` };
            yield* record({
              type: "tool_execution_end",
              run,
              turn,
              toolCall: extra,
              call: { ...call, id: extra.value },
              result: {
                isError: false,
                content: [{ type: "text", text: "done" }],
              },
            });
          }
          // A new owner over the same durable store cannot recover process memory.
          expect(
            readSessionActivity(
              { ...fixture.owner },
              sessionId,
              {
                state: "running",
                jobId: accepted.commandId,
                cancellationRequested: false,
              },
              null,
            ),
          ).toMatchObject({ toolOutput: [], facts: { activeTool: "execute" } });
          yield* record({
            type: "tool_execution_end",
            run,
            turn,
            toolCall,
            call,
            result: {
              isError: false,
              content: [{ type: "text", text: "done" }],
            },
          });
          expect(read()).toMatchObject({ toolOutput: [] });
          yield* record({
            type: "tool_execution_update",
            run,
            turn,
            toolCall,
            call,
            result: {
              isError: false,
              content: [{ type: "text", text: "late" }],
              data: { droppedBytes: 0, totalBytes: 4 },
            },
          });
          expect(read()).toMatchObject({ toolOutput: [] });
          expect(fixture.ownerWrites()).toBeGreaterThan(writes);
        }),
      ),
    );
  } finally {
    fixture.close();
  }
});
