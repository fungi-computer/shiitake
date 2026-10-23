import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fauxModel } from "@fungi.computer/mule/testing";
import {
  muleEffect,
  Type,
  type MuleToolExecutionPart,
  type MuleToolResult,
} from "@fungi.computer/mule/effect";
import {
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { expect, it } from "vitest";

import { parseSessionSnapshot } from "../src/client-wire.js";
import {
  loadAcceptedRun,
  openMuleRecorder,
  prepareWoodstockSchema,
} from "../src/internal/woodstock/index.js";
import {
  readClientReplayPage,
  readClientSnapshot,
} from "../src/internal/woodstock/client-projection.js";
import {
  buildSessionContext,
  readSelectedPath,
} from "../src/internal/woodstock/session-store.js";
import { admitStoredCommand } from "./fixtures/admission-door.fixture.js";
import { idleActivity } from "./fixtures/client.fixture.js";
import { requestMessages } from "./fixtures/model-context.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

it("retains real tool data through persistence, wire, replay and reopen without model leakage", async () => {
  const directory = mkdtempSync(join(tmpdir(), "shiitake-tool-data-"));
  const databasePath = join(directory, "session.sqlite");
  let fixture = createNativeSqliteOwnerFixture(databasePath);
  const data = {
    error: { kind: "permission_denied", message: "Write access was revoked." },
    images: [{ bytes: 3, mimeType: "image/png", path: "diagram.png" }],
  };
  const content: MuleToolResult["content"] = [
    { type: "text", text: "The capability was refused." },
    { type: "image", data: "YWJj", mimeType: "image/png" },
  ];
  const meta = { error: "host attribution" };
  const observation = {
    facts: idleActivity,
    progress: null,
    toolOutput: [],
  };
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const accepted = admitStoredCommand(
      fixture.owner,
      "tool-data",
      {
        type: "prompt",
        requestId: "ask-data",
        text: "Try the tool",
        delivery: "followUp",
      },
      { meta },
    ).accepted;
    const prepared = await Effect.runPromise(
      loadAcceptedRun(fixture.owner, {
        sessionId: accepted.sessionId,
        commandFactId: accepted.commandId,
      }),
    );
    const faux = fauxModel([]);
    faux.setResponses([
      fauxAssistantMessage(
        fauxToolCall("display", {}, { id: "display-call" }),
        {
          stopReason: "toolUse",
        },
      ),
      (context) => {
        const result = requestMessages(context).find(
          (message) => message.role === "toolResult",
        );
        expect(result).toMatchObject({ content, isError: true });
        expect(result).not.toHaveProperty("details");
        expect(result).not.toHaveProperty("data");
        expect(result).not.toHaveProperty("meta");
        return fauxAssistantMessage("Reported");
      },
    ]);
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const record = yield* openMuleRecorder(
            fixture.owner,
            accepted,
            prepared.command.entry.contract,
          );
          yield* muleEffect.run(
            {
              model: faux.model,
              retryable: () => false,
              stream: faux.stream,
              systemPrompt: "Use the tool",
              messages: prepared.messages,
              tools: [
                {
                  descriptor: {
                    name: "display",
                    description: "Return a structured tool diagnostic",
                    parameters: Type.Object({}),
                  },
                  prepare: () =>
                    Effect.succeed({
                      execute: () =>
                        Stream.succeed<MuleToolExecutionPart>({
                          type: "result",
                          result: { content, data, isError: true },
                        }),
                    }),
                },
              ],
            },
            record,
          );
        }),
      ),
    );
    expect(faux.state.callCount).toBe(2);
    const before = readClientSnapshot(
      fixture.owner,
      "tool-data",
      { limit: 200 },
      observation,
    );
    const tool = before.messages.find(
      ({ value }) => value.role === "toolResult",
    );
    expect(tool).toMatchObject({ data, value: { content, isError: true } });
    expect(tool?.value).not.toHaveProperty("details");
    expect(tool?.value).not.toHaveProperty("data");
    expect(
      before.messages.find(({ value }) => value.role === "user"),
    ).toMatchObject({
      meta,
    });
    const path = readSelectedPath(fixture.owner, { sessionId: "tool-data" });
    const outcome = path.find(({ entry }) => entry.kind === "tool_outcome");
    expect(outcome?.entry).toMatchObject({ data, result: { content } });
    expect(outcome?.entry).not.toHaveProperty("result.details");
    expect(outcome?.entry).not.toHaveProperty("result.data");
    const context = buildSessionContext(path);
    expect(context.find((message) => message.role === "toolResult")).toEqual(
      tool?.value,
    );
    for (const message of context) {
      expect(message).not.toHaveProperty("meta");
      expect(message).not.toHaveProperty("data");
    }
    fixture.close();
    fixture = createNativeSqliteOwnerFixture(databasePath);
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const reopened = readClientSnapshot(
      fixture.owner,
      "tool-data",
      { limit: 200 },
      observation,
    );
    expect(reopened.messages).toEqual(before.messages);
    expect(parseSessionSnapshot(JSON.stringify(reopened)).messages).toEqual(
      before.messages,
    );
    expect(
      readClientReplayPage(
        fixture.owner,
        "tool-data",
        undefined,
        "replay",
        observation,
      ),
    ).toMatchObject({
      kind: "page",
      frames: [
        { kind: "update", messages: before.messages },
        { kind: "tool_output" },
      ],
    });
    expect(
      buildSessionContext(
        readSelectedPath(fixture.owner, { sessionId: "tool-data" }),
      ),
    ).toEqual(context);
  } finally {
    fixture.close();
    rmSync(directory, { recursive: true, force: true });
  }
});
