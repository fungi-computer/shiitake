import { directSandbox } from "@fungi.computer/mycelium/testing";
import { fauxModel } from "@fungi.computer/mule/testing";
import type { Context } from "@earendil-works/pi-ai";
import {
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { Mycelium } from "@fungi.computer/mycelium";
import * as Schema from "effect/Schema";
import { expect, it } from "vitest";
import { z } from "zod";
import { bindSessionExtension } from "../src/client.js";
import { defineExtension, ExtensionId } from "../src/extensions.js";
import { Shiitake } from "../src/index.js";
import {
  compatibilityRunDependencies,
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

it("observes a pending steer without consuming it before the waiting tool returns", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const faux = fauxModel([]);
  const requests: Context[] = [];
  let markHeld: () => void = () => undefined;
  let release: () => void = () => undefined;
  const started = new Promise<void>((resolve) => {
    markHeld = resolve;
  });
  const held = new Promise<void>((resolve) => {
    release = resolve;
  });
  const contract = {
    id: ExtensionId.make("pending-steer-witness"),
    commands: {
      pending: {
        scope: "session",
        input: Schema.Struct({ workId: Schema.String }),
        output: Schema.Boolean,
      },
    },
    projection: null,
  } as const;
  const extension = defineExtension({
    id: contract.id,
    version: "1.0.0",
  }).define({
    commands: {
      specs: contract.commands,
      handlers: (ctx) => ({
        pending: ({ sessionId, input }) =>
          ctx.hasPendingSteer({ sessionId, workId: input.workId }),
      }),
    },
    tools: () =>
      Mycelium.module({
        id: "steering",
        operations: {
          wait: Mycelium.operation({
            input: z.strictObject({}),
            output: z.null(),
            execute: async () => {
              markHeld();
              await held;
              return null;
            },
          }),
        },
      }),
  });
  faux.setResponses([
    fauxAssistantMessage(
      fauxToolCall("execute", { code: "return steering.wait({})" }),
      { stopReason: "toolUse" },
    ),
    (context) => {
      requests.push(structuredClone(context));
      return fauxAssistantMessage("Steer received");
    },
  ]);
  const agent = await Shiitake.make({
    ...compatibilityRunDependencies(),
    store: fixture.owner,
    runtime: runtime.runtime,
    extensions: {
      roster: [extension],
      grants: [{ id: contract.id, capabilities: ["commands", "run_tools"] }],
    },
    model: {
      acquire: () =>
        Promise.resolve({
          model: faux.model,
          stream: faux.stream,
          release: () => Promise.resolve(),
        }),
    },
    tools: {
      sandbox: directSandbox((bindings) => {
        const wait = bindings.steering?.wait;
        if (wait === undefined)
          return Promise.reject(new Error("Expected waiting tool"));
        return wait({});
      }),
    },
  });
  let running: Promise<void> | undefined;
  try {
    const session = agent.session("steering");
    const reader = bindSessionExtension(session, contract);
    const accepted = await session.send("Wait for direction", {
      requestId: "parent",
      delivery: "followUp",
    });
    running = runtime.runNext();
    await started;
    expect(await reader.call("pending", { workId: accepted.commandId })).toBe(
      false,
    );
    const steer = await session.send("Use the second option", {
      requestId: "steer",
      delivery: "steer",
    });
    expect(await reader.call("pending", { workId: accepted.commandId })).toBe(
      true,
    );
    expect(await reader.call("pending", { workId: accepted.commandId })).toBe(
      true,
    );
    expect(await reader.call("pending", { workId: "another-work" })).toBe(
      false,
    );
    expect((await session.read()).pending.prompts).toHaveLength(1);
    release();
    await running;
    expect(requests).toHaveLength(1);
    expect(
      requests[0]?.messages.filter(
        (message) =>
          message.role === "user" &&
          message.content === "Use the second option",
      ),
    ).toHaveLength(1);
    const snapshot = await session.read();
    expect(snapshot.terminals).toContainEqual(
      expect.objectContaining({
        commandId: steer.commandId,
        outcome: "completed",
      }),
    );
    expect(snapshot.pending.prompts).toEqual([]);
    expect(await reader.call("pending", { workId: accepted.commandId })).toBe(
      false,
    );
  } finally {
    release();
    await running;
    await agent.close();
    fixture.close();
  }
});
