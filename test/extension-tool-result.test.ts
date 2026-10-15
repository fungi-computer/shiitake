import { directSandbox } from "@fungi.computer/mycelium/testing";
import { fauxModel } from "@fungi.computer/mule/testing";
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

it.each([false, true])(
  "acknowledges only a durable successful tool result (failure=%s)",
  async (failure) => {
    const fixture = createNativeSqliteOwnerFixture();
    const runtime = createCompatibilityRuntimeHost();
    const faux = fauxModel([]);
    let release: () => void = () => undefined;
    let markHeld: () => void = () => undefined;
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    const started = new Promise<void>((resolve) => {
      markHeld = resolve;
    });
    let callId: string | undefined;
    const contract = {
      id: ExtensionId.make("tool-result-witness"),
      commands: {
        recorded: {
          scope: "session",
          input: Schema.Struct({
            workId: Schema.String,
            toolCallId: Schema.String,
          }),
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
          recorded: ({ sessionId, input }) =>
            ctx.hasSuccessfulToolResult(
              { sessionId, workId: input.workId },
              input.toolCallId,
            ),
        }),
      },
      tools: () =>
        Mycelium.module({
          id: "receipt-witness",
          name: "receipt",
          operations: {
            wait: Mycelium.operation({
              input: z.strictObject({}),
              output: z.null(),
              execute: async (_input, context) => {
                callId = context.toolCallId;
                markHeld();
                await held;
                if (failure) throw new Error("Deliberately failed result");
                return null;
              },
            }),
          },
        }),
    });
    faux.setResponses([
      fauxAssistantMessage(
        fauxToolCall("execute", { code: "return receipt.wait({})" }),
        { stopReason: "toolUse" },
      ),
      fauxAssistantMessage("Done"),
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
          const wait = bindings.receipt?.wait;
          if (wait === undefined)
            return Promise.reject(new Error("Missing receipt operation"));
          return wait({});
        }),
      },
    });
    let running: Promise<void> | undefined;
    try {
      const session = agent.session("receipt");
      const reader = bindSessionExtension(session, contract);
      const accepted = await session.send("Read delivery", {
        requestId: "delivery",
        delivery: "followUp",
      });
      running = runtime.runNext();
      await started;
      if (callId === undefined)
        throw new Error("Expected enclosing model tool call identity");
      const query = { workId: accepted.commandId, toolCallId: callId };
      expect(await reader.call("recorded", query)).toBe(false);
      release();
      await running;
      expect(await reader.call("recorded", query)).toBe(!failure);
      expect(
        await reader.call("recorded", { ...query, workId: "another-work" }),
      ).toBe(false);
      expect(
        await reader.call("recorded", { ...query, toolCallId: "another-call" }),
      ).toBe(false);
      await agent
        .session("another-session")
        .send("Unrelated session", { requestId: "other" });
      expect(
        await bindSessionExtension(
          agent.session("another-session"),
          contract,
        ).call("recorded", query),
      ).toBe(false);
    } finally {
      release();
      await running;
      await agent.close();
      fixture.close();
    }
  },
);

it("refuses tool-result observation without the run_tools grant", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const contract = {
    id: ExtensionId.make("ungranted-receipt-witness"),
    commands: {
      recorded: {
        scope: "session",
        input: Schema.Struct({}),
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
        recorded: ({ sessionId }) =>
          ctx.hasSuccessfulToolResult(
            { sessionId, workId: "ungranted" },
            "call",
          ),
      }),
    },
  });
  const agent = await Shiitake.make({
    ...compatibilityRunDependencies(),
    store: fixture.owner,
    runtime: runtime.runtime,
    extensions: {
      roster: [extension],
      grants: [{ id: contract.id, capabilities: ["commands"] }],
    },
  });
  try {
    const session = agent.session("ungranted");
    await session.send("Create the session", { requestId: "ungranted" });
    await expect(
      bindSessionExtension(session, contract).call("recorded", {}),
    ).rejects.toThrow();
  } finally {
    await agent.close();
    fixture.close();
  }
});
