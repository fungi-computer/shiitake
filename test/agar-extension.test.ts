import { Mycelium } from "@fungi.computer/mycelium";
import {
  defineExtension,
  ExtensionId,
} from "@fungi.computer/shiitake/extensions";
import { agentExtensionMethodNames } from "@fungi.computer/shiitake/hosted";
import { expect, test } from "vitest";
import * as z from "zod";

import { agentExtensionMethods } from "../src/agar-extension.js";
import { buildAppApi } from "@fungi.computer/agar/backend-declaration";
import {
  defineAppBackend,
  defineAppMethod,
} from "@fungi.computer/agar/backend-definition";

const caller = {
  computerId: null,
  actor: { kind: "agent" as const, agentId: "agent_fixture" },
};
const run = { sessionId: "session-1", workId: "work-1", origin: null };

const greeter = defineExtension({
  id: ExtensionId.make("greeter"),
  version: "1.0.0",
}).define({
  prompt: () => [{ name: "voice", text: "Answer in one short sentence." }],
  tools: () =>
    Mycelium.module({
      id: "greeter",
      name: "greeter",
      operations: {
        greet: Mycelium.operation({
          input: z.strictObject({ name: z.string().max(40) }),
          output: z.strictObject({ text: z.string() }),
          execute: ({ name }) => Promise.resolve({ text: `Hello, ${name}` }),
        }),
      },
    }),
});

const backend = defineAppBackend({
  methods: {
    ...agentExtensionMethods(greeter),
    ping: defineAppMethod({
      description: "Own method",
      effect: "read",
      requires: [],
      input: z.strictObject({}),
      output: z.literal("pong"),
      handler: () => "pong" as const,
    }),
  },
});

test("the build declares the extension method family in the App API", () => {
  const api = buildAppApi(backend);
  expect(
    Object.fromEntries(
      Object.entries(api.methods).map(([name, method]) => [
        name,
        [method.effect, method.requires],
      ]),
    ),
  ).toEqual({
    [agentExtensionMethodNames.prompt]: ["read", []],
    [agentExtensionMethodNames.tools]: ["read", []],
    [agentExtensionMethodNames.call]: ["write", []],
    ping: ["read", []],
  });
});

test("the generated methods serve prompt sections, tools and calls", async () => {
  await expect(
    backend.call(agentExtensionMethodNames.prompt, { run }, undefined, caller),
  ).resolves.toEqual({
    sections: [{ name: "voice", text: "Answer in one short sentence." }],
  });
  const tools = z
    .strictObject({
      module: z.strictObject({
        operations: z.record(z.string(), z.unknown()),
      }),
    })
    .parse(
      await backend.call(
        agentExtensionMethodNames.tools,
        { run },
        undefined,
        caller,
      ),
    );
  expect(Object.keys(tools.module.operations)).toEqual(["greet"]);
  await expect(
    backend.call(
      agentExtensionMethodNames.call,
      {
        run,
        operation: "greet",
        input: { name: "Levi" },
        invocation: { executionId: "exec-1", deadlineAt: null },
      },
      undefined,
      caller,
    ),
  ).resolves.toEqual({ value: { text: "Hello, Levi" } });
});
