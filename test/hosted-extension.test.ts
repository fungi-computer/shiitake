import { Mycelium } from "@fungi.computer/mycelium";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { expect, it } from "vitest";
import { z } from "zod";

import {
  defineCollection,
  defineExtension,
  ExtensionId,
  type Json,
} from "../src/extensions.js";
import {
  hostAgentExtension,
  hostedCallInputSchema,
  hostedModuleDescriptorSchema,
  hostedPromptInputSchema,
  hostedPromptOutputSchema,
  hostedRunSchema,
} from "../src/hosted.js";

const run = { sessionId: "session-1", workId: "work-1", origin: null };
const seen: string[] = [];

const bookmarks = defineExtension({
  id: ExtensionId.make("bookmarks"),
  version: "0.1.0",
}).define({
  prompt: (context, identity) => {
    seen.push(`prompt:${identity.sessionId}:${identity.workId}`);
    return [
      { name: "guide", text: "Check links with check() before sharing." },
      { name: "revision", text: context.revision },
    ];
  },
  tools: (_context, identity) =>
    identity.sessionId === "no-tools"
      ? null
      : Mycelium.module({
          id: "example.bookmarks",
          name: "bookmarks",
          description: "Bookmark tools",
          operations: {
            check: Mycelium.operation({
              replay: "safe",
              description: "Check that a link is http(s)",
              input: z.strictObject({ url: z.string().max(2048) }),
              output: z.strictObject({ ok: z.boolean(), host: z.string() }),
              execute: ({ url }, invocation) => {
                seen.push(`call:${invocation.executionId}`);
                return Promise.resolve(
                  URL.canParse(url)
                    ? { ok: true, host: new URL(url).host }
                    : { ok: false, host: "" },
                );
              },
            }),
          },
        }),
});

it("serves prompt sections with the core-derived revision", async () => {
  const hosted = hostAgentExtension(bookmarks);
  expect(hosted.manifest.requests).toEqual(["run_tools", "prompt"]);
  const { sections } = await hosted.prompt({ run });
  expect(sections[0]).toEqual({
    name: "guide",
    text: "Check links with check() before sharing.",
  });
  expect(sections[1]?.text).toMatch(/^sha256:[0-9a-f]{64}$/u);
  expect(seen).toContain("prompt:session-1:work-1");
});

it("describes the run's tools and invokes one by path", async () => {
  const hosted = hostAgentExtension(bookmarks);
  const { module } = await hosted.tools({ run });
  expect(module?.description).toBe("Bookmark tools");
  expect(module?.operations.check).toMatchObject({
    replay: "safe",
    input: { type: "object", required: ["url"] },
  });
  await expect(
    hosted.call({
      run,
      operation: "check",
      input: { url: "https://shiit.app/notes" },
      invocation: { executionId: "exec-1", deadlineAt: null },
    }),
  ).resolves.toEqual({ value: { ok: true, host: "shiit.app" } });
  expect(seen).toContain("call:exec-1");
  await expect(
    hosted.tools({ run: { ...run, sessionId: "no-tools" } }),
  ).resolves.toEqual({ module: null });
  await expect(
    hosted.call({
      run: { ...run, sessionId: "no-tools" },
      operation: "check",
      input: { url: "https://shiit.app/" },
      invocation: { executionId: "exec-2", deadlineAt: null },
    }),
  ).rejects.toThrow("Hosted extension hook failed");
});

it("owns the wire grammar the App method door parses with", () => {
  expect(
    hostedPromptInputSchema.safeParse({ run: { sessionId: "" } }).success,
  ).toBe(false);
  expect(
    hostedCallInputSchema.safeParse({
      run,
      operation: "check; drop",
      input: {},
      invocation: { executionId: "exec-3", deadlineAt: null },
    }).success,
  ).toBe(false);
  expect(hostedRunSchema.safeParse({ ...run, extra: 1 }).success).toBe(false);
  expect(
    hostedPromptOutputSchema.safeParse({
      sections: [{ name: "Upper", text: "x" }],
    }).success,
  ).toBe(false);
});

it("refuses a capability hosting does not provide instead of dropping it", () => {
  const stateful = defineExtension({
    id: ExtensionId.make("stateful"),
    version: "0.1.0",
  })
    .state({
      version: 1,
      migrations: {},
      collections: {
        rows: defineCollection(Schema.Struct({ id: Schema.String }), {
          key: (row) => row.id,
        }),
      },
    })
    .define({ prompt: () => [{ name: "x", text: "y" }] });
  expect(() => hostAgentExtension(stateful)).toThrow(
    "Hosted extensions do not support: state",
  );
});

it("fails a prompt hook that reaches for Agent-only doors", async () => {
  const sender = defineExtension({
    id: ExtensionId.make("sender"),
    version: "0.1.0",
  }).define({
    prompt: (context, identity) =>
      context
        .hasPendingSteer(identity)
        .pipe(Effect.map(() => [{ name: "never", text: "never" }])),
  });
  await expect(hostAgentExtension(sender).prompt({ run })).rejects.toThrow(
    "Hosted extension hook failed",
  );
});

it("refuses guest JSON Schemas the Agent host would compile into regexes or deep trees", () => {
  const operation = (input: Json) => ({
    operations: { check: { replay: "safe", input, output: null } },
  });
  expect(
    hostedModuleDescriptorSchema.safeParse(
      operation({
        type: "object",
        // A property may be named "pattern"; only the keyword is refused.
        properties: { url: { type: "string" }, pattern: { type: "string" } },
      }),
    ).success,
  ).toBe(true);
  expect(
    hostedModuleDescriptorSchema.safeParse(
      operation({ type: "string", pattern: "^(a+)+$" }),
    ).success,
  ).toBe(false);
  expect(
    hostedModuleDescriptorSchema.safeParse(
      operation({
        type: "object",
        properties: {
          tags: { type: "object", patternProperties: { "^x": {} } },
        },
      }),
    ).success,
  ).toBe(false);
  // Shared references expand exponentially when rendered or validated: a
  // 1.3 KB chain of twenty doubling $refs renders to megabytes.
  const entries: [string, Json][] = [["a0", { type: "string" }]];
  for (let index = 1; index <= 20; index += 1) {
    const previous = { $ref: `#/$defs/a${String(index - 1)}` };
    entries.push([`a${String(index)}`, { anyOf: [previous, previous] }]);
  }
  const defs = Object.fromEntries(entries);
  expect(
    hostedModuleDescriptorSchema.safeParse(
      operation({ $defs: defs, $ref: "#/$defs/a20" }),
    ).success,
  ).toBe(false);
  for (const keyword of ["$ref", "$dynamicRef", "$recursiveRef"])
    expect(
      hostedModuleDescriptorSchema.safeParse(
        operation({ type: "object", properties: { x: { [keyword]: "#" } } }),
      ).success,
    ).toBe(false);
  expect(
    hostedModuleDescriptorSchema.safeParse(
      operation({ definitions: { a: { type: "string" } } }),
    ).success,
  ).toBe(false);
  // Property names are names, not keywords.
  expect(
    hostedModuleDescriptorSchema.safeParse(
      operation({ type: "object", properties: { $ref: { type: "string" } } }),
    ).success,
  ).toBe(true);
  let deep: Json = { type: "string" };
  for (let level = 0; level < 20; level += 1)
    deep = { type: "array", items: deep };
  expect(hostedModuleDescriptorSchema.safeParse(operation(deep)).success).toBe(
    false,
  );
});
