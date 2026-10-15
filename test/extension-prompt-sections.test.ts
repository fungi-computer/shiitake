import { fauxModel } from "@fungi.computer/mule/testing";
import { directSandbox } from "@fungi.computer/mycelium/testing";
import {
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { Mycelium } from "@fungi.computer/mycelium";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { expect, it } from "vitest";
import { z } from "zod";

import { Shiitake } from "../src/index.js";
import { defineExtension, ExtensionId } from "../src/extensions.js";
import { forkSessionAt } from "../src/internal/woodstock/index.js";
import {
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

const id = ExtensionId.make("test-prompt-builder");
const decodePromptFact = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({
      kind: Schema.Literal("model_prompt"),
      attempt: Schema.Number,
      systemPrompt: Schema.String,
    }),
  ),
);

it("rebuilds named prompt sections for each model request and retains the exact submitted prompt", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const faux = fauxModel([]);
  let rendered = 0;
  const submitted: string[] = [];
  const extension = defineExtension({ id, version: "1.0.0" }).define({
    prompt: () =>
      Promise.resolve([
        { name: "guide", text: `Section ${String(++rendered)}` },
      ]),
    tools: () =>
      Effect.succeed(
        Option.some(
          Mycelium.module({
            id: "test-note",
            name: "note",
            operations: {
              ping: Mycelium.operation({
                input: z.object({}),
                output: z.object({ ok: z.boolean() }),
                execute: () => Promise.resolve({ ok: true }),
              }),
            },
          }),
        ),
      ),
  });
  faux.setResponses([
    fauxAssistantMessage(fauxToolCall("execute", { code: "note.ping" }), {
      stopReason: "toolUse",
    }),
    fauxAssistantMessage("Done"),
  ]);
  const agent = await Shiitake.make({
    store: fixture.owner,
    runtime: runtime.runtime,
    extensions: {
      roster: [extension],
      grants: [{ id, capabilities: ["prompt", "run_tools"] }],
    },
    instructions: {
      load: () => Promise.resolve({ revision: "v1", text: "Host policy." }),
    },
    model: {
      acquire: () =>
        Promise.resolve({
          model: faux.model,
          stream: (model, context, options) => {
            const system = context.messages[0];
            if (system?.role !== "system")
              throw new Error("Model request is missing its system prompt");
            submitted.push(z.string().parse(system.content));
            return faux.stream(model, context, options);
          },
          release: () => Promise.resolve(),
        }),
    },
    tools: {
      sandbox: directSandbox(
        async (bindings) => (await bindings.note?.ping?.({})) ?? { ok: false },
      ),
    },
  });
  try {
    await agent.session("prompt-session").command({
      delivery: "followUp",
      type: "prompt",
      requestId: "prompt-sections",
      text: "Ask note.",
    });
    await runtime.runNext();
    expect(submitted).toEqual([
      "Host policy.\n\n[extension:test-prompt-builder.guide]\nSection 1",
      "Host policy.\n\n[extension:test-prompt-builder.guide]\nSection 2",
    ]);
    const facts = fixture.owner.sql
      .exec(
        "SELECT id, data_json FROM session_entries WHERE session_id = ? AND type = 'model_prompt' ORDER BY sequence",
        "prompt-session",
      )
      .toArray()
      .map((row) =>
        Schema.decodeUnknownSync(
          Schema.Struct({ id: Schema.String, data_json: Schema.String }),
        )(row),
      )
      .map(({ id: factId, data_json }) => ({
        id: factId,
        entry: decodePromptFact(data_json),
      }));
    expect(facts.map(({ entry }) => entry.systemPrompt)).toEqual(submitted);
    expect(facts.map(({ entry }) => entry.attempt)).toEqual([1, 1]);
    const last = facts.at(-1);
    if (last === undefined) throw new Error("Model prompt fact is missing");
    const forkId = forkSessionAt(fixture.owner, {
      sessionId: "prompt-session",
      entryId: last.id,
    });
    const forkPrompts = fixture.owner.sql
      .exec(
        "SELECT data_json FROM session_entries WHERE session_id = ? AND type = 'model_prompt' ORDER BY sequence",
        forkId,
      )
      .toArray()
      .map(({ data_json }) => decodePromptFact(data_json).systemPrompt);
    expect(forkPrompts).toEqual(submitted);
  } finally {
    await agent.close();
    fixture.close();
  }
});

it.each([
  { granted: false, text: "Allowed", expected: "Host policy.", calls: 0 },
  { granted: true, text: "x".repeat(8193), expected: null, calls: 1 },
])(
  "a host may refuse prompt; an oversized section stops before the provider ($granted)",
  async ({ granted, text, expected, calls }) => {
    const fixture = createNativeSqliteOwnerFixture();
    const runtime = createCompatibilityRuntimeHost();
    const faux = fauxModel([]);
    faux.setResponses([fauxAssistantMessage("done")]);
    let renders = 0;
    const prompts: string[] = [];
    const extension = defineExtension({ id, version: "1.0.0" }).define({
      prompt: () => {
        renders++;
        return [{ name: "guide", text }];
      },
    });
    const agent = await Shiitake.make({
      store: fixture.owner,
      runtime: runtime.runtime,
      extensions: {
        roster: [extension],
        grants: [{ id, capabilities: granted ? ["prompt"] : [] }],
      },
      instructions: {
        load: () => Promise.resolve({ revision: "v1", text: "Host policy." }),
      },
      model: {
        acquire: () =>
          Promise.resolve({
            model: faux.model,
            stream: (model, context, options) => {
              const system = context.messages[0];
              if (system?.role !== "system")
                throw new Error("Missing system prompt");
              prompts.push(z.string().parse(system.content));
              return faux.stream(model, context, options);
            },
            release: () => Promise.resolve(),
          }),
      },
    });
    try {
      await agent.session("refusal-session").command({
        type: "prompt",
        delivery: "followUp",
        requestId: "refuse-prompt",
        text: "Reply.",
      });
      await runtime.runNext();
      expect(renders).toBe(calls);
      expect(prompts).toEqual(expected === null ? [] : [expected]);
      const recorded = fixture.owner.sql
        .exec(
          "SELECT data_json FROM session_entries WHERE type = 'model_prompt'",
        )
        .toArray();
      expect(recorded).toHaveLength(expected === null ? 0 : 1);
    } finally {
      await agent.close();
      fixture.close();
    }
  },
);
