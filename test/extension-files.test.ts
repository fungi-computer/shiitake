import {
  directSandbox,
  rejectingSandbox,
} from "@fungi.computer/mycelium/testing";
import { fauxModel } from "@fungi.computer/mule/testing";
import {
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import {
  Knapsack,
  type FileTool,
  type FilesPort,
} from "@fungi.computer/knapsack";
import type { MuleTool } from "@fungi.computer/mule/effect";
import * as Effect from "effect/Effect";
import { expect, expectTypeOf, it } from "vitest";

import { defineExtension, ExtensionId } from "../src/extensions.js";
import { Shiitake, type ShiitakeOptions } from "../src/index.js";
import {
  admitExtensionRosterV0,
  coreProtocolDigest,
} from "../src/internal/extension-roster.js";
import { openExtensionRuntime } from "../src/internal/extension-runtime.js";
import { openTestAdmissionDoor } from "./fixtures/admission-door.fixture.js";
import { prepareWoodstockSchema } from "../src/internal/woodstock/index.js";
import { readSessionEntriesForWork } from "../src/internal/woodstock/index.js";
import {
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

function port(name: string, reads: string[]): FilesPort {
  return {
    read: (input) => {
      reads.push(name);
      return Promise.resolve({
        path: input.path,
        content: name,
        offset: 0,
        nextOffset: null,
        truncated: false,
      });
    },
    list: ({ path }) =>
      Promise.resolve({ path, entries: [], truncated: false }),
    glob: ({ path }) =>
      Promise.resolve({ path, matches: [], truncated: false }),
    grep: ({ path }) =>
      Promise.resolve({ path, matches: [], truncated: false }),
    replace: ({ path }) => Promise.resolve({ path, bytes: 0 }),
  };
}

function extension(id: string, files: () => readonly FileTool[] | null) {
  return defineExtension({ id: ExtensionId.make(id), version: "1.0.0" }).define(
    { files },
  );
}

it("admits one Files holder and refuses a second before activation", async () => {
  const first = extension("first-files", () => null);
  const second = extension("second-files", () => null);
  expect(first.manifest.requests).toEqual(["files"]);
  const grants = [first, second].map((entry) => ({
    id: entry.manifest.id,
    capabilities: entry.manifest.requests,
  }));
  await expect(
    Effect.runPromise(
      admitExtensionRosterV0({
        roster: [first, second],
        grants,
      }),
    ),
  ).rejects.toMatchObject({ boundary: "extensions", cause: "conflict" });
  expect(
    await Effect.runPromise(
      admitExtensionRosterV0({
        roster: [first, second],
        grants: grants.slice(0, 1),
      }),
    ),
  ).toHaveLength(2);
});

it("does not let an extension mint an unbranded Files tool", () => {
  type Definition = Parameters<ReturnType<typeof defineExtension>["define"]>[0];
  type FilesHook = NonNullable<Definition["files"]>;
  expectTypeOf<() => readonly MuleTool[]>().not.toExtend<FilesHook>();
  expectTypeOf<() => readonly FileTool[]>().toExtend<FilesHook>();
});

it.each([true, false])(
  "captures granted Files once per run (allowed=%s), without a shared module",
  async (allowed) => {
    const fixture = createNativeSqliteOwnerFixture();
    const runtime = createCompatibilityRuntimeHost();
    const faux = fauxModel([]);
    const reads: string[] = [];
    let selected = "A";
    let captures = 0;
    let refusals = 0;
    let sandboxCalls = 0;
    const member = extension("captured-files", () => {
      captures += 1;
      return Knapsack.files({ port: port(selected, reads) }).tools;
    });
    const read = () =>
      fauxAssistantMessage(fauxToolCall("read", { path: "/x" }), {
        stopReason: "toolUse",
      });
    faux.setResponses([
      read(),
      () => {
        selected = "B";
        return read();
      },
      fauxAssistantMessage(fauxToolCall("execute", { code: "return null" }), {
        stopReason: "toolUse",
      }),
      fauxAssistantMessage("first complete"),
      read(),
      fauxAssistantMessage("second complete"),
    ]);
    const agent = await Shiitake.make({
      store: fixture.owner,
      runtime: runtime.runtime,
      extensions: {
        roster: [member],
        grants: [{ id: member.manifest.id, capabilities: ["files"] }],
      },
      instructions: {
        load: () => Promise.resolve({ revision: "1", text: "Read twice." }),
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
          sandboxCalls += 1;
          expect(bindings.files).toBeUndefined();
          return Promise.resolve(null);
        }),
        access: {
          grants: allowed ? {} : { files: "none" },
          onRefused: () => {
            refusals += 1;
          },
        },
      },
    });
    try {
      const session = agent.session("files");
      const first = await session.command({
        delivery: "followUp",
        type: "prompt",
        requestId: "first",
        text: "Read twice.",
      });
      await runtime.runNext();
      expect(captures).toBe(1);
      expect(sandboxCalls).toBe(1);
      expect(reads).toEqual(allowed ? ["A", "A"] : []);
      const facts = readSessionEntriesForWork(
        fixture.owner,
        "files",
        first.commandId,
      );
      const digest = await Effect.runPromise(
        coreProtocolDigest(["read", "list", "glob", "grep"]),
      );
      const started = facts.find(
        ({ entry }) => entry.kind === "run_started",
      )?.entry;
      expect(started?.kind === "run_started" && started.contract.core).toBe(
        digest,
      );
      await session.command({
        delivery: "followUp",
        type: "prompt",
        requestId: "second",
        text: "Read once.",
      });
      await runtime.runNext();
      expect(captures).toBe(2);
      expect(reads).toEqual(allowed ? ["A", "A", "B"] : []);
      expect(refusals).toBe(allowed ? 0 : 3);
      expect(
        (await session.read()).terminals.map((entry) => entry.outcome),
      ).toEqual(["completed", "completed"]);
    } finally {
      await agent.close();
      fixture.close();
    }
  },
);

it.each(["ungranted", "none", "throwing"])(
  "handles %s Files without exposing an implicit surface",
  async (mode) => {
    const fixture = createNativeSqliteOwnerFixture();
    const runtime = createCompatibilityRuntimeHost();
    const faux = fauxModel([]);
    let captures = 0;
    let modelCalls = 0;
    const reads: string[] = [];
    const member = extension("optional-files", () => {
      captures += 1;
      if (mode === "throwing") throw new Error("fixture-capture-failed");
      return mode === "none"
        ? null
        : Knapsack.files({ port: port("A", reads) }).tools;
    });
    faux.setResponses([fauxAssistantMessage("acknowledged")]);
    const options: ShiitakeOptions = {
      store: fixture.owner,
      runtime: runtime.runtime,
      extensions: {
        roster: [member],
        grants:
          mode === "ungranted"
            ? []
            : [{ id: member.manifest.id, capabilities: ["files"] }],
      },
      instructions: {
        load: () => Promise.resolve({ revision: "1", text: "Reply once." }),
      },
      model: {
        acquire: () =>
          Promise.resolve({
            model: faux.model,
            stream: (model, context, streamOptions) => {
              modelCalls += 1;
              const system = context.messages.find(
                (message) => message.role === "system",
              );
              expect(
                system?.role === "system" &&
                  system.toolsAdded?.map((tool) => tool.name),
              ).toEqual(["execute"]);
              return faux.stream(model, context, streamOptions);
            },
            release: () => Promise.resolve(),
          }),
      },
      tools: {
        sandbox: rejectingSandbox("Unexpected execute"),
      },
    };
    const agent = await Shiitake.make(options);
    try {
      const session = agent.session("optional");
      await session.command({
        delivery: "followUp",
        type: "prompt",
        requestId: "optional",
        text: "Hello",
      });
      await runtime.runNext();
      const failed = mode === "throwing";
      expect(captures).toBe(mode === "ungranted" ? 0 : 1);
      expect(modelCalls).toBe(failed ? 0 : 1);
      expect(reads).toEqual([]);
      expect((await session.read()).terminals[0]?.outcome).toBe(
        failed ? "failed" : "completed",
      );
    } finally {
      await agent.close();
      fixture.close();
    }
  },
);
it("never captures Files from a holder outside the run participants", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  let captures = 0;
  const holder = extension("participant-files", () => {
    captures += 1;
    return Knapsack.files({ port: port("A", []) }).tools;
  });
  await Effect.runPromise(prepareWoodstockSchema(fixture.owner));
  const admitted = await Effect.runPromise(
    admitExtensionRosterV0({
      roster: [holder],
      grants: [{ id: holder.manifest.id, capabilities: ["files"] }],
    }),
  );
  const runtime = await Effect.runPromise(
    openExtensionRuntime({
      owner: fixture.owner,
      door: openTestAdmissionDoor(fixture.owner),
      readQueueHead: () => ({ state: "idle" }),
      admitted,
      requestWake: Effect.void,
    }),
  );
  const run = { sessionId: "files", workId: "captured", origin: null };
  try {
    expect(
      await Effect.runPromise(runtime.filesForRun(run, new Set(["other"]))),
    ).toEqual([]);
    expect(captures).toBe(0);
    expect(
      await Effect.runPromise(
        runtime.filesForRun(run, new Set([holder.manifest.id])),
      ),
    ).toHaveLength(4);
    expect(captures).toBe(1);
  } finally {
    await Effect.runPromise(runtime.close);
    fixture.close();
  }
});
