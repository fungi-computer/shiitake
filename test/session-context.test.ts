import { expect, it } from "vitest";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { fauxModel } from "@fungi.computer/mule/testing";
import type {
  ModelPreference,
  ModelSource,
  SessionFrame,
} from "../src/index.js";
import { memoryHost } from "../src/testing.js";
import {
  defineExtension,
  ExtensionId,
  type CompactionHandoffResultV0,
} from "../src/extensions.js";

it("uses the changed Agent default's resolved window for the footer and compaction", async () => {
  const large = fauxModel([fauxAssistantMessage("x".repeat(132_000))], {
    models: [{ id: "large", contextWindow: 200_000 }],
    tokenSize: { min: 1_000, max: 1_000 },
  });
  const small = fauxModel(
    [fauxAssistantMessage("Reply under the smaller model.")],
    { models: [{ id: "small", contextWindow: 30_000 }] },
  );
  let selected = large;
  const acquired: string[] = [];
  let compactionAttempts = 0;
  const handoffId = ExtensionId.make("context-window-witness");
  const handoff = defineExtension({
    id: handoffId,
    version: "1.0.0",
  }).define({
    compaction: () => ({
      consume: () => {
        compactionAttempts += 1;
        return Promise.resolve<CompactionHandoffResultV0>({
          type: "discarded_by_policy",
        });
      },
    }),
  });
  const host = await memoryHost({
    model: {
      acquire: () => {
        acquired.push(selected.model.id);
        return Promise.resolve({
          model: selected.model,
          retryable: () => false,
          stream: selected.stream,
          release: () => Promise.resolve(),
        });
      },
      describe: () =>
        Promise.resolve({ contextWindow: selected.model.contextWindow }),
    },
    compaction: {
      reserveTokens: 1_000,
      keepRecentTokens: 1_000,
      backgroundTokens: 0,
    },
    extensions: {
      roster: [handoff],
      grants: [{ id: handoffId, capabilities: ["compaction_handoff"] }],
    },
  });
  try {
    const session = host.agent.session("default-model-window");
    await session.send("Build context.", { requestId: "large-turn" });
    const first = await host.settle("default-model-window");
    expect(first.context?.window).toBe(200_000);
    expect(first.context?.tokens).toBeGreaterThan(30_000);
    expect(compactionAttempts).toBe(0);

    // Local's actual /model contract changes its Agent default, then clears
    // the Session override. Both describe and acquire select that new default.
    selected = small;
    await session.modelPreference.set(null);
    expect(await session.modelPreference.read()).toBeNull();
    const idle = await session.read();
    expect(acquired).toEqual(["large"]);
    await session.send("Continue.", { requestId: "small-turn" });
    const second = await host.settle("default-model-window");

    // Actual compaction must have used the smaller acquired model, not the
    // old large window. Policy discards the summary; this proves threshold
    // entry, not successful transcript compaction.
    expect(acquired).toEqual(["large", "small"]);
    expect(compactionAttempts).toBeGreaterThan(0);
    expect(idle.context?.window).toBe(30_000);
    expect(second.context?.window).toBe(30_000);
  } finally {
    await host.close();
  }
});

const wide = { providerId: "faux", modelId: "wide" } as const;

/** Faux replies; the window follows the saved preference, as a host's would. */
function describedSource(described: (ModelPreference | null)[]): ModelSource {
  const ordinary = fauxModel([fauxAssistantMessage("First reply.")], {
    models: [{ id: "ordinary", contextWindow: 200_000 }],
  });
  const roomy = fauxModel(
    [fauxAssistantMessage("A second, longer reply about the forage notes.")],
    { models: [{ id: "wide", contextWindow: 1_000_000 }] },
  );
  const select = (preference: ModelPreference | null) =>
    preference?.modelId === "wide" ? roomy : ordinary;
  return {
    acquire: ({ preference, signal }) => {
      signal.throwIfAborted();
      const faux = select(preference);
      return Promise.resolve({
        model: faux.model,
        retryable: () => false,
        stream: faux.stream,
        release: () => Promise.resolve(),
      });
    },
    describe: ({ preference, signal }) => {
      signal.throwIfAborted();
      described.push(preference);
      return Promise.resolve({
        contextWindow: select(preference).model.contextWindow,
      });
    },
  };
}

it("reports compaction's context use against the described window, after a turn and after /model", async () => {
  const described: (ModelPreference | null)[] = [];
  const host = await memoryHost({ model: describedSource(described) });
  const session = host.agent.session("context");
  try {
    // No context yet: nothing to report, and no host description is needed.
    expect((await session.read()).context).toBeUndefined();
    expect(described).toEqual([]);

    await session.send("Hello", { requestId: "first" });
    const first = (await host.settle("context")).context;
    expect(first?.window).toBe(200_000);
    expect(first?.tokens).toBeGreaterThan(0);

    // A saved preference is described at once, while the Session is idle.
    await session.modelPreference.set(wide);
    const idle = await session.read();
    expect(idle.activity.queue.state).toBe("idle");
    expect(idle.context).toEqual({ tokens: first?.tokens, window: 1_000_000 });
    expect(described.at(-1)).toEqual(wide);

    // The next turn's update carries the larger context it settles on.
    const watch = await session.watch();
    await session.send("Tell me more", { requestId: "second" });
    await host.settle("context");
    const updates: SessionFrame[] = [];
    for await (const frame of watch.changes) {
      updates.push(frame);
      if (frame.kind === "update" && frame.activity.queue.state === "idle")
        break;
    }
    await watch.close();
    const latest = updates
      .flatMap((frame) => (frame.kind === "update" ? [frame.context] : []))
      .at(-1);
    expect(latest?.window).toBe(1_000_000);
    expect(latest?.tokens).toBeGreaterThan(first?.tokens ?? 0);
    expect((await session.read()).context).toEqual(latest);
    expect(described.at(-1)).toEqual(wide);
  } finally {
    await host.close();
  }
});
