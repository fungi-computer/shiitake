import { expect, it } from "vitest";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { fauxModel } from "@fungi.computer/mule/testing";
import type {
  ModelPreference,
  ModelSource,
  SessionFrame,
} from "../src/index.js";
import { memoryHost } from "../src/testing.js";

const wide = { providerId: "faux", modelId: "wide" } as const;

/** Faux replies; the window follows the saved preference, as a host's would. */
function describedSource(described: (ModelPreference | null)[]): ModelSource {
  const replies = [
    fauxAssistantMessage("First reply."),
    fauxAssistantMessage("A second, longer reply about the forage notes."),
  ];
  const faux = fauxModel(replies);
  return {
    acquire: ({ signal }) => {
      signal.throwIfAborted();
      return Promise.resolve({
        model: faux.model,
        stream: faux.stream,
        release: () => Promise.resolve(),
      });
    },
    describe: ({ preference, signal }) => {
      signal.throwIfAborted();
      described.push(preference);
      return Promise.resolve({
        contextWindow: preference?.modelId === "wide" ? 1_000_000 : 200_000,
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
    expect(described).toEqual([null, wide]);

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
    // The described window is reused until the preference changes again.
    expect(described).toEqual([null, wide]);
  } finally {
    await host.close();
  }
});
