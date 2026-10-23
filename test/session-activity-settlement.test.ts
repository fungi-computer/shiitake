import { fauxModel } from "@fungi.computer/mule/testing";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { expect, it } from "vitest";

import { Shiitake, type SessionFrame } from "../src/index.js";
import {
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

it("a live watch observes idle once Watchdog settles the replied work", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const faux = fauxModel([]);
  faux.setResponses([fauxAssistantMessage("Settled reply")]);
  const agent = await Shiitake.make({
    store: fixture.owner,
    runtime: runtime.runtime,
    instructions: {
      load: () => Promise.resolve({ revision: "settle-v1", text: "Answer." }),
    },
    model: {
      acquire: () =>
        Promise.resolve({
          model: faux.model,
          retryable: () => false,
          stream: faux.stream,
          release: () => Promise.resolve(),
        }),
    },
    extensions: { roster: [], grants: [] },
  });
  const session = agent.session("settled-activity");
  const watch = await session.watch();
  const changes = watch.changes[Symbol.asyncIterator]();
  const frames: SessionFrame[] = [];
  const settledFrame = (async () => {
    for (;;) {
      const next = await changes.next();
      if (next.done === true) return undefined;
      frames.push(next.value);
      const replied = frames.some(
        (frame) => frame.kind === "update" && frame.terminals.length > 0,
      );
      if (
        replied &&
        next.value.kind === "update" &&
        next.value.activity.queue.state === "idle"
      )
        return next.value;
    }
  })();
  try {
    const accepted = await session.command({
      delivery: "followUp",
      type: "prompt",
      requestId: "settle-one",
      text: "Reply once.",
    });
    await runtime.runNext();

    // Between the durable run terminal and Watchdog settlement the head is
    // still running, so the owner honestly reports `thinking`. Settlement must
    // then publish, and the live watch must carry `idle` without new history.
    await expect(settledFrame).resolves.toMatchObject({
      kind: "update",
      delivery: "live",
      messages: [],
      activity: { queue: { state: "idle" } },
    });
    const snapshot = await session.read();
    expect(snapshot.activity).toEqual({
      queue: { state: "idle" },
      cancellationRequested: false,
      retry: null,
      activeTool: null,
      lastModelChoiceSequence: null,
      compaction: null,
    });
    expect(snapshot.progress).toBeNull();
    expect(snapshot.terminals).toMatchObject([
      { commandId: accepted.commandId, outcome: "completed", diagnostic: null },
    ]);
  } finally {
    await watch.close();
    await changes.return?.();
    await agent.close();
    fixture.close();
  }
});

it("publishes failure uncertainty with observed model identity as Session state", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const faux = fauxModel([]);
  faux.setResponses([
    fauxAssistantMessage("", {
      stopReason: "error",
      errorMessage: "Model rejected this run",
    }),
  ]);
  const agent = await Shiitake.make({
    store: fixture.owner,
    runtime: runtime.runtime,
    instructions: {
      load: () => Promise.resolve({ revision: "error-v1", text: "Answer." }),
    },
    model: {
      acquire: () =>
        Promise.resolve({
          model: faux.model,
          retryable: () => false,
          stream: faux.stream,
          release: () => Promise.resolve(),
        }),
    },
    extensions: { roster: [], grants: [] },
  });
  try {
    const session = agent.session("failed-activity");
    await session.modelPreference.set({
      providerId: "faux",
      modelId: "failed-model",
    });
    await session.command({
      delivery: "followUp",
      type: "prompt",
      requestId: "failed-one",
      text: "Reply once.",
    });
    await runtime.runNext();
    const snapshot = await session.read();
    expect(snapshot.terminals).toHaveLength(1);
    const failed = snapshot.terminals[0];
    expect(failed).toMatchObject({
      outcome: "failed",
    });
    expect(failed?.diagnostic).toContain("reason_unavailable: run=mule_run_");
    expect(failed?.diagnostic).toContain("provider=faux");
    expect(failed?.diagnostic).toContain(`model=${faux.model.id}`);
    expect(failed?.diagnostic).not.toContain("failed-model");
    expect(failed?.diagnostic).not.toContain("Model rejected this run");
    // The first model choice predates the failed run.
    expect(snapshot.activity.lastModelChoiceSequence).toBeLessThan(
      failed?.sequence ?? 0,
    );
    const watch = await session.watch();
    const changes = watch.changes[Symbol.asyncIterator]();
    try {
      const changed = changes.next();
      await session.modelPreference.set({
        providerId: "faux",
        modelId: "working-model",
      });
      expect((await changed).value).toMatchObject({ kind: "update" });
      const afterSwitch = await session.read();
      // A later model choice is a fact; the client decides it acknowledges the failure.
      expect(afterSwitch.activity.lastModelChoiceSequence).toBeGreaterThan(
        failed?.sequence ?? Number.POSITIVE_INFINITY,
      );
      expect(afterSwitch.terminals).toEqual(snapshot.terminals);
    } finally {
      await watch.close();
      await changes.return?.();
    }
  } finally {
    await agent.close();
    fixture.close();
  }
});
