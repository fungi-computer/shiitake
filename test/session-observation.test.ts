import { fauxModel } from "@fungi.computer/mule/testing";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import * as Effect from "effect/Effect";
import { expect, it } from "vitest";

import { Shiitake } from "../src/index.js";
import {
  mergeObservationSignals,
  openObservationKernel,
  publishOwnerObservation,
} from "../src/internal/observation-kernel.js";
import {
  createCompatibilityRuntimeHost,
  compatibilityRunDependencies,
  createNativeSqliteOwnerFixture,
  noExtensions,
} from "./fixtures/native-sqlite-owner.fixture.js";

it("coalesces progress wakes without hiding a durable observation", async () => {
  expect(mergeObservationSignals("progress", "reset")).toBe("reset");
  expect(mergeObservationSignals("durable", "closed")).toBe("closed");

  const fixture = createNativeSqliteOwnerFixture();
  const kernel = openObservationKernel(fixture.owner);
  const first = await Effect.runPromise(kernel.register("observation-race"));
  const second = await Effect.runPromise(kernel.register("observation-race"));

  try {
    publishOwnerObservation(fixture.owner, "observation-race", "progress");
    expect(await Effect.runPromise(first.pollSignal)).toBe("progress");
    expect(await Effect.runPromise(second.pollSignal)).toBe("progress");

    publishOwnerObservation(fixture.owner, "observation-race", "progress");
    publishOwnerObservation(fixture.owner, "observation-race", "durable");
    expect(await Effect.runPromise(first.pollSignal)).toBe("durable");
    expect(await Effect.runPromise(second.pollSignal)).toBe("durable");

    first.close();
    publishOwnerObservation(fixture.owner, "observation-race", "durable");
    expect(await Effect.runPromise(second.pollSignal)).toBe("durable");
    expect(await Effect.runPromise(first.pollSignal)).toBe("closed");
  } finally {
    second.close();
    await Effect.runPromise(kernel.close);
    fixture.close();
  }
});

it("continues live observation from the baseline carried by a reset", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const faux = fauxModel([]);
  faux.setResponses([fauxAssistantMessage("Observed response")]);
  const agent = await Shiitake.make({
    store: fixture.owner,
    extensions: noExtensions,
    runtime: runtime.runtime,
    instructions: {
      load: () =>
        Promise.resolve({ revision: "observation-v1", text: "Answer." }),
    },
    model: {
      acquire: () =>
        Promise.resolve({
          model: faux.model,
          stream: faux.stream,
          release: () => Promise.resolve(),
        }),
    },
  });
  const session = agent.session("reset-session");
  const subscription = session.subscribe({ after: "unavailable-cursor" });
  const changes = subscription[Symbol.asyncIterator]();

  try {
    await expect(changes.next()).resolves.toMatchObject({
      done: false,
      value: {
        kind: "reset",
        cursor: null,
        baseline: { messages: [] },
      },
    });

    const next = changes.next();
    const accepted = await session.command({
      delivery: "followUp",

      type: "prompt",
      requestId: "after-reset",
      text: "Continue after reset.",
    });

    const execution = runtime.runNext();
    await expect(next).resolves.toMatchObject({
      done: false,
      value: {
        kind: "update",
        cursor: null,
        messages: [],
        pending: {
          total: 1,
          prompts: [
            {
              commandId: accepted.commandId,
              text: "Continue after reset.",
              delivery: "followUp",
            },
          ],
          hasMore: false,
        },
      },
    });
    await execution;
  } finally {
    await changes.return?.();
    await agent.close();
    fixture.close();
  }
});

it("observes admissions across the watch baseline race without a conversation cursor", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const agent = await Shiitake.make({
    store: fixture.owner,
    runtime: runtime.runtime,
    ...compatibilityRunDependencies(),
  });
  const session = agent.session("waiting-admissions");
  const watch = await session.watch();
  const changes = watch.changes[Symbol.asyncIterator]();
  try {
    expect(watch.snapshot.pending).toEqual({
      total: 0,
      prompts: [],
      hasMore: false,
    });
    const first = {
      delivery: "followUp",

      type: "prompt",
      requestId: "waiting-one",
      text: "First accepted task",
    } as const;
    const firstAccepted = await session.command(first);
    await expect(changes.next()).resolves.toMatchObject({
      value: {
        kind: "update",
        cursor: null,
        messages: [],
        pending: {
          total: 1,
          prompts: [
            {
              commandId: firstAccepted.commandId,
              text: first.text,
              delivery: "followUp",
            },
          ],
          hasMore: false,
        },
      },
    });
    const next = changes.next();
    const secondAccepted = await session.command({
      delivery: "followUp",

      type: "prompt",
      requestId: "waiting-two",
      text: "Second accepted task",
    });
    await expect(next).resolves.toMatchObject({
      value: {
        kind: "update",
        cursor: null,
        messages: [],
        pending: {
          total: 2,
          prompts: [
            {
              commandId: firstAccepted.commandId,
              text: first.text,
              delivery: "followUp",
            },
            {
              commandId: secondAccepted.commandId,
              text: "Second accepted task",
              delivery: "followUp",
            },
          ],
          hasMore: false,
        },
      },
    });
    const beforeReplay = await session.read();
    const writes = fixture.ownerWrites();
    await session.command(first);
    expect(await session.read()).toEqual(beforeReplay);
    expect(fixture.ownerWrites()).toBe(writes);
  } finally {
    await watch.close();
    await changes.return?.();
    await agent.close();
    fixture.close();
  }
});

it("wakes queued Session observers when Agent-wide execution changes", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const kernel = openObservationKernel(fixture.owner);
  const a = await Effect.runPromise(kernel.register("a"));
  const b = await Effect.runPromise(kernel.register("b"));
  try {
    publishOwnerObservation(fixture.owner, "a");
    expect(await Effect.runPromise(a.pollSignal)).toBe("durable");
    expect(await Effect.runPromise(b.pollSignal)).toBeUndefined();
    publishOwnerObservation(fixture.owner, null);
    expect(await Effect.runPromise(a.pollSignal)).toBe("durable");
    expect(await Effect.runPromise(b.pollSignal)).toBe("durable");
  } finally {
    a.close();
    b.close();
    await Effect.runPromise(kernel.close);
    fixture.close();
  }
});

it("publishes a withdrawn prompt before the host settles its scheduler head", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const agent = await Shiitake.make({
    ...compatibilityRunDependencies(),
    store: fixture.owner,
    runtime: runtime.runtime,
  });
  const session = agent.session("withdraw-without-host");
  const first = await session.send("First", {
    delivery: "followUp",
    requestId: "first",
  });
  const second = await session.send("Second", {
    delivery: "followUp",
    requestId: "second",
  });
  const watch = await session.watch();
  const changes = watch.changes[Symbol.asyncIterator]();
  try {
    expect(watch.snapshot.pending.total).toBe(2);
    // The watch replays the initial admission page before live notifications.
    await expect(changes.next()).resolves.toMatchObject({
      value: { kind: "update", pending: { total: 2 } },
    });
    const next = changes.next();
    await session.command({
      type: "cancel",
      commandId: first.commandId,
      onlyIfPending: true,
    });
    await expect(next).resolves.toMatchObject({
      done: false,
      value: {
        kind: "update",
        pending: {
          total: 1,
          prompts: [{ commandId: second.commandId, text: "Second" }],
        },
      },
    });
    expect((await session.read()).activity.queue).toMatchObject({
      state: "queued",
      commandId: first.commandId,
    });
  } finally {
    await watch.close();
    await changes.return?.();
    await agent.close();
    fixture.close();
  }
});
