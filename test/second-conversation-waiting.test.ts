import { fauxModel } from "@fungi.computer/mule/testing";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { expect, it } from "vitest";

import { Shiitake, type ShiitakeAgent } from "../src/index.js";
import {
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
  noExtensions,
} from "./fixtures/native-sqlite-owner.fixture.js";

type Reply = ReturnType<typeof fauxAssistantMessage>;

/** A model reply that holds its run open until the test lets it finish. */
function gatedReply(text: string) {
  let open: () => void = () => undefined;
  const gate = new Promise<void>((resolve) => {
    open = resolve;
  });
  let began: () => void = () => undefined;
  const started = new Promise<void>((resolve) => {
    began = resolve;
  });
  return {
    started,
    release: open,
    respond: async () => {
      began();
      await gate;
      return fauxAssistantMessage(text);
    },
  };
}

/** One Agent whose model answers, in order, with the supplied replies. */
async function openAgent(responses: readonly (() => Promise<Reply>)[]) {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const faux = fauxModel([]);
  faux.setResponses([...responses]);
  const agent: ShiitakeAgent = await Shiitake.make({
    store: fixture.owner,
    runtime: runtime.runtime,
    extensions: noExtensions,
    instructions: {
      load: () => Promise.resolve({ revision: "waiting-v1", text: "Answer." }),
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
  });
  const drain = async () => {
    while (runtime.snapshot().state === "pending") await runtime.runNext();
  };
  return {
    agent,
    runtime,
    drain,
    close: async () => {
      await agent.close();
      fixture.close();
    },
  };
}

it("the owner issues the queued command id while another conversation runs", async () => {
  const first = gatedReply("first reply");
  const host = await openAgent([
    first.respond,
    () => Promise.resolve(fauxAssistantMessage("second reply")),
  ]);
  let running: Promise<void> | undefined;
  try {
    const firstCommand = await host.agent
      .session("conversation-a")
      .send("first", {
        delivery: "followUp",
        requestId: "waiting-a",
      });
    running = host.drain();
    await first.started;
    const queued = await host.agent.session("conversation-b").send("second", {
      delivery: "followUp",
      requestId: "waiting-b",
    });

    const waitingSession = host.agent.session("conversation-b");
    const watch = await waitingSession.watch();
    const waiting = watch.snapshot;
    expect(waiting.activity.queue).toEqual({
      state: "queued",
      commandId: queued.commandId,
      runningWork: {
        kind: "session",
        sessionId: "conversation-a",
        commandId: firstCommand.commandId,
      },
    });
    // The conversation that is actually running is not the waiting one.
    expect(
      (await host.agent.session("conversation-a").read()).activity.queue.state,
    ).toBe("running");
    await watch.close();
  } finally {
    first.release();
    await running;
    await host.close();
  }
});

it("delivers Agent execution changes to a queued Session as activity", async () => {
  const first = gatedReply("first reply");
  const host = await openAgent([
    first.respond,
    () => Promise.resolve(fauxAssistantMessage("second reply")),
  ]);
  let running: Promise<void> | undefined;
  let watch:
    | Awaited<ReturnType<ReturnType<typeof host.agent.session>["watch"]>>
    | undefined;
  let changes:
    AsyncIterator<import("../src/client.js").SessionFrame> | undefined;
  try {
    await host.agent.session("conversation-a").send("first", {
      delivery: "followUp",
      requestId: "activity-a",
    });
    running = host.drain();
    await first.started;
    const waiting = host.agent.session("conversation-b");
    await waiting.send("second", {
      delivery: "followUp",
      requestId: "activity-b",
    });
    watch = await waiting.watch();
    expect(watch.snapshot.activity.queue.state).toBe("queued");
    changes = watch.changes[Symbol.asyncIterator]();

    first.release();
    let activity: import("../src/client.js").SessionFrame | undefined;
    for (let attempt = 0; attempt < 8; attempt++) {
      const next = await changes.next();
      if (next.done) break;
      if (next.value.kind === "activity") {
        activity = next.value;
        break;
      }
    }
    expect(activity).toMatchObject({ kind: "activity" });
  } finally {
    first.release();
    await changes?.return?.();
    await watch?.close();
    await running;
    await host.close();
  }
});

it("cancels the waiting conversation by its queued id before it starts", async () => {
  const first = gatedReply("first reply");
  const models: string[] = [];
  const host = await openAgent([
    () => {
      models.push("first");
      return first.respond();
    },
    () => {
      models.push("second");
      return Promise.resolve(fauxAssistantMessage("second reply"));
    },
  ]);
  let running: Promise<void> | undefined;
  try {
    await host.agent.session("conversation-a").send("first", {
      delivery: "followUp",
      requestId: "cancel-a",
    });
    running = host.drain();
    await first.started;
    const waiting = host.agent.session("conversation-b");
    await waiting.send("second", {
      delivery: "followUp",
      requestId: "cancel-b",
    });
    const view = (await waiting.read()).activity.queue;
    if (view.state !== "queued") throw new Error("expected a waiting Session");

    await expect(
      waiting.cancel({ commandId: view.commandId, onlyIfPending: true }),
    ).resolves.toEqual({
      status: "accepted",
      commandId: view.commandId,
    });
    first.release();
    await running;
    running = undefined;
    await host.drain();

    const settled = await waiting.read();
    expect(settled.activity.queue.state).toBe("idle");
    expect(settled.terminals).toMatchObject([
      { commandId: view.commandId, outcome: "cancelled" },
    ]);
    // The cancelled conversation never reached the model.
    expect(models).toEqual(["first"]);
  } finally {
    first.release();
    await running;
    await host.close();
  }
});

it("answers too late, not an error, once the waiting conversation has started", async () => {
  const first = gatedReply("first reply");
  const second = gatedReply("second reply");
  const host = await openAgent([first.respond, second.respond]);
  let running: Promise<void> | undefined;
  try {
    await host.agent.session("conversation-a").send("first", {
      delivery: "followUp",
      requestId: "late-a",
    });
    running = host.drain();
    await first.started;
    const waiting = host.agent.session("conversation-b");
    const queued = await waiting.send("second", {
      delivery: "followUp",
      requestId: "late-b",
    });

    first.release();
    await second.started;
    expect((await waiting.read()).activity.queue.state).not.toBe("queued");

    const tooLate = { status: "too_late", commandId: queued.commandId };
    await expect(
      waiting.cancel({ commandId: queued.commandId, onlyIfPending: true }),
    ).resolves.toEqual(tooLate);
    // An id the Session never issued is the same ordinary outcome.
    await expect(
      waiting.cancel({ commandId: "never-issued", onlyIfPending: true }),
    ).resolves.toEqual({ status: "too_late", commandId: "never-issued" });

    // The started work is untouched and finishes normally.
    second.release();
    await running;
    expect((await waiting.read()).terminals).toMatchObject([
      { commandId: queued.commandId, outcome: "completed" },
    ]);
    // Once settled, cancelling is too late as well.
    await expect(
      waiting.cancel({ commandId: queued.commandId }),
    ).resolves.toEqual(tooLate);
  } finally {
    first.release();
    second.release();
    await running;
    await host.close();
  }
});

it("the second conversation waits for the first and then runs", async () => {
  const first = gatedReply("first reply");
  const host = await openAgent([
    first.respond,
    () => Promise.resolve(fauxAssistantMessage("second reply")),
  ]);
  let running: Promise<void> | undefined;
  try {
    await host.agent.session("conversation-a").send("first", {
      delivery: "followUp",
      requestId: "run-a",
    });
    running = host.drain();
    await first.started;
    const waiting = host.agent.session("conversation-b");
    const queued = await waiting.send("second", {
      delivery: "followUp",
      requestId: "run-b",
    });
    expect((await waiting.read()).activity.queue.state).toBe("queued");

    first.release();
    await running;
    running = undefined;
    await host.drain();

    const done = await waiting.read();
    expect(done.activity.queue.state).toBe("idle");
    expect(done.terminals).toMatchObject([
      { commandId: queued.commandId, outcome: "completed" },
    ]);
    expect(done.messages.at(-1)?.value).toMatchObject({
      role: "assistant",
      content: [{ type: "text", text: "second reply" }],
    });
  } finally {
    first.release();
    await running;
    await host.close();
  }
});

it("publishes the executor identity to a queued Session when another Session starts and settles", async () => {
  const first = gatedReply("first reply");
  const host = await openAgent([
    first.respond,
    () => Promise.resolve(fauxAssistantMessage("second reply")),
  ]);
  const a = await host.agent
    .session("conversation-a")
    .send("first", { delivery: "followUp", requestId: "observe-a" });
  const session = host.agent.session("conversation-b");
  await session.send("second", {
    delivery: "followUp",
    requestId: "observe-b",
  });
  const watch = await session.watch();
  const changes = watch.changes[Symbol.asyncIterator]();
  let running: Promise<void> | undefined;
  try {
    expect(watch.snapshot.activity.queue).toMatchObject({
      state: "queued",
      runningWork: null,
    });
    expect(watch.snapshot.hasMore).toBe(false);
    running = host.runtime.runNext();
    await first.started;
    await expect(changes.next()).resolves.toMatchObject({
      value: {
        kind: "activity",
        activity: {
          queue: {
            state: "queued",
            runningWork: {
              kind: "session",
              sessionId: "conversation-a",
              commandId: a.commandId,
            },
          },
        },
      },
    });
    first.release();
    await running;
    running = undefined;
    await expect(changes.next()).resolves.toMatchObject({
      value: {
        kind: "activity",
        activity: { queue: { state: "queued", runningWork: null } },
      },
    });
    expect((await session.read()).activity.queue).toMatchObject({
      state: "queued",
      runningWork: null,
    });
    await host.drain();
    expect((await session.read()).activity.queue.state).toBe("idle");
  } finally {
    await changes.return?.();
    await watch.close();
    first.release();
    await running;
    await host.close();
  }
});

it("counts queued messages outside the bounded pending page", async () => {
  const host = await openAgent([]);
  try {
    const session = host.agent.session("bounded-pending");
    for (let index = 0; index < 201; index++) {
      await session.send(`Prompt ${String(index)}`, {
        delivery: "followUp",
        requestId: `count-${String(index)}`,
      });
    }
    const { pending } = await session.read();
    expect(pending.total).toBe(201);
    expect(pending.prompts).toHaveLength(200);
    expect(pending.hasMore).toBe(true);
    const first = pending.prompts[0];
    if (first === undefined) throw new Error("Missing pending prompt");
    await session.cancel({ commandId: first.commandId, onlyIfPending: true });
    expect((await session.read()).pending.total).toBe(200);
  } finally {
    await host.close();
  }
});
