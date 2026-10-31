import { fauxModel } from "@fungi.computer/mule/testing";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import * as Effect from "effect/Effect";
import { expect, it } from "vitest";
import {
  makeCloudflareShiitake,
  type CloudflareHostJobs,
  type CloudflareShiitakeOptions,
  type CloudflareShiitakeState,
} from "../src/cloudflare.js";
import { defineExtension, ExtensionId } from "../src/extensions.js";
import { cloudflareStorageFixture } from "./fixtures/cloudflare-storage.fixture.js";
import {
  compatibilityRunDependencies,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

function gate() {
  let resolve: () => void = () => undefined;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

it("fences a short settled page without spinning, then replays core repair after a cursor-write crash in enqueue order", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const native = cloudflareStorageFixture(fixture.owner);
  const tasks: Promise<unknown>[] = [];
  const seen: string[] = [];
  let crashCursor = false;
  let refusedCursors = 0;
  const storage: CloudflareShiitakeState["storage"] = {
    ...native,
    sql: {
      exec: (sql, ...bindings) => {
        if (
          crashCursor &&
          sql.includes("INSERT INTO shiitake_settled_cursors") &&
          bindings[0] === "core"
        ) {
          refusedCursors += 1;
          throw new Error("Crash before core cursor commit");
        }
        return native.sql.exec(sql, ...bindings);
      },
    },
  };
  const id = ExtensionId.make("settlement-witness");
  const extension = defineExtension({ id, version: "1.0.0" }).define({
    hooks: {
      onSettled: (_ctx, page) =>
        Effect.sync(() => {
          seen.push(...page.map((run) => run.sessionId));
        }),
    },
  });
  let hostRuntime: Parameters<CloudflareHostJobs["open"]>[0] | undefined;
  const held = gate();
  const release = gate();
  const options: CloudflareShiitakeOptions = {
    ...compatibilityRunDependencies(),
    extensions: {
      roster: [extension],
      grants: [{ id, capabilities: ["settled"] }],
    },
    hostJobs: {
      lane: "post",
      open: (runtime) => {
        hostRuntime = runtime;
        return {
          execute: async () => {
            held.resolve();
            await release.promise;
            return { kind: "completed" };
          },
        };
      },
    },
    hostWork: [
      {
        pending: () => ({ due: false, nextWakeAt: null }),
        run: async () => {
          if (hostRuntime === undefined) throw new Error("Host lane missing");
          await hostRuntime.tickConcurrent();
        },
      },
    ],
  };
  const open = () =>
    makeCloudflareShiitake(
      {
        storage,
        waitUntil: (task) => {
          tasks.push(task);
        },
      },
      options,
    );
  let agent = await open();
  try {
    for (const task of tasks) await task;
    if (hostRuntime === undefined) throw new Error("Host lane missing");
    const transactional = hostRuntime.transactional;
    storage.transactionSync(() => {
      transactional.enqueueAcceptedJob({
        jobId: "7",
        queue: "Q",
        lane: "post",
        state: "queued",
        priority: 0,
        payload: {},
      });
    });
    await agent.alarm();
    await held.promise;
    const heldTask = tasks.at(-1);
    await agent
      .session("Q")
      .send("8", { requestId: "8", delivery: "followUp" });
    await agent
      .session("R")
      .send("9", { requestId: "9", delivery: "followUp" });
    await expect
      .poll(async () => (await agent.session("R").read()).activity.queue.state)
      .toBe("idle");
    expect(seen).toEqual([]);
    // The first task is the held host pass. Later Agent passes must all finish:
    // a false full page or settledLag would keep creating more immediate wakes.
    for (const task of tasks) if (task !== heldTask) await task;
    const before = tasks.length;
    await Promise.resolve();
    await Promise.resolve();
    expect(tasks).toHaveLength(before);
    expect(await storage.getAlarm()).not.toBeNull();
    crashCursor = true;
    release.resolve();
    await expect.poll(() => refusedCursors).toBeGreaterThan(0);
    for (const task of tasks) await task;
    expect(seen).toEqual([]);
    await expect(agent.close()).rejects.toThrow();
    crashCursor = false;
    agent = await open();
    for (const task of tasks) await task;
    expect(seen).toEqual(["Q", "R"]);
    await agent.alarm();
    for (const task of tasks) await task;
    expect(seen).toEqual(["Q", "R"]);
  } finally {
    crashCursor = false;
    release.resolve();
    await agent.close();
    fixture.close();
  }
});

it("a host completion cannot clear the Agent partition blocking a queued Session", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const storage = cloudflareStorageFixture(fixture.owner);
  const tasks: Promise<unknown>[] = [];
  const agentStarted = gate();
  const releaseAgent = gate();
  const hostStarted = gate();
  const releaseHost = gate();
  const faux = fauxModel([]);
  faux.setResponses([
    async () => {
      agentStarted.resolve();
      await releaseAgent.promise;
      return fauxAssistantMessage("Agent completed");
    },
    fauxAssistantMessage("Queued Session completed"),
  ]);
  let hostRuntime: Parameters<CloudflareHostJobs["open"]>[0] | undefined;
  const agent = await makeCloudflareShiitake(
    {
      storage,
      waitUntil: (task) => {
        tasks.push(task);
      },
    },
    {
      ...compatibilityRunDependencies(),
      model: {
        acquire: () =>
          Promise.resolve({
            model: faux.model,
            retryable: () => false,
            stream: (model, context, options) =>
              faux.stream(model, context, options),
            release: () => Promise.resolve(),
          }),
      },
      hostJobs: {
        lane: "delivery",
        open: (runtime) => {
          hostRuntime = runtime;
          return {
            execute: async () => {
              hostStarted.resolve();
              await releaseHost.promise;
              return { kind: "completed" };
            },
          };
        },
      },
    },
  );
  let hostTick: Promise<unknown> | undefined;
  try {
    for (const task of tasks) await task;
    await agent
      .session("Agent-A")
      .send("Hold", { delivery: "followUp", requestId: "agent-a" });
    await agentStarted.promise;
    const waiting = agent.session("Agent-B");
    const queued = await waiting.send("Wait", {
      delivery: "followUp",
      requestId: "agent-b",
    });
    const expected = {
      state: "queued",
      commandId: queued.commandId,
      runningWork: { phase: "thinking" },
    };
    expect((await waiting.read()).activity.queue).toEqual(expected);
    if (hostRuntime === undefined)
      throw new Error("Host partition did not open");
    const runtime = hostRuntime;
    storage.transactionSync(() => {
      runtime.transactional.enqueueAcceptedJob({
        jobId: "delivery-one",
        queue: "delivery-session",
        lane: "delivery",
        priority: 0,
        state: "queued",
        payload: {},
      });
    });
    hostTick = runtime.tickConcurrent();
    await hostStarted.promise;
    expect((await waiting.read()).activity.queue).toEqual(expected);
    releaseHost.resolve();
    await hostTick;
    expect((await waiting.read()).activity.queue).toEqual(expected);
    releaseAgent.resolve();
    await expect
      .poll(async () => (await waiting.read()).activity.queue.state)
      .toBe("idle");
    expect((await waiting.read()).terminals).toMatchObject([
      { outcome: "completed" },
    ]);
  } finally {
    releaseHost.resolve();
    releaseAgent.resolve();
    await hostTick;
    for (const task of tasks) await task;
    await agent.close();
    fixture.close();
  }
});
