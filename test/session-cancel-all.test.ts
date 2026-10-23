import * as Effect from "effect/Effect";
import { openCore } from "./fixtures/extension-host.fixture.js";
import { grantAll, recorderExtension } from "./fixtures/fixture-extensions.js";
import { fauxModel } from "@fungi.computer/mule/testing";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { expect, it } from "vitest";

import { Shiitake } from "../src/index.js";
import { createShiitakeHttpRoutes } from "../src/http.js";
import { createShiitakeClient } from "../src/remote-client.js";
import {
  compatibilityRunDependencies,
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

it("a fresh client can see then cancel the running turn and every queued prompt", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const faux = fauxModel([]);
  let markStarted!: () => void;
  let releaseRun!: () => void;
  const started = new Promise<void>((resolve) => {
    markStarted = resolve;
  });
  const released = new Promise<void>((resolve) => {
    releaseRun = resolve;
  });
  let signal: AbortSignal | undefined;
  faux.setResponses([
    async (_context, options) => {
      signal = options?.signal;
      markStarted();
      await released;
      return fauxAssistantMessage("stopped", { stopReason: "aborted" });
    },
  ]);
  const agent = await Shiitake.make({
    ...compatibilityRunDependencies(),
    store: fixture.owner,
    runtime: runtime.runtime,
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
  let running: Promise<void> | undefined;
  let client: ReturnType<typeof createShiitakeClient> | undefined;
  try {
    const session = agent.session("cancel-all");
    const active = await session.send("active instruction");
    running = runtime.runNext();
    await started;
    const queued = await session.send("queued follow-up", {
      delivery: "followUp",
    });
    const steer = await session.send("queued steer", { delivery: "steer" });
    const routes = createShiitakeHttpRoutes(
      (id) => agent.session(id),
      agent.listSessions,
      agent.createSession,
      agent.deleteSession,
      () => agent.extensions,
    );
    client = createShiitakeClient({
      baseUrl: "http://fixture.test",
      fetch: (input, init) =>
        Promise.resolve(routes.fetch(new Request(input, init))),
    });
    const fresh = client.session("cancel-all");
    expect(
      (await fresh.read()).pending.prompts.map(({ text }) => text),
    ).toEqual(["queued follow-up", "queued steer"]);
    await fresh.abort();
    expect(signal?.aborted).toBe(true);
    expect((await fresh.read()).pending.prompts).toEqual([]);
    releaseRun();
    await running;
    running = undefined;
    while (runtime.snapshot().state === "pending") await runtime.runNext();
    const snapshot = await fresh.read();
    for (const { commandId } of [active, queued, steer])
      expect(
        snapshot.terminals.find((terminal) => terminal.commandId === commandId),
      ).toMatchObject({ commandId, outcome: "cancelled" });
    expect(
      snapshot.messages
        .filter(({ value }) => value.role === "user")
        .map(({ value }) => value.content),
    ).toEqual(["active instruction", "queued follow-up", "queued steer"]);
    expect(faux.state.callCount).toBe(1);
  } finally {
    releaseRun();
    await running;
    await client?.close();
    await agent.close();
    fixture.close();
  }
});

// A Session's pending page is bounded in bytes (2 MiB) as well as in rows
// (200). Nine prompts at the 256 KiB prompt limit overflow the byte bound, so
// the queue is "beyond the visible queue" without 205 sequential sends,
// cancellations and runs (about 0.85 s alone, 5x that under load). The row
// bound is covered at the projection level in woodstock-retained-session.test.ts.
const WITHDRAWN_PROMPTS = 9;
const withdrawnText = (index: number) => {
  const label = ["withdrawn instruction", String(index), ""].join(" ");
  return label + "x".repeat(256 * 1024 - label.length);
};

it("cancels beyond the visible queue, retains text, and excludes unrun prompts from later model context", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const requests: string[] = [];
  const faux = fauxModel([
    (context) => {
      requests.push(JSON.stringify(context.messages));
      return fauxAssistantMessage("unrelated reply");
    },
    (context) => {
      requests.push(JSON.stringify(context.messages));
      return fauxAssistantMessage("new reply");
    },
  ]);
  const agent = await Shiitake.make({
    ...compatibilityRunDependencies(),
    store: fixture.owner,
    runtime: runtime.runtime,
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
  try {
    const session = agent.session("cancel-many");
    for (let index = 0; index < WITHDRAWN_PROMPTS; index++)
      await session.send(withdrawnText(index));
    const other = agent.session("unrelated");
    await other.send("unrelated instruction");
    expect((await session.read()).pending.hasMore).toBe(true);
    await session.abort();
    await session.abort();
    expect((await session.read()).pending).toEqual({
      total: 0,
      prompts: [],
      hasMore: false,
    });
    expect((await other.read()).pending.prompts).toHaveLength(1);
    await session.send("new instruction");
    while (runtime.snapshot().state === "pending") await runtime.runNext();
    let snapshot = await session.read({ limit: 200 });
    const retained = new Set<string>();
    const cancelled = new Set<string>();
    for (;;) {
      for (const message of snapshot.messages)
        if (message.value.role === "user") retained.add(message.id);
      for (const terminal of snapshot.terminals)
        if (terminal.outcome === "cancelled") cancelled.add(terminal.commandId);
      if (!snapshot.hasMore) break;
      if (snapshot.oldestCursor === undefined || snapshot.oldestCursor === null)
        throw new Error("Missing history cursor");
      snapshot = await session.read({
        limit: 200,
        before: snapshot.oldestCursor,
      });
    }
    expect(retained.size).toBe(WITHDRAWN_PROMPTS + 1);
    expect(cancelled.size).toBe(WITHDRAWN_PROMPTS);
    expect(faux.state.callCount).toBe(2);
    expect(requests.join("\n")).not.toContain("withdrawn instruction");
    expect(requests.join("\n")).toContain("new instruction");
  } finally {
    await agent.close();
    fixture.close();
  }
});

it("never uses a cancelled queued prompt as the Session title", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const agent = await Shiitake.make({
    ...compatibilityRunDependencies(),
    store: fixture.owner,
    runtime: runtime.runtime,
  });
  try {
    const session = agent.session("cancelled-title");
    await session.send("Withdrawn title");
    await session.abort();
    const cancelled = (await agent.listSessions()).sessions.find(
      ({ sessionId }) => sessionId === "cancelled-title",
    );
    expect(cancelled).toBeDefined();
    expect(cancelled?.state === "unavailable" ? "" : cancelled?.title).toBe("");
    await session.send("Real title");
    for (
      let pass = 0;
      pass < 10 && runtime.snapshot().state === "pending";
      pass++
    )
      await runtime.runNext();
    const completed = (await agent.listSessions()).sessions.find(
      ({ sessionId }) => sessionId === "cancelled-title",
    );
    expect(completed?.state === "unavailable" ? "" : completed?.title).toBe(
      "Real title",
    );
  } finally {
    await agent.close();
    fixture.close();
  }
});

it("records a cancelled settlement for queued scheduled extension work", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const { core, host } = await openCore(
    fixture.owner,
    grantAll([recorderExtension()]),
  );
  try {
    const session = core.agent.session("cancel-scheduled");
    await Effect.runPromise(
      session.extension("test-recorder").call("arm", {
        key: "cancel-me",
        dueAt: 0,
      }),
    );
    // This pass admits the timer's prompt after the work executor has run.
    await host.reconcile();
    const queued = await Effect.runPromise(session.read());
    expect(queued.pending.prompts.map(({ text }) => text)).toEqual([
      "timer cancel-me",
    ]);
    await Effect.runPromise(session.abort);
    await host.drain();
    const cancelled = await Effect.runPromise(session.read());
    expect(cancelled.pending.prompts).toEqual([]);
    expect(cancelled.terminals).toEqual([
      expect.objectContaining({ outcome: "cancelled" }),
    ]);
    expect(cancelled.messages.map(({ value }) => value.content)).toEqual([
      "timer cancel-me",
    ]);
    expect(
      fixture.owner.sql
        .exec(
          `SELECT json_extract(value_json, '$.outcome') AS outcome,
                json_extract(value_json, '$.originExtension') AS origin_extension,
                json_extract(value_json, '$.correlation') AS correlation
         FROM shiitake_extension_records WHERE collection = 'seen'`,
        )
        .toArray(),
    ).toEqual([
      {
        outcome: "cancelled",
        origin_extension: "test-recorder",
        correlation: "cancel-me",
      },
    ]);
    await host.reconcile();
    await expect(
      Effect.runPromise(
        core.agent.extensions.call("test-recorder", "seen", {}),
      ),
    ).resolves.toEqual({ count: 1 });
  } finally {
    await Effect.runPromise(core.agent.close);
    fixture.close();
  }
});
