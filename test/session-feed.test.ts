import * as Effect from "effect/Effect";
import { expect, it, vi } from "vitest";
import {
  parseSessionFrame,
  type SessionFrame,
  type SessionWatch,
} from "../src/client.js";
import {
  encodeSessionFrame,
  openSessionFrameFeed,
} from "../src/internal/observation-kernel.js";
import { runningActivity } from "./fixtures/client.fixture.js";
import { Shiitake } from "../src/index.js";
import { appendCompactionNotice } from "../src/internal/woodstock/index.js";
import {
  compatibilityRunDependencies,
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

it("admits a typed heartbeat without advancing the durable cursor", () => {
  expect(parseSessionFrame('{"kind":"heartbeat"}')).toEqual({
    kind: "heartbeat",
  });
  expect(() =>
    parseSessionFrame('{"kind":"heartbeat","cursor":"must-not-advance"}'),
  ).toThrow();
});

it("drops a slow watcher's backlog once and resets without another publication", async () => {
  const idle = vi.fn();
  const feed = openSessionFrameFeed(idle);
  const slow = feed.register();
  const fast = feed.register();
  const frame = { kind: "heartbeat" } as const;
  try {
    for (let i = 0; i < 257; i += 1) {
      feed.publish(frame);
      expect(await Effect.runPromise(fast.take)).toBe(frame);
    }
    // A blocked client cannot accumulate more nodes after overflow.
    for (let i = 0; i < 10_000; i += 1) feed.publish(frame);
    expect(await Effect.runPromise(slow.take)).toBe("reset");
    slow.checkpoint();
    feed.publish(frame);
    expect(await Effect.runPromise(slow.take)).toBe(frame);
  } finally {
    slow.close();
    expect(idle).not.toHaveBeenCalled();
    fast.close();
  }
  expect(idle).toHaveBeenCalledOnce();
});

it("bounds UTF-8 bytes as well as frame count and encodes shared frames once", async () => {
  const feed = openSessionFrameFeed(() => undefined);
  const queue = feed.register();
  const stringify = vi.spyOn(JSON, "stringify");
  try {
    for (let revision = 1; revision <= 16; revision += 1) {
      const frame = {
        kind: "progress",
        activity: runningActivity,
        progress: {
          messageId: "reply",
          revision,
          text: {
            offset: (revision - 1) * 32_768,
            append: "🙂".repeat(16_384),
          },
          thinking: { offset: 0, append: "" },
        },
      } as const;
      feed.publish(frame);
      expect(encodeSessionFrame(frame)).toBe(encodeSessionFrame(frame));
    }
    expect(await Effect.runPromise(queue.take)).toBe("reset");
    expect(stringify).toHaveBeenCalledTimes(16);
  } finally {
    stringify.mockRestore();
    queue.close();
  }
});

it("closing releases a waiting watcher without waiting for another write", async () => {
  const feed = openSessionFrameFeed(() => undefined);
  const queue = feed.register();
  const waiting = Effect.runPromise(queue.take);
  queue.close();
  await expect(waiting).resolves.toBe("closed");
});

it("does not publish a transaction that the owner refused to commit", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const agent = await Shiitake.make({
    store: fixture.owner,
    runtime: runtime.runtime,
    ...compatibilityRunDependencies(),
  });
  const session = agent.session("failed-write");
  const watch = await session.watch();
  const changes = watch.changes[Symbol.asyncIterator]();
  try {
    const transaction = vi
      .spyOn(fixture.owner, "transactionSync")
      .mockImplementationOnce(() => {
        throw new Error("Refused owner transaction");
      });
    await expect(session.modelPreference.set(null)).rejects.toThrow();
    transaction.mockRestore();
    // The first observable update must be the next successful durable change,
    // never an in-memory echo of the rejected selection.
    const next = changes.next();
    const accepted = await session.command({
      type: "prompt",
      delivery: "followUp",
      requestId: "after-refusal",
      text: "A committed admission",
    });
    await expect(next).resolves.toMatchObject({
      value: {
        kind: "update",
        pending: { prompts: [{ commandId: accepted.commandId }] },
      },
    });
  } finally {
    await watch.close();
    await changes.return?.();
    await agent.close();
    fixture.close();
  }
});

it("keeps a quiet Session's queued commit during sibling churn and replays strictly after its real cursor", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const agent = await Shiitake.make({
    store: fixture.owner,
    runtime: runtime.runtime,
    ...compatibilityRunDependencies(),
  });
  const quiet = agent.session("quiet-commit");
  const slow = await quiet.watch();
  const fast = await quiet.watch();
  const sibling = await agent.session("busy-sibling").watch();
  const slowChanges = slow.changes[Symbol.asyncIterator]();
  const fastChanges = fast.changes[Symbol.asyncIterator]();
  const siblingChanges = sibling.changes[Symbol.asyncIterator]();
  const notice = (sessionId: string, workId: string) =>
    appendCompactionNotice(fixture.owner, {
      sessionId,
      workId,
      trigger: "manual",
      result: "nothing_to_compact",
    });
  const nextUpdate = async (changes: AsyncIterator<SessionFrame, unknown>) => {
    for (;;) {
      const next = await changes.next();
      if (next.done) throw new Error("Session ended before its commit");
      if (next.value.kind === "update") return next.value;
      if (next.value.kind === "reset")
        throw new Error("Unexpected replay reset");
    }
  };
  let replay: AsyncIterator<SessionFrame, unknown> | undefined;
  try {
    const delivered = nextUpdate(fastChanges);
    const row = notice("quiet-commit", "first");
    const first = await delivered;
    expect(first).toMatchObject({
      kind: "update",
      compactionNotices: [{ sequence: row.sequence }],
    });
    if (first.cursor === null) throw new Error("Missing committed cursor");
    expect(first.cursor).toBe((await quiet.read()).cursor);
    for (let index = 0; index < 260; index += 1) {
      const next = nextUpdate(siblingChanges);
      notice("busy-sibling", `sibling-${String(index)}`);
      expect(await next).toMatchObject({ kind: "update" });
    }
    expect((await slowChanges.next()).value).toBe(first);

    const second = notice("quiet-commit", "second");
    replay = quiet.subscribe({ after: first.cursor })[Symbol.asyncIterator]();
    expect((await replay.next()).value).toMatchObject({
      kind: "update",
      compactionNotices: [{ sequence: second.sequence }],
    });
    const next = nextUpdate(replay);
    const third = notice("quiet-commit", "third");
    expect(await next).toMatchObject({
      kind: "update",
      compactionNotices: [{ sequence: third.sequence }],
    });
  } finally {
    await replay?.return?.();
    await Promise.all([slow.close(), fast.close(), sibling.close()]);
    await agent.close();
    fixture.close();
  }
});

it("caps watchers even when they share one producer and releases capacity on close", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const agent = await Shiitake.make({
    store: fixture.owner,
    runtime: runtime.runtime,
    ...compatibilityRunDependencies(),
  });
  const watches: SessionWatch[] = [];
  try {
    const session = agent.session("watcher-capacity");
    for (let index = 0; index < 64; index += 1)
      watches.push(await session.watch());
    await expect(session.watch()).rejects.toMatchObject({
      boundary: "runtime",
      cause: "rejected",
    });
    await watches.pop()?.close();
    watches.push(await session.watch());
  } finally {
    await Promise.all(watches.map((watch) => watch.close()));
    await agent.close();
    fixture.close();
  }
});
