import { expect, it, vi } from "vitest";
import { type SessionFrame } from "../src/client.js";
import { Shiitake } from "../src/index.js";
import {
  compatibilityRunDependencies,
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

it("fans one Session read and immutable update out to every watcher", async () => {
  async function observe(watchers: number) {
    const fixture = createNativeSqliteOwnerFixture();
    const runtime = createCompatibilityRuntimeHost();
    const agent = await Shiitake.make({
      store: fixture.owner,
      runtime: runtime.runtime,
      ...compatibilityRunDependencies(),
    });
    const session = agent.session("fan-out");
    const watches = await Promise.all(
      Array.from({ length: watchers }, () => session.watch()),
    );
    const iterators: AsyncIterator<SessionFrame, unknown>[] = watches.map(
      (watch) => watch.changes[Symbol.asyncIterator](),
    );
    try {
      const next = iterators.map(async (iterator) => {
        let frame = await iterator.next();
        while (
          !frame.done &&
          (frame.value.kind !== "update" ||
            !frame.value.pending.prompts.some(
              (prompt) => prompt.requestId === "fan-out-command",
            ))
        )
          frame = await iterator.next();
        return frame;
      });
      const before = fixture.ownerOperations();
      const accepted = await session.command({
        type: "prompt",
        delivery: "followUp",
        requestId: "fan-out-command",
        text: "Queued, without starting a model call",
      });
      const frames = await Promise.all(next);
      const reads = fixture.ownerOperations() - before;
      for (const frame of frames) {
        if (frame.done) throw new Error("Watcher ended before its update");
        expect(frame.value).toMatchObject({
          kind: "update",
          pending: { prompts: [{ commandId: accepted.commandId }] },
        });
      }
      return { reads, frames };
    } finally {
      await Promise.all(watches.map((watch) => watch.close()));
      await Promise.all(
        iterators.map(async (iterator) => {
          await iterator.return?.();
        }),
      );
      await agent.close();
      fixture.close();
    }
  }
  const many = await observe(8);
  const one = await observe(1);
  expect(many.reads).toBe(one.reads);
  for (const frame of many.frames)
    expect(frame.value).toBe(many.frames[0]?.value);
});

it("delivers a heartbeat every fifteen seconds without a Woodstock read or cursor", async () => {
  vi.useFakeTimers();
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const agent = await Shiitake.make({
    store: fixture.owner,
    runtime: runtime.runtime,
    ...compatibilityRunDependencies(),
  });
  const watch = await agent.session("quiet").watch();
  const received: SessionFrame[] = [];
  const collecting = (async () => {
    for await (const frame of watch.changes)
      if (frame.kind === "heartbeat") received.push(frame);
  })();
  try {
    await vi.advanceTimersByTimeAsync(0);
    const before = fixture.ownerOperations();
    for (let beat = 0; beat < 3; beat += 1) {
      await vi.advanceTimersByTimeAsync(15_000);
      expect(received).toEqual(
        Array.from({ length: beat + 1 }, () => ({ kind: "heartbeat" })),
      );
    }
    expect(fixture.ownerOperations()).toBe(before);
  } finally {
    await watch.close();
    await collecting;
    await agent.close();
    fixture.close();
    expect(vi.getTimerCount()).toBe(0);
    vi.useRealTimers();
  }
});

it("resets only the byte-lagging watcher without another durable write", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const agent = await Shiitake.make({
    store: fixture.owner,
    runtime: runtime.runtime,
    ...compatibilityRunDependencies(),
  });
  const session = agent.session("byte-lag");
  await session.command({
    type: "prompt",
    delivery: "followUp",
    requestId: "byte-lag-seed",
    text: "Create the durable Session without starting model work.",
  });
  await runtime.runNext();
  const slow = await session.watch();
  const fast = await session.watch();
  const slowChanges = slow.changes[Symbol.asyncIterator]();
  const fastChanges = fast.changes[Symbol.asyncIterator]();
  try {
    for (let index = 0; index < 18; index += 1) {
      await session.command({
        type: "operator",
        author: "operator",
        requestId: `large-row-${String(index)}`,
        text: "x".repeat(64_000),
      });
      let frame = await fastChanges.next();
      while (!frame.done && frame.value.kind !== "update") {
        expect(frame.value.kind).not.toBe("reset");
        frame = await fastChanges.next();
      }
      expect(frame).toMatchObject({ done: false, value: { kind: "update" } });
    }
    const fresh = await session.read();
    expect(await slowChanges.next()).toMatchObject({
      done: false,
      value: { kind: "reset", baseline: { cursor: fresh.cursor } },
    });
  } finally {
    await Promise.all([slow.close(), fast.close()]);
    await slowChanges.return?.();
    await fastChanges.return?.();
    await agent.close();
    fixture.close();
  }
});

it("recovers one oversized pending frame through fresh baselines even for waiting watchers", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const agent = await Shiitake.make({
    store: fixture.owner,
    runtime: runtime.runtime,
    ...compatibilityRunDependencies(),
  });
  const session = agent.session("oversized-frame");
  const watches = await Promise.all([session.watch(), session.watch()]);
  const changes = watches.map((watch) => watch.changes[Symbol.asyncIterator]());
  try {
    for (let index = 0; index < 4; index += 1) {
      // Both consumers drain each frame before the next publication. The
      // fourth complete pending view, not either backlog, exceeds the cap.
      const next = changes.map((iterator) => iterator.next());
      await session.command({
        type: "prompt",
        delivery: "followUp",
        requestId: `oversized-pending-${String(index)}`,
        text: "x".repeat(262_144),
      });
      for (const frame of await Promise.all(next))
        expect(frame).toMatchObject({
          done: false,
          value:
            index < 3
              ? { kind: "update", pending: { total: index + 1 } }
              : { kind: "reset", baseline: { pending: { total: 4 } } },
        });
    }
    const fresh = await session.read();
    expect(
      new TextEncoder().encode(JSON.stringify(fresh.pending)).byteLength,
    ).toBeGreaterThan(1_048_576);
    expect(fresh.pending.prompts).toHaveLength(4);
  } finally {
    await Promise.all(watches.map((watch) => watch.close()));
    for (const iterator of changes) await iterator.return?.();
    await agent.close();
    fixture.close();
  }
});
