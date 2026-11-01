import * as Effect from "effect/Effect";
import * as Context from "effect/Context";
import * as Fiber from "effect/Fiber";
import * as Scheduler from "effect/Scheduler";
import * as Stream from "effect/Stream";
import { setImmediate } from "node:timers/promises";
import { expect, it, vi } from "vitest";
import { make, type SessionFrame } from "../src/effect.js";
import {
  appendCompactionNotice,
  appendCompactionTransition,
} from "../src/internal/woodstock/index.js";
import {
  createNativeSqliteOwnerFixture,
  noExtensions,
} from "./fixtures/native-sqlite-owner.fixture.js";

async function observerFixture() {
  const fixture = createNativeSqliteOwnerFixture();
  const agent = await Effect.runPromise(
    make({
      store: fixture.owner,
      extensions: noExtensions,
      runtime: {
        bind: () =>
          Effect.succeed({
            commit: (mutation) => mutation,
            requestWake: Effect.void,
            close: Effect.void,
          }),
      },
      instructions: {
        load: () =>
          Effect.succeed({ revision: "join", text: "No model work." }),
      },
      model: {
        acquire: () => Effect.die(new Error("This test admits no model work")),
      },
    }),
  );
  return { fixture, agent, session: agent.session("join") };
}

async function driveRequestUntil(
  scheduled: (() => void)[],
  done: () => boolean,
  maxSteps = 2_000,
) {
  for (let step = 0; step < maxSteps && !done(); step += 1) {
    await setImmediate();
    scheduled.shift()?.();
  }
}

it.each(["watch", "subscribe"] as const)(
  "%s retains new compactions whose presentation horizon precedes its cursor",
  async (path) => {
    const { fixture, agent } = await observerFixture();
    const sessionId = await Effect.runPromise(
      agent.createSession(`compaction-horizon-${path}`),
    );
    const session = agent.session(sessionId);
    const anchor = await Effect.runPromise(session.watch());
    for (const requestId of ["kept", "later"])
      await Effect.runPromise(
        session.command({
          type: "operator",
          requestId,
          author: "operator",
          text: requestId,
        }),
      );
    const before = await Effect.runPromise(session.read());
    const head = before.messages[0];
    const leaf = before.messages.at(-1);
    if (head === undefined || leaf === undefined || before.cursor === null)
      throw new Error("Missing compaction lineage");
    expect(head.sequence).toBeLessThan(leaf.sequence);
    const observation =
      path === "watch"
        ? await Effect.runPromise(session.watch())
        : {
            changes: session.subscribe({ after: before.cursor }),
            close: Effect.void,
          };
    const frames = Effect.runFork(
      observation.changes.pipe(
        Stream.takeUntil(
          (frame) =>
            frame.kind === "update" &&
            frame.compactionNotices?.some(
              (notice) => notice.diagnostic === "compactions committed",
            ) === true,
        ),
        Stream.runCollect,
      ),
    );
    try {
      let expectedLeafId = leaf.id;
      const committed: string[] = [];
      for (const workId of ["first-summary", "second-summary"]) {
        const row = appendCompactionTransition(fixture.owner, {
          sessionId,
          workId,
          expectedLeafId,
          head: head.id,
          summary: workId,
          trigger: "manual",
          tokensBefore: 100,
          tokensAfter: 40,
          contextRevision: expectedLeafId,
          usage: {
            input: 1,
            output: 1,
            cacheRead: 0,
            cacheWrite: 0,
            totalTokens: 2,
            cost: {
              input: 0,
              output: 0,
              cacheRead: 0,
              cacheWrite: 0,
              total: 0,
            },
          },
        });
        committed.push(row.id);
        expectedLeafId = row.id;
      }
      appendCompactionNotice(fixture.owner, {
        sessionId,
        workId: "barrier",
        trigger: "manual",
        result: "failed",
        diagnostic: "compactions committed",
      });
      const received = (await Effect.runPromise(Fiber.join(frames))).flatMap(
        (frame) => (frame.kind === "update" ? (frame.compactions ?? []) : []),
      );
      expect(received.map(({ id }) => id)).toEqual(committed);
      expect(received.map(({ sequence }) => sequence)).toEqual([
        head.sequence,
        head.sequence,
      ]);
    } finally {
      frames.interruptUnsafe();
      await Effect.runPromise(observation.close);
      await Effect.runPromise(anchor.close);
      await Effect.runPromise(agent.close);
      fixture.close();
    }
  },
);

it.each(
  (["watch", "subscribe"] as const).flatMap((path) =>
    Array.from({ length: 10 }, (_, round) => ({ path, round: round + 1 })),
  ),
)(
  "$path retains every durable row written during its replay/live join (round $round)",
  async ({ path }) => {
    const { fixture, agent, session } = await observerFixture();
    // Keep the existing producer on the Agent's scheduler while the new
    // request yields between its operations.
    const anchor = await Effect.runPromise(session.watch());
    const seed = appendCompactionNotice(fixture.owner, {
      sessionId: "join",
      workId: "baseline",
      trigger: "manual",
      result: "nothing_to_compact",
    });
    const after = (await Effect.runPromise(session.read())).cursor;
    if (after === null)
      throw new Error("Missing owner cursor after seed commit");
    const scheduled: (() => void)[] = [];
    const scheduler = new Scheduler.MixedScheduler("async", (task) => {
      scheduled.push(task);
      return () => undefined;
    });
    const rows: number[] = [];
    const observed: number[] = [];
    const joinState = { ready: false };
    const isBarrier = (frame: SessionFrame) =>
      frame.kind === "update" &&
      frame.compactionNotices?.some(
        (notice) => notice.diagnostic === "join complete",
      ) === true;
    const record = (frame: SessionFrame) => {
      if (frame.kind === "update") joinState.ready = true;
      if (frame.kind === "update")
        observed.push(
          ...(frame.compactionNotices ?? [])
            .filter((row) => row.sequence > seed.sequence)
            .map((row) => row.sequence),
        );
      if (frame.kind === "reset")
        throw new Error("Unexpected reset during join");
    };
    const join = Effect.gen(function* () {
      if (path === "watch") {
        const watch = yield* session.watch();
        joinState.ready = true;
        observed.push(
          ...(watch.snapshot.compactionNotices ?? [])
            .filter((row) => row.sequence > seed.sequence)
            .map((row) => row.sequence),
        );
        yield* watch.changes.pipe(
          Stream.tap((frame) =>
            Effect.sync(() => {
              record(frame);
            }),
          ),
          Stream.takeUntil(isBarrier),
          Stream.runDrain,
        );
        yield* watch.close;
      } else {
        yield* session.subscribe({ after }).pipe(
          Stream.tap((frame) =>
            Effect.sync(() => {
              record(frame);
            }),
          ),
          Stream.takeUntil(isBarrier),
          Stream.runDrain,
        );
      }
    });
    const joining = Effect.runForkWith(
      Context.make(Scheduler.Scheduler, scheduler).pipe(
        Context.add(Scheduler.MaxOpsBeforeYield, 3),
      ),
    )(join);
    await Effect.runPromise(Effect.yieldNow);
    const write = (barrier = false) => {
      const outcome = barrier
        ? { result: "failed" as const, diagnostic: "join complete" }
        : { result: "nothing_to_compact" as const };
      const row = appendCompactionNotice(fixture.owner, {
        sessionId: "join",
        workId: `row-${String(rows.length)}`,
        trigger: "manual",
        ...outcome,
      });
      rows.push(row.sequence);
      return row.sequence;
    };
    try {
      // Advance only the joining request, committing real owner facts between
      // owner-read yields. The independent producer can publish each fact
      // before the request resumes; no SQL or read result is replaced.
      for (let step = 0; step < 2_000; step += 1) {
        await setImmediate();
        const resume = scheduled.shift();
        if (resume === undefined) {
          if (joinState.ready) break;
          continue;
        }
        const before = fixture.ownerOperations();
        resume();
        if (fixture.ownerOperations() > before) write();
      }
      expect(
        rows.length,
        "writes must interleave with the joining owner reads",
      ).toBeGreaterThan(0);
      write(true);
      await driveRequestUntil(
        scheduled,
        () => joining.pollUnsafe() !== undefined,
      );
      expect([...observed].sort((a, b) => a - b)).toEqual(rows);
      expect(
        joining.pollUnsafe(),
        "join must reach the final committed row",
      ).toBeDefined();
      await Effect.runPromise(Fiber.join(joining));
    } finally {
      joining.interruptUnsafe();
      await driveRequestUntil(scheduled, () => scheduled.length === 0, 500);
      await Effect.runPromise(anchor.close);
      await Effect.runPromise(agent.close);
      fixture.close();
    }
  },
);

it.each(["producer", "heartbeat"] as const)(
  "keeps the shared %s independent of the first watcher's request scheduler",
  async (source) => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    const { fixture, agent, session } = await observerFixture();
    const scheduled: (() => void)[] = [];
    const scheduler = new Scheduler.MixedScheduler("async", (task) => {
      scheduled.push(task);
      return () => undefined;
    });
    const request = Effect.runForkWith(
      Context.make(Scheduler.Scheduler, scheduler).pipe(
        Context.add(Scheduler.MaxOpsBeforeYield, 3),
      ),
    )(session.watch());
    await driveRequestUntil(
      scheduled,
      () => request.pollUnsafe() !== undefined,
    );
    expect(request.pollUnsafe(), "first request must finish").toBeDefined();
    const first = await Effect.runPromise(Fiber.join(request));
    const second = await Effect.runPromise(session.watch());
    const received: SessionFrame[] = [];
    const reading = Effect.runFork(
      second.changes.pipe(
        Stream.take(1),
        Stream.runForEach((frame) =>
          Effect.sync(() => {
            received.push(frame);
          }),
        ),
      ),
    );
    try {
      // The request has ended. Never drive its scheduler again until cleanup.
      if (source === "producer") {
        const row = appendCompactionNotice(fixture.owner, {
          sessionId: "join",
          workId: "after-request",
          trigger: "manual",
          result: "nothing_to_compact",
        });
        await vi.advanceTimersByTimeAsync(0);
        expect(received).toMatchObject([
          { kind: "update", compactionNotices: [{ sequence: row.sequence }] },
        ]);
      } else {
        await vi.advanceTimersByTimeAsync(15_000);
        expect(received).toEqual([{ kind: "heartbeat" }]);
      }
    } finally {
      reading.interruptUnsafe();
      const closing = Effect.runFork(
        first.close.pipe(
          Effect.andThen(second.close),
          Effect.andThen(agent.close),
        ),
      );
      await driveRequestUntil(
        scheduled,
        () => closing.pollUnsafe() !== undefined,
      );
      await Effect.runPromise(Fiber.join(closing));
      fixture.close();
      vi.useRealTimers();
    }
  },
);
