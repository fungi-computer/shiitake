import { BirdDog } from "../../bird-dog/src/index.js";
import { makeRuntime } from "../../watchdog/src/effect.js";
import * as Effect from "effect/Effect";
import { expect, it } from "vitest";

import {
  prepareSessionRun,
  prepareSessionStore,
} from "../src/internal/woodstock/session-store.js";
import { admitStoredCommand } from "./fixtures/admission-door.fixture.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";
import { promptCommand } from "../src/internal/woodstock/session-entry.js";

const digest = "a".repeat(64);
const contract = {
  core: digest,
  extensions: [],
} as const;

it("owns SQLite persistence behind one deep runtime construction", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    const job = {
      jobId: "command-entry-1",
      queue: "session-1",
      lane: "default",
      priority: 0,
      payload: {
        work: { sessionId: "session-1", commandFactId: "command-entry-1" },
      },
      state: "queued",
    } as const;

    let executed = 0;
    const options = {
      owner: fixture.owner,
      executor: {
        execute: (running: Readonly<{ payload: unknown }>) =>
          Effect.sync(() => {
            executed += 1;
            expect(running.payload).toEqual(job.payload);
            return { kind: "completed" as const };
          }),
      },
      isAlive: () => Effect.succeed(true),
      wake: { recompute: () => Effect.void },
    };
    const runtime = Effect.runSync(makeRuntime(options));
    expect(runtime).toHaveProperty("transactional");
    fixture.owner.transactionSync(() => {
      runtime.transactional.enqueueAcceptedJob(job);
    });

    const settled = await Effect.runPromise(runtime.tick());
    expect(settled).toMatchObject({
      status: "settled",
      job: { ...job, state: "settled", outcome: "completed" },
    });
    expect(executed).toBe(1);

    const reconstructed = Effect.runSync(makeRuntime(options));
    const read = await Effect.runPromise(reconstructed.read(job.jobId));
    expect(read).toMatchObject({
      status: "found",
      job: { ...job, state: "settled", outcome: "completed" },
    });
  } finally {
    fixture.close();
  }
});

it("commits a Woodstock command and its opaque Watchdog job together", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    prepareSessionStore(fixture.owner);
    let preparedContext: ReturnType<typeof prepareSessionRun> | undefined;
    const runtime = Effect.runSync(
      makeRuntime({
        owner: fixture.owner,
        executor: {
          execute: (running) =>
            Effect.sync(() => {
              const payload = BirdDog.decodeAgentJob(running);
              preparedContext = prepareSessionRun(fixture.owner, payload.work);
              return { kind: "completed" as const };
            }),
        },
        isAlive: () => Effect.succeed(true),
        wake: { recompute: () => Effect.void },
      }),
    );
    const input = {
      requestId: "watchdog-owner-request",
      delivery: "followUp",

      type: "prompt",
      text: "Remember this exact command",
    } as const;
    const accepted = admitStoredCommand(
      fixture.owner,
      "watchdog-owner-session",
      input,
      {
        contract,
        enqueue: (work) => {
          runtime.transactional.enqueueAcceptedJob(
            BirdDog.agentJob({ work, delivery: work.delivery }),
          );
        },
      },
    );

    const queued = await Effect.runPromise(
      runtime.read(accepted.command.entry.workId),
    );
    expect(queued).toMatchObject({
      status: "found",
      job: {
        jobId: accepted.command.entry.workId,
        queue: accepted.command.sessionId,
        lane: "default",
        payload: {
          work: {
            sessionId: accepted.command.sessionId,
            commandFactId: accepted.command.id,
          },
        },
        state: "queued",
      },
    });

    const writesAfterAcceptance = fixture.ownerWrites();
    const replay = admitStoredCommand(
      fixture.owner,
      accepted.command.sessionId,
      input,
      {
        contract,
        enqueue: (work) => {
          runtime.transactional.enqueueAcceptedJob(
            BirdDog.agentJob({ work, delivery: work.delivery }),
          );
        },
      },
    );
    expect(replay.delivery).toBe("replay");
    expect(fixture.ownerWrites()).toBe(writesAfterAcceptance);

    const settled = await Effect.runPromise(runtime.tick());
    expect(settled).toMatchObject({
      status: "settled",
      job: { jobId: accepted.command.entry.workId, outcome: "completed" },
    });
    expect(preparedContext?.messages).toEqual([
      promptCommand(accepted.command.entry)?.message,
    ]);
  } finally {
    fixture.close();
  }
});
