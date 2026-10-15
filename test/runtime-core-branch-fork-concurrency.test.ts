import { fauxModel } from "@fungi.computer/mule/testing";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Scheduler from "effect/Scheduler";
import { expect, it } from "vitest";

import {
  make,
  type ForkedSession,
  type ShiitakeEffectAgent,
  type ShiitakeEffectOptions,
  type ShiitakeEffectSession,
  type SessionSnapshot,
} from "../src/effect.js";
import type { SqliteOwner } from "../src/internal/contracts.js";
import {
  createNativeSqliteOwnerFixture,
  type NativeSqliteOwnerFixture,
  noExtensions,
} from "./fixtures/native-sqlite-owner.fixture.js";

type Operation = "branch" | "fork";
type RaceRuntime = Readonly<{
  runtime: ShiitakeEffectOptions["runtime"];
  reconcile: () => Promise<void>;
}>;
type RaceContext = Readonly<{
  fixture: NativeSqliteOwnerFixture;
  events: string[];
  agent: ShiitakeEffectAgent;
  session: ShiitakeEffectSession;
  reconcile: () => Promise<void>;
}>;

function instrumentOwner(
  fixture: NativeSqliteOwnerFixture,
  events: string[],
): SqliteOwner {
  return {
    transactionSync: (operation) => fixture.owner.transactionSync(operation),
    sql: {
      exec: (statement, ...bindings) => {
        if (
          statement.includes("FROM watchdog_jobs") &&
          statement.includes("queue_name = ?")
        )
          events.push("queue-read");
        if (statement.includes("INSERT INTO session_command_admissions"))
          events.push("prompt-admission-write");
        if (statement.includes("INSERT INTO session_entries"))
          events.push("lineage-entry-write");
        if (statement.includes("INSERT INTO sessions"))
          events.push("lineage-session-write");
        if (statement.includes("UPDATE sessions SET leaf_id"))
          events.push("lineage-leaf-write");
        return fixture.owner.sql.exec(statement, ...bindings);
      },
    },
  };
}

function makeRaceRuntime(): RaceRuntime {
  let reconcile: Effect.Effect<void, Error> | undefined;
  const runtime: ShiitakeEffectOptions["runtime"] = {
    bind: ({ reconcile: next }) =>
      Effect.sync(() => {
        reconcile = next;
        return {
          commit: (mutation) => mutation,
          requestWake: Effect.void,
          close: Effect.void,
        };
      }),
  };
  return {
    runtime,
    reconcile: () => {
      if (reconcile === undefined)
        throw new Error("Missing runtime reconciler");
      return Effect.runPromise(reconcile);
    },
  };
}

async function openRace(): Promise<RaceContext> {
  const fixture = createNativeSqliteOwnerFixture();
  const events: string[] = [];
  const owner = instrumentOwner(fixture, events);
  const raceRuntime = makeRaceRuntime();
  const faux = fauxModel([]);
  faux.setResponses([fauxAssistantMessage("first answer")]);
  const agent = await Effect.runPromise(
    make({
      store: owner,
      extensions: noExtensions,
      runtime: raceRuntime.runtime,
      instructions: {
        load: () => Effect.succeed({ revision: "race-v1", text: "Answer." }),
      },
      model: {
        acquire: () =>
          Effect.succeed({
            model: faux.model,
            stream: faux.stream,
            release: Effect.void,
          }),
      },
    }),
  );
  return {
    fixture,
    events,
    agent,
    session: agent.session("scheduler-race"),
    reconcile: raceRuntime.reconcile,
  };
}

async function completeFirstTurn(context: RaceContext) {
  await Effect.runPromise(
    context.session.command({
      delivery: "followUp",

      type: "prompt",
      requestId: "first",
      text: "first",
    }),
  );
  await context.reconcile();
  const complete = await Effect.runPromise(context.session.read());
  const entryId = complete.messages[1]?.id;
  if (entryId === undefined) throw new Error("Missing completed target");
  return { complete, entryId };
}

function startMutation(
  context: RaceContext,
  operation: Operation,
  entryId: string,
) {
  const scheduled: (() => void)[] = [];
  const scheduler = new Scheduler.MixedScheduler("async", (task) => {
    scheduled.push(task);
    return () => undefined;
  });
  const mutation =
    operation === "branch"
      ? { type: "branch" as const, entryId }
      : { type: "fork" as const, entryId };
  const fiber = Effect.runFork(
    context.session
      .command(mutation)
      .pipe(
        Effect.provideService(Scheduler.MaxOpsBeforeYield, 10),
        Effect.provideService(Scheduler.Scheduler, scheduler),
      ),
  );
  return { fiber, scheduled };
}

async function driveToQueueRead(
  context: RaceContext,
  fiber: ReturnType<typeof Effect.runFork>,
  scheduled: (() => void)[],
) {
  await Effect.runPromise(Effect.yieldNow);
  for (
    let steps = 0;
    !context.events.includes("queue-read") && steps < 5_000;
    steps += 1
  ) {
    const next = scheduled.shift();
    if (next === undefined)
      throw new Error(
        `Mutation did not schedule: ${JSON.stringify(fiber.pollUnsafe())}`,
      );
    next();
  }
}

function assertLineageBeforePrompt(context: RaceContext, operation: Operation) {
  expect(context.events).toContain("queue-read");
  expect(context.events).toContain(
    operation === "branch" ? "lineage-leaf-write" : "lineage-entry-write",
  );
}

async function finishMutation(
  context: RaceContext,
  fiber: ReturnType<typeof Effect.runFork>,
  scheduled: (() => void)[],
  complete: SessionSnapshot,
  operation: Operation,
) {
  await Effect.runPromise(
    context.session.command({
      delivery: "followUp",

      type: "prompt",
      requestId: "between",
      text: "between",
    }),
  );
  expect(context.events).toContain("prompt-admission-write");
  for (
    let steps = 0;
    fiber.pollUnsafe() === undefined && steps < 100;
    steps += 1
  ) {
    const next = scheduled.shift();
    if (next === undefined) throw new Error("Mutation did not resume");
    next();
  }
  const result = await Effect.runPromise(Fiber.join(fiber));
  const lineageWrite = context.events.findIndex((event) =>
    event.startsWith("lineage-"),
  );
  expect(lineageWrite).toBeGreaterThan(context.events.indexOf("queue-read"));
  expect(lineageWrite).toBeLessThan(
    context.events.indexOf("prompt-admission-write"),
  );
  if (operation === "fork") {
    // SAFETY: this fork-only branch uses the public fork overload; branch returns undefined.
    await assertForkResult(context.agent, result as ForkedSession, complete);
  }
}

async function assertForkResult(
  agent: ShiitakeEffectAgent,
  result: ForkedSession,
  complete: SessionSnapshot,
) {
  expect(result.sessionId).toEqual(expect.any(String));
  const forked = await Effect.runPromise(
    agent.session(result.sessionId).read(),
  );
  expect(forked.messages.map(({ value }) => value)).toEqual(
    complete.messages.slice(0, 2).map(({ value }) => value),
  );
}

async function runSchedulerRace(operation: Operation) {
  const context = await openRace();
  try {
    const { complete, entryId } = await completeFirstTurn(context);
    context.events.length = 0;
    const { fiber, scheduled } = startMutation(context, operation, entryId);
    await driveToQueueRead(context, fiber, scheduled);
    assertLineageBeforePrompt(context, operation);
    await finishMutation(context, fiber, scheduled, complete, operation);
  } finally {
    await Effect.runPromise(context.agent.close);
    context.fixture.close();
  }
}

it("keeps branch-first work serial around idle inspection and mutation", async () => {
  await runSchedulerRace("branch");
});

it("keeps fork-first work serial around idle inspection and mutation", async () => {
  await runSchedulerRace("fork");
});
