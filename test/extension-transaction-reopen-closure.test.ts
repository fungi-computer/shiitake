import {
  makeRuntime as makeWatchdog,
  type WatchdogRuntimeEffect,
} from "@fungi.computer/watchdog/effect";
import { openExtensionRuntime } from "../src/internal/extension-runtime.js";
import { admitExtensionRosterV0 } from "../src/internal/extension-roster.js";
import { prepareWoodstockSchema } from "../src/internal/woodstock/index.js";
import { readTimerJob } from "../src/internal/woodstock/extension-store.js";
import { openTestAdmissionDoor } from "./fixtures/admission-door.fixture.js";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath, URL } from "node:url";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { expect, it } from "vitest";

import * as Shiitake from "../src/effect.js";
import {
  defineCollection,
  defineExtension,
  ExtensionStop,
  type RosteredExtensionV0,
} from "../src/extensions.js";
import {
  completingRunOptions,
  manualExtensionRuntime,
} from "./fixtures/extension-host.fixture.js";
import { grantAll } from "./fixtures/fixture-extensions.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

/** Each phase closes both the Agent and its borrowed SQLite connection. */
async function withAgent(
  databasePath: string,
  extension: RosteredExtensionV0,
  check: (
    agent: Shiitake.ShiitakeEffectOwner,
    host: ReturnType<typeof manualExtensionRuntime>,
  ) => Promise<void>,
) {
  const fixture = createNativeSqliteOwnerFixture(databasePath);
  const host = manualExtensionRuntime();
  let agent: Shiitake.ShiitakeEffectOwner | undefined;
  try {
    agent = await Effect.runPromise(
      Shiitake.make(
        completingRunOptions(
          fixture.owner,
          host.runtime,
          grantAll([extension]),
        ),
      ),
    );
    await check(agent, host);
  } finally {
    try {
      if (agent !== undefined) await Effect.runPromise(agent.close);
    } finally {
      fixture.close();
    }
  }
}

it.each(["explicit resume", "new revision"])(
  "rolls back timer consumption and fires once after physical reopen through %s",
  async (restart) => {
    let refusing = true;
    const makeExtension = (version: string) =>
      defineExtension({ id: "timer-transaction-reopen", version })
        .state({
          counts: defineCollection(
            Schema.Struct({ id: Schema.String, count: Schema.Number }),
            { key: (row) => row.id },
          ),
        })
        .define({
          commands: {
            specs: {
              arm: {
                scope: "agent",
                input: Schema.Struct({}),
                output: Schema.Struct({}),
              },
              inspect: {
                scope: "agent",
                input: Schema.Struct({}),
                output: Schema.Struct({
                  count: Schema.Number,
                  dueAt: Schema.NullOr(Schema.Number),
                }),
              },
            },
            handlers: (ctx) => ({
              arm: () =>
                Effect.gen(function* () {
                  yield* ctx.timers.set("once", 0);
                  return {};
                }),
              inspect: () =>
                Effect.gen(function* () {
                  const row = yield* ctx.state.counts.get("once");
                  const timer = yield* ctx.timers.read("once");
                  return {
                    count: Option.getOrUndefined(row)?.count ?? 0,
                    dueAt: Option.getOrUndefined(timer) ?? null,
                  };
                }),
            }),
          },
          hooks: {
            onTimer: (ctx, timer) =>
              Effect.gen(function* () {
                const prior = yield* ctx.state.counts.get(timer.key);
                yield* ctx.state.counts.put({
                  id: timer.key,
                  count: (Option.getOrUndefined(prior)?.count ?? 0) + 1,
                });
                if (refusing)
                  yield* Effect.fail(
                    new ExtensionStop({
                      reason: "timer rejected after state write",
                    }),
                  );
              }),
          },
        });
    const extension = makeExtension("1.0.0");
    const root = fileURLToPath(
      new URL("../../../.botanical/", import.meta.url),
    );
    mkdirSync(root, { recursive: true });
    const directory = mkdtempSync(join(root, "extension-timer-"));
    const databasePath = join(directory, "store.sqlite");
    const id = extension.manifest.id;
    try {
      await withAgent(databasePath, extension, async (agent, host) => {
        await Effect.runPromise(agent.extensions.call(id, "arm", {}));
        expect(
          await Effect.runPromise(agent.extensions.call(id, "inspect", {})),
        ).toEqual({ count: 0, dueAt: 0 });
        await host.reconcile();
        expect(await Effect.runPromise(agent.extensions.list)).toMatchObject([
          {
            id: "timer-transaction-reopen",
            status: "stopped",
            reason: "timer rejected after state write",
          },
        ]);
        expect(await host.pendingWork()).toEqual({
          pending: false,
          nextWakeAt: null,
        });
      });

      refusing = false;
      const reopened =
        restart === "new revision" ? makeExtension("1.0.1") : extension;
      await withAgent(databasePath, reopened, async (agent, host) => {
        expect(await Effect.runPromise(agent.extensions.list)).toMatchObject([
          {
            id: "timer-transaction-reopen",
            status: restart === "new revision" ? "running" : "stopped",
          },
        ]);
        if (restart === "explicit resume")
          await Effect.runPromise(agent.extensions.resume(id));
        expect(
          await Effect.runPromise(agent.extensions.call(id, "inspect", {})),
        ).toEqual({ count: 0, dueAt: 0 });
        expect((await host.pendingWork()).pending).toBe(true);
        await host.drain();
        expect(
          await Effect.runPromise(agent.extensions.call(id, "inspect", {})),
        ).toEqual({ count: 1, dueAt: null });
      });

      await withAgent(databasePath, reopened, async (agent, host) => {
        expect(await host.pendingWork()).toEqual({
          pending: false,
          nextWakeAt: null,
        });
        await host.reconcile();
        expect(
          await Effect.runPromise(agent.extensions.call(id, "inspect", {})),
        ).toEqual({ count: 1, dueAt: null });
      });
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  },
);

it("fences a replaced timer by its actual job pointer and removes its wake with the extension", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const fired: number[] = [];
  const extension = defineExtension("timer-pointer-owner").define({
    commands: {
      specs: {
        arm: {
          scope: "agent",
          input: Schema.Struct({ at: Schema.Number }),
          output: Schema.Struct({}),
        },
      },
      handlers: (ctx) => ({
        arm: ({ input: { at } }: { input: { at: number } }) =>
          ctx.timers.set("same", at).pipe(Effect.as({})),
      }),
    },
    hooks: {
      onTimer: (_ctx, timer) =>
        Effect.sync(() => {
          fired.push(timer.dueAt);
        }),
    },
  });
  await Effect.runPromise(prepareWoodstockSchema(fixture.owner));
  const watchdog = Effect.runSync(
    makeWatchdog({
      owner: fixture.owner,
      isAlive: () => Effect.succeed(false),
      executor: { execute: () => Effect.succeed({ kind: "completed" }) },
      wake: { recompute: () => Effect.void },
    }),
  );
  const runtime = await Effect.runPromise(
    openExtensionRuntime({
      owner: fixture.owner,
      watchdog: watchdog.transactional,
      door: openTestAdmissionDoor(fixture.owner),
      readQueueHead: watchdog.transactional.readQueueHead,
      admitted: await Effect.runPromise(
        admitExtensionRosterV0(grantAll([extension])),
      ),
      requestWake: Effect.void,
    }),
  );
  try {
    await Effect.runPromise(
      runtime.call({ scope: "agent" }, extension.manifest.id, "arm", {
        at: 100,
      }),
    );
    const firstId = readTimerJob(fixture.owner, extension.manifest.id, "same");
    if (firstId === undefined) throw new Error("Missing first timer job");
    const first = await Effect.runPromise(watchdog.read(firstId));
    if (first.status !== "found")
      throw new Error("Missing first timer claim payload");
    await Effect.runPromise(
      runtime.call({ scope: "agent" }, extension.manifest.id, "arm", {
        at: 200,
      }),
    );
    const replacementId = readTimerJob(
      fixture.owner,
      extension.manifest.id,
      "same",
    );
    expect(replacementId).not.toBe(firstId);
    // A previously claimed callback carries its old job identity after replacement.
    await Effect.runPromise(
      runtime.executeTimer({ ...first.job, state: "running" }),
    );
    expect(fired).toEqual([]);
    expect(readTimerJob(fixture.owner, extension.manifest.id, "same")).toBe(
      replacementId,
    );
    if (replacementId === undefined)
      throw new Error("Missing replacement timer");
    const replacement = await Effect.runPromise(watchdog.read(replacementId));
    if (replacement.status !== "found")
      throw new Error("Missing replacement job");
    await Effect.runPromise(
      runtime.executeTimer({ ...replacement.job, state: "running" }),
    );
    expect(fired).toEqual([200]);
    expect(
      readTimerJob(fixture.owner, extension.manifest.id, "same"),
    ).toBeUndefined();
    await Effect.runPromise(
      runtime.call({ scope: "agent" }, extension.manifest.id, "arm", {
        at: 300,
      }),
    );
    expect(readTimerJob(fixture.owner, extension.manifest.id, "same")).not.toBe(
      replacementId,
    );
    const removal = await Effect.runPromise(runtime.stage([]));
    await Effect.runPromise(removal.apply);
    expect(
      readTimerJob(fixture.owner, extension.manifest.id, "same"),
    ).toBeUndefined();
    for (let count = 0; count < 3; count++)
      await Effect.runPromise(watchdog.tick());
    expect(await Effect.runPromise(watchdog.wakeState(0))).toEqual({
      pending: false,
      nextWakeAt: null,
    });
    expect(fired).toEqual([200]);
  } finally {
    await Effect.runPromise(runtime.close);
    fixture.close();
  }
});

it.each([true, false])(
  "keeps queued and claimed timer identity across reopen and checks its current grant: %s",
  async (allowTimers) => {
    const fixture = createNativeSqliteOwnerFixture();
    const fired: number[] = [];
    const extension = defineExtension("timer-reopen-authority").define({
      commands: {
        specs: {
          arm: {
            scope: "agent",
            input: Schema.Struct({}),
            output: Schema.Struct({}),
          },
        },
        handlers: (ctx) => ({
          arm: () => ctx.timers.set("once", 123).pipe(Effect.as({})),
        }),
      },
      hooks: {
        onTimer: (_ctx, timer) =>
          Effect.sync(() => {
            fired.push(timer.dueAt);
          }),
      },
    });
    await Effect.runPromise(prepareWoodstockSchema(fixture.owner));
    let runtime: Awaited<ReturnType<typeof open>> | undefined;
    const watchdog: WatchdogRuntimeEffect = Effect.runSync(
      makeWatchdog({
        owner: fixture.owner,
        isAlive: () => Effect.succeed(false),
        executor: {
          execute: (job) =>
            Effect.gen(function* () {
              if (runtime === undefined)
                throw new Error("Timer runtime is not open");
              const claim = fixture.owner.sql
                .exec(
                  "SELECT state,claim_token,not_before FROM watchdog_jobs WHERE job_id=?",
                  job.jobId,
                )
                .toArray();
              expect(claim).toHaveLength(1);
              expect(claim).toMatchObject([
                { state: "running", not_before: 123 },
              ]);
              expect(claim[0]?.claim_token).toBeTypeOf("string");
              yield* runtime.close;
              runtime = yield* Effect.promise(() => open(allowTimers));
              expect(
                readTimerJob(fixture.owner, extension.manifest.id, "once"),
              ).toBe(job.jobId);
              expect(
                fixture.owner.sql
                  .exec(
                    "SELECT state,claim_token,not_before FROM watchdog_jobs WHERE job_id=?",
                    job.jobId,
                  )
                  .toArray(),
              ).toEqual(claim);
              yield* runtime.executeTimer(job);
              return { kind: "completed" } as const;
            }).pipe(Effect.orDie),
        },
        wake: { recompute: () => Effect.void },
      }),
    );
    async function open(timers: boolean) {
      const roster = grantAll([extension]);
      return Effect.runPromise(
        openExtensionRuntime({
          owner: fixture.owner,
          watchdog: watchdog.transactional,
          door: openTestAdmissionDoor(fixture.owner),
          readQueueHead: watchdog.transactional.readQueueHead,
          admitted: await Effect.runPromise(
            admitExtensionRosterV0({
              ...roster,
              grants: roster.grants.map((grant) => ({
                ...grant,
                capabilities: grant.capabilities.filter(
                  (capability) => timers || capability !== "timers",
                ),
              })),
            }),
          ),
          requestWake: Effect.void,
        }),
      );
    }
    try {
      runtime = await open(true);
      await Effect.runPromise(
        runtime.call({ scope: "agent" }, extension.manifest.id, "arm", {}),
      );
      const queuedId = readTimerJob(
        fixture.owner,
        extension.manifest.id,
        "once",
      );
      await Effect.runPromise(runtime.close);
      runtime = await open(true);
      expect(readTimerJob(fixture.owner, extension.manifest.id, "once")).toBe(
        queuedId,
      );
      expect(await Effect.runPromise(watchdog.tick())).toMatchObject({
        status: "settled",
      });
      expect(fired).toEqual(allowTimers ? [123] : []);
    } finally {
      if (runtime !== undefined) await Effect.runPromise(runtime.close);
      fixture.close();
    }
  },
);
