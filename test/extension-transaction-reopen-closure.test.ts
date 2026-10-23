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

it("rolls back timer consumption with state and commits both once across SQLite close and reopen", async () => {
  let refusing = true;
  const extension = defineExtension("timer-transaction-reopen")
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
  const root = fileURLToPath(new URL("../../../.botanical/", import.meta.url));
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
    await withAgent(databasePath, extension, async (agent, host) => {
      expect(await Effect.runPromise(agent.extensions.list)).toMatchObject([
        { id: "timer-transaction-reopen", status: "stopped" },
      ]);
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

    await withAgent(databasePath, extension, async (agent, host) => {
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
});
