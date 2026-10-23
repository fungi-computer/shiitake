import type { WatchdogSettledJob } from "@fungi.computer/watchdog";
import type { WatchdogRuntimeEffect } from "@fungi.computer/watchdog/effect";
import * as Effect from "effect/Effect";
import { expect, it } from "vitest";

import type { SqliteCursor, SqliteOwner } from "../src/internal/contracts.js";
import type { ExtensionRuntime } from "../src/internal/extension-runtime.js";
import { openObservationKernel } from "../src/internal/observation-kernel.js";
import { reconcileRuntime } from "../src/internal/runtime-reconciliation.js";

// Work whose Session repair cannot run: the payload no longer decodes, so
// every reconcile fails before Session controls are touched.
const unrepairable: WatchdogSettledJob = {
  jobId: "work-1",
  queue: "session-1",
  lane: "default",
  priority: 0,
  payload: { broken: true },
  state: "settled",
  outcome: "failed",
};

const unused = () => {
  throw new Error("No extension is rostered in this fixture");
};

// A host with no rostered extension: nothing consumes settled runs.
const noExtensions: ExtensionRuntime = {
  stage: unused,
  runParticipants: () => [],
  incompatibleMigrations: () => [],
  modulesForRun: () => Effect.succeed([]),
  filesForRun: () => Effect.succeed([]),
  promptSections: () => Effect.succeed([]),
  compactionHandoff: () => undefined,
  settledConsumers: () => [],
  deliverSettled: () => Effect.void,
  fireDueTimers: () => Effect.succeed(false),
  timerWake: () => ({ due: false, nextWakeAt: null }),
  project: () => ({}),
  call: unused,
  list: () => [],
  resume: unused,
  close: Effect.void,
};

function reconcileWith(
  store: SqliteOwner,
  watchdog: Pick<WatchdogRuntimeEffect, "tick" | "readSettled">,
) {
  return reconcileRuntime({
    // SAFETY: reconcileRuntime reads only tick and readSettled from Watchdog.
    watchdog: watchdog as WatchdogRuntimeEffect,
    store,
    mailRoom: undefined,
    extensions: noExtensions,
    requestWake: Effect.void,
  });
}

// An owner with no settled cursor yet: every cursor read finds no row.
function emptyCursorOwner(): SqliteOwner {
  return {
    sql: {
      exec: (): SqliteCursor => ({
        toArray: () => [],
        one: () => {
          throw new Error("No row in this fixture");
        },
      }),
    },
    transactionSync: (closure) => closure(),
  };
}

async function publishedFor(
  run: (store: SqliteOwner) => Effect.Effect<void, unknown>,
) {
  const store = emptyCursorOwner();
  const kernel = openObservationKernel(store);
  const watcher = await Effect.runPromise(kernel.register("session-1"));
  const outcome = await Effect.runPromise(Effect.result(run(store)));
  const signal = await Effect.runPromise(watcher.pollSignal);
  await Effect.runPromise(kernel.close);
  return { failed: outcome._tag === "Failure", signal };
}

it("announces a fresh Watchdog settle even when its Session repair fails", async () => {
  const result = await publishedFor((store) =>
    reconcileWith(store, {
      tick: () => Effect.succeed({ status: "settled", job: unrepairable }),
      // Core settles the fresh record through Watchdog's settled page.
      readSettled: () => Effect.succeed([{ sequence: 1, job: unrepairable }]),
    }),
  );
  expect(result).toEqual({ failed: true, signal: "durable" });
});

it("recovery announces a settle whose earlier reconcile never published", async () => {
  // The settle was committed by an earlier tick that stopped before its
  // publish; this reconcile only sees it through Watchdog's settled records.
  const result = await publishedFor((store) =>
    reconcileWith(store, {
      tick: () => Effect.succeed({ status: "no_ready" }),
      readSettled: () => Effect.succeed([{ sequence: 1, job: unrepairable }]),
    }),
  );
  expect(result).toEqual({ failed: true, signal: "durable" });
});
