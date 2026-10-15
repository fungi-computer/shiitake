import { fauxModel } from "@fungi.computer/mule/testing";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import type { BirdDogEffect } from "@fungi.computer/bird-dog/effect";
import * as Effect from "effect/Effect";

import type { ShiitakeEffectOptions } from "../../src/effect.js";
import type { ExtensionRosterV0 } from "../../src/extensions.js";
import type { SqliteOwner } from "../../src/index.js";
import type { RuntimePendingWork } from "../../src/internal/contracts.js";
import { openRuntimeCore } from "../../src/internal/runtime-core.js";

/** Manual Effect runtime host exposing reconcile and pending-work directly. */
export function manualExtensionRuntime() {
  let bound:
    | Readonly<{
        reconcile: Effect.Effect<void, Error>;
        pendingWork: Effect.Effect<RuntimePendingWork, Error>;
      }>
    | undefined;
  let wakes = 0;
  const runtime: ShiitakeEffectOptions["runtime"] = {
    bind: (input) =>
      Effect.sync(() => {
        bound = input;
        return {
          commit: (mutation) => mutation,
          requestWake: Effect.sync(() => {
            wakes += 1;
          }),
          close: Effect.void,
        };
      }),
  };
  const require = () => {
    if (bound === undefined) throw new Error("Runtime is not bound");
    return bound;
  };
  return {
    runtime,
    wakes: () => wakes,
    reconcile: () => Effect.runPromise(require().reconcile),
    reconcileExit: () => Effect.runPromiseExit(require().reconcile),
    pendingWork: () => Effect.runPromise(require().pendingWork),
    /** Reconcile until nothing is pending, bounded. */
    drain: async (limit = 1_000) => {
      for (let pass = 0; pass < limit; pass += 1) {
        if (!(await Effect.runPromise(require().pendingWork)).pending)
          return pass;
        await Effect.runPromise(require().reconcile);
      }
      throw new Error("Runtime did not drain");
    },
  };
}

/** Deterministic Effect run dependencies: every run completes with one reply. */
export function completingRunOptions(
  store: SqliteOwner,
  runtime: ShiitakeEffectOptions["runtime"],
  extensions: ExtensionRosterV0,
): ShiitakeEffectOptions {
  return {
    store,
    runtime,
    extensions,
    instructions: {
      load: () =>
        Effect.succeed({ revision: "extension-host-v1", text: "Reply once." }),
    },
    model: {
      acquire: () =>
        Effect.sync(() => {
          const faux = fauxModel([]);
          faux.setResponses([fauxAssistantMessage("done")]);
          return {
            model: faux.model,
            stream: faux.stream,
            release: Effect.void,
          };
        }),
    },
  };
}

/** Open Runtime Core over one store with the given roster. */
export function openCore(
  store: SqliteOwner,
  extensions: ExtensionRosterV0,
  mailRoom?: BirdDogEffect,
) {
  const host = manualExtensionRuntime();
  return Effect.runPromise(
    openRuntimeCore(
      completingRunOptions(store, host.runtime, extensions),
      mailRoom,
    ),
  ).then((core) => ({ core, host }));
}
