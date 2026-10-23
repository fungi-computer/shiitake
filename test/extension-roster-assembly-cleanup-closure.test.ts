import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import { expect, it } from "vitest";

import {
  defineExtension,
  ExtensionActivationError,
} from "../src/extensions.js";
import { Shiitake } from "../src/index.js";
import { make as makeEffectOwner } from "../src/effect.js";
import { grantAll } from "./fixtures/fixture-extensions.js";
import {
  completingRunOptions,
  manualExtensionRuntime,
} from "./fixtures/extension-host.fixture.js";
import {
  compatibilityRunDependencies,
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

it.each(["failed assembly", "explicit discard"] as const)(
  "awaits sequential reverse-order cleanup on %s without closing unchanged members",
  async (path) => {
    const cleaning = await Effect.runPromise(Deferred.make<undefined>());
    const allowCleanup = await Effect.runPromise(Deferred.make<undefined>());
    const started: string[] = [];
    const closing: string[] = [];
    const released: string[] = [];
    let unchangedLive = false;
    let cleanupSettled = false;
    const unchanged = defineExtension("unchanged").define({
      hooks: {
        start: () =>
          Effect.acquireRelease(
            Effect.sync(() => {
              started.push("unchanged");
              unchangedLive = true;
            }),
            () =>
              Effect.sync(() => {
                unchangedLive = false;
                released.push("unchanged");
              }),
          ),
      },
      commands: {
        specs: {
          ping: {
            scope: "agent",
            input: Schema.Struct({}),
            output: Schema.Boolean,
          },
        },
        handlers: () => ({
          ping: () => Effect.sync(() => unchangedLive),
        }),
      },
    });
    const addition = (
      id: string,
      cleanup: Effect.Effect<void> = Effect.void,
      fail = false,
    ) =>
      defineExtension(id).define({
        hooks: {
          start: () =>
            Effect.gen(function* () {
              yield* Effect.acquireRelease(
                Effect.sync(() => {
                  started.push(id);
                }),
                () =>
                  Effect.sync(() => {
                    closing.push(id);
                  }).pipe(
                    Effect.andThen(cleanup),
                    Effect.andThen(
                      Effect.sync(() => {
                        released.push(id);
                      }),
                    ),
                  ),
              );
              if (fail)
                yield* Effect.fail(
                  new ExtensionActivationError({
                    message: "Staged activation refused",
                  }),
                );
            }),
        },
      });
    const first = addition("a-staged");
    const second = addition(
      "b-staged",
      Effect.gen(function* () {
        yield* Deferred.succeed(cleaning, undefined);
        yield* Deferred.await(allowCleanup);
      }),
    );
    const failed = addition("z-failed", Effect.void, true);
    const fixture = createNativeSqliteOwnerFixture();
    const runtime = createCompatibilityRuntimeHost();
    const agent = await Shiitake.make({
      ...compatibilityRunDependencies(),
      store: fixture.owner,
      runtime: runtime.runtime,
      extensions: grantAll([unchanged]),
    });
    let preparing: Promise<unknown> | undefined;
    try {
      expect(await agent.extensions.call("unchanged", "ping", {})).toBe(true);
      const assemble = async () => {
        const prepared = await agent.prepareConfiguration({
          extensions: grantAll(
            path === "failed assembly"
              ? [unchanged, first, second, failed]
              : [unchanged, first, second],
          ),
        });
        if (path === "failed assembly") return { status: "prepared" as const };
        await prepared.discard();
        await prepared.discard();
        return { status: "discarded" as const };
      };
      const pending = assemble().then(
        (result) => {
          cleanupSettled = true;
          return result;
        },
        (error: unknown) => {
          cleanupSettled = true;
          return { status: "rejected" as const, error };
        },
      );
      preparing = pending;
      await Effect.runPromise(Deferred.await(cleaning));
      expect(started).toEqual(
        path === "failed assembly"
          ? ["unchanged", "a-staged", "b-staged", "z-failed"]
          : ["unchanged", "a-staged", "b-staged"],
      );
      expect(cleanupSettled).toBe(false);
      expect(await agent.extensions.call("unchanged", "ping", {})).toBe(true);
      expect([...closing]).toEqual(
        path === "failed assembly" ? ["z-failed", "b-staged"] : ["b-staged"],
      );
      expect([...released]).toEqual(
        path === "failed assembly" ? ["z-failed"] : [],
      );

      await Effect.runPromise(Deferred.succeed(allowCleanup, undefined));
      expect(await pending).toMatchObject(
        path === "failed assembly"
          ? {
              status: "rejected",
              error: { boundary: "extensions", cause: "rejected" },
            }
          : { status: "discarded" },
      );
      expect(await agent.extensions.call("unchanged", "ping", {})).toBe(true);
      expect((await agent.extensions.list()).map(({ id }) => id)).toEqual([
        "unchanged",
      ]);
      expect([...released]).toEqual(
        path === "failed assembly"
          ? ["z-failed", "b-staged", "a-staged"]
          : ["b-staged", "a-staged"],
      );
      await agent.close();
      expect(unchangedLive).toBe(false);
      expect(released).toHaveLength(path === "failed assembly" ? 4 : 3);
      expect(released.at(-1)).toBe("unchanged");
    } finally {
      await Effect.runPromise(Deferred.succeed(allowCleanup, undefined));
      try {
        await preparing;
        await agent.close();
      } finally {
        fixture.close();
      }
    }
  },
);

it.each([
  ["explicit discard", "throws"],
  ["explicit discard", "dies"],
  ["failed assembly", "throws"],
  ["failed assembly", "dies"],
] as const)(
  "closes every staged addition on %s when the newer finalizer %s and reports all failures",
  async (path, failureMode) => {
    const released: string[] = [];
    const newerFailure = new Error("Newer staged resource cleanup failed");
    const olderFailure = new Error("Older staged resource cleanup failed");
    const resource = (id: string, release: () => Effect.Effect<void>) =>
      defineExtension(id).define({
        hooks: {
          start: () => Effect.acquireRelease(Effect.void, release),
        },
      });
    const unchanged = resource("unchanged", () =>
      Effect.sync(() => {
        released.push("unchanged");
      }),
    );
    const older = resource("a-staged", () => {
      released.push("a-staged");
      return Effect.die(olderFailure);
    });
    const newer = resource("b-staged", () => {
      released.push("b-staged");
      if (failureMode === "throws") throw newerFailure;
      return Effect.die(newerFailure);
    });
    const failed = defineExtension("z-failed").define({
      hooks: {
        start: () =>
          Effect.fail(
            new ExtensionActivationError({ message: "Assembly refused" }),
          ),
      },
    });
    const fixture = createNativeSqliteOwnerFixture();
    const runtime = manualExtensionRuntime();
    const owner = await Effect.runPromise(
      makeEffectOwner(
        completingRunOptions(
          fixture.owner,
          runtime.runtime,
          grantAll([unchanged]),
        ),
      ),
    );
    try {
      const exit = await Effect.runPromiseExit(
        owner
          .prepareConfiguration({
            extensions: grantAll(
              path === "failed assembly"
                ? [unchanged, older, newer, failed]
                : [unchanged, older, newer],
            ),
          })
          .pipe(Effect.flatMap((prepared) => prepared.discard)),
      );
      // Observe before owner shutdown: it must not be needed to close A.
      expect([...released]).toEqual(["b-staged", "a-staged"]);
      expect(Exit.isFailure(exit)).toBe(true);
      if (Exit.isFailure(exit)) {
        const defects = exit.cause.reasons
          .filter(Cause.isDieReason)
          .map(({ defect }) => defect);
        expect(defects).toHaveLength(2);
        expect(defects[0]).toBe(newerFailure);
        expect(defects[1]).toBe(olderFailure);
        const failures = exit.cause.reasons
          .filter(Cause.isFailReason)
          .map(({ error }) => error);
        if (path === "failed assembly")
          expect(failures).toMatchObject([
            { boundary: "extensions", cause: "rejected" },
          ]);
        else expect(failures).toEqual([]);
      }
      expect(
        (await Effect.runPromise(owner.extensions.list)).map(({ id }) => id),
      ).toEqual(["unchanged"]);
      await Effect.runPromise(owner.close);
      expect(released).toEqual(["b-staged", "a-staged", "unchanged"]);
    } finally {
      try {
        await Effect.runPromiseExit(owner.close);
      } finally {
        fixture.close();
      }
    }
  },
);
