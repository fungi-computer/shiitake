import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import { expect, it } from "vitest";

import * as Shiitake from "../src/effect.js";
import {
  defineExtension,
  ExtensionActivationError,
} from "../src/extensions.js";
import {
  completingRunOptions,
  manualExtensionRuntime,
} from "./fixtures/extension-host.fixture.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

it("awaits reverse-order, once-only activation teardown on Agent close", async () => {
  const releasing = await Effect.runPromise(Deferred.make<undefined>());
  const allowRelease = await Effect.runPromise(Deferred.make<undefined>());
  const acquisitions: string[] = [];
  const releases: string[] = [];
  let closed = false;
  const extension = defineExtension("activation-lifetime").define({
    hooks: {
      start: () =>
        Effect.gen(function* () {
          yield* Effect.acquireRelease(
            Effect.sync(() => {
              acquisitions.push("first");
            }),
            () =>
              Effect.sync(() => {
                releases.push("first");
              }),
          );
          yield* Effect.acquireRelease(
            Effect.sync(() => {
              acquisitions.push("second");
            }),
            () =>
              Effect.gen(function* () {
                yield* Deferred.succeed(releasing, undefined);
                yield* Deferred.await(allowRelease);
                releases.push("second");
              }),
          );
        }),
    },
  });
  const fixture = createNativeSqliteOwnerFixture();
  const host = manualExtensionRuntime();
  const opening = Effect.runPromise(
    Shiitake.make(
      completingRunOptions(fixture.owner, host.runtime, {
        roster: [extension],
        grants: [],
      }),
    ),
  );
  let closing: Promise<void> | undefined;
  try {
    const agent = await opening;
    expect(acquisitions).toEqual(["first", "second"]);
    expect(releases).toEqual([]);

    closing = Effect.runPromise(agent.close).then(() => {
      closed = true;
    });
    await Effect.runPromise(Deferred.await(releasing));
    // Let the Effect scheduler advance while the finalizer remains blocked.
    await Effect.runPromise(Effect.yieldNow);
    expect(closed).toBe(false);
    expect(releases).toEqual([]);
    await Effect.runPromise(Deferred.succeed(allowRelease, undefined));
    await closing;
    expect(closed).toBe(true);
    expect(releases).toEqual(["second", "first"]);
    await Effect.runPromise(agent.close);
    expect(releases).toEqual(["second", "first"]);
  } finally {
    await Effect.runPromise(Deferred.succeed(allowRelease, undefined));
    try {
      await (closing ??
        opening.then((agent) => Effect.runPromise(agent.close)));
    } finally {
      fixture.close();
    }
  }
});

it("awaits failed activation cleanup before opening and retains the unrelated activation until close", async () => {
  const releasing = await Effect.runPromise(Deferred.make<undefined>());
  const allowRelease = await Effect.runPromise(Deferred.make<undefined>());
  const acquisitions: string[] = [];
  const releases: string[] = [];
  let opened = false;
  const healthy = defineExtension("alpha-kept").define({
    hooks: {
      start: () =>
        Effect.acquireRelease(
          Effect.sync(() => {
            acquisitions.push("healthy");
          }),
          () =>
            Effect.sync(() => {
              releases.push("healthy");
            }),
        ),
    },
  });
  const failed = defineExtension("bravo-failed").define({
    hooks: {
      start: () =>
        Effect.acquireRelease(
          Effect.sync(() => {
            acquisitions.push("failed");
          }),
          () =>
            Effect.gen(function* () {
              yield* Deferred.succeed(releasing, undefined);
              yield* Deferred.await(allowRelease);
              releases.push("failed");
            }),
        ).pipe(
          Effect.andThen(
            Effect.fail(
              new ExtensionActivationError({ message: "activation refused" }),
            ),
          ),
        ),
    },
  });
  const fixture = createNativeSqliteOwnerFixture();
  const host = manualExtensionRuntime();
  const opening = Effect.runPromise(
    Shiitake.make(
      completingRunOptions(fixture.owner, host.runtime, {
        roster: [healthy, failed],
        grants: [],
      }),
    ),
  ).then((agent) => {
    opened = true;
    return agent;
  });
  try {
    await Effect.runPromise(Deferred.await(releasing));
    await Effect.runPromise(Effect.yieldNow);
    expect(opened).toBe(false);
    expect(acquisitions).toEqual(["healthy", "failed"]);
    expect(releases).toEqual([]);
    await Effect.runPromise(Deferred.succeed(allowRelease, undefined));
    const agent = await opening;
    expect(releases).toEqual(["failed"]);
    const statuses = await Effect.runPromise(agent.extensions.list);
    expect(statuses).toMatchObject([
      { id: "alpha-kept", status: "running" },
      {
        id: "bravo-failed",
        status: "stopped",
      },
    ]);
    expect(statuses[1]?.reason).toMatch(/\S/u);
    await Effect.runPromise(agent.close);
    expect(releases).toEqual(["failed", "healthy"]);
    await Effect.runPromise(agent.close);
    expect(releases).toEqual(["failed", "healthy"]);
  } finally {
    await Effect.runPromise(Deferred.succeed(allowRelease, undefined));
    try {
      await Effect.runPromise((await opening).close);
    } finally {
      fixture.close();
    }
  }
});
