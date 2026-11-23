import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import { expect, it } from "vitest";

import { ShiitakeError } from "../src/errors.js";
import { composeAgentClose } from "../src/internal/agent-close.js";

it("finalizes extensions once when coordinator close fails and does not retry close", async () => {
  const coordinatorFailure = new ShiitakeError(
    "runtime",
    "rejected",
    "Coordinator close failed",
  );
  let extensionFinalizations = 0;
  const close = composeAgentClose(
    Effect.fail(coordinatorFailure),
    Effect.sync(() => {
      extensionFinalizations += 1;
    }),
  );

  const first = await Effect.runPromiseExit(close);
  expect(Exit.isFailure(first)).toBe(true);
  if (Exit.isFailure(first)) {
    expect(Option.getOrUndefined(Cause.findErrorOption(first.cause))).toBe(
      coordinatorFailure,
    );
  }
  expect(extensionFinalizations).toBe(1);

  const second = await Effect.runPromiseExit(close);
  expect(Exit.isSuccess(second)).toBe(true);
  expect(extensionFinalizations).toBe(1);
});

it.each(["both", "extensions", "neither"] as const)(
  "joins concurrent close callers and preserves failures from %s steps",
  async (failures) => {
    const coordinatorFailure = new ShiitakeError(
      "runtime",
      "rejected",
      "Coordinator close failed",
    );
    const extensionFailure = new ShiitakeError(
      "extensions",
      "rejected",
      "Extension close failed",
    );
    const started = Deferred.makeUnsafe<undefined>();
    const release = Deferred.makeUnsafe<undefined>();
    const steps: string[] = [];
    const close = composeAgentClose(
      Effect.sync(() => {
        steps.push("coordinator");
      }).pipe(
        Effect.andThen(
          failures === "both" ? Effect.fail(coordinatorFailure) : Effect.void,
        ),
      ),
      Effect.gen(function* () {
        steps.push("extensions");
        yield* Deferred.succeed(started, undefined);
        yield* Deferred.await(release);
        if (failures !== "neither") yield* Effect.fail(extensionFailure);
      }),
    );
    const first = Effect.runPromiseExit(close);
    await Effect.runPromise(Deferred.await(started));
    let joinedSettled = false;
    const joined = Effect.runPromiseExit(close).then((exit) => {
      joinedSettled = true;
      return exit;
    });
    try {
      await Effect.runPromise(Effect.yieldNow);
      expect(joinedSettled).toBe(false);
    } finally {
      await Effect.runPromise(Deferred.succeed(release, undefined));
    }
    for (const exit of await Promise.all([first, joined])) {
      expect(Exit.isSuccess(exit)).toBe(failures === "neither");
      if (Exit.isFailure(exit)) {
        const errors = exit.cause.reasons
          .filter(Cause.isFailReason)
          .map(({ error }) => error);
        expect(errors).toHaveLength(failures === "both" ? 2 : 1);
        expect(errors.at(-1)).toBe(extensionFailure);
        if (failures === "both") expect(errors[0]).toBe(coordinatorFailure);
      }
    }
    expect(Exit.isSuccess(await Effect.runPromiseExit(close))).toBe(true);
    expect(steps).toEqual(["coordinator", "extensions"]);
  },
);
