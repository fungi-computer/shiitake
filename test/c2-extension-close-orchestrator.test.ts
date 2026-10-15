import * as Cause from "effect/Cause";
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
