// Runtime: Node 24, Effect's own scheduler. The owner's one compaction slot.
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Scheduler from "effect/Scheduler";
import { expect, it } from "vitest";

import {
  claimSlot,
  slotTaken,
} from "../src/internal/compaction/compaction-slot.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

it("an interrupt at any point of a claim leaves no slot, and a later claim succeeds", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    // Yield every three operations (fewer livelocks Effect's scheduler) and
    // interrupt the claim after each number of yields in turn, so interrupts
    // land between its operations. The old fork-then-register order leaked
    // the entry at the first boundary.
    for (let boundary = 0; boundary < 24; boundary += 1) {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const scope = yield* Effect.scope;
            const claiming = yield* Effect.forkChild(
              claimSlot(fixture.owner, scope, () => Effect.never),
            );
            for (let step = 0; step < boundary; step += 1)
              yield* Effect.yieldNow;
            yield* Fiber.interrupt(claiming);
          }),
        ).pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, 3)),
      );
      expect(slotTaken(fixture.owner), `after ${String(boundary)} yields`).toBe(
        false,
      );
    }
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const scope = yield* Effect.scope;
          yield* claimSlot(fixture.owner, scope, () => Effect.never);
          expect(slotTaken(fixture.owner)).toBe(true);
        }),
      ),
    );
    expect(slotTaken(fixture.owner)).toBe(false);
  } finally {
    fixture.close();
  }
});
