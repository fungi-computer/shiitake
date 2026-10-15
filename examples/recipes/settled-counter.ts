import {
  defineCollection,
  defineExtension,
} from "@fungi.computer/shiitake/extensions";
import { memoryHost } from "@fungi.computer/shiitake/testing";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

/** Count successful settlements; core owns the durable delivery cursor. */
export async function settledCounterRoundTrip() {
  const counter = defineExtension("settled-counter")
    .state({
      counts: defineCollection(
        Schema.Struct({ sessionId: Schema.String, count: Schema.Int }),
        { key: (row) => row.sessionId },
      ),
    })
    .define({
      hooks: {
        onSettled: (ctx, page) =>
          Effect.gen(function* () {
            for (const run of page) {
              if (run.outcome !== "completed") continue;
              const previous = yield* ctx.state.counts.get(run.sessionId);
              yield* ctx.state.counts.put({
                sessionId: run.sessionId,
                count: Option.isSome(previous) ? previous.value.count + 1 : 1,
              });
              yield* ctx.observe(run.sessionId);
            }
          }),
      },
      projection: {
        schema: Schema.Int,
        read: (ctx, sessionId) =>
          Option.match(ctx.state.counts.peek(sessionId), {
            onNone: () => 0,
            onSome: (row) => row.count,
          }),
      },
    });
  const host = await memoryHost({ extensions: [counter] });
  try {
    const session = host.agent.session("counter-example");
    await session.send("First.", { requestId: "counter-first" });
    const first = await host.settle("counter-example");
    await session.send("Second.", { requestId: "counter-second" });
    const second = await host.settle("counter-example");
    await host.reconcile();
    const repeated = await session.read();
    return { first, second, repeated };
  } finally {
    await host.close();
  }
}
