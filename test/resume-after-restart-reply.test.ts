import { expect, it } from "vitest";
import * as Schema from "effect/Schema";
import {
  finishedValue,
  killed,
  withDatabase,
} from "./fixtures/restart.fixture.js";

const outcome = Schema.decodeUnknownSync(
  Schema.Struct({
    rows: Schema.Array(
      Schema.Struct({
        key: Schema.String,
        to_session: Schema.String,
        content: Schema.String,
      }),
    ),
    requests: Schema.Number,
    settled: Schema.Array(Schema.Struct({ state: Schema.String })),
  }),
);

it(
  "a SIGKILL after the turn settled and before its reply was staged replays one reply and no second model call",
  () =>
    withDatabase(async (path) => {
      await killed(path, "crash-reply", "reply-staging", "restart-reply");
      const result = outcome(
        await finishedValue(path, "finish-reply", "restart-reply"),
      );
      expect(result.rows).toEqual([
        {
          key: "mail-1:reply:0",
          to_session: "parent-session",
          content: "The request is fixed",
        },
      ]);
      // The first process asked the model once; the restart must not ask again.
      expect(result.requests).toBe(1);
      expect(result.settled.map(({ state }) => state)).toEqual(["settled"]);
    }),
  30_000,
);
