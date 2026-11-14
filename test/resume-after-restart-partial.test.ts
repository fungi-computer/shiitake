import { readFile } from "node:fs/promises";
import { DatabaseSync } from "node:sqlite";
import { expect, it } from "vitest";
import * as Schema from "effect/Schema";
import { SessionSnapshotSchema } from "../src/client-wire.js";
import {
  killed,
  finishedValue,
  withDatabase,
} from "./fixtures/restart.fixture.js";

const outcome = Schema.decodeUnknownSync(
  Schema.Struct({
    snapshot: SessionSnapshotSchema,
    seenAtRestart: Schema.NullOr(Schema.String),
  }),
);

it(
  "a SIGKILL mid-stream keeps the flushed reply text and the restarted turn completes",
  () =>
    withDatabase(async (path) => {
      await killed(path, "crash-partial", "partial-flushed", "restart-partial");
      const stored = new DatabaseSync(path);
      const row = stored
        .prepare(
          "SELECT data_json FROM session_entries WHERE state = 'streaming'",
        )
        .get();
      stored.close();
      const flushed = JSON.stringify(row?.data_json);
      expect(flushed).toContain("streams the first");

      const result = outcome(
        await finishedValue(path, "finish-partial", "restart-partial"),
      );
      // The restarted turn saw the lost reply's text as its unfinished reply.
      expect(result.seenAtRestart).toContain("streams the first");
      expect(result.snapshot.terminals.map(({ outcome }) => outcome)).toContain(
        "completed",
      );
      const answers = result.snapshot.messages.flatMap(({ value }) =>
        value.role === "assistant"
          ? value.content.flatMap((part) =>
              part.type === "text" ? [part.text] : [],
            )
          : [],
      );
      expect(answers).toEqual(["the restarted answer"]);
      expect(await readFile(path + ".requests", "utf8")).toBe("request\n");
    }),
  60_000,
);
