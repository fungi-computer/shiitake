import { expect, it } from "vitest";
import {
  killed,
  finished,
  withDatabase,
  expectOutcome,
  expectVerifiedAnswer,
} from "./fixtures/restart.fixture.js";
import { readFile } from "node:fs/promises";
import * as Schema from "effect/Schema";
import * as woodstock from "../src/internal/woodstock/session-store.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

const decodeSessionRow = Schema.decodeUnknownSync(
  Schema.Struct({ id: Schema.String }),
);

it(
  "passes pre-restart assistant and recovered tool result into real compaction and rebuilds context from its stored model message",
  () =>
    withDatabase(async (path) => {
      await killed(path, "crash-tool", "tool-started");
      expectOutcome(await finished(path, "compact-recovered"), "completed");
      expect(await readFile(path + ".summary", "utf8")).toBe("paired\n");
      const fixture = createNativeSqliteOwnerFixture(path);
      try {
        const { id } = decodeSessionRow(
          fixture.owner.sql.exec("SELECT id FROM sessions").one(),
        );
        const rows = woodstock.readSelectedPath(fixture.owner, {
          sessionId: id,
        });
        const compaction = rows.find(
          ({ entry }) => entry.kind === "compaction",
        );
        if (compaction?.entry.kind !== "compaction")
          throw new Error("expected a compaction");
        expect(JSON.stringify(compaction.entry.model[0])).toContain(
          "<summary>",
        );
        expect(woodstock.buildSessionContext(rows)[0]).toBe(
          compaction.entry.model[0],
        );
      } finally {
        fixture.close();
      }
    }),
  30_000,
);

for (const boundary of ["abnormal", "retry-abnormal"]) {
  it(
    `settles a known ${boundary} failure without reopening inference`,
    () =>
      withDatabase(async (path) => {
        await killed(path, `crash-${boundary}`, "terminal-saved");
        const snapshot = await finished(path);
        expectOutcome(snapshot, "failed");
        expect(snapshot.messages.map(({ value }) => value.role)).toEqual(
          boundary === "abnormal" ? ["user"] : ["user", "assistant"],
        );
      }),
    30_000,
  );
}

it(
  "records exhaustion after an earlier compaction attempt terminal",
  () =>
    withDatabase(async (path) => {
      await killed(path, "crash-summary", "model-started");
      for (let recovery = 0; recovery < 2; recovery += 1)
        await killed(path, "crash-summary-resume", "model-started");
      const snapshot = await finished(path);
      expect(snapshot.terminals.at(-1)).toMatchObject({
        outcome: "interrupted",
        diagnostic: "Restart recovery budget exhausted (2 recoveries)",
      });
    }),
  60_000,
);

it(
  "places a summary pinned before a crash without paying for a second one",
  () =>
    withDatabase(async (path) => {
      await killed(path, "crash-pinned", "summary-pinned");
      const snapshot = await finished(path, "finish-pinned");
      expectVerifiedAnswer(snapshot);
      // One request after the restart: the answer. A second summary call
      // would have been a second request.
      expect(await readFile(path + ".requests", "utf8")).toBe("request\n");
      const fixture = createNativeSqliteOwnerFixture(path);
      try {
        const { id } = decodeSessionRow(
          fixture.owner.sql.exec("SELECT id FROM sessions").one(),
        );
        const kinds = woodstock
          .readSelectedPath(fixture.owner, { sessionId: id })
          .map(({ entry }) => entry.kind);
        expect(
          kinds.filter((kind) => kind === "compaction_checkpoint"),
        ).toHaveLength(1);
        expect(kinds.filter((kind) => kind === "compaction")).toHaveLength(1);
        expect(kinds.indexOf("compaction")).toBeGreaterThan(
          kinds.indexOf("compaction_checkpoint"),
        );
      } finally {
        fixture.close();
      }
    }),
  60_000,
);
