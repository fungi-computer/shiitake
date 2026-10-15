import { expect, it } from "vitest";
import {
  killed,
  finished,
  withDatabase,
  expectOutcome,
  toolResult,
} from "./fixtures/restart.fixture.js";
import { readFile } from "node:fs/promises";
import { readSessionEntriesForWork } from "../src/internal/woodstock/index.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

it(
  "does not repeat an unsafe tool and continues with an interrupted result",
  () =>
    withDatabase(async (path) => {
      await killed(path, "crash-tool", "tool-started");
      const snapshot = await finished(path);
      expectOutcome(snapshot, "completed");
      const result = toolResult(snapshot, "send-call");
      expect(result).toMatchObject({ role: "toolResult", isError: true });
      if (result?.role !== "toolResult")
        throw new Error("Missing recovered result");
      expect(result.content).toEqual([
        {
          type: "text",
          text: "Interrupted by a restart; it may or may not have completed; check before retrying.",
        },
      ]);
    }),
  30_000,
);

it(
  "replays a safe tool once, and a second restart reuses its committed result",
  () =>
    withDatabase(async (path) => {
      await killed(path, "crash-safe", "safe-started", "restart-mule");
      await killed(path, "crash-after-result", "result-saved", "restart-mule");
      expect(await readFile(path + ".calls", "utf8")).toBe("read\nread\n");
      const snapshot = await finished(path, "finish", "restart-mule");
      expectOutcome(snapshot, "completed");
      expect(toolResult(snapshot, "read-call")).toMatchObject({
        role: "toolResult",
        isError: false,
      });
      expect(await readFile(path + ".calls", "utf8")).toBe("read\nread\n");
    }),
  45_000,
);

it(
  "records the new revision on a safe tool rerun after process loss",
  () =>
    withDatabase(async (path) => {
      await killed(path, "crash-safe-upgrade", "safe-started", "restart-mule");
      const snapshot = await finished(path, "finish-upgrade", "restart-mule");
      expectOutcome(snapshot, "completed");
      expect(await readFile(path + ".calls", "utf8")).toBe("read\nread\n");
      const workId = snapshot.terminals[0]?.commandId;
      if (workId === undefined) throw new Error("Missing recovered work");
      const reopened = createNativeSqliteOwnerFixture(path);
      try {
        const facts = readSessionEntriesForWork(
          reopened.owner,
          "restart",
          workId,
        );
        const revisions = facts.flatMap(({ entry }) =>
          entry.kind === "tool_intent"
            ? [entry.contract.extensions[0]?.[1]]
            : [],
        );
        expect(revisions).toEqual([
          `sha256:${"a".repeat(64)}`,
          `sha256:${"b".repeat(64)}`,
        ]);
      } finally {
        reopened.close();
      }
    }),
  30_000,
);

it(
  "keeps an unsafe old-revision intent and resumes on the new revision without rerunning it",
  () =>
    withDatabase(async (path) => {
      await killed(
        path,
        "crash-unsafe-upgrade",
        "unsafe-started",
        "restart-mule",
      );
      const snapshot = await finished(path, "finish-upgrade", "restart-mule");
      expectOutcome(snapshot, "completed");
      expect(toolResult(snapshot, "read-call")).toMatchObject({
        role: "toolResult",
        isError: true,
      });
      expect(await readFile(path + ".calls", "utf8")).toBe("read\n");
      const workId = snapshot.terminals[0]?.commandId;
      if (workId === undefined) throw new Error("Missing recovered work");
      const reopened = createNativeSqliteOwnerFixture(path);
      try {
        const facts = readSessionEntriesForWork(
          reopened.owner,
          "restart",
          workId,
        );
        // Mule emits a new intent when the new revision applies the old
        // unsafe replay policy; the tool itself is not executed again.
        expect(
          facts.flatMap(({ entry }) =>
            entry.kind === "tool_intent"
              ? [entry.contract.extensions[0]?.[1]]
              : [],
          ),
        ).toEqual([`sha256:${"a".repeat(64)}`, `sha256:${"b".repeat(64)}`]);
        expect(
          facts.some(
            ({ entry }) =>
              entry.kind === "run_started" &&
              entry.contract.extensions[0]?.[1] === `sha256:${"b".repeat(64)}`,
          ),
        ).toBe(true);
      } finally {
        reopened.close();
      }
    }),
  30_000,
);
