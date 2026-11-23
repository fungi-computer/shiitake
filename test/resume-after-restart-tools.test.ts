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

type Snapshot = Awaited<ReturnType<typeof finished>>;

/** Every stored result for the interrupted call, in transcript order. */
function sendResults(snapshot: Snapshot) {
  return snapshot.messages.flatMap(({ value }) =>
    value.role === "toolResult" && value.toolCallId === "send-call"
      ? [value]
      : [],
  );
}

const retainedInterrupted = [
  {
    type: "text",
    text: '[interrupted] Tool "execute" (id: send-call) was still executing when the run was terminated.',
  },
];

it(
  "persists one interrupted result when a run is cancelled mid-tool",
  () =>
    withDatabase(async (path) => {
      // The next prompt's model request rejects an unanswered or twice-answered call.
      const snapshot = await finished(path, "cancel-mid-tool");
      expectOutcome(snapshot, "cancelled");
      expectOutcome(snapshot, "completed");
      const results = sendResults(snapshot);
      expect(results).toHaveLength(1);
      expect(results[0]).toMatchObject({
        isError: true,
        toolName: "execute",
        content: retainedInterrupted,
      });
      // The result is the call's answer, before the next prompt.
      const roles = snapshot.messages.map(({ value }) => value.role);
      expect(roles).toEqual([
        "user",
        "assistant",
        "toolResult",
        "user",
        "assistant",
      ]);
      const again = await finished(path, "finish-answered");
      expect(sendResults(again)).toEqual(results);
    }),
  45_000,
);

it(
  "persists one interrupted result when a killed tool's run is cancelled, and adds none on the next resume",
  () =>
    withDatabase(async (path) => {
      await killed(path, "crash-tool", "tool-started");
      const cancelled = await finished(path, "cancel");
      expectOutcome(cancelled, "cancelled");
      const results = sendResults(cancelled);
      expect(results).toHaveLength(1);
      expect(results[0]).toMatchObject({
        isError: true,
        content: retainedInterrupted,
      });
      expect(sendResults(await finished(path))).toEqual(results);
      expect(sendResults(await finished(path))).toEqual(results);
    }),
  60_000,
);

it(
  "keeps the restart result as the only answer on a second resume",
  () =>
    withDatabase(async (path) => {
      await killed(path, "crash-tool", "tool-started");
      const resumed = await finished(path);
      const results = sendResults(resumed);
      expect(results).toHaveLength(1);
      // The next prompt's request carries exactly that one answer.
      const next = await finished(path, "finish-answered");
      expectOutcome(next, "completed");
      expect(sendResults(next)).toEqual(results);
    }),
  45_000,
);

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
  "stores the host's account of an interrupted unsafe tool once, never reruns it, and does not ask again",
  () =>
    withDatabase(async (path) => {
      await killed(path, "crash-tool", "tool-started");
      const snapshot = await finished(path, "finish-recovered");
      expectOutcome(snapshot, "completed");
      const result = toolResult(snapshot, "send-call");
      if (result?.role !== "toolResult")
        throw new Error("Missing recovered result");
      expect(result.isError).toBe(true);
      expect(result.content).toEqual([
        {
          type: "text",
          text: "The command finished while the Hub restarted: exit code 0.",
        },
      ]);
      // A second start finds the answer stored and does not ask the host again.
      const again = await finished(path, "finish-recovered");
      expect(sendResults(again)).toEqual(sendResults(snapshot));
      expect(await readFile(path + ".recovered", "utf8")).toBe(
        "restart:send-call\n",
      );
    }),
  60_000,
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
