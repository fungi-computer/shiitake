import * as Effect from "effect/Effect";
import { expect, it } from "vitest";

import {
  appendSessionEntry,
  prepareSessionRun,
  prepareSessionStore,
  readSelectedPath,
  readSessionEntryById,
  recordWorkSettlement,
  settleControls,
  takeSessionControls,
} from "../src/internal/woodstock/index.js";
import { admitStoredCommand } from "./fixtures/admission-door.fixture.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

function prompt(
  requestId: string,
  text: string,
  delivery: "steer" | "followUp",
) {
  return { requestId, text, delivery, type: "prompt" } as const;
}

it.each([
  ["completed", "safe parent reason", undefined],
  ["cancelled", "safe parent reason", undefined],
  ["failed", "safe parent reason", "safe parent reason"],
  ["interrupted", "safe parent reason", "safe parent reason"],
  ["failed", undefined, undefined],
  ["interrupted", undefined, undefined],
] as const)(
  "settles a delivered %s control with reason %s as %s exactly once",
  (outcome, diagnostic, expectedDiagnostic) => {
    const fixture = createNativeSqliteOwnerFixture();
    try {
      prepareSessionStore(fixture.owner);
      const parent = admitStoredCommand(
        fixture.owner,
        "control-session",
        prompt("parent-request", "parent", "followUp"),
        {
          enqueue: () => undefined,
          queueHead: () => ({ state: "idle" as const }),
        },
      );
      const steer = admitStoredCommand(
        fixture.owner,
        "control-session",
        prompt("steer-request", "change direction", "steer"),
        {
          enqueue: () => {
            throw new Error(
              "active steer must not enqueue a second Watchdog job",
            );
          },
          queueHead: () => ({
            state: "running" as const,
            jobId: parent.accepted.commandId,
            cancellationRequested: false,
          }),
        },
      );

      expect(
        readSessionEntryById(fixture.owner, steer.accepted.commandId),
      ).toBeUndefined();

      const delivered = Effect.runSync(
        takeSessionControls(fixture.owner, {
          sessionId: "control-session",
          workId: parent.accepted.commandId,
          delivery: "steer",
        }),
      );
      expect(delivered).toHaveLength(1);
      expect(delivered[0]?.requestId).toBe("steer-request");

      const settle = () => {
        Effect.runSync(
          settleControls(fixture.owner, {
            sessionId: "control-session",
            parentWorkId: parent.accepted.commandId,
            outcome,
            diagnostic,
            enqueue: () => {
              throw new Error("delivered steer must not enqueue");
            },
          }),
        );
      };
      settle();
      settle();
      const path = readSelectedPath(fixture.owner, {
        sessionId: "control-session",
      });
      const expectedTerminal = {
        kind: "work_terminal",
        workId: steer.accepted.commandId,
        outcome,
      };
      expect(
        path
          .filter(
            ({ entry }) =>
              entry.kind === "work_terminal" &&
              entry.workId === steer.accepted.commandId,
          )
          .map(({ entry }) => entry),
      ).toEqual([
        expectedDiagnostic === undefined
          ? expectedTerminal
          : { ...expectedTerminal, diagnostic: expectedDiagnostic },
      ]);
    } finally {
      fixture.close();
    }
  },
);

it("queues a steer instead of attaching it to a run with a recorded terminal", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    prepareSessionStore(fixture.owner);
    const sessionId = "terminal-before-steer";
    const parent = admitStoredCommand(
      fixture.owner,
      sessionId,
      prompt("first", "start", "followUp"),
      { enqueue: () => undefined },
    );
    prepareSessionRun(fixture.owner, {
      sessionId,
      commandFactId: parent.accepted.commandId,
    });
    const run = { kind: "run", value: "first-run" } as const;
    appendSessionEntry(fixture.owner, {
      sessionId,
      id: "first-terminal",
      entry: {
        kind: "run_terminal",
        workId: parent.accepted.commandId,
        run,
        terminal: { kind: "terminal", value: "first-terminal-id", parent: run },
        outcome: "completed",
      },
    });
    const queued: string[] = [];
    const steer = admitStoredCommand(
      fixture.owner,
      sessionId,
      prompt("next", "change course", "steer"),
      {
        enqueue: (work) => queued.push(work.jobId),
        queueHead: () => ({
          state: "running",
          jobId: parent.accepted.commandId,
          cancellationRequested: false,
        }),
      },
    );
    expect(queued).toEqual([steer.accepted.commandId]);
    expect(steer.command.entry).toMatchObject({ delivery: "steer" });
  } finally {
    fixture.close();
  }
});

it.each(["interrupted", "failed", "cancelled"] as const)(
  "removes a streaming placeholder when its work is %s",
  (outcome) => {
    const fixture = createNativeSqliteOwnerFixture();
    try {
      prepareSessionStore(fixture.owner);
      const sessionId = "interrupted-stream-session";
      const parent = admitStoredCommand(
        fixture.owner,
        sessionId,
        prompt("stream-request", "start", "followUp"),
        { enqueue: () => undefined },
      );
      prepareSessionRun(fixture.owner, {
        sessionId,
        commandFactId: parent.accepted.commandId,
      });
      const run = { kind: "run", value: "stream-run" } as const;
      const turn = { kind: "turn", value: "stream-turn", parent: run } as const;
      const message = {
        kind: "message",
        value: "stream-message",
        parent: turn,
      } as const;
      appendSessionEntry(fixture.owner, {
        sessionId,
        id: message.value,
        state: "streaming",
        entry: {
          kind: "message_start",
          workId: parent.accepted.commandId,
          run,
          turn,
          message,
          role: "assistant",
        },
      });
      Effect.runSync(
        recordWorkSettlement(fixture.owner, {
          sessionId,
          commandFactId: parent.accepted.commandId,
          workId: parent.accepted.commandId,
          outcome,
        }),
      );
      expect(
        fixture.owner.sql
          .exec(
            "SELECT id FROM session_entries WHERE session_id=? AND work_id=? AND state='streaming'",
            sessionId,
            parent.accepted.commandId,
          )
          .toArray(),
      ).toEqual([]);
    } finally {
      fixture.close();
    }
  },
);

it.each(["completed", "failed", "cancelled", "interrupted"] as const)(
  "queues a late steer after a %s parent as its own run",
  (outcome) => {
    const fixture = createNativeSqliteOwnerFixture();
    try {
      prepareSessionStore(fixture.owner);
      const parent = admitStoredCommand(
        fixture.owner,
        "late-control-session",
        prompt("parent-request", "parent", "followUp"),
        {
          enqueue: () => undefined,
          queueHead: () => ({ state: "idle" as const }),
        },
      );
      const late = admitStoredCommand(
        fixture.owner,
        "late-control-session",
        prompt("late-request", "arrived too late", "steer"),
        {
          enqueue: () => {
            throw new Error(
              "late steer must not enqueue a second Watchdog job",
            );
          },
          queueHead: () => ({
            state: "running" as const,
            jobId: parent.accepted.commandId,
            cancellationRequested: false,
          }),
        },
      );

      const queued: Readonly<{
        jobId: string;
        delivery: "steer" | "followUp";
      }>[] = [];
      Effect.runSync(
        settleControls(fixture.owner, {
          sessionId: "late-control-session",
          parentWorkId: parent.accepted.commandId,
          outcome,
          diagnostic: "safe parent reason belongs to the parent",
          enqueue: (work) => {
            queued.push({ jobId: work.jobId, delivery: work.delivery });
          },
        }),
      );
      expect(queued).toEqual([
        { jobId: late.accepted.commandId, delivery: "steer" },
      ]);
      const run = prepareSessionRun(fixture.owner, {
        sessionId: "late-control-session",
        commandFactId: late.accepted.commandId,
      });
      expect(run.command.entry).toMatchObject({
        kind: "command",
        message: { content: "arrived too late" },
      });
      const path = readSelectedPath(fixture.owner, {
        sessionId: "late-control-session",
      });
      expect(
        path.some(
          ({ entry }) =>
            entry.kind === "work_terminal" &&
            entry.workId === late.accepted.commandId,
        ),
      ).toBe(false);
      expect(
        path.some(
          ({ entry }) =>
            entry.kind === "command" && entry.requestId === "late-request",
        ),
      ).toBe(true);
      Effect.runSync(
        recordWorkSettlement(fixture.owner, {
          sessionId: "late-control-session",
          commandFactId: late.accepted.commandId,
          workId: late.accepted.commandId,
          outcome: "completed",
        }),
      );
      expect(
        readSelectedPath(fixture.owner, {
          sessionId: "late-control-session",
        })
          .filter(
            ({ entry }) =>
              entry.kind === "work_terminal" &&
              entry.workId === late.accepted.commandId,
          )
          .map(({ entry }) => entry),
      ).toEqual([
        {
          kind: "work_terminal",
          workId: late.accepted.commandId,
          outcome: "completed",
        },
      ]);
    } finally {
      fixture.close();
    }
  },
);

it("queues a steer submitted after Stop as separate work", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    prepareSessionStore(fixture.owner);
    const parent = admitStoredCommand(
      fixture.owner,
      "stopping-session",
      prompt("parent-request", "parent", "followUp"),
      { enqueue: () => undefined },
    );
    const queued: string[] = [];
    const steer = admitStoredCommand(
      fixture.owner,
      "stopping-session",
      prompt("steer-after-stop", "correction", "steer"),
      {
        enqueue: (work) => {
          queued.push(work.jobId);
        },
        queueHead: () => ({
          state: "running" as const,
          jobId: parent.accepted.commandId,
          cancellationRequested: true,
        }),
      },
    );
    expect(queued).toEqual([steer.accepted.commandId]);
  } finally {
    fixture.close();
  }
});
