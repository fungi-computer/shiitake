import { expect, it } from "vitest";

import {
  appendSessionEntry,
  prepareSessionRun,
  prepareSessionStore,
  readSessionEntriesForWork,
  recordStreamingPartialOwned,
} from "../src/internal/woodstock/index.js";
import { admitStoredCommand } from "./fixtures/admission-door.fixture.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

const run = { kind: "run", value: "partial-run" } as const;
const turn = { kind: "turn", value: "partial-turn", parent: run } as const;
const identity = (value: string) =>
  ({ kind: "message", value, parent: turn }) as const;

function openedWork() {
  const fixture = createNativeSqliteOwnerFixture();
  prepareSessionStore(fixture.owner);
  const admitted = admitStoredCommand(
    fixture.owner,
    "partial-session",
    {
      requestId: "partial-request",
      text: "ask",
      delivery: "followUp",
      type: "prompt",
    },
    { enqueue: () => undefined },
  );
  const workId = admitted.accepted.commandId;
  const placeholder = (value: string) => {
    const message = identity(value);
    const entry = {
      kind: "message_start",
      workId,
      run,
      turn,
      message,
      role: "assistant",
    } as const;
    appendSessionEntry(fixture.owner, {
      sessionId: "partial-session",
      id: value,
      entry,
      state: "streaming",
    });
    return entry;
  };
  const claim = () =>
    prepareSessionRun(fixture.owner, {
      sessionId: "partial-session",
      commandFactId: workId,
    });
  claim();
  const placeholders = () =>
    readSessionEntriesForWork(fixture.owner, "partial-session", workId)
      .filter(({ state }) => state === "streaming")
      .map(({ id }) => id);
  return { fixture, placeholder, claim, placeholders };
}

it("opens a placeholder stored before partial text existed and drops it as empty", () => {
  const { fixture, placeholder, claim, placeholders } = openedWork();
  try {
    placeholder("previous-shape");
    // The exact row an earlier release wrote: no `partial` key at all.
    const keyAt = fixture.owner.sql
      .exec(
        `SELECT instr(data_json, '"partial":') AS at FROM session_entries WHERE id = 'previous-shape'`,
      )
      .toArray()[0]?.at;
    expect(keyAt).toBe(0);
    expect(claim().recoveredPartial).toBeUndefined();
    expect(placeholders()).toEqual([]);
  } finally {
    fixture.close();
  }
});

it("keeps only the newest placeholder that holds flushed text and hands it to the restart", () => {
  const { fixture, placeholder, claim, placeholders } = openedWork();
  try {
    const older = placeholder("older-shape");
    placeholder("empty-newest");
    recordStreamingPartialOwned(fixture.owner, {
      sessionId: "partial-session",
      entry: older,
      text: "half an answ",
      offset: 0,
    });
    const newer = placeholder("newer-shape");
    recordStreamingPartialOwned(fixture.owner, {
      sessionId: "partial-session",
      entry: newer,
      text: "half an answer so far",
      offset: 7,
    });
    placeholder("empty-last");
    expect(claim().recoveredPartial).toEqual({
      messageId: "newer-shape",
      text: "half an answer so far",
      offset: 7,
    });
    expect(placeholders()).toEqual(["newer-shape"]);
  } finally {
    fixture.close();
  }
});
