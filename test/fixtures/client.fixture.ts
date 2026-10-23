import * as Arbitrary from "effect/Arbitrary";
import * as Schema from "effect/Schema";
import * as Struct from "effect/Struct";

import type { SessionActivity, SessionSnapshot } from "../../src/client.js";
import {
  SessionFrameSchema,
  SessionSnapshotSchema,
} from "../../src/client-wire.js";

const decodeSnapshot = Schema.decodeUnknownSync(SessionSnapshotSchema);

/** Nothing queued or running: the Session waits for a person. */
export const idleActivity: SessionActivity = {
  queue: { state: "idle" },
  cancellationRequested: false,
  retry: null,
  activeTool: null,
  lastModelChoiceSequence: null,
  compaction: null,
};

/** The Session's work is running with no tool or retry in flight. */
export const runningActivity: SessionActivity = {
  ...idleActivity,
  queue: { state: "running", commandId: "running-command" },
};

/** The Session's work waits behind other work in the Agent. */
export function queuedActivity(commandId: string): SessionActivity {
  return {
    ...idleActivity,
    queue: { state: "queued", commandId, runningWork: null },
  };
}

/** One readable baseline, still checked by Shiitake's canonical grammar. */
export function sessionSnapshot(
  input: Partial<SessionSnapshot> = {},
): SessionSnapshot {
  return decodeSnapshot({
    pending: { total: 0, prompts: [], hasMore: false },
    sessionId: "test-session",
    cursor: null,
    hasMore: false,
    messages: [],
    terminals: [],
    activity: idleActivity,
    toolOutput: [],
    progress: null,
    extensions: {},
    attention: [],
    ...input,
  });
}

/** Schema-derived representations for broad wire-contract laws. */
export const emptyMessageSnapshotArbitrary = Arbitrary.schema(
  SessionSnapshotSchema.mapFields(
    Struct.assign({ messages: Schema.Tuple([]) }),
  ),
);
export const sessionFrameArbitrary = Arbitrary.schema(SessionFrameSchema);
