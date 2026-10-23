import * as Arbitrary from "effect/Arbitrary";
import * as Effect from "effect/Effect";
import { expect, it } from "vitest";
import type { Json } from "../src/extensions.js";

import {
  parseAccepted,
  parseCancelResult,
  parseWithdrawResult,
  parseSessionSnapshot,
  parseSessionFrame,
  parseSessionSummaryPage,
} from "../src/client.js";
import {
  emptyMessageSnapshotArbitrary,
  idleActivity,
  runningActivity,
  sessionSnapshot,
} from "./fixtures/client.fixture.js";

it("decodes bounded tool output and rejects excess wire keys", () => {
  const frame = {
    kind: "tool_output",
    activity: runningActivity,
    toolOutput: [
      {
        toolCallId: "call",
        revision: 2,
        tail: "step 2",
        droppedBytes: 0,
        totalBytes: 6,
      },
    ],
  };
  expect(parseSessionFrame(JSON.stringify(frame))).toEqual(frame);
  expect(() =>
    parseSessionFrame(JSON.stringify({ ...frame, extra: true })),
  ).toThrow();
  expect(() =>
    parseSessionFrame(
      JSON.stringify({
        ...frame,
        toolOutput: [{ ...frame.toolOutput[0], extra: true }],
      }),
    ),
  ).toThrow();
  expect(() =>
    parseSessionFrame(
      JSON.stringify({
        ...frame,
        toolOutput: [{ ...frame.toolOutput[0], tail: "a".repeat(8193) }],
      }),
    ),
  ).toThrow();
});

it("parses schema-derived Session snapshot representations", async () => {
  const snapshots = await Effect.runPromise(
    Arbitrary.sampleEffect(emptyMessageSnapshotArbitrary, {
      count: 25,
      seed: 42,
    }),
  );

  // The wire is JSON: the parser must return exactly what JSON carried, so a
  // generated -0 or an absent optional compares as its JSON form.
  for (const snapshot of snapshots) {
    const wire = JSON.stringify(snapshot);
    expect(parseSessionSnapshot(wire)).toEqual(JSON.parse(wire));
  }
});

it("parses owner inbox state and unavailable rows in Session summaries", () => {
  const sessions = [
    {
      sessionId: "queued",
      createdAt: 1,
      lastActivityAt: 2,
      title: "Waiting",
      state: "running",
    },
    {
      sessionId: "settled",
      createdAt: 3,
      lastActivityAt: 5,
      settledAt: 4,
      title: "Done",
      state: "errored",
    },
    { sessionId: "damaged", state: "unavailable" },
  ];
  expect(
    parseSessionSummaryPage(JSON.stringify({ sessions, next: "cursor" })),
  ).toEqual({ sessions, next: "cursor" });
  for (const invalid of [
    {
      sessionId: "missing",
      createdAt: 1,
      lastActivityAt: 2,
      title: "No state",
    },
    {
      sessionId: "retired",
      createdAt: 1,
      lastActivityAt: 2,
      title: "Retired status",
      state: "idle",
    },
    { sessionId: "", state: "unavailable" },
  ])
    expect(() =>
      parseSessionSummaryPage(JSON.stringify({ sessions: [invalid] })),
    ).toThrow();
});

it("requires the current admission projection instead of silently reading an old snapshot", () => {
  expect(() =>
    parseSessionSnapshot(
      JSON.stringify({
        sessionId: "old",
        cursor: null,
        hasMore: false,
        messages: [],
        terminals: [],
        status: "idle",
        toolOutput: [],
        progress: null,
      }),
    ),
  ).toThrow();
});

it("parses accepted, known-too-late and unknown cancellation outcomes", () => {
  expect(
    parseCancelResult(JSON.stringify({ status: "accepted", commandId: "a" })),
  ).toEqual({ status: "accepted", commandId: "a" });
  expect(
    parseCancelResult(JSON.stringify({ status: "too_late", commandId: "a" })),
  ).toEqual({ status: "too_late", commandId: "a" });
  expect(parseCancelResult('{"status":"unknown"}')).toEqual({
    status: "unknown",
  });
  expect(() => parseCancelResult('{"status":"too_late"}')).toThrow();
  expect(() =>
    parseCancelResult(JSON.stringify({ status: "conflict", commandId: "a" })),
  ).toThrow();
});

it("requires an exact nonnegative integer pending total on the public wire", () => {
  const snapshot = sessionSnapshot();
  for (const total of [undefined, -1, 1.5])
    expect(() =>
      parseSessionSnapshot(
        JSON.stringify({
          ...snapshot,
          pending: { prompts: [], hasMore: false, total },
        }),
      ),
    ).toThrow();
  expect(
    parseSessionSnapshot(
      JSON.stringify({
        ...snapshot,
        pending: { prompts: [], hasMore: true, total: 201 },
      }),
    ).pending.total,
  ).toBe(201);
});

it("requires the owner-issued id on a queued activity", () => {
  const snapshot = (queue: Json) =>
    JSON.stringify({
      pending: { total: 0, prompts: [], hasMore: false },
      sessionId: "queued",
      cursor: null,
      hasMore: false,
      messages: [],
      terminals: [],
      activity: { ...idleActivity, queue },
      toolOutput: [],
      progress: null,
      extensions: {},
      attention: [],
    });
  expect(
    parseSessionSnapshot(
      snapshot({ state: "queued", commandId: "command-1", runningWork: null }),
    ).activity.queue,
  ).toEqual({ state: "queued", commandId: "command-1", runningWork: null });
  expect(() => parseSessionSnapshot(snapshot({ state: "queued" }))).toThrow();
  for (const runningWork of [
    { kind: "session", sessionId: "a", commandId: "running" },
    { kind: "host", lane: "delivery", jobId: "mail-1" },
  ]) {
    const queue = { state: "queued", commandId: "command-1", runningWork };
    expect(parseSessionSnapshot(snapshot(queue)).activity.queue).toEqual(queue);
  }
  expect(() =>
    parseSessionSnapshot(snapshot({ state: "queued", commandId: "command-1" })),
  ).toThrow();
  expect(() =>
    parseSessionSnapshot(
      snapshot({
        state: "queued",
        commandId: "command-1",
        runningWork: { kind: "session", sessionId: "a" },
      }),
    ),
  ).toThrow();
});

it("parses owner-issued command identities without accepting caller request identities", () => {
  expect(
    parseAccepted(JSON.stringify({ status: "accepted", commandId: "work-1" })),
  ).toEqual({
    status: "accepted",
    commandId: "work-1",
  });
  expect(() =>
    parseAccepted(
      JSON.stringify({ status: "accepted", requestId: "caller-1" }),
    ),
  ).toThrow();
});

it("keeps pending prompt delivery in the canonical wire projection", () => {
  const snapshot = {
    sessionId: "pending-delivery",
    cursor: null,
    hasMore: false,
    messages: [],
    terminals: [],
    pending: {
      total: 2,
      prompts: [
        {
          commandId: "steer-command",
          requestId: "steer-request",
          text: "Change course",
          delivery: "steer",
        },
        {
          commandId: "follow-up-command",
          requestId: "follow-up-request",
          text: "After this run",
          delivery: "followUp",
        },
      ],
      hasMore: false,
    },
    activity: runningActivity,
    toolOutput: [],
    progress: null,
    extensions: {},
    attention: [],
  } as const;
  expect(parseSessionSnapshot(JSON.stringify(snapshot))).toEqual(snapshot);
  expect(() =>
    parseSessionSnapshot(
      JSON.stringify({
        ...snapshot,
        pending: {
          total: 1,
          prompts: [
            {
              commandId: "missing-delivery",
              requestId: "missing-delivery-request",
              text: "No policy",
            },
          ],
          hasMore: false,
        },
      }),
    ),
  ).toThrow();
});

it("rejects a pending prompt without its caller request identity", () => {
  expect(() =>
    parseSessionSnapshot(
      JSON.stringify({
        ...sessionSnapshot(),
        pending: {
          total: 1,
          hasMore: false,
          prompts: [
            {
              commandId: "owned-command",
              text: "No request identity",
              delivery: "followUp",
            },
          ],
        },
      }),
    ),
  ).toThrow();
});

it("carries an extension's advertised catalog through the Session wire", () => {
  const snapshot = {
    sessionId: "catalog",
    cursor: null,
    hasMore: false,
    messages: [],
    terminals: [],
    pending: { total: 0, prompts: [], hasMore: false },
    activity: idleActivity,
    toolOutput: [],
    progress: null,
    attention: [],
    extensions: {
      "test-hello": {
        status: "running",
        revision: "sha256:0",
        value: null,
        catalog: { protocol: "whistle.catalog/v1", commands: [] },
      },
      "test-plain": { status: "running", revision: "sha256:1", value: null },
    },
  } as const;
  expect(parseSessionSnapshot(JSON.stringify(snapshot))).toEqual(snapshot);
});

it("rejects undeclared diagnostic fields at snapshot and frame boundaries", () => {
  const diagnostic = {
    kind: "sandbox_failure",
    ref: "failure-1",
    executionId: "execution-1",
    adapter: "local",
    buildSha: null,
    tag: "GuestThrew",
    fields: { message: "guest failed" },
    healthAlert: false,
  };
  const baseline = sessionSnapshot();
  const snapshot = { ...baseline, sandboxFailures: [diagnostic] };
  const update = {
    kind: "update",
    delivery: "live",
    cursor: null,
    messages: [],
    terminals: [],
    sandboxFailures: [diagnostic],
    pending: baseline.pending,
    activity: baseline.activity,
    extensions: baseline.extensions,
    attention: [],
  };
  expect(
    parseSessionSnapshot(JSON.stringify(snapshot)).sandboxFailures,
  ).toEqual([diagnostic]);
  expect(parseSessionFrame(JSON.stringify(update))).toEqual(update);
  const invalidDiagnostic = { ...diagnostic, unexpected: true };
  expect(() =>
    parseSessionSnapshot(
      JSON.stringify({ ...snapshot, sandboxFailures: [invalidDiagnostic] }),
    ),
  ).toThrow();
  expect(() =>
    parseSessionFrame(
      JSON.stringify({ ...update, sandboxFailures: [invalidDiagnostic] }),
    ),
  ).toThrow();
  expect(() =>
    parseSessionFrame(
      JSON.stringify({
        kind: "reset",
        reason: "history_unavailable",
        cursor: null,
        baseline: { ...snapshot, sandboxFailures: [invalidDiagnostic] },
      }),
    ),
  ).toThrow();
});

it("preserves opaque host metadata on pending prompts across the JSON boundary", () => {
  const meta = {
    author: { id: "visitor-one", name: "Visitor one" },
  };
  const snapshot = {
    ...sessionSnapshot(),
    pending: {
      total: 1,
      hasMore: false,
      prompts: [
        {
          commandId: "pending-one",
          requestId: "pending-request",
          text: "hello",
          delivery: "followUp",
          meta,
        },
      ],
    },
  };
  expect(
    parseSessionSnapshot(JSON.stringify(snapshot)).pending.prompts[0],
  ).toEqual(snapshot.pending.prompts[0]);
});

it("decodes unknown request resolution without inventing a command identity", () => {
  expect(parseWithdrawResult('{"status":"unknown"}')).toEqual({
    status: "unknown",
  });
  expect(
    parseWithdrawResult('{"status":"accepted","commandId":"owner-command"}'),
  ).toEqual({ status: "accepted", commandId: "owner-command" });
  expect(() => parseWithdrawResult('{"status":"withdrawn"}')).toThrow();
});
