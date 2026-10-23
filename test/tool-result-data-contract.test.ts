import * as Result from "effect/Result";
import { expect, it } from "vitest";

import {
  decodeStoredSessionEntry,
  encodeStoredSessionEntry,
} from "../src/internal/woodstock/session-entry.js";
import { parseSessionFrame, parseSessionSnapshot } from "../src/client-wire.js";
import { checkHostMeta } from "../src/internal/host-meta.js";
import { sessionSnapshot } from "./fixtures/client.fixture.js";
import * as Schema from "effect/Schema";

// These are JSON payloads produced by Mule's completion and message events,
// not executable objects. The display payload is the typed tool failure.
const run = { kind: "run", value: "run-data" };
const turn = { kind: "turn", value: "turn-data", parent: run };
const toolCall = { kind: "tool_call", value: "tool-data", parent: turn };
const message = { kind: "message", value: "message-data", parent: turn };
const content = [{ type: "text", text: "The capability was refused." }];
const data = {
  error: { kind: "permission_denied", message: "Write access was revoked." },
};
const result = { content, isError: true };
const value = {
  role: "toolResult",
  toolCallId: "call-data",
  toolName: "execute",
  ...result,
  timestamp: 1,
};
const outcomeEntry = {
  kind: "tool_outcome",
  workId: "work-data",
  run,
  turn,
  toolCall,
  call: { type: "toolCall", id: "call-data", name: "execute", arguments: {} },
};
const messageEntry = {
  kind: "message",
  workId: "work-data",
  run,
  turn,
  message,
};

it.each([
  { ...outcomeEntry, result: { ...result, details: data } },
  { ...messageEntry, value: { ...value, details: data } },
  { ...outcomeEntry, result: { ...result, data }, data },
  { ...messageEntry, value: { ...value, data }, data },
])("refuses nested tool display payloads in persisted entry $kind", (entry) => {
  expect(
    Result.isFailure(decodeStoredSessionEntry(JSON.stringify(entry))),
  ).toBe(true);
});

it("round-trips independent application data and host meta with overlapping keys", () => {
  const meta = checkHostMeta(
    { schema: Schema.Struct({ error: Schema.String }) },
    { error: "host attribution, not a tool failure" },
  );
  const message = {
    id: "message-data",
    sequence: 1,
    commandId: "work-data",
    value,
    data: { ...data, large: "x".repeat(8_192) },
    meta,
  };
  const snapshot = sessionSnapshot();
  const decoded = parseSessionSnapshot(
    JSON.stringify({ ...snapshot, messages: [message] }),
  );
  expect(decoded.messages).toEqual([message]);
  expect(decoded.messages[0]?.value).not.toHaveProperty("details");
  expect(decoded.messages[0]?.value).not.toHaveProperty("data");
  expect(decoded.messages[0]?.value).not.toHaveProperty("meta");
  const frame = parseSessionFrame(
    JSON.stringify({
      kind: "update",
      delivery: "replay",
      cursor: null,
      messages: [message],
      terminals: [],
      pending: snapshot.pending,
      activity: snapshot.activity,
      extensions: {},
      attention: [],
    }),
  );
  expect(frame).toHaveProperty("messages", [message]);
  expect(() =>
    checkHostMeta(
      { schema: Schema.Struct({ error: Schema.String }) },
      { error: 1 },
    ),
  ).toThrow("host's schema");
});

it.each(["x".repeat(4_094), "é".repeat(2_047)])(
  "preserves the independent 4096-byte encoded metadata boundary",
  (meta) => {
    expect(new TextEncoder().encode(JSON.stringify(meta)).byteLength).toBe(
      4_096,
    );
    expect(checkHostMeta(undefined, meta)).toBe(meta);
    const snapshot = sessionSnapshot();
    const message = {
      id: "bounded-meta",
      sequence: 1,
      commandId: "work-data",
      value,
      meta,
      data: { large: "x".repeat(8_192) },
    };
    expect(
      parseSessionSnapshot(JSON.stringify({ ...snapshot, messages: [message] }))
        .messages,
    ).toEqual([message]);
    expect(() => checkHostMeta(undefined, meta + "x")).toThrow("4096");
    expect(() =>
      parseSessionSnapshot(
        JSON.stringify({
          ...snapshot,
          messages: [{ ...message, meta: meta + "x" }],
        }),
      ),
    ).toThrow();
  },
);

it("refuses retired details on both snapshot and live/replay wire messages", () => {
  const message = {
    id: "old",
    sequence: 1,
    commandId: "work-data",
    value: { ...value, details: data },
  };
  const snapshot = sessionSnapshot();
  expect(() =>
    parseSessionSnapshot(JSON.stringify({ ...snapshot, messages: [message] })),
  ).toThrow();
  for (const delivery of ["live", "replay"])
    expect(() =>
      parseSessionFrame(
        JSON.stringify({
          kind: "update",
          delivery,
          cursor: null,
          messages: [message],
          terminals: [],
          pending: snapshot.pending,
          activity: snapshot.activity,
          extensions: {},
          attention: [],
        }),
      ),
    ).toThrow();
});

it.each([
  { ...outcomeEntry, result, data },
  { ...messageEntry, value, data },
])(
  "round-trips tool display data directly on entry $kind, not inside the model result",
  (entry) => {
    const decoded = decodeStoredSessionEntry(JSON.stringify(entry));
    if (Result.isFailure(decoded))
      throw new Error(
        "The current entry codec refused the tool-result data contract",
      );
    expect(JSON.parse(encodeStoredSessionEntry(decoded.success))).toEqual(
      entry,
    );
  },
);
