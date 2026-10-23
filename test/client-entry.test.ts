import { expect, expectTypeOf, it } from "vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { ExtensionId } from "../src/extension-contract.js";
import {
  clientContributionSchemaV1,
  defineClientEntry,
  parseClientEntryV1,
} from "../src/client-entry.js";

const contract = {
  id: ExtensionId.make("probe"),
  commands: {},
  projection: { schema: Schema.Struct({ title: Schema.String }) },
};
const empty = {
  chips: [],
  panel: [],
  timeline: { cells: [], omitted: 0 },
  waiting: null,
};
it("captures a typed projection in the same entry admitted for builtins and installed clients", () => {
  const entry = parseClientEntryV1(
    defineClientEntry(contract, {
      contribute: (view) => {
        if (view.status !== "running" || view.value === null) return empty;
        expectTypeOf(view.value.title).toEqualTypeOf<string>();
        return { ...empty, panel: [{ id: "title", title: view.value.title }] };
      },
    }),
  );
  expect(
    entry.contribute?.({
      status: "running",
      revision: "sha256:fixture",
      value: { title: "Current" },
    }),
  ).toMatchObject({ panel: [{ title: "Current" }] });
  expect(entry.contribute?.(undefined)).toEqual(empty);
  expect(
    entry.contribute?.({
      status: "stopped",
      revision: "sha256:fixture",
      reason: "stopped",
    }),
  ).toEqual(empty);
  expect(
    entry.contribute?.({
      status: "running",
      revision: "sha256:fixture",
      value: { title: 1 },
    }),
  ).toEqual(empty);
  expect(Object.keys(entry).sort()).toEqual(["contribute", "id", "protocol"]);
});
it("preserves an owner's footer presentation for its own prompts", () => {
  const entry = defineClientEntry(contract, {
    contribute: () => empty,
    promptPresentation: "footer",
  });
  expect(parseClientEntryV1(entry)).toHaveProperty(
    "promptPresentation",
    "footer",
  );
  expect(
    defineClientEntry(contract, { contribute: () => empty }),
  ).not.toHaveProperty("promptPresentation");
  expect(() =>
    parseClientEntryV1({ ...entry, promptPresentation: "note" }),
  ).toThrow();
});
it("normalizes plain Promise attachment without requiring Effect from its author", async () => {
  let attached = 0;
  const entry = defineClientEntry(contract, {
    attach: () => {
      attached += 1;
      return Promise.resolve();
    },
  });
  if (entry.attach === undefined) throw new Error("Missing fixture attachment");
  await Effect.runPromise(
    Effect.scoped(
      entry.attach({
        activity: {
          async *[Symbol.asyncIterator]() {
            yield await Promise.resolve({ state: "idle" as const });
          },
        },
        signal: new AbortController().signal,
        onError: (error) => {
          throw error;
        },
      }),
    ),
  );
  expect(attached).toBe(1);
});
it("rejects old protocol, unknown fields, no callbacks and a contribution without projection", () => {
  expect(() =>
    parseClientEntryV1({
      protocol: "shiitake.client/v0",
      id: "probe",
      attach: () => Effect.void,
    }),
  ).toThrow("shiitake.client/v1");
  expect(() =>
    parseClientEntryV1({
      protocol: "shiitake.client/v1",
      id: "probe",
      manifest: {},
      attach: () => Effect.void,
    }),
  ).toThrow();
  expect(() =>
    parseClientEntryV1({ protocol: "shiitake.client/v1", id: "probe" }),
  ).toThrow();
  expect(() =>
    defineClientEntry(
      { ...contract, projection: null },
      { contribute: () => empty },
    ),
  ).toThrow("requires a projection");
});
it("strictly admits the four bounded data placements and rejects executable or ambiguous output", () => {
  const decode = Schema.decodeUnknownSync(clientContributionSchemaV1, {
    onExcessProperty: "error",
  });
  expect(decode(empty)).toEqual(empty);
  expect(() =>
    decode({
      ...empty,
      panel: [
        {
          id: "r",
          title: "Row",
          actions: [{ id: "ask", label: "Ask", input: "text" }],
        },
      ],
    }),
  ).toThrow();
  expect(() =>
    decode({
      ...empty,
      chips: [
        {
          id: "chip",
          order: 0,
          row: { id: "row", title: "Chip", actions: [] },
        },
      ],
    }),
  ).toThrow();
  expect(() =>
    decode({
      ...empty,
      chips: [
        {
          id: "chip",
          order: 0,
          row: {
            id: "row",
            title: "Chip",
            open: { kind: "session", agentId: "a", sessionId: "s" },
          },
        },
      ],
    }),
  ).toThrow();
  const cell = { id: "1", sequence: 1, at: 1, commandId: null, rows: [] };
  expect(
    decode({
      ...empty,
      timeline: { cells: [{ ...cell, afterSequence: 0 }], omitted: 0 },
    }).timeline.cells[0]?.afterSequence,
  ).toBe(0);
  for (const afterSequence of [-1, 0.5, "1"]) {
    expect(() =>
      decode({
        ...empty,
        timeline: { cells: [{ ...cell, afterSequence }], omitted: 0 },
      }),
    ).toThrow();
  }
  expect(() =>
    decode({
      ...empty,
      timeline: {
        cells: [{ ...cell, afterSequence: 0, commandId: "conflicting-anchor" }],
        omitted: 0,
      },
    }),
  ).toThrow();
  expect(() =>
    decode({ ...empty, timeline: { cells: [cell, cell], omitted: 0 } }),
  ).toThrow();
  expect(() =>
    decode({
      ...empty,
      timeline: {
        cells: [
          {
            ...cell,
            rows: Array.from({ length: 200 }, (_, i) => ({
              id: String(i),
              title: "row",
            })),
          },
        ],
        omitted: 0,
      },
      panel: [{ id: "extra", title: "extra" }],
    }),
  ).toThrow();
  expect(() => decode({ ...empty, extra: true })).toThrow();
});
it("bounds display-only inspection rows inside the existing contribution budget", () => {
  const decode = Schema.decodeUnknownSync(clientContributionSchemaV1, {
    onExcessProperty: "error",
  });
  const panel = (rows: unknown[]) => ({
    ...empty,
    panel: [
      {
        id: "summary",
        title: "Current",
        inspection: { commandId: "inspect", title: "Inspect", rows },
      },
    ],
  });
  expect(
    decode(panel([{ id: "step", title: "Step", tone: "running" }])).panel[0]
      ?.inspection?.rows,
  ).toHaveLength(1);
  for (const rows of [
    [{ id: "step", title: "Step", actions: [{ id: "write", label: "Write" }] }],
    [
      {
        id: "step",
        title: "Step",
        open: { kind: "session", agentId: "a", sessionId: "s" },
      },
    ],
    [
      { id: "step", title: "Step" },
      { id: "step", title: "Duplicate" },
    ],
    Array.from({ length: 200 }, (_, i) => ({ id: String(i), title: "Step" })),
  ])
    expect(() => decode(panel(rows))).toThrow();
});

it("admits navigate params in client rows and keeps chips display-only", () => {
  const decode = Schema.decodeUnknownSync(clientContributionSchemaV1, {
    onExcessProperty: "error",
  });
  const row = {
    id: "conversation",
    title: "Open conversation",
    open: {
      params: [
        { key: "agent", value: "garden" },
        { key: "session", value: "session-one" },
      ],
    },
  };
  expect(decode({ ...empty, panel: [row] }).panel[0]?.open).toEqual(row.open);
  expect(() =>
    decode({ ...empty, chips: [{ id: "chip", order: 0, row }] }),
  ).toThrow();
  expect(() =>
    decode({
      ...empty,
      panel: [
        {
          ...row,
          open: {
            kind: "session",
            agentId: "garden",
            sessionId: "session-one",
          },
        },
      ],
    }),
  ).toThrow();
});
