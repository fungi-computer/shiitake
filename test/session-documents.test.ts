import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { expect, it } from "vitest";
import { Shiitake, type DocumentHandlesV0 } from "../src/index.js";
import {
  defineExtension,
  ExtensionId,
  ExtensionCommandError,
} from "../src/extensions.js";
import {
  readSelectedPath,
  readSessionDocument,
} from "../src/internal/woodstock/index.js";
import { admitExtensionRosterV0 } from "../src/internal/extension-roster.js";
import {
  createNativeSqliteOwnerFixture,
  createCompatibilityRuntimeHost,
  compatibilityRunDependencies,
} from "./fixtures/native-sqlite-owner.fixture.js";

const id = ExtensionId.make("test-documents");
const counter = Schema.Struct({ count: Schema.Int });
const specs = {
  inherited: {
    schema: counter,
    initial: { count: 0 },
    fork: "inherit-at-fork-point",
    maxBytes: 64,
  },
  current: {
    schema: counter,
    initial: { count: 0 },
    fork: "current",
    maxBytes: 64,
  },
  fresh: {
    schema: counter,
    initial: { count: 0 },
    fork: "fresh",
    maxBytes: 64,
  },
  text: {
    schema: Schema.String,
    initial: "",
    fork: "inherit-at-fork-point",
    maxBytes: 16,
  },
} as const;
const counters = Schema.Struct({
  inherited: Schema.Int,
  current: Schema.Int,
  fresh: Schema.Int,
});

function documentExtension(settled = false) {
  let handles: DocumentHandlesV0<typeof specs> | undefined;
  const extension = defineExtension({ id, version: "1.0.0" })
    .config(null)
    .documents(specs)
    .define({
      commands: {
        specs: {
          read: { scope: "session", input: Schema.Null, output: counters },
          add: { scope: "session", input: Schema.Null, output: Schema.Null },
          text: { scope: "session", input: Schema.String, output: Schema.Null },
          rollback: {
            scope: "session",
            input: Schema.Null,
            output: Schema.Null,
          },
        },
        handlers: (context) => {
          const { documents } = context;
          handles = documents;
          return {
            read: ({ sessionId }) =>
              Effect.sync(() => ({
                inherited: documents.inherited.read(sessionId).count,
                current: documents.current.read(sessionId).count,
                fresh: documents.fresh.read(sessionId).count,
              })),
            add: ({ sessionId }) =>
              Effect.sync(() => {
                for (const document of [
                  documents.inherited,
                  documents.current,
                  documents.fresh,
                ])
                  document.update(sessionId, (value) => ({
                    count: value.count + 1,
                  }));
                return null;
              }),
            text: ({ sessionId, input }) =>
              Effect.sync(() => {
                documents.text.update(sessionId, () => input);
                return null;
              }),
            rollback: ({ sessionId }) =>
              Effect.sync(() => {
                documents.inherited.update(sessionId, (value) => ({
                  count: value.count + 100,
                }));
              }).pipe(
                Effect.andThen(
                  Effect.fail(
                    new ExtensionCommandError({
                      reason: "rejected",
                      message: "Rollback the document and its entry",
                    }),
                  ),
                ),
              ),
          };
        },
      },
      hooks: {
        onSettled: (context, page) =>
          Effect.sync(() => {
            if (settled) {
              for (const run of page)
                context.documents.inherited.update(run.sessionId, (value) => ({
                  count: value.count + 1,
                }));
            }
          }),
      },
    });
  return {
    extension,
    handles: () => {
      if (handles === undefined) throw new Error("Extension not activated");
      return handles;
    },
  };
}

async function open(path?: string, settled = false, granted = true) {
  const fixture = createNativeSqliteOwnerFixture(path);
  const runtime = createCompatibilityRuntimeHost();
  const entry = documentExtension(settled);
  const agent = await Shiitake.make({
    ...compatibilityRunDependencies(),
    store: fixture.owner,
    runtime: runtime.runtime,
    extensions: {
      roster: [entry.extension],
      grants: [
        {
          id,
          capabilities: granted
            ? ["state", "commands", "settled"]
            : ["commands", "settled"],
        },
      ],
    },
  });
  const sessionId = await agent.createSession("documents");
  const session = agent.session(sessionId);
  await session.command({
    type: "operator",
    author: "party",
    requestId: "start",
    text: "initial transcript",
  });
  return {
    fixture,
    sessionId,
    runtime,
    agent,
    session,
    entry,
    close: async () => {
      await agent.close();
      fixture.close();
    },
  };
}

async function marker(
  state: Awaited<ReturnType<typeof open>>,
  requestId: string,
) {
  await state.session.command({
    type: "operator",
    author: "party",
    requestId,
    text: requestId,
  });
  const entryId = (await state.session.read()).messages.at(-1)?.id;
  if (entryId === undefined) throw new Error("Missing transcript marker");
  return entryId;
}

it("forks all three policies atomically and keeps branches independent", async () => {
  const state = await open();
  try {
    const commands = state.session.extension(id);
    await commands.call("add", null);
    const point = await marker(state, "fork-point");
    await commands.call("add", null);
    await marker(state, "later");
    const fork = await state.session.command({ type: "fork", entryId: point });
    expect(
      await state.agent
        .session(fork.sessionId)
        .extension(id)
        .call("read", null),
    ).toEqual({ inherited: 1, current: 2, fresh: 0 });
    await state.agent.session(fork.sessionId).extension(id).call("add", null);
    expect(await commands.call("read", null)).toEqual({
      inherited: 2,
      current: 2,
      fresh: 2,
    });
    await state.session.command({ type: "branch", entryId: point });
    expect(await commands.call("read", null)).toEqual({
      inherited: 1,
      current: 1,
      fresh: 1,
    });
    const fromBranch = await state.session.command({
      type: "fork",
      entryId: point,
    });
    expect(
      await state.agent
        .session(fromBranch.sessionId)
        .extension(id)
        .call("read", null),
    ).toEqual({ inherited: 1, current: 1, fresh: 0 });
  } finally {
    await state.close();
  }
});

it("copies current state even when the document was first written after the fork point", async () => {
  const state = await open();
  try {
    const point = await marker(state, "before-documents");
    await state.session.extension(id).call("add", null);
    const fork = await state.session.command({ type: "fork", entryId: point });
    expect(
      await state.agent
        .session(fork.sessionId)
        .extension(id)
        .call("read", null),
    ).toEqual({ inherited: 0, current: 1, fresh: 0 });
  } finally {
    await state.close();
  }
});

it("rolls back the document snapshot with a rejected extension transaction", async () => {
  const state = await open();
  try {
    const before = readSelectedPath(state.fixture.owner, {
      sessionId: state.sessionId,
    });
    await expect(
      state.session.extension(id).call("rollback", null),
    ).rejects.toThrow(/Rollback/u);
    expect(
      readSelectedPath(state.fixture.owner, { sessionId: state.sessionId }),
    ).toEqual(before);
    expect(await state.session.extension(id).call("read", null)).toEqual({
      inherited: 0,
      current: 0,
      fresh: 0,
    });
  } finally {
    await state.close();
  }
});

it("reopens durable state and does not redeliver a settled hook across two restarts", async () => {
  const directory = await mkdtemp(join(tmpdir(), "session-documents-"));
  const path = join(directory, "agent.sqlite");
  try {
    const first = await open(path, true);
    try {
      await first.session.send("produce one settled document update", {
        requestId: "once",
      });
      await first.runtime.runNext();
      expect(await first.session.extension(id).call("read", null)).toEqual({
        inherited: 1,
        current: 0,
        fresh: 0,
      });
    } finally {
      await first.close();
    }
    for (let restart = 0; restart < 2; restart += 1) {
      const next = await open(path, true);
      try {
        await next.runtime.runNext();
        expect(await next.session.extension(id).call("read", null)).toEqual({
          inherited: 1,
          current: 0,
          fresh: 0,
        });
      } finally {
        await next.close();
      }
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

it("enforces encoded UTF-8 size and leaves the previous fact unchanged on overflow", async () => {
  const state = await open();
  try {
    await state.session.extension(id).call("text", "é".repeat(7)); // 14 UTF-8 bytes plus two quotes.
    const before = readSelectedPath(state.fixture.owner, {
      sessionId: state.sessionId,
    });
    await expect(
      state.session.extension(id).call("text", "é".repeat(8)),
    ).rejects.toThrow();
    expect(
      readSelectedPath(state.fixture.owner, { sessionId: state.sessionId }),
    ).toEqual(before);
    expect(
      readSessionDocument(state.fixture.owner, state.sessionId, id, "text"),
    ).toBe(JSON.stringify("é".repeat(7)));
  } finally {
    await state.close();
  }
});

it("refuses handles outside their own transaction and without the state grant", async () => {
  const state = await open(undefined, false, false);
  try {
    expect(() => state.entry.handles().inherited.read(state.sessionId)).toThrow(
      /transaction/u,
    );
    await expect(
      state.session.extension(id).call("add", null),
    ).rejects.toThrow();
    expect(
      readSessionDocument(
        state.fixture.owner,
        state.sessionId,
        id,
        "inherited",
      ),
    ).toBeUndefined();
  } finally {
    await state.close();
  }
});

it("validates initial values and size declarations before activation", async () => {
  for (const document of [
    { ...specs.text, maxBytes: 0 },
    { ...specs.text, initial: "too long", maxBytes: 2 },
    { ...specs.text, initial: 1 },
  ]) {
    const entry = defineExtension({ id, version: "1.0.0" })
      .documents({ invalid: document })
      .define({});
    await expect(
      Effect.runPromise(
        admitExtensionRosterV0({
          roster: [entry],
          grants: [],
        }),
      ),
    ).rejects.toThrow();
  }
});

it("includes schema, initial value, fork policy and size bound in the revision", async () => {
  const variants = [
    specs.text,
    {
      ...specs.text,
      schema: Schema.String.pipe(Schema.check(Schema.isMinLength(0))),
    },
    { ...specs.text, initial: "changed" },
    { ...specs.text, fork: "fresh" as const },
    { ...specs.text, maxBytes: 32 },
  ];
  const revisions: string[] = [];
  for (const text of variants) {
    const entry = defineExtension({ id, version: "1.0.0" })
      .documents({ text })
      .define({});
    const [admitted] = await Effect.runPromise(
      admitExtensionRosterV0({
        roster: [entry],
        grants: [],
      }),
    );
    if (admitted === undefined) throw new Error("Missing admitted extension");
    expect(admitted.manifest.requests).toContain("state");
    revisions.push(admitted.revision);
  }
  expect(new Set(revisions).size).toBe(variants.length);
});
