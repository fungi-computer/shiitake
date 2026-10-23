import { fauxModel } from "@fungi.computer/mule/testing";
import {
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { Knapsack, type FilesPort } from "@fungi.computer/knapsack";
import { rejectingSandbox } from "@fungi.computer/mycelium/testing";
import { expect, it, vi } from "vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  defineCollection,
  defineExtension,
  ExtensionId,
  ExtensionActivationError,
  type RosteredExtensionV0,
} from "../src/extensions.js";
import { Shiitake, type DocumentSpecV0 } from "../src/index.js";
import {
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
  compatibilityRunDependencies,
} from "./fixtures/native-sqlite-owner.fixture.js";

it("diffs lifecycle members and refuses failed additions and same-ID replacements atomically", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const started: string[] = [];
  const closed: string[] = [];
  const fired: string[] = [];
  const member = (id: string, version = "1.0.0", fail = false) =>
    defineExtension({ id: ExtensionId.make(id), version }).define({
      hooks: {
        start: () =>
          Effect.gen(function* () {
            yield* Effect.acquireRelease(
              Effect.sync(() => {
                started.push(id);
              }),
              () =>
                Effect.sync(() => {
                  closed.push(id);
                }),
            );
            if (fail)
              yield* Effect.fail(
                new ExtensionActivationError({
                  message: "probe startup failed",
                }),
              );
          }),
        onTimer: () => {
          fired.push(id);
        },
      },
      commands: {
        specs: {
          arm: {
            scope: "session",
            input: Schema.Struct({}),
            output: Schema.Null,
          },
          ping: {
            scope: "session",
            input: Schema.Struct({}),
            output: Schema.String,
          },
        },
        handlers: (context) => ({
          arm: () =>
            context.timers
              .set("probe", Date.now() + 60_000)
              .pipe(Effect.as(null)),
          ping: () => Effect.succeed(id),
        }),
      },
    });
  const unchanged = member("unchanged");
  const removed = member("removed");
  const roster = (entries: readonly RosteredExtensionV0[]) => ({
    extensions: {
      roster: entries,
      grants: entries.map((entry) => ({
        id: entry.manifest.id,
        capabilities: entry.manifest.requests,
      })),
    },
  });
  const agent = await Shiitake.make({
    ...compatibilityRunDependencies(),
    store: fixture.owner,
    runtime: runtime.runtime,
    ...roster([unchanged, removed]),
  });
  try {
    const session = agent.session("lifecycle");
    await session.extension("removed").call("arm", {});
    const handoff = (id: string) =>
      defineExtension({
        id: ExtensionId.make(id),
        version: "1.0.0",
      }).define({
        compaction: () => ({
          consume: (): Effect.Effect<{ type: "discarded_by_policy" }> =>
            Effect.succeed({ type: "discarded_by_policy" }),
        }),
      });
    await expect(
      agent.prepareConfiguration(
        roster([unchanged, handoff("handoff-a"), handoff("handoff-b")]),
      ),
    ).rejects.toThrow("Compaction handoff slot is already owned");
    expect([...started].sort()).toEqual(["removed", "unchanged"]);
    await expect(
      agent.prepareConfiguration(
        roster([member("unchanged", "2.0.0"), removed]),
      ),
    ).rejects.toMatchObject({
      message:
        "Extensions unchanged: this change needs a restart: run fungi stop, then reopen fungi",
    });
    await expect(
      agent.prepareConfiguration(
        roster([unchanged, member("failed", "1.0.0", true)]),
      ),
    ).rejects.toThrow(/probe startup failed/u);
    expect(await session.extension("removed").call("ping", {})).toBe("removed");
    expect(closed).toEqual(["failed"]);
    const prepared = await agent.prepareConfiguration(
      roster([unchanged, member("added")]),
    );
    prepared.commit();
    expect(await session.extension("removed").call("ping", {})).toBe("removed");
    await session.send("Apply the next turn.", { requestId: "next" });
    await runtime.runNext();
    await expect(
      session.extension("removed").call("ping", {}),
    ).rejects.toThrow();
    expect(await session.extension("added").call("ping", {})).toBe("added");
    expect(started.filter((id) => id === "unchanged")).toHaveLength(1);
    expect(closed).toEqual(["failed", "removed"]);
    expect(fired).toEqual([]);
    const clock = vi.spyOn(Date, "now").mockReturnValue(Date.now() + 120_000);
    try {
      const readded = await agent.prepareConfiguration(
        roster([unchanged, removed]),
      );
      readded.commit();
      await session.send("A later turn.", { requestId: "later" });
      await runtime.runNext();
      // Removing the holder releases its stored timer too: reenabling it
      // must not resurrect a timer that belonged to the disposed instance.
      expect(fired).toEqual([]);
      expect(started.filter((id) => id === "unchanged")).toHaveLength(1);
    } finally {
      clock.mockRestore();
    }
  } finally {
    await agent.close();
    fixture.close();
  }
});

it("rolls back core preparation when a document declaration fails after migration", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const id = ExtensionId.make("core-preparation");
  const row = Schema.Struct({ key: Schema.String, value: Schema.Number });
  const collections = {
    values: defineCollection(row, { key: (value) => value.key }),
  };
  const document = (value: number): DocumentSpecV0<number> => ({
    schema: Schema.Number,
    initial: value,
    fork: "current",
    maxBytes: 64,
  });
  const old = defineExtension({ id, version: "1.0.0" })
    .state({ version: 1, collections, migrations: {} })
    .documents({ first: document(1), second: document(1) })
    .define({
      commands: {
        specs: {
          seed: {
            scope: "agent",
            input: Schema.Struct({}),
            output: Schema.Null,
          },
        },
        handlers: (context) => ({
          seed: () =>
            context.state.values
              .put({ key: "saved", value: 1 })
              .pipe(Effect.as(null)),
        }),
      },
    });
  const candidate = defineExtension({ id, version: "2.0.0" })
    .state({
      version: 2,
      collections,
      migrations: {
        1: (stored) => [
          {
            ...stored,
            value: { key: stored.key, value: 2 },
          },
        ],
      },
    })
    .documents({ first: document(2), second: document(2) })
    .define({});
  let failDeclaration = false;
  let declarationFailures = 0;
  const agent = await Shiitake.make({
    ...compatibilityRunDependencies(),
    runtime: runtime.runtime,
    store: {
      transactionSync: fixture.owner.transactionSync,
      sql: {
        exec: (statement, ...bindings) => {
          if (
            failDeclaration &&
            statement.includes("INSERT INTO session_document_specs") &&
            bindings[0] === id &&
            bindings[1] === "second"
          ) {
            declarationFailures += 1;
            throw new Error("fixture document declaration unavailable");
          }
          return fixture.owner.sql.exec(statement, ...bindings);
        },
      },
    },
    extensions: {
      roster: [old],
      grants: [{ id, capabilities: old.manifest.requests }],
    },
  });
  try {
    await agent.extensions.call(id, "seed", {});
    const removed = await agent.prepareConfiguration({
      extensions: { roster: [], grants: [] },
    });
    removed.commit();
    const session = agent.session("core-preparation");
    await session.send("Remove the live extension.", { requestId: "remove" });
    await runtime.runNext();
    expect(
      (await session.read()).terminals.map(({ outcome }) => outcome),
    ).toEqual(["completed"]);
    expect(await agent.extensions.list()).toEqual([]);
    const snapshot = () => ({
      ledger: fixture.owner.sql
        .exec(
          "SELECT * FROM shiitake_extension_state WHERE extension_id = ?",
          id,
        )
        .toArray(),
      records: fixture.owner.sql
        .exec(
          "SELECT * FROM shiitake_extension_records WHERE extension_id = ? ORDER BY collection, key",
          id,
        )
        .toArray(),
      declarations: fixture.owner.sql
        .exec(
          "SELECT * FROM session_document_specs WHERE extension_id = ? ORDER BY name",
          id,
        )
        .toArray(),
    });
    const before = snapshot();
    expect(before.ledger).toMatchObject([{ state_version: 1 }]);
    expect(before.records).toHaveLength(1);
    expect(before.declarations).toHaveLength(2);
    failDeclaration = true;
    await expect(
      agent.prepareConfiguration({
        extensions: {
          roster: [candidate],
          grants: [{ id, capabilities: candidate.manifest.requests }],
        },
      }),
    ).rejects.toThrow("fixture document declaration unavailable");
    expect(declarationFailures).toBe(1);
    expect(snapshot()).toEqual(before);
  } finally {
    failDeclaration = false;
    await agent.close();
    fixture.close();
  }
});

it("keeps the old live roster and timers when publication storage fails", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const closed: string[] = [];
  let failAttention = false;
  const old = defineExtension({
    id: ExtensionId.make("publication-old"),
    version: "1.0.0",
  }).define({
    attention: true,
    hooks: {
      start: () =>
        Effect.acquireRelease(Effect.void, () =>
          Effect.sync(() => {
            closed.push("publication-old");
          }),
        ),
      onTimer: () => undefined,
    },
    commands: {
      specs: {
        arm: {
          scope: "session",
          input: Schema.Struct({}),
          output: Schema.Null,
        },
      },
      handlers: (context) => ({
        arm: () =>
          context.timers
            .set("retained", Date.now() + 60_000)
            .pipe(Effect.as(null)),
      }),
    },
  });
  const agent = await Shiitake.make({
    ...compatibilityRunDependencies(),
    runtime: runtime.runtime,
    store: {
      transactionSync: fixture.owner.transactionSync,
      sql: {
        exec: (statement, ...bindings) => {
          if (
            failAttention &&
            statement.includes("session_attention_grants") &&
            !statement.trimStart().startsWith("SELECT")
          )
            throw new Error("fixture attention write unavailable");
          return fixture.owner.sql.exec(statement, ...bindings);
        },
      },
    },
    extensions: {
      roster: [old],
      grants: [{ id: old.manifest.id, capabilities: old.manifest.requests }],
    },
  });
  try {
    const session = agent.session("publication");
    await session.extension(old.manifest.id).call("arm", {});
    const timers = () =>
      fixture.owner.sql
        .exec("SELECT * FROM shiitake_extension_timers")
        .toArray();
    const before = timers();
    expect(before).toHaveLength(1);
    const prepared = await agent.prepareConfiguration({
      extensions: { roster: [], grants: [] },
    });
    prepared.commit();
    failAttention = true;
    await session.send("Apply the prepared roster.", {
      requestId: "publication",
    });
    await runtime.runNext();
    expect(
      (await session.read()).terminals.map(({ outcome }) => outcome),
    ).toEqual(["failed"]);
    expect(closed).toEqual([]);
    expect(timers()).toEqual(before);
    expect((await agent.extensions.list()).map(({ id }) => id)).toEqual([
      old.manifest.id,
    ]);
  } finally {
    failAttention = false;
    await agent.close();
    fixture.close();
  }
});

it("captures a reloaded roster at the next turn, never during an in-flight turn", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const faux = fauxModel([]);
  const reads: string[] = [];
  const configuration = (name: string) => {
    const port: FilesPort = {
      read: ({ path }) => {
        reads.push(name);
        return Promise.resolve({
          path,
          content: name,
          offset: 0,
          nextOffset: null,
          truncated: false,
        });
      },
      list: ({ path }) =>
        Promise.resolve({ path, results: [], truncated: null }),
      glob: ({ path }) =>
        Promise.resolve({ path, results: [], truncated: null }),
      grep: ({ path }) =>
        Promise.resolve({ path, results: [], truncated: null }),
      replace: ({ path }) => Promise.resolve({ path, bytes: 0 }),
    };
    const member = defineExtension({
      id: ExtensionId.make(`files-${name}`),
      version: "1.0.0",
    }).define({ files: () => Knapsack.files({ port }).tools });
    return {
      extensions: {
        roster: [member],
        grants: [
          {
            id: member.manifest.id,
            capabilities: member.manifest.requests,
          },
        ],
      },
      tools: { sandbox: rejectingSandbox("Unexpected sandbox use") },
    };
  };
  let current = configuration("old");
  const read = () =>
    fauxAssistantMessage(fauxToolCall("read", { path: "/test" }), {
      stopReason: "toolUse",
    });
  faux.setResponses([
    read(),
    async () => {
      current = configuration("new");
      const prepared = await agent.prepareConfiguration(current);
      prepared.commit();
      return read();
    },
    fauxAssistantMessage("first complete"),
    read(),
    fauxAssistantMessage("second complete"),
  ]);
  const options = {
    store: fixture.owner,
    runtime: runtime.runtime,
    ...current,
    instructions: {
      load: () => Promise.resolve({ revision: "1", text: "Read the file." }),
    },
    model: {
      acquire: () =>
        Promise.resolve({
          model: faux.model,
          retryable: () => false,
          stream: faux.stream,
          release: () => Promise.resolve(),
        }),
    },
  };
  const agent = await Shiitake.make(options);
  try {
    const session = agent.session("reload");
    const conflicting = configuration("conflict");
    await expect(
      agent.prepareConfiguration({
        extensions: {
          roster: [
            ...current.extensions.roster,
            ...conflicting.extensions.roster,
          ],
          grants: [
            ...current.extensions.grants,
            ...conflicting.extensions.grants,
          ],
        },
      }),
    ).rejects.toThrow(/Files/u);
    await session.send("Read twice.", { requestId: "first" });
    await runtime.runNext();
    expect(reads).toEqual(["old", "old"]);
    await session.send("Read once.", { requestId: "second" });
    await runtime.runNext();
    expect(reads).toEqual(["old", "old", "new"]);
    expect(
      (await session.read()).terminals.map(({ outcome }) => outcome),
    ).toEqual(["completed", "completed"]);
  } finally {
    await agent.close();
    fixture.close();
  }
});
