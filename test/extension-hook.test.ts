import * as Cause from "effect/Cause";
import * as Result from "effect/Result";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { expect, expectTypeOf, it } from "vitest";

import { Shiitake } from "../src/index.js";
import {
  defineCollection,
  defineExtension,
  ExtensionId,
  type ExtensionContextV0,
  type CommandsV0,
  type CommandHandlersV0,
} from "../src/extensions.js";
import {
  ExtensionHookError,
  invokeExtensionHook,
  invokeSyncHandler,
  type StateUpdate,
} from "../src/internal/extension-hook.js";
import { readRecord } from "../src/internal/woodstock/index.js";
import {
  compatibilityRunDependencies,
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

it("lifts a synchronous value, a Promise and an Effect without wrapping the Effect", async () => {
  expect(await Effect.runPromise(invokeExtensionHook(() => 17))).toBe(17);
  expect(
    await Effect.runPromise(
      invokeExtensionHook(() => Promise.resolve("ready")),
    ),
  ).toBe("ready");
  expect(
    await Effect.runPromise(
      invokeExtensionHook(() => Effect.succeed("effect")),
    ),
  ).toBe("effect");
});

it("reports a rejected Promise as an ExtensionHookError with its cause", async () => {
  const failure = new Error("unavailable");
  const exit = await Effect.runPromiseExit(
    invokeExtensionHook(() => Promise.reject(failure)),
  );
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isFailure(exit)) {
    const error = Cause.findErrorOption(exit.cause);
    expect(error._tag).toBe("Some");
    if (error._tag === "Some")
      expect(error.value).toEqual(new ExtensionHookError({ cause: failure }));
  }
});

it("defers a synchronous throw into the Effect defect channel", async () => {
  const failure = new Error("thrown");
  let called = false;
  const hook = invokeExtensionHook((): number => {
    called = true;
    throw failure;
  });
  expect(called).toBe(false);
  const exit = await Effect.runPromiseExit(hook);
  expect(called).toBe(true);
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isFailure(exit))
    expect(Result.getSuccess(Cause.findDefect(exit.cause))).toMatchObject({
      _tag: "Some",
      value: failure,
    });
});

it("rejects a Promise or Effect from a transactional callback", () => {
  const tx = { count: 0 };
  expect(
    invokeSyncHandler((state) => {
      state.count += 1;
      return state.count;
    }, tx),
  ).toBe(1);
  expect(() =>
    invokeSyncHandler<{ count: number }, unknown>(
      // SAFETY: this emulates an untyped runtime extension violating the public contract.
      () => Promise.resolve(2),
      tx,
    ),
  ).toThrow("must return synchronously");
  expect(() =>
    invokeSyncHandler<{ count: number }, unknown>(() => Effect.succeed(2), tx),
  ).toThrow("must return synchronously");
});

it("rejects an async callback before the owner transaction can commit", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    fixture.owner.sql.exec(
      "CREATE TABLE sync_update_probe (value INTEGER NOT NULL)",
    );
    expect(() =>
      fixture.owner.transactionSync(() => {
        fixture.owner.sql.exec(
          "INSERT INTO sync_update_probe (value) VALUES (1)",
        );
        return invokeSyncHandler<undefined, unknown>(
          () => Promise.resolve(2),
          undefined,
        );
      }),
    ).toThrow("must return synchronously");
    expect(
      fixture.owner.sql
        .exec("SELECT COUNT(*) AS count FROM sync_update_probe")
        .one().count,
    ).toBe(0);
  } finally {
    fixture.close();
  }
});

it("commits schema-typed state.update synchronously and rolls back a Promise", async () => {
  const Row = Schema.Struct({ id: Schema.String, count: Schema.Number });
  const collections = { rows: defineCollection(Row, { key: (row) => row.id }) };
  const id = ExtensionId.make("sync-state-update");
  let context:
    ExtensionContextV0<typeof collections, Record<string, never>> | undefined;
  const extension = defineExtension({ id, version: "1.0.0" })
    .config({})
    .state({ version: 1, collections, migrations: {} })
    .define({
      hooks: {
        start: (ctx) =>
          Effect.sync(() => {
            context = ctx;
          }),
      },
    });
  const fixture = createNativeSqliteOwnerFixture();
  const agent = await Shiitake.make({
    ...compatibilityRunDependencies(),
    store: fixture.owner,
    runtime: createCompatibilityRuntimeHost().runtime,
    extensions: {
      roster: [extension],
      grants: [{ id, capabilities: ["state"] }],
    },
  });
  try {
    if (context === undefined) throw new Error("Extension did not activate");
    const state = context.state;
    expect(
      state.update((tx) => {
        tx.rows.put({ id: "row", count: 1 });
        tx.observe("update-session");
        return Option.getOrUndefined(tx.rows.get("row"))?.count;
      }),
    ).toBe(1);
    expect(() =>
      state.update((tx) => {
        tx.rows.put({ id: "row", count: 2 });
        // An untyped outside extension can still violate the public type.
        const result: unknown = Promise.resolve(2);
        return result;
      }),
    ).toThrow(
      expect.objectContaining({
        _tag: "ExtensionStop",
        reason: "Extension transaction handler must return synchronously",
      }),
    );
    expect(
      Schema.decodeUnknownSync(Schema.fromJsonString(Row))(
        readRecord(fixture.owner, id, "rows", "row"),
      ).count,
    ).toBe(1);
  } finally {
    await agent.close();
    fixture.close();
  }
});

it("stops a command that returns a Promise before its transaction commits", async () => {
  const id = ExtensionId.make("untyped-promise-command");
  const extension = defineExtension({ id, version: "1.0.0" }).define({
    commands: {
      specs: {
        ping: {
          scope: "agent",
          input: Schema.Struct({}),
          output: Schema.Struct({}),
        },
      },
      handlers: () => ({
        ping: () => {
          // An untyped external handler may return a Promise despite the API.
          const result: object = Promise.resolve({});
          return result;
        },
      }),
    },
  });
  const fixture = createNativeSqliteOwnerFixture();
  const agent = await Shiitake.make({
    ...compatibilityRunDependencies(),
    store: fixture.owner,
    runtime: createCompatibilityRuntimeHost().runtime,
    extensions: {
      roster: [extension],
      grants: [{ id, capabilities: ["commands"] }],
    },
  });
  try {
    await expect(agent.extensions.call(id, "ping", {})).rejects.toMatchObject({
      boundary: "extensions",
      cause: "unavailable",
    });
    const status = await agent.extensions.list();
    expect(status).toHaveLength(1);
    expect(status[0]?.id).toBe(id);
    expect(status[0]?.status).toBe("stopped");
    expect(status[0]?.reason).toContain("must return synchronously");
  } finally {
    await agent.close();
    fixture.close();
  }
});

// Type-level proof of the public state.update callback, including TS's usual
// allowance for value-returning functions assigned to a void callback.
function checkUpdateTypes(update: StateUpdate<{ count: number }>) {
  update((state) => state.count + 1);
  const promiseUpdate = update<Promise<number>>;
  const effectUpdate = update<Effect.Effect<number>>;
  expectTypeOf(promiseUpdate).parameter(0).returns.toEqualTypeOf<never>();
  expectTypeOf(effectUpdate).parameter(0).returns.toEqualTypeOf<never>();
}

it("has a type-checked synchronous state update contract", () => {
  expect(checkUpdateTypes).toBeDefined();
});

function checkTransactionalDefinitionTypes() {
  const id = ExtensionId.make("sync-handler-type-proof");
  const commands = {
    ping: {
      scope: "agent",
      input: Schema.Struct({}),
      output: Schema.Struct({ accepted: Schema.Boolean }),
    },
  } satisfies CommandsV0;
  const builder = defineExtension({ id, version: "1.0.0" });
  type Definition = Parameters<typeof builder.define>[0];
  type Settled = NonNullable<NonNullable<Definition["hooks"]>["onSettled"]>;
  type Ping = CommandHandlersV0<typeof commands>["ping"];
  expectTypeOf<() => Promise<void>>().not.toExtend<Settled>();
  expectTypeOf<
    () => Promise<typeof commands.ping.output.Type>
  >().not.toExtend<Ping>();
  builder.define({
    hooks: { onSettled: () => Effect.void },
    commands: {
      specs: commands,
      handlers: () => ({ ping: () => Effect.succeed({ accepted: true }) }),
    },
  });
}

it("checks transactional handler types without running them", () => {
  expect(checkTransactionalDefinitionTypes).toBeDefined();
});
