import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { expect, expectTypeOf, it } from "vitest";
import {
  type CommandsV0,
  type EmptyExtensionFieldsV0,
  defineCollection,
  defineExtension,
  ExtensionId,
} from "../src/extensions.js";
import { readDefinition } from "../src/internal/extension-definitions.js";
import { openCore } from "./fixtures/extension-host.fixture.js";
import { grantAll } from "./fixtures/fixture-extensions.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

it("owns honest defaults without requesting undeclared capabilities", () => {
  const identity = { id: ExtensionId.make("typed-defaults"), version: "1.0.0" };
  const bare = defineExtension(identity).define({
    hooks: {
      start: (context) => {
        expectTypeOf(context.config).toEqualTypeOf<EmptyExtensionFieldsV0>();
        expectTypeOf(context.documents).toEqualTypeOf<EmptyExtensionFieldsV0>();
      },
    },
  });
  const definition = readDefinition(bare);
  expect(definition?.config).toEqual({});
  expect(definition?.state).toEqual({
    version: 1,
    collections: {},
    migrations: {},
  });
  expect(bare.manifest.requests).toEqual([]);
  expect(bare.contract.commands).toEqual({});
  expect(
    defineExtension(identity)
      .state({ version: 1, collections: {}, migrations: {} })
      .define({}).manifest.requests,
  ).toEqual(["state"]);
  expect(
    defineExtension(identity).documents({}).define({}).manifest.requests,
  ).toEqual(["state"]);
  expect(
    defineExtension(identity).define({
      commands: { specs: {}, handlers: () => ({}) },
    }).manifest.requests,
  ).toEqual(["commands", "send"]);
});

it("binds reused collection declarations to distinct names with their transformed row types", async () => {
  const row = Schema.Struct({
    key: Schema.String,
    count: Schema.NumberFromString.pipe(Schema.check(Schema.isFinite())),
  });
  const keys = {
    key: (value: typeof row.Type) => value.key,
    index: (value: typeof row.Type) => String(value.count),
  };
  const collection = defineCollection(row, keys);
  keys.key = () => "changed";
  keys.index = () => "changed";
  expect(
    collection.indexFromEncoded({ key: "same", count: "9" }),
  ).toMatchObject({ _tag: "Success", success: "9" });
  expect(
    collection.indexFromEncoded({ key: "same", count: "invalid" })._tag,
  ).toBe("Failure");
  const specs = {
    write: {
      scope: "agent",
      input: Schema.Struct({}),
      output: Schema.Struct({ left: Schema.Number, right: Schema.Number }),
    },
  } satisfies CommandsV0;
  const extension = defineExtension({
    id: ExtensionId.make("typed-collections"),
    version: "1.0.0",
  })
    .config<{ label: "bound" }>({ label: "bound" })
    .state({
      version: 1,
      collections: { left: collection, right: collection },
      migrations: {},
    })
    .define({
      commands: {
        specs,
        handlers: (context) => ({
          write: () =>
            Effect.gen(function* () {
              expectTypeOf(context.config.label).toEqualTypeOf<"bound">();
              yield* context.state.left.put({ key: "same", count: 3 });
              yield* context.state.right.put({ key: "same", count: 7 });
              const left = Option.getOrThrow(
                yield* context.state.left.get("same"),
              );
              const right = Option.getOrThrow(
                yield* context.state.right.get("same"),
              );
              expectTypeOf(left.count).toEqualTypeOf<number>();
              return { left: left.count, right: right.count };
            }),
        }),
      },
    });
  const fixture = createNativeSqliteOwnerFixture();
  const { core } = await openCore(fixture.owner, grantAll([extension]));
  try {
    await expect(
      Effect.runPromise(
        core.agent.extensions.call(extension.manifest.id, "write", {}),
      ),
    ).resolves.toEqual({ left: 3, right: 7 });
  } finally {
    await Effect.runPromise(core.agent.close);
    fixture.close();
  }
});
