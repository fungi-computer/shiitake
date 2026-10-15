import * as Option from "effect/Option";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { expect, it } from "vitest";

import {
  defineExtension,
  ExtensionId,
  parseExtensionJson,
} from "../src/extensions.js";
import { admitExtensionRosterV0 } from "../src/internal/extension-roster.js";
import { openCore } from "./fixtures/extension-host.fixture.js";
import { grantAll } from "./fixtures/fixture-extensions.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

const catalog = {
  protocol: "whistle.catalog/v1",
  commands: [
    {
      id: "hello",
      title: "Say hello",
      category: "Test",
      projections: { slash: { aliases: ["hello"] } },
    },
  ],
};

const helloContract = {
  id: ExtensionId.make("test-hello"),
  commands: {
    hello: {
      scope: "session",
      input: Schema.Struct({}),
      output: Schema.Struct({ type: Schema.Literal("done") }),
    },
  },
  projection: null,
} as const;

function helloExtension(includeCommands = true) {
  const identity = {
    id: helloContract.id,
    version: "1.0.0",
    catalog,
  };
  if (!includeCommands) return defineExtension(identity).define({ catalog });
  return defineExtension(identity).define({
    catalog,
    commands: {
      specs: helloContract.commands,
      handlers: () => ({
        hello: () => Effect.succeed({ type: "done" as const }),
      }),
    },
  });
}

it("advertises a commands-only extension's catalog in every Session slot", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const { core } = await openCore(fixture.owner, grantAll([helloExtension()]));
  try {
    const session = core.agent.session("catalog");
    const snapshot = await Effect.runPromise(session.read());
    expect(snapshot.extensions["test-hello"]).toMatchObject({
      status: "running",
      value: null,
      catalog,
    });
    await expect(
      Effect.runPromise(session.extension("test-hello").call("hello", {})),
    ).resolves.toEqual({ type: "done" });
  } finally {
    await Effect.runPromise(core.agent.close);
    fixture.close();
  }
});

it("parses extension JSON and rejects invalid serialized values", () => {
  expect(parseExtensionJson('{"catalog":[1,true,null]}')).toEqual({
    catalog: [1, true, null],
  });
  expect(() => parseExtensionJson('{"catalog":undefined}')).toThrow();
});

it("refuses a catalog without the commands capability and hashes it into the revision", async () => {
  await expect(
    Effect.runPromise(
      admitExtensionRosterV0(grantAll([helloExtension(false)])),
    ),
  ).rejects.toThrow(/commands capability/u);
  const [admitted] = await Effect.runPromise(
    admitExtensionRosterV0(grantAll([helloExtension()])),
  );
  expect(admitted?.revision).toMatch(/^sha256:/u);
});

it("projects command availability from the owning Session projection", async () => {
  const extension = projectedExtension();
  const fixture = createNativeSqliteOwnerFixture();
  const { core } = await openCore(fixture.owner, grantAll([extension]));
  try {
    const idle = await Effect.runPromise(core.agent.session("idle").read());
    const waiting = await Effect.runPromise(
      core.agent.session("waiting").read(),
    );
    expect(idle.extensions["test-hello"]).toMatchObject({
      status: "running",
      catalog: { commands: [{ availability: { status: "unavailable" } }] },
    });
    expect(waiting.extensions["test-hello"]).toMatchObject({
      status: "running",
      catalog: { commands: [{ availability: { status: "available" } }] },
    });
  } finally {
    await Effect.runPromise(core.agent.close);
    fixture.close();
  }
});

function projectedExtension(fail = false) {
  return defineExtension({ id: helloContract.id, version: "1.0.0" }).define({
    catalog,
    commands: {
      specs: helloContract.commands,
      handlers: () => ({
        hello: () => Effect.succeed({ type: "done" as const }),
      }),
    },
    projection: {
      schema: Schema.Struct({ waiting: Schema.Boolean }),
      read: (_context, sessionId) =>
        Option.some({ waiting: sessionId === "waiting" }),
      catalog: (value) => {
        if (fail) throw new Error("Catalog owner failed");
        return {
          ...catalog,
          commands: [
            {
              ...catalog.commands[0],
              availability: value.waiting
                ? { status: "available" }
                : { status: "unavailable", reason: "No waiting question" },
            },
          ],
        };
      },
    },
  });
}

it("keeps the static catalog when Session projection is not granted", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const roster = [projectedExtension(true)];
  const grants = grantAll(roster);
  const { core } = await openCore(fixture.owner, {
    roster,
    grants: grants.grants.map((grant) => ({
      ...grant,
      capabilities: grant.capabilities.filter(
        (capability) => capability !== "projection",
      ),
    })),
  });
  try {
    const snapshot = await Effect.runPromise(core.agent.session("idle").read());
    expect(snapshot.extensions["test-hello"]).toMatchObject({
      status: "running",
      value: null,
      catalog,
    });
  } finally {
    await Effect.runPromise(core.agent.close);
    fixture.close();
  }
});

it("stops an extension whose projected catalog fails", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const { core } = await openCore(
    fixture.owner,
    grantAll([projectedExtension(true)]),
  );
  try {
    const snapshot = await Effect.runPromise(core.agent.session("idle").read());
    const slot = snapshot.extensions["test-hello"];
    expect(slot?.status).toBe("stopped");
    if (slot?.status !== "stopped")
      throw new Error("Catalog defect did not stop its owner");
    expect(slot.reason).toContain("Catalog owner failed");
  } finally {
    await Effect.runPromise(core.agent.close);
    fixture.close();
  }
});

it("does not project a catalog without the commands grant", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const roster = [projectedExtension(true)];
  const grants = grantAll(roster);
  const { core } = await openCore(fixture.owner, {
    roster,
    grants: grants.grants.map((grant) => ({
      ...grant,
      capabilities: grant.capabilities.filter(
        (capability) => capability !== "commands",
      ),
    })),
  });
  try {
    const snapshot = await Effect.runPromise(
      core.agent.session("waiting").read(),
    );
    expect(snapshot.extensions["test-hello"]).toMatchObject({
      status: "running",
      value: { waiting: true },
    });
    expect(snapshot.extensions["test-hello"]).not.toHaveProperty("catalog");
  } finally {
    await Effect.runPromise(core.agent.close);
    fixture.close();
  }
});

it.each([
  ["function", () => catalog],
  ["Promise", Promise.resolve(catalog)],
])(
  "stops a JavaScript extension returning a non-JSON %s catalog",
  async (_name, value) => {
    const projection = {
      schema: Schema.Struct({ waiting: Schema.Boolean }),
      read: () => Option.some({ waiting: true }),
      catalog: () => catalog,
    };
    // JavaScript extension authors can return values that the TypeScript contract excludes.
    Reflect.set(projection, "catalog", () => value);
    const extension = defineExtension({
      id: helloContract.id,
      version: "1.0.0",
    }).define({
      catalog,
      projection,
      commands: {
        specs: helloContract.commands,
        handlers: () => ({
          hello: () => Effect.succeed({ type: "done" as const }),
        }),
      },
    });
    const fixture = createNativeSqliteOwnerFixture();
    const { core } = await openCore(fixture.owner, grantAll([extension]));
    try {
      const snapshot = await Effect.runPromise(
        core.agent.session("invalid-catalog").read(),
      );
      const slot = snapshot.extensions["test-hello"];
      expect(slot?.status).toBe("stopped");
      if (slot?.status !== "stopped")
        throw new Error("Non-JSON catalog did not stop its owner");
      expect(slot.reason).toContain("project defect");
    } finally {
      await Effect.runPromise(core.agent.close);
      fixture.close();
    }
  },
);

it("requires a static advertised catalog before projecting command availability", () => {
  expect(() =>
    defineExtension({ id: helloContract.id, version: "1.0.0" }).define({
      projection: {
        schema: Schema.Struct({ waiting: Schema.Boolean }),
        read: () => Option.some({ waiting: true }),
        catalog: () => catalog,
      },
    }),
  ).toThrow("A projected catalog requires an advertised catalog");
});
