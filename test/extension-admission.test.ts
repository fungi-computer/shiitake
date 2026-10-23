import * as Effect from "effect/Effect";
import { expect, it, vi } from "vitest";

import {
  defineExtension,
  ExtensionActivationError,
  ExtensionId,
  type ExtensionCapabilityV0,
  type ExtensionRosterV0,
  type RosteredExtensionV0,
} from "../src/extensions.js";
import { Shiitake } from "../src/index.js";
import {
  compatibilityRunDependencies,
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

type Probe = { activated: string[]; released: string[] };

it("releases a failed activation's resources without closing unchanged members", async () => {
  const probe: Probe = { activated: [], released: [] };
  const failed = defineExtension({
    id: ExtensionId.make("failed-start"),
    version: "1.0.0",
  }).define({
    hooks: {
      start: () =>
        Effect.acquireRelease(
          Effect.sync(() => probe.activated.push("failed-start")),
          () => Effect.sync(() => probe.released.push("failed-start")),
        ).pipe(
          Effect.andThen(
            Effect.fail(
              new ExtensionActivationError({ message: "startup refused" }),
            ),
          ),
        ),
    },
  });
  const { fixture, outcome } = await make({
    roster: [probeExtension("unchanged", probe), failed],
    grants: [],
  });
  try {
    if (outcome.status !== "fulfilled") throw outcome.reason;
    expect(probe.released).toEqual(["failed-start"]);
    expect(await outcome.agent.extensions.list()).toMatchObject([
      { id: "failed-start", status: "stopped" },
      { id: "unchanged", status: "running" },
    ]);
    await outcome.agent.close();
    expect(probe.released).toEqual(["failed-start", "unchanged"]);
  } finally {
    if (outcome.status === "fulfilled") await outcome.agent.close();
    fixture.close();
  }
});

/** A minimal extension that records its activation and scope release. */
function probeExtension(
  id: string,
  probe: Probe,
  requests: readonly ExtensionCapabilityV0[] = [],
  activate: "succeed" | "throw" = "succeed",
): RosteredExtensionV0 {
  const builder = defineExtension({
    id: ExtensionId.make(id),
    version: "1.0.0",
  });
  const definition = {
    hooks: {
      start: () => {
        if (activate === "throw") throw new Error(`${id} threw synchronously`);
        return Effect.acquireRelease(
          Effect.sync(() => probe.activated.push(id)),
          () => Effect.sync(() => probe.released.push(id)),
        ).pipe(Effect.asVoid);
      },
    },
  };
  const builderWithState = requests.includes("state")
    ? builder.state({ version: 1, migrations: {}, collections: {} })
    : builder;
  if (requests.includes("compaction_handoff"))
    return builderWithState.define({
      ...definition,
      compaction: () => ({
        consume: () => Effect.succeed({ type: "discarded_by_policy" as const }),
      }),
    });
  return builderWithState.define(definition);
}

async function make(extensions: ExtensionRosterV0) {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  let outcome:
    | Readonly<{
        status: "fulfilled";
        agent: Awaited<ReturnType<typeof Shiitake.make>>;
      }>
    | Readonly<{ status: "rejected"; reason: unknown }>;
  try {
    outcome = {
      status: "fulfilled",
      agent: await Shiitake.make({
        ...compatibilityRunDependencies(),
        store: fixture.owner,
        runtime: runtime.runtime,
        extensions,
      }),
    };
  } catch (reason) {
    outcome = { status: "rejected", reason };
  }
  return { fixture, runtime, outcome };
}

it.each([
  [
    "a sixty-fifth roster entry",
    (probe: Probe): ExtensionRosterV0 => ({
      roster: Array.from({ length: 65 }, (_, index) =>
        probeExtension(`bound-${String(index).padStart(2, "0")}`, probe),
      ),
      grants: [],
    }),
    "invalid",
  ],
  [
    "a grant for an unrostered extension",
    (probe: Probe): ExtensionRosterV0 => ({
      roster: [probeExtension("alpha", probe)],
      grants: [{ id: ExtensionId.make("beta"), capabilities: [] }],
    }),
    "invalid",
  ],
  [
    "a grant of an unrequested capability",
    (probe: Probe): ExtensionRosterV0 => ({
      roster: [probeExtension("alpha", probe, ["state"])],
      grants: [{ id: ExtensionId.make("alpha"), capabilities: ["send"] }],
    }),
    "invalid",
  ],
  [
    "the retired admit capability",
    (probe: Probe): ExtensionRosterV0 => ({
      roster: [probeExtension("alpha", probe)],
      grants: [
        {
          id: ExtensionId.make("alpha"),
          // SAFETY: Deliberately bypass the authoring type to test rejection at the decoded roster boundary.
          capabilities: ["admit" as ExtensionCapabilityV0],
        },
      ],
    }),
    "invalid",
  ],
  [
    "two grant rows for one extension",
    (probe: Probe): ExtensionRosterV0 => ({
      roster: [probeExtension("alpha", probe, ["state"])],
      grants: [
        { id: ExtensionId.make("alpha"), capabilities: ["state"] },
        { id: ExtensionId.make("alpha"), capabilities: [] },
      ],
    }),
    "invalid",
  ],
  [
    "two extensions with one identity",
    (probe: Probe): ExtensionRosterV0 => ({
      roster: [probeExtension("alpha", probe), probeExtension("alpha", probe)],
      grants: [],
    }),
    "conflict",
  ],
  [
    "two compaction handoff grants",
    (probe: Probe): ExtensionRosterV0 => ({
      roster: [
        probeExtension("alpha", probe, ["compaction_handoff"]),
        probeExtension("beta", probe, ["compaction_handoff"]),
      ],
      grants: [
        {
          id: ExtensionId.make("alpha"),
          capabilities: ["compaction_handoff"] as const,
        },
        {
          id: ExtensionId.make("beta"),
          capabilities: ["compaction_handoff"] as const,
        },
      ],
    }),
    "conflict",
  ],
] as const)(
  "rejects %s before any activation or owner write",
  async (_case, roster, cause) => {
    const probe: Probe = { activated: [], released: [] };
    const { fixture, runtime, outcome } = await make(roster(probe));
    try {
      expect(outcome).toMatchObject({
        status: "rejected",
        reason: { boundary: "extensions", cause },
      });
      expect(probe.activated).toEqual([]);
      expect(runtime.snapshot().binds).toBe(0);
      expect(fixture.ownerWrites()).toBe(0);
    } finally {
      if (outcome.status === "fulfilled") await outcome.agent.close();
      fixture.close();
    }
  },
);

it("refuses a v1 host manifest before activation or store writes", async () => {
  const probe: Probe = { activated: [], released: [] };
  const id = ExtensionId.make("old-protocol");
  const entry = defineExtension({ id, version: "1.0.0" }).define({
    hooks: {
      start: () =>
        Effect.sync(() => {
          probe.activated.push("old-protocol");
        }),
    },
  });
  // Simulate malformed installed code after definition but before host admission.
  Object.assign(entry.manifest, { protocol: "shiitake.extension/v1" });
  const { fixture, runtime, outcome } = await make({
    roster: [entry],
    grants: [],
  });
  try {
    expect(outcome).toMatchObject({
      status: "rejected",
      reason: { boundary: "extensions", cause: "invalid" },
    });
    expect(probe.activated).toEqual([]);
    expect(runtime.snapshot().binds).toBe(0);
    expect(fixture.ownerWrites()).toBe(0);
  } finally {
    if (outcome.status === "fulfilled") await outcome.agent.close();
    fixture.close();
  }
});

it("admits sixty-four extensions, activates them in ASCII order and releases each once on close", async () => {
  const probe: Probe = { activated: [], released: [] };
  const ids = Array.from(
    { length: 64 },
    (_, index) => `ext-${String(63 - index).padStart(2, "0")}`,
  );
  const { fixture, outcome } = await make({
    roster: ids.map((id) => probeExtension(id, probe)),
    grants: ids.map((id) => ({ id: ExtensionId.make(id), capabilities: [] })),
  });
  try {
    if (outcome.status !== "fulfilled") throw outcome.reason;
    expect(probe.activated).toEqual([...ids].sort());
    await Promise.all([outcome.agent.close(), outcome.agent.close()]);
    expect([...probe.released].sort()).toEqual([...ids].sort());
  } finally {
    fixture.close();
  }
});

it("stops only the extension whose activation throws synchronously", async () => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  const probe: Probe = { activated: [], released: [] };
  const { fixture, outcome } = await make({
    roster: [
      probeExtension("bravo", probe, [], "throw"),
      probeExtension("alpha", probe),
    ],
    grants: [],
  });
  try {
    if (outcome.status !== "fulfilled") throw outcome.reason;
    const listed = await outcome.agent.extensions.list();
    expect(listed.map(({ id, status }) => [id, status])).toEqual([
      ["alpha", "running"],
      ["bravo", "stopped"],
    ]);
    expect(listed[1]?.reason).toContain("bravo threw synchronously");
    expect(probe.activated).toEqual(["alpha"]);
    await outcome.agent.close();
    expect(probe.released).toEqual(["alpha"]);
  } finally {
    vi.restoreAllMocks();
    fixture.close();
  }
});
