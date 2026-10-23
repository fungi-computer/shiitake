import { createMemoryOutbox } from "@fungi.computer/carrier/testing";
import type { BirdDogMail } from "@fungi.computer/bird-dog";
import { make as makeBirdDog } from "@fungi.computer/bird-dog/effect";
import {
  Knapsack,
  type FilesPort,
  type FilesSurface,
} from "@fungi.computer/knapsack";
import * as Effect from "effect/Effect";
import type * as Context from "effect/Context";
import * as Cause from "effect/Cause";
import * as Option from "effect/Option";
import * as Exit from "effect/Exit";
import * as References from "effect/References";
import * as Logger from "effect/Logger";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import { inspect } from "node:util";
import { afterEach, expect, it, vi } from "vitest";

import {
  defineCollection,
  defineExtension,
  ExtensionId,
  ExtensionStop,
  type ExtensionSlotV0,
} from "../src/extensions.js";
import {
  admitExtensionRosterV0,
  coreProtocolDigest,
} from "../src/internal/extension-roster.js";
import type { SqliteCursor, SqliteOwner, SqliteValue } from "../src/index.js";
import { readSessionEntriesForWork } from "../src/internal/woodstock/index.js";
import { openCore } from "./fixtures/extension-host.fixture.js";
import {
  crasherExtension,
  grantAll,
  notesExtension,
  recorderContract,
  recorderExtension,
  toolExtension,
  type CrashSwitch,
} from "./fixtures/fixture-extensions.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

afterEach(() => {
  vi.restoreAllMocks();
});

// Core logs through Effect.logError, never console.*; with no host Logger
// provided, Effect's own default logger writes formatted lines through
// console.log. Spying there keeps these intentional-crash tests quiet and
// still lets them assert the logged event by substring.
function quietStops() {
  return vi.spyOn(console, "log").mockImplementation(() => undefined);
}

type CapturedLog = Readonly<{
  message: string;
  annotations: Context.Service.Shape<typeof References.CurrentLogAnnotations>;
}>;

/** A host-shaped test Logger: captures entries instead of writing them anywhere. */
function capturingLogger() {
  const entries: CapturedLog[] = [];
  const logger = Logger.make<unknown, undefined>(({ message, fiber }) => {
    // Effect.log* takes variadic message parts; one string part is a single-
    // element array here, so join to match the plain string every call site
    // in this package logs with.
    entries.push({
      message: Array.isArray(message) ? message.join(" ") : String(message),
      annotations: fiber.getRef(References.CurrentLogAnnotations),
    });
  });
  return { entries, layer: Logger.layer([logger]) };
}

/**
 * Wrap one owner so the single durable write that records a *stopped*
 * status fails, while every other write (including the "running" status
 * write construction performs) still lands normally.
 */
function withFailingStoppedWrite(owner: SqliteOwner): SqliteOwner {
  return {
    transactionSync: owner.transactionSync,
    sql: {
      exec: (statement: string, ...bindings: SqliteValue[]): SqliteCursor => {
        if (
          statement.includes("INSERT INTO shiitake_extension_status") &&
          bindings[1] === "stopped"
        )
          throw new Error("disk full");
        return owner.sql.exec(statement, ...bindings);
      },
    },
  };
}

function slot(
  extensions: Readonly<Record<string, ExtensionSlotV0>>,
  id: string,
): ExtensionSlotV0 | undefined {
  return extensions[id];
}

it("stops only the extension whose hook dies, with a visible reason, and resume replays its backlog once", async () => {
  const errors = quietStops();
  const fixture = createNativeSqliteOwnerFixture();
  const control: CrashSwitch = { failing: true, delivered: [] };
  const { core, host } = await openCore(
    fixture.owner,
    grantAll([recorderExtension(), crasherExtension(control)]),
  );
  try {
    const session = core.agent.session("isolation");
    await Effect.runPromise(session.send("first", { requestId: "first" }));
    await host.drain();

    const listed = await Effect.runPromise(core.agent.extensions.list);
    expect(listed.map(({ id, status }) => [id, status])).toEqual([
      ["test-crasher", "stopped"],
      ["test-recorder", "running"],
    ]);
    expect(listed[0]?.reason).toContain("fixture onSettled crashed");
    const snapshot = await Effect.runPromise(session.read());
    const crasherSlot = slot(snapshot.extensions, "test-crasher");
    expect(crasherSlot?.status).toBe("stopped");
    expect(crasherSlot?.status === "stopped" && crasherSlot.reason).toContain(
      "fixture onSettled crashed",
    );
    expect(slot(snapshot.extensions, "test-recorder")).toMatchObject({
      status: "running",
      value: { seen: 1 },
    });
    expect(snapshot.terminals).toEqual([
      expect.objectContaining({ outcome: "completed" }),
    ]);
    expect(await host.pendingWork()).toEqual({
      pending: false,
      nextWakeAt: null,
    });
    expect(errors).toHaveBeenCalledWith(
      expect.any(String),
      "shiitake_extension_stopped",
      expect.objectContaining({ extensionId: "test-crasher", durable: true }),
    );

    // A second run settles while the crasher is stopped; only the recorder sees it.
    await Effect.runPromise(session.send("second", { requestId: "second" }));
    await host.drain();
    expect(control.delivered).toEqual([]);

    control.failing = false;
    await expect(
      Effect.runPromise(core.agent.extensions.resume("test-crasher")),
    ).resolves.toMatchObject({ status: "running", reason: null });
    await host.drain();
    await host.reconcile();
    expect(control.delivered).toHaveLength(2);
    expect(new Set(control.delivered).size).toBe(2);
    await expect(
      Effect.runPromise(
        core.agent.extensions.call("test-recorder", "seen", {}),
      ),
    ).resolves.toEqual({ count: 2 });
  } finally {
    await Effect.runPromise(core.agent.close);
    fixture.close();
  }
});

it("shares the earliest wake between extension timers and reply retries", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const extensionAt = Date.now() + 60_000;
  let mailAt: number | undefined = extensionAt + 1_000;
  const { core, host } = await openCore(
    fixture.owner,
    grantAll([recorderExtension()]),
    {
      fanOut: () => Effect.void,
      drainOutbox: () => Effect.void,
      nextWakeAt: () => Effect.sync(() => mailAt),
    },
  );
  try {
    await Effect.runPromise(
      core.agent.session("timers").extension("test-recorder").call("arm", {
        key: "later",
        dueAt: extensionAt,
      }),
    );
    expect(await host.pendingWork()).toEqual({
      pending: false,
      nextWakeAt: extensionAt,
    });
    mailAt = extensionAt - 1_000;
    expect(await host.pendingWork()).toEqual({
      pending: false,
      nextWakeAt: mailAt,
    });
    mailAt = undefined;
    expect(await host.pendingWork()).toEqual({
      pending: false,
      nextWakeAt: extensionAt,
    });
  } finally {
    await Effect.runPromise(core.agent.close);
    fixture.close();
  }
});

it("fires a due timer once, admitting through the door with the extension as origin", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const { core, host } = await openCore(
    fixture.owner,
    grantAll([recorderExtension()]),
  );
  try {
    const session = core.agent.session("timers");
    const later = Date.now() + 3_600_000;
    await expect(
      Effect.runPromise(
        session.extension("test-recorder").call("arm", {
          key: "later",
          dueAt: later,
        }),
      ),
    ).resolves.toEqual({ armed: true });
    expect(await host.pendingWork()).toEqual({
      pending: false,
      nextWakeAt: later,
    });

    await Effect.runPromise(
      session.extension("test-recorder").call("arm", { key: "now", dueAt: 0 }),
    );
    expect((await host.pendingWork()).pending).toBe(true);
    await host.drain();

    const snapshot = await Effect.runPromise(session.read());
    expect(snapshot.messages[0]?.value).toMatchObject({
      role: "user",
      content: "timer now",
    });
    const origin = fixture.owner.sql
      .exec(
        `SELECT json_extract(value_json, '$.originExtension') AS origin_extension,
                json_extract(value_json, '$.correlation') AS correlation
         FROM shiitake_extension_records WHERE collection = 'seen'`,
      )
      .toArray();
    expect(origin).toEqual([
      { origin_extension: "test-recorder", correlation: "now" },
    ]);
    expect(await host.pendingWork()).toEqual({
      pending: false,
      nextWakeAt: later,
    });
  } finally {
    await Effect.runPromise(core.agent.close);
    fixture.close();
  }
});

it("returns typed command failures without stopping, and stops an extension that crosses an async boundary", async () => {
  quietStops();
  const fixture = createNativeSqliteOwnerFixture();
  const { core } = await openCore(
    fixture.owner,
    grantAll([
      recorderExtension(),
      crasherExtension({ failing: false, delivered: [] }),
    ]),
  );
  try {
    const recorder = core.agent.session("commands").extension("test-recorder");
    const failure = async (effect: Effect.Effect<unknown, unknown>) => {
      const exit = await Effect.runPromiseExit(effect);
      return Exit.isFailure(exit)
        ? Option.getOrUndefined(Cause.findErrorOption(exit.cause))
        : "succeeded";
    };
    expect(await failure(recorder.call("refuse", {}))).toMatchObject({
      boundary: "store",
      cause: "conflict",
    });
    expect(await failure(recorder.call("missing", {}))).toMatchObject({
      boundary: "contract",
      cause: "invalid",
    });
    expect(
      await failure(recorder.call("arm", { key: "k", dueAt: "soon" })),
    ).toMatchObject({ boundary: "contract", cause: "invalid" });
    expect(await failure(recorder.call("seen", {}))).toMatchObject({
      boundary: "contract",
      cause: "invalid",
    });
    expect(
      await failure(core.agent.extensions.call("test-crasher", "wait", {})),
    ).toMatchObject({ boundary: "extensions", cause: "unavailable" });

    const listed = await Effect.runPromise(core.agent.extensions.list);
    expect(listed.map(({ id, status }) => [id, status])).toEqual([
      ["test-crasher", "stopped"],
      ["test-recorder", "running"],
    ]);
    expect(listed[0]?.reason).toContain("asynchronous boundary");
  } finally {
    await Effect.runPromise(core.agent.close);
    fixture.close();
  }
});

it("logs the stop through the host's Logger with the extension, reason, and cause as annotations", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const { core } = await openCore(
    fixture.owner,
    grantAll([crasherExtension({ failing: false, delivered: [] })]),
  );
  try {
    const { entries, layer } = capturingLogger();
    const exit = await Effect.runPromiseExit(
      core.agent.extensions
        .call("test-crasher", "wait", {})
        .pipe(Effect.provide(layer)),
    );
    expect(Exit.isFailure(exit)).toBe(true);

    const stopped = entries.filter(
      (entry) => entry.message === "shiitake_extension_stopped",
    );
    expect(stopped).toHaveLength(1);
    expect(stopped[0]?.annotations).toMatchObject({
      extensionId: "test-crasher",
      durable: true,
    });
    expect(stopped[0]?.annotations.reason).toContain("asynchronous boundary");
    expect(stopped[0]?.annotations.cause).not.toBeNull();
  } finally {
    await Effect.runPromise(core.agent.close);
    fixture.close();
  }
});

it("retains an extension stop's original cause in a redacted Logger annotation", async () => {
  const marker = "synthetic-extension-secret-marker";
  const original = Object.assign(
    new ExtensionStop({ reason: "Fixture stopped" }),
    {
      cause: new Error(`Nested extension I/O failure ${marker}`),
    },
  );
  const extension = defineExtension({
    id: ExtensionId.make("redacted-stop"),
    version: "1.0.0",
  }).define({
    commands: {
      specs: {
        stop: {
          scope: "agent",
          input: Schema.Struct({}),
          output: Schema.Struct({}),
        },
      },
      handlers: () => ({ stop: () => Effect.fail(original) }),
    },
  });
  const fixture = createNativeSqliteOwnerFixture();
  const { core } = await openCore(fixture.owner, grantAll([extension]));
  const { entries, layer } = capturingLogger();
  try {
    await Effect.runPromiseExit(
      core.agent.extensions
        .call("redacted-stop", "stop", {})
        .pipe(Effect.provide(layer)),
    );
    const stopped = entries.find(
      (entry) => entry.message === "shiitake_extension_stopped",
    );
    const value = stopped?.annotations.cause;
    expect(Redacted.isRedacted(value)).toBe(true);
    if (!Redacted.isRedacted(value))
      throw new Error("Stop log omitted its redacted cause");
    const cause = Redacted.value(value);
    expect(Cause.isCause(cause)).toBe(true);
    if (!Cause.isCause(cause))
      throw new Error("Stop log did not retain the Effect cause");
    expect(Option.getOrUndefined(Cause.findErrorOption(cause))).toBe(original);
    expect(JSON.stringify(entries)).not.toContain(marker);
    expect(inspect(entries, { depth: null })).not.toContain(marker);
  } finally {
    await Effect.runPromise(core.agent.close);
    fixture.close();
  }
});

it("never swallows a failed durable stop write: it fails the triggering call and never records the stop as durable", async () => {
  quietStops();
  const fixture = createNativeSqliteOwnerFixture();
  const roster = () =>
    grantAll([crasherExtension({ failing: false, delivered: [] })]);
  const { core } = await openCore(
    withFailingStoppedWrite(fixture.owner),
    roster(),
  );
  try {
    const exit = await Effect.runPromiseExit(
      core.agent.extensions.call("test-crasher", "wait", {}),
    );
    expect(Exit.isFailure(exit)).toBe(true);
    expect(
      Exit.isFailure(exit)
        ? Option.getOrUndefined(Cause.findErrorOption(exit.cause))
        : undefined,
    ).toMatchObject({ boundary: "store", cause: "unavailable" });
  } finally {
    await Effect.runPromise(core.agent.close);
  }

  // Nothing durable landed: a fresh open over the same store reports the
  // extension as still running, never as a stop that was never written.
  const reopened = await openCore(fixture.owner, roster());
  try {
    const listed = await Effect.runPromise(reopened.core.agent.extensions.list);
    expect(listed[0]?.status).toBe("running");
  } finally {
    await Effect.runPromise(reopened.core.agent.close);
    fixture.close();
  }
});

it("derives honest revisions and fingerprints and checks only captured participants", async () => {
  const [a] = await Effect.runPromise(
    admitExtensionRosterV0(grantAll([recorderExtension({ note: "a" })])),
  );
  const [b] = await Effect.runPromise(
    admitExtensionRosterV0(grantAll([recorderExtension({ note: "b" })])),
  );
  const both = await Effect.runPromise(
    admitExtensionRosterV0(
      grantAll([recorderExtension({ note: "a" }), toolExtension("x")]),
    ),
  );
  expect(a?.revision).toMatch(/^sha256:[0-9a-f]{64}$/u);
  expect(a?.revision).not.toBe(b?.revision);
  expect(both.find(({ id }) => id === "test-recorder")?.fingerprint).toBe(
    a?.fingerprint,
  );
  const [narrow] = await Effect.runPromise(
    admitExtensionRosterV0({
      roster: [recorderExtension({ note: "a" })],
      grants: [
        {
          id: recorderContract.id,
          capabilities: ["state"],
        },
      ],
    }),
  );
  expect(narrow?.fingerprint).not.toBe(a?.fingerprint);
  // The core digest names the run protocol and the Files tool set, never a
  // roster.
  expect(await Effect.runPromise(coreProtocolDigest([]))).toMatch(
    /^[0-9a-f]{64}$/u,
  );
});

it("changes the core digest only with the Files tool set, never a roster", async () => {
  const bare = await Effect.runPromise(coreProtocolDigest([]));
  const withPatch = await Effect.runPromise(
    coreProtocolDigest(["apply_patch"]),
  );
  const reordered = await Effect.runPromise(
    coreProtocolDigest(["b", "apply_patch"]),
  );
  const sameSetDifferentOrder = await Effect.runPromise(
    coreProtocolDigest(["apply_patch", "b"]),
  );
  expect(withPatch).not.toBe(bare);
  expect(reordered).toBe(sameSetDifferentOrder);
});

async function queuedUnder(
  store: SqliteOwner,
  roster: ReturnType<typeof grantAll>,
  requestId: string,
) {
  const { core } = await openCore(store, roster);
  await Effect.runPromise(
    core.agent.session("contract").send(requestId, { requestId }),
  );
  await Effect.runPromise(core.agent.close);
}

/** A Files port offering no patch tool by default. */
function trivialFilesPort(): FilesPort {
  return {
    read: (input) =>
      Promise.resolve({
        path: input.path,
        content: "",
        offset: 0,
        nextOffset: null,
        truncated: false,
      }),
    list: (input) =>
      Promise.resolve({ path: input.path, results: [], truncated: null }),
    glob: (input) =>
      Promise.resolve({ path: input.path, results: [], truncated: null }),
    grep: (input) =>
      Promise.resolve({ path: input.path, results: [], truncated: null }),
    replace: (input) => Promise.resolve({ path: input.path, bytes: 0 }),
  };
}

it("runs queued and subsequent calls on the reopened revision and records it per step", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const prompt = (text: string) =>
    defineExtension({
      id: ExtensionId.make("test-upgrade-prompt"),
      version: "1.0.0",
    })
      .config({ text })
      .define({
        prompt: (context) => [{ name: "selected", text: context.config.text }],
      });
  try {
    const [previous] = await Effect.runPromise(
      admitExtensionRosterV0(grantAll([prompt("old revision")])),
    );
    const [upgraded] = await Effect.runPromise(
      admitExtensionRosterV0(grantAll([prompt("new revision")])),
    );
    if (previous === undefined || upgraded === undefined)
      throw new Error("Missing test extension");
    await queuedUnder(
      fixture.owner,
      grantAll([prompt("old revision")]),
      "changed",
    );
    const { core, host } = await openCore(
      fixture.owner,
      grantAll([prompt("new revision")]),
    );
    await host.drain();
    const snapshot = await Effect.runPromise(
      core.agent.session("contract").read(),
    );
    expect(snapshot.terminals[0]?.outcome).toBe("completed");
    const workId = snapshot.terminals[0]?.commandId;
    if (workId === undefined) throw new Error("Missing work identity");
    const facts = readSessionEntriesForWork(fixture.owner, "contract", workId);
    const command = facts.find(({ entry }) => entry.kind === "command")?.entry;
    expect(command?.kind === "command" && command.contract.extensions).toEqual([
      ["test-upgrade-prompt", previous.fingerprint],
    ]);
    for (const { entry } of facts) {
      if (entry.kind === "run_started" || entry.kind === "model_prompt")
        expect(entry.contract.extensions).toEqual([
          ["test-upgrade-prompt", upgraded.fingerprint],
        ]);
      if (entry.kind === "model_prompt") {
        expect(entry.systemPrompt).toContain(
          "[extension:test-upgrade-prompt.selected]\nnew revision",
        );
        expect(entry.systemPrompt).not.toContain("old revision");
      }
    }
    expect(facts.some(({ entry }) => entry.kind === "run_started")).toBe(true);
    expect(facts.some(({ entry }) => entry.kind === "model_prompt")).toBe(true);
    await Effect.runPromise(
      core.agent.session("contract").send("next", { requestId: "next" }),
    );
    await host.drain();
    const next = await Effect.runPromise(core.agent.session("contract").read());
    expect(next.terminals.map(({ outcome }) => outcome)).toEqual([
      "completed",
      "completed",
    ]);
    await Effect.runPromise(core.agent.close);
  } finally {
    fixture.close();
  }
});

it("does not label a changed Files set contract_changed", async () => {
  const errors = quietStops();
  const fixture = createNativeSqliteOwnerFixture();
  try {
    const bareFiles = Knapsack.files({ port: trivialFilesPort() });
    const patchFiles = Knapsack.files({
      port: {
        ...trivialFilesPort(),
        patch: () =>
          Promise.resolve({
            status: "completed" as const,
            exitCode: 0,
            stdout: "",
            stderr: "",
            truncated: false,
          }),
      },
    });
    const filesRoster = (files: FilesSurface) =>
      grantAll([
        defineExtension({
          id: ExtensionId.make("test-files"),
          version: "1.0.0",
        }).define({ files: () => files.tools }),
      ]);
    await queuedUnder(fixture.owner, filesRoster(bareFiles), "changed-files");
    const { core, host } = await openCore(
      fixture.owner,
      filesRoster(patchFiles),
    );
    await host.drain();
    const snapshot = await Effect.runPromise(
      core.agent.session("contract").read(),
    );
    expect(snapshot.terminals).toEqual([
      expect.objectContaining({ outcome: "failed" }),
    ]);
    // The fixture has no Mycelium tool runtime, so patch-enabled Files still
    // fails independently; a Files set change is not a state migration.
    expect(errors).not.toHaveBeenCalledWith(
      expect.any(String),
      "shiitake_run_contract_changed",
      expect.objectContaining({
        terminal: "contract_changed: core",
      }),
    );
    await Effect.runPromise(core.agent.close);
  } finally {
    fixture.close();
  }
});

it("reserves contract_changed for a failed declared state migration", async () => {
  const errors = quietStops();
  const fixture = createNativeSqliteOwnerFixture();
  const extension = (version: 1 | 2, compatible: boolean) =>
    defineExtension({
      id: ExtensionId.make("test-migrating-tools"),
      version: `${String(version)}.${compatible ? "1" : "0"}.0`,
    })
      .state({
        version,
        collections: {
          records: defineCollection(Schema.Struct({ key: Schema.String }), {
            key: (row) => row.key,
          }),
        },
        migrations:
          version === 1
            ? {}
            : {
                1: (row) => {
                  if (!compatible) throw new Error("Declared migration failed");
                  return [row];
                },
              },
      })
      .define({
        tools: () => null,
        commands: {
          specs: {
            seed: {
              scope: "agent",
              input: Schema.Struct({}),
              output: Schema.Null,
            },
          },
          handlers: (ctx) => ({
            seed: () =>
              ctx.state.records
                .put({ key: "migration-input" })
                .pipe(Effect.as(null)),
          }),
        },
      });
  try {
    const previous = await openCore(
      fixture.owner,
      grantAll([extension(1, false)]),
    );
    await Effect.runPromise(
      previous.core.agent.extensions.call("test-migrating-tools", "seed", {}),
    );
    await Effect.runPromise(
      previous.core.agent
        .session("contract")
        .send("migration", { requestId: "migration" }),
    );
    await Effect.runPromise(previous.core.agent.close);
    const blocked = await openCore(
      fixture.owner,
      grantAll([extension(2, false)]),
    );
    await blocked.host.drain();
    const snapshot = await Effect.runPromise(
      blocked.core.agent.session("contract").read(),
    );
    expect(snapshot.terminals[0]?.outcome).toBe("failed");
    expect(errors).toHaveBeenCalledWith(
      expect.any(String),
      "shiitake_run_contract_changed",
      expect.objectContaining({
        terminal: "contract_changed: test-migrating-tools",
      }),
    );
    await Effect.runPromise(blocked.core.agent.close);

    const repaired = await openCore(
      fixture.owner,
      grantAll([extension(2, true)]),
    );
    await Effect.runPromise(
      repaired.core.agent.session("contract").send("repaired", {
        requestId: "repaired",
      }),
    );
    await repaired.host.drain();
    const after = await Effect.runPromise(
      repaired.core.agent.session("contract").read(),
    );
    expect(after.terminals.at(-1)?.outcome).toBe("completed");
    await Effect.runPromise(repaired.core.agent.close);
  } finally {
    fixture.close();
  }
});

it("selects an extension added after queued admission at execution", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    await queuedUnder(fixture.owner, grantAll([]), "unaffected");
    const { core, host } = await openCore(
      fixture.owner,
      grantAll([toolExtension("added")]),
    );
    await host.drain();
    const snapshot = await Effect.runPromise(
      core.agent.session("contract").read(),
    );
    expect(snapshot.terminals).toEqual([
      expect.objectContaining({ outcome: "completed" }),
    ]);
    const workId = snapshot.terminals[0]?.commandId;
    if (workId === undefined) throw new Error("Missing work identity");
    const facts = readSessionEntriesForWork(fixture.owner, "contract", workId);
    const command = facts.find(({ entry }) => entry.kind === "command")?.entry;
    expect(command?.kind === "command" && command.contract.extensions).toEqual(
      [],
    );
    expect(
      facts.find(({ entry }) => entry.kind === "model_prompt")?.entry,
    ).toMatchObject({
      contract: { extensions: [["test-tools", expect.any(String)]] },
    });
    await Effect.runPromise(core.agent.close);
  } finally {
    fixture.close();
  }
});

function memoryMailRoom() {
  const rows = createMemoryOutbox();
  const posted: BirdDogMail[] = [];
  const room = Effect.runSync(
    makeBirdDog({
      outbox: {
        read: (key) => Effect.promise(() => rows.read(key)),
        put: (key, value) => Effect.promise(() => rows.put(key, value)),
        list: (limit, afterKey) =>
          Effect.promise(() => rows.list(limit, afterKey)),
        delete: (key) => Effect.promise(() => rows.delete(key)),
      },
      post: (mail) =>
        Effect.sync(() => {
          posted.push(mail);
          return true;
        }),
      scheduleWake: () => Effect.void,
      now: () => 100,
    }),
  );
  return { room, rows, posted };
}

// 200 mails through the paged outbox take ~4s alone; 5s default flakes under load.
it("does not re-fan mail or re-deliver settled runs after a cold reopen", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const mail = memoryMailRoom();
  const roster = () => grantAll([recorderExtension()]);
  const parent = {
    kind: "agent",
    agentId: "parent",
    sessionId: "parent-session",
  } as const;
  try {
    const first = await openCore(fixture.owner, roster(), mail.room);
    for (let index = 0; index < 200; index += 1)
      await Effect.runPromise(
        first.core.acceptMail({
          key: `mail-${String(index)}`,
          to: {
            kind: "agent",
            agentId: "child",
            sessionId: `child-${String(index % 7)}`,
          },
          from: parent,
          replyTo: [parent],
          content: `request ${String(index)}`,
          createdAt: 1,
        }),
      );
    await first.host.drain();
    // BirdDog drains its outbox in bounded pages on later ticks.
    for (let pass = 0; pass < 8 && mail.posted.length < 200; pass += 1)
      await first.host.reconcile();
    expect(mail.posted).toHaveLength(200);
    const outboxRows = (await mail.rows.list(1_000)).length;
    await Effect.runPromise(first.core.agent.close);

    const second = await openCore(fixture.owner, roster(), mail.room);
    expect(await second.host.pendingWork()).toEqual({
      pending: false,
      nextWakeAt: null,
    });
    await second.host.reconcile();
    expect(mail.posted).toHaveLength(200);
    expect((await mail.rows.list(1_000)).length).toBe(outboxRows);
    await expect(
      Effect.runPromise(
        second.core.agent.extensions.call("test-recorder", "seen", {}),
      ),
    ).resolves.toEqual({ count: 200 });
    expect(
      (await Effect.runPromise(second.core.agent.extensions.list))[0]?.status,
    ).toBe("running");
    await Effect.runPromise(second.core.agent.close);
  } finally {
    fixture.close();
  }
}, 30_000);

it("delivers a settled run exactly once when the process dies before the extension commit", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  let crashNextExtensionCommit = false;
  // Simulate process death at commit: the transaction rolls back and the tick fails.
  const crashing: SqliteOwner = {
    sql: fixture.owner.sql,
    transactionSync: (body) =>
      fixture.owner.transactionSync(() => {
        const result = body();
        const recording = () =>
          Schema.decodeUnknownSync(Schema.Struct({ n: Schema.Number }))(
            fixture.owner.sql
              .exec(
                "SELECT COUNT(*) AS n FROM shiitake_extension_records WHERE collection = 'seen'",
              )
              .one(),
          ).n > 0;
        if (crashNextExtensionCommit && recording()) {
          crashNextExtensionCommit = false;
          throw new Error("process died before commit");
        }
        return result;
      }),
  };
  const roster = () => grantAll([recorderExtension()]);
  try {
    const first = await openCore(crashing, roster());
    await Effect.runPromise(
      first.core.agent.session("crash").send("once", { requestId: "once" }),
    );
    crashNextExtensionCommit = true;
    expect(Exit.isFailure(await first.host.reconcileExit())).toBe(true);
    // Close reports the retained reconciliation failure.
    await Effect.runPromiseExit(first.core.agent.close);

    for (let reopen = 0; reopen < 2; reopen += 1) {
      const next = await openCore(crashing, roster());
      await next.host.drain();
      await expect(
        Effect.runPromise(
          next.core.agent.extensions.call("test-recorder", "seen", {}),
        ),
      ).resolves.toEqual({ count: 1 });
      await Effect.runPromise(next.core.agent.close);
    }
  } finally {
    fixture.close();
  }
});

it("migrates stored records by pure per-version functions and refuses a downgrade", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    const first = await openCore(fixture.owner, grantAll([notesExtension(1)]));
    await Effect.runPromise(
      first.core.agent.extensions.call("test-notes", "write", {
        key: "a",
        text: "hello",
      }),
    );
    await Effect.runPromise(first.core.agent.close);

    const second = await openCore(fixture.owner, grantAll([notesExtension(3)]));
    await expect(
      Effect.runPromise(
        second.core.agent.extensions.call("test-notes", "read", { key: "a" }),
      ),
    ).resolves.toBe('migrated: {"key":"a","text":"hello"}');
    await Effect.runPromise(second.core.agent.close);

    await expect(
      openCore(fixture.owner, grantAll([notesExtension(1)])),
    ).rejects.toThrow("state version 3 is newer than 1");
  } finally {
    fixture.close();
  }
});

it("keeps an extension stopped across a reopen and replays only its missed runs on resume", async () => {
  quietStops();
  const fixture = createNativeSqliteOwnerFixture();
  const control: CrashSwitch = { failing: true, delivered: [] };
  const roster = () =>
    grantAll([recorderExtension(), crasherExtension(control)]);
  try {
    const first = await openCore(fixture.owner, roster());
    await Effect.runPromise(
      first.core.agent.session("reopen").send("one", { requestId: "one" }),
    );
    await first.host.drain();
    await Effect.runPromise(first.core.agent.close);

    const second = await openCore(fixture.owner, roster());
    expect(
      (await Effect.runPromise(second.core.agent.extensions.list)).map(
        ({ status }) => status,
      ),
    ).toEqual(["stopped", "running"]);
    await Effect.runPromise(
      second.core.agent.session("reopen").send("two", { requestId: "two" }),
    );
    await second.host.drain();
    control.failing = false;
    await Effect.runPromise(
      second.core.agent.extensions.resume("test-crasher"),
    );
    await second.host.drain();
    expect(control.delivered).toHaveLength(2);
    expect(new Set(control.delivered).size).toBe(2);
    await Effect.runPromise(second.core.agent.close);
  } finally {
    fixture.close();
  }
});
