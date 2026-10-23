import * as Stream from "effect/Stream";
import * as Option from "effect/Option";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { expect, it, vi } from "vitest";
import {
  AttentionItemSchema,
  defineExtension,
  ExtensionId,
} from "../src/extensions.js";
import { attentionExtension } from "./fixtures/attention.fixture.js";
import {
  readSessionInboxChanges,
  readSessionInboxPage,
} from "../src/internal/woodstock/index.js";
import { openCore } from "./fixtures/extension-host.fixture.js";
import { grantAll } from "./fixtures/fixture-extensions.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

const item = {
  id: "choice",
  kind: "question",
  summary: "Choose a destination",
  since: 1,
  expiresAt: 2,
};

it("reports typed Session absence without constructing a Session or recreating deleted attention", async () => {
  const extension = defineExtension({
    id: ExtensionId.make("absence-probe"),
    version: "1.0.0",
  }).define({
    attention: true,
    commands: {
      specs: {
        set: {
          scope: "session",
          input: AttentionItemSchema,
          output: Schema.Literals(["set", "session_absent"]),
        },
      },
      handlers: (ctx) => ({
        set: ({ sessionId, input }) =>
          ctx.attention.set(sessionId, input).pipe(
            Effect.map((): "set" => "set"),
            Effect.catchTag("ExtensionStateError", (error) =>
              error.reason === "session_absent"
                ? Effect.succeed<"session_absent">("session_absent")
                : Effect.fail(error),
            ),
          ),
      }),
    },
  });
  const fixture = createNativeSqliteOwnerFixture();
  const { core, host } = await openCore(fixture.owner, grantAll([extension]));
  try {
    const session = core.agent.session("absent-attention");
    const set = () =>
      Effect.runPromise(
        session.extension(extension.manifest.id).call("set", item),
      );
    expect(await set()).toBe("session_absent");
    expect(readSessionInboxPage(fixture.owner, undefined).sessions).toEqual([]);
    await Effect.runPromise(session.send("hello", { requestId: "hello" }));
    await host.drain();
    expect(await set()).toBe("set");
    expect((await Effect.runPromise(session.read())).attention).toHaveLength(1);
    expect(
      await Effect.runPromise(core.agent.deleteSession("absent-attention")),
    ).toBe(true);
    expect(await set()).toBe("session_absent");
    expect(readSessionInboxPage(fixture.owner, undefined).sessions).toEqual([]);
    expect((await Effect.runPromise(session.read())).attention).toEqual([]);
  } finally {
    await Effect.runPromise(core.agent.close);
    fixture.close();
  }
});

it("keeps inbox:false attention visible across restart without changing the inbox state or hiding ordinary attention", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const roster = grantAll([attentionExtension()]);
  let opened = await openCore(fixture.owner, roster);
  const sessionId = "drawer-attention";
  const drawerItem = { ...item, id: "paused-goal", inbox: false };
  try {
    let session = opened.core.agent.session(sessionId);
    await Effect.runPromise(session.send("hello", { requestId: "hello" }));
    await opened.host.drain();
    expect(
      readSessionInboxPage(fixture.owner, undefined).sessions[0],
    ).toMatchObject({ state: "done" });

    await Effect.runPromise(
      session.extension("attention-probe").call("set", drawerItem),
    );
    expect((await Effect.runPromise(session.read())).attention).toEqual([
      { ...drawerItem, extensionId: "attention-probe" },
    ]);
    expect(
      readSessionInboxPage(fixture.owner, undefined).sessions[0],
    ).toMatchObject({ state: "done" });

    await Effect.runPromise(
      session.extension("attention-probe").call("set", item),
    );
    expect(
      readSessionInboxPage(fixture.owner, undefined).sessions[0],
    ).toMatchObject({ state: "needs_you" });
    await Effect.runPromise(
      session.extension("attention-probe").call("clear", { id: item.id }),
    );
    expect(
      readSessionInboxPage(fixture.owner, undefined).sessions[0],
    ).toMatchObject({ state: "done" });

    await Effect.runPromise(opened.core.agent.close);
    opened = await openCore(fixture.owner, roster);
    session = opened.core.agent.session(sessionId);
    expect((await Effect.runPromise(session.read())).attention).toEqual([
      { ...drawerItem, extensionId: "attention-probe" },
    ]);
    expect(
      readSessionInboxPage(fixture.owner, undefined).sessions[0],
    ).toMatchObject({ state: "done" });
  } finally {
    await Effect.runPromise(opened.core.agent.close);
    fixture.close();
  }
});

it.each(["omitted", "revoked"])(
  "excludes %s owners after restart and restores their retained items",
  async (mode) => {
    const fixture = createNativeSqliteOwnerFixture();
    const extension = attentionExtension();
    const granted = grantAll([extension]);
    let opened = await openCore(fixture.owner, granted);
    try {
      const sessionId = "grant-restart";
      const session = opened.core.agent.session(sessionId);
      await Effect.runPromise(session.send("hello", { requestId: "hello" }));
      await opened.host.drain();
      await Effect.runPromise(
        session.extension(extension.manifest.id).call("set", item),
      );
      await Effect.runPromise(opened.core.agent.close);
      opened = await openCore(
        fixture.owner,
        mode === "omitted"
          ? grantAll([])
          : {
              roster: [extension],
              grants: [
                {
                  id: extension.manifest.id,
                  capabilities: ["commands", "send"],
                },
              ],
            },
      );
      expect(
        (await Effect.runPromise(opened.core.agent.session(sessionId).read()))
          .attention,
      ).toEqual([]);
      expect(
        readSessionInboxPage(fixture.owner, undefined).sessions[0],
      ).toMatchObject({ state: "done" });
      await Effect.runPromise(opened.core.agent.close);
      opened = await openCore(fixture.owner, granted);
      expect(
        (await Effect.runPromise(opened.core.agent.session(sessionId).read()))
          .attention,
      ).toEqual([{ ...item, extensionId: extension.manifest.id }]);
      expect(
        readSessionInboxPage(fixture.owner, undefined).sessions[0],
      ).toMatchObject({ state: "needs_you" });
    } finally {
      await Effect.runPromise(opened.core.agent.close);
      fixture.close();
    }
  },
);

it("refuses excess items as a bound error and releases capacity on clear", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const { core, host } = await openCore(
    fixture.owner,
    grantAll([attentionExtension()]),
  );
  try {
    const session = core.agent.session("capacity-session");
    await Effect.runPromise(session.send("hello", { requestId: "hello" }));
    await host.drain();
    const extension = session.extension("attention-probe");
    for (let index = 0; index < 64; index++)
      await Effect.runPromise(
        extension.call("set", { ...item, id: `item-${String(index)}` }),
      );
    await expect(
      Effect.runPromise(extension.call("set", item)),
    ).rejects.toMatchObject({
      boundary: "contract",
      cause: "invalid",
      message: "Session attention capacity reached",
    });
    await Effect.runPromise(extension.call("clear", { id: "item-0" }));
    await Effect.runPromise(extension.call("set", item));
    expect((await Effect.runPromise(session.read())).attention).toHaveLength(
      64,
    );
  } finally {
    await Effect.runPromise(core.agent.close);
    fixture.close();
  }
});

it("publishes durable attention, keeps tier zero through work, and restores done after the last owner clears", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const roster = grantAll([
    attentionExtension(),
    attentionExtension("second-owner"),
  ]);
  let opened = await openCore(fixture.owner, roster);
  try {
    let session = opened.core.agent.session("attention-session");
    await Effect.runPromise(session.send("hello", { requestId: "hello" }));
    await opened.host.drain();
    const prior = readSessionInboxChanges(fixture.owner, 0, 64).latest;
    await Effect.runPromise(
      session.extension("attention-probe").call("set", item),
    );
    const changes = readSessionInboxChanges(fixture.owner, prior, 64);
    expect(changes.latest).toBeGreaterThan(prior);
    expect(changes.rows).toEqual([
      expect.objectContaining({
        sessionId: "attention-session",
        state: "needs_you",
      }),
    ]);
    expect(
      fixture.owner.sql
        .exec(
          "SELECT tier FROM session_inbox WHERE session_id = ?",
          "attention-session",
        )
        .one(),
    ).toEqual({ tier: 0 });
    expect((await Effect.runPromise(session.read())).attention).toEqual([
      { ...item, extensionId: "attention-probe" },
    ]);
    await Effect.runPromise(
      session.extension("second-owner").call("set", item),
    );
    await Effect.runPromise(
      session.send("continue", { requestId: "continue" }),
    );
    expect(
      readSessionInboxPage(fixture.owner, undefined).sessions[0],
    ).toMatchObject({ state: "needs_you" });
    await opened.host.drain();
    await Effect.runPromise(opened.core.agent.close);
    opened = await openCore(fixture.owner, roster);
    session = opened.core.agent.session("attention-session");
    expect((await Effect.runPromise(session.read())).attention).toHaveLength(2);
    await Effect.runPromise(
      session.extension("attention-probe").call("clear", { id: item.id }),
    );
    expect(
      readSessionInboxPage(fixture.owner, undefined).sessions[0],
    ).toMatchObject({ state: "needs_you" });
    await Effect.runPromise(
      session.extension("second-owner").call("clear", { id: item.id }),
    );
    expect((await Effect.runPromise(session.read())).attention).toEqual([]);
    expect(
      readSessionInboxPage(fixture.owner, undefined).sessions[0],
    ).toMatchObject({ state: "done" });
  } finally {
    await Effect.runPromise(opened.core.agent.close);
    fixture.close();
  }
});

it("rolls back attention and inbox revision with a rejected owner transaction", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const { core, host } = await openCore(
    fixture.owner,
    grantAll([attentionExtension()]),
  );
  try {
    const session = core.agent.session("rollback-session");
    await Effect.runPromise(session.send("hello", { requestId: "hello" }));
    await host.drain();
    const before = readSessionInboxChanges(fixture.owner, 0, 64);
    await expect(
      Effect.runPromise(
        session.extension("attention-probe").call("rollback", item),
      ),
    ).rejects.toMatchObject({ cause: "rejected" });
    expect((await Effect.runPromise(session.read())).attention).toEqual([]);
    expect(readSessionInboxChanges(fixture.owner, 0, 64)).toEqual(before);
  } finally {
    await Effect.runPromise(core.agent.close);
    fixture.close();
  }
});

it("refuses attention writes without the attention grant", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const extension = attentionExtension();
  const { core, host } = await openCore(fixture.owner, {
    roster: [extension],
    grants: [{ id: extension.manifest.id, capabilities: ["commands", "send"] }],
  });
  try {
    const session = core.agent.session("ungranted-session");
    await Effect.runPromise(session.send("hello", { requestId: "hello" }));
    await host.drain();
    await expect(
      Effect.runPromise(
        session.extension(extension.manifest.id).call("set", item),
      ),
    ).rejects.toMatchObject({ message: "Extension is not granted attention" });
    expect((await Effect.runPromise(session.read())).attention).toEqual([]);
  } finally {
    await Effect.runPromise(core.agent.close);
    fixture.close();
  }
});

it("hides stopped attention until resume and keeps its durable items", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const log = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const { core, host } = await openCore(
    fixture.owner,
    grantAll([attentionExtension()]),
  );
  try {
    const session = core.agent.session("stopped-attention");
    await Effect.runPromise(session.send("hello", { requestId: "hello" }));
    await host.drain();
    await Effect.runPromise(
      session.extension("attention-probe").call("set", item),
    );
    await expect(
      Effect.runPromise(session.extension("attention-probe").call("stop", {})),
    ).rejects.toMatchObject({ boundary: "extensions" });
    expect((await Effect.runPromise(session.read())).attention).toEqual([]);
    expect(
      readSessionInboxPage(fixture.owner, undefined).sessions[0],
    ).toMatchObject({ state: "done" });
    await Effect.runPromise(core.agent.extensions.resume("attention-probe"));
    expect((await Effect.runPromise(session.read())).attention).toEqual([
      { ...item, extensionId: "attention-probe" },
    ]);
    expect(
      readSessionInboxPage(fixture.owner, undefined).sessions[0],
    ).toMatchObject({ state: "needs_you" });
  } finally {
    await Effect.runPromise(core.agent.close);
    log.mockRestore();
    fixture.close();
  }
});

it("delivers an attention-only live frame without advancing the history cursor", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const { core, host } = await openCore(
    fixture.owner,
    grantAll([attentionExtension()]),
  );
  const session = core.agent.session("observed-attention");
  await Effect.runPromise(session.send("hello", { requestId: "hello" }));
  await host.drain();
  const watch = await Effect.runPromise(session.watch());
  try {
    const observed = Effect.runPromise(
      watch.changes.pipe(
        Stream.filter(
          (frame) => frame.kind === "update" && frame.attention.length === 1,
        ),
        Stream.runHead,
      ),
    );
    await Effect.runPromise(
      session.extension("attention-probe").call("set", item),
    );
    expect(Option.getOrThrow(await observed)).toMatchObject({
      kind: "update",
      cursor: watch.snapshot.cursor,
      attention: [{ ...item, extensionId: "attention-probe" }],
    });
  } finally {
    await Effect.runPromise(watch.close);
    await Effect.runPromise(core.agent.close);
    fixture.close();
  }
});
