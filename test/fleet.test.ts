import { expect, it } from "vitest";
import {
  createAgentDirectory,
  agentGrantSchema,
  agentIds,
  type AgentGrant,
} from "../src/fleet.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

it("reserves one granted Agent across reopen and refuses changed creation or missing authority", () => {
  const fixture = createNativeSqliteOwnerFixture();
  const grant: AgentGrant = {
    agents: "all",
    actions: ["create", "read", "manage"],
    expiresAt: Date.now() + 60_000,
    provenance: { actor: "member-ada" },
  };
  const input = {
    requestId: "create-moss",
    name: "Moss",
    configuration: "none",
  };
  try {
    const directory = createAgentDirectory(fixture.owner.sql);
    expect(() =>
      fixture.owner.transactionSync(() =>
        directory.create({ ...grant, actions: ["read"] }, input),
      ),
    ).toThrow("action");
    expect(directory.list(grant)).toEqual([]);
    const created = fixture.owner.transactionSync(() =>
      directory.create(grant, input),
    );
    const reopened = createAgentDirectory(fixture.owner.sql);
    expect(
      fixture.owner.transactionSync(() => reopened.create(grant, input)),
    ).toEqual({ agent: created.agent, fact: null });
    expect(created.fact).toMatchObject({
      kind: "created",
      agentId: created.agent.agentId,
      provenance: grant.provenance,
    });
    expect(reopened.list(grant)).toEqual([created.agent]);
    expect(() =>
      fixture.owner.transactionSync(() =>
        reopened.create(grant, { ...input, configuration: "new" }),
      ),
    ).toThrow("request_conflict");
    expect(reopened.list({ ...grant, agents: [] })).toEqual([]);
    fixture.owner.transactionSync(() =>
      reopened.remove(grant, created.agent.agentId),
    );
    expect(reopened.list(grant)).toEqual([]);
    expect(reopened.listErasureIds()).toEqual([created.agent.agentId]);
  } finally {
    fixture.close();
  }
});

it("renames a granted Agent, reads the new name back, and refuses conflicts, missing authority, or missing Agents", () => {
  const fixture = createNativeSqliteOwnerFixture();
  const grant: AgentGrant = {
    agents: "all",
    actions: ["create", "read", "manage"],
    expiresAt: Date.now() + 60_000,
    provenance: { actor: "owner-ada" },
  };
  try {
    const directory = createAgentDirectory(fixture.owner.sql);
    const bob = fixture.owner.transactionSync(() =>
      directory.create(grant, {
        requestId: "create-bob",
        name: "bob",
        configuration: "none",
      }),
    ).agent;
    const other = fixture.owner.transactionSync(() =>
      directory.create(grant, {
        requestId: "create-other",
        name: "Other",
        configuration: "none",
      }),
    ).agent;
    const renamed = fixture.owner.transactionSync(() =>
      directory.rename(grant, bob.agentId, { name: "Gardener" }),
    );
    expect(renamed.name).toBe("Gardener");
    expect(directory.read(grant, bob.agentId)?.name).toBe("Gardener");
    expect(directory.list(grant)).toContainEqual(renamed);
    // Renaming to the same name already held is a no-op, not a conflict.
    expect(
      fixture.owner.transactionSync(() =>
        directory.rename(grant, bob.agentId, { name: "Gardener" }),
      ).name,
    ).toBe("Gardener");
    expect(() =>
      fixture.owner.transactionSync(() =>
        directory.rename(grant, bob.agentId, { name: "Other" }),
      ),
    ).toThrow("name_taken");
    expect(() =>
      fixture.owner.transactionSync(() =>
        directory.rename({ ...grant, actions: ["read"] }, bob.agentId, {
          name: "Someone",
        }),
      ),
    ).toThrow("action");
    expect(() =>
      fixture.owner.transactionSync(() =>
        directory.rename(grant, other.agentId, { name: "" }),
      ),
    ).toThrow();
    expect(() =>
      fixture.owner.transactionSync(() =>
        directory.rename(grant, agentIds.agent.generate(), {
          name: "Ghost",
        }),
      ),
    ).toThrow("not_found");
    // Renaming to its own current name is never blocked by itself.
    expect(directory.read(grant, other.agentId)?.name).toBe("Other");
  } finally {
    fixture.close();
  }
});

it("delegation polls one durable BirdDog receipt and preserves a cancelled terminal's host metadata", async () => {
  const { createFleetDelegation } = await import("../src/fleet.js");
  const { makeHost } = await import("../src/index.js");
  const { compatibilityRunDependencies, createCompatibilityRuntimeHost } =
    await import("./fixtures/native-sqlite-owner.fixture.js");
  const Effect = await import("effect/Effect");
  const directoryStore = createNativeSqliteOwnerFixture();
  const childStore = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const host = await makeHost(
    {
      ...compatibilityRunDependencies(),
      store: childStore.owner,
      runtime: runtime.runtime,
    },
    { fanOut: () => Effect.void, drainOutbox: () => Effect.void },
  );
  const mail: import("@fungi.computer/bird-dog").BirdDogMail[] = [];
  const options = {
    storage: directoryStore.owner,
    readSettlement: (_agentId: string, sessionId: string, commandId: string) =>
      host.readSettlement(sessionId, commandId),
    deliver: async (value: import("@fungi.computer/bird-dog").BirdDogMail) => {
      mail.push(value);
      return host.acceptMail(value, value.meta);
    },
    open: () =>
      Promise.resolve({ agent: host.agent, release: () => Promise.resolve() }),
  };
  const grant = {
    agents: ["parent", "child"],
    actions: ["delegate"],
    expiresAt: Date.now() + 60_000,
    provenance: {
      actor: "parent",
      session: { agentId: "parent", sessionId: "parent-session" },
    },
  } satisfies AgentGrant;
  const task = {
    requestId: "delegate-once",
    agentId: "child",
    sessionId: "main",
    text: "Work once",
  };
  try {
    const fleet = createFleetDelegation(options);
    expect(await fleet.delegate(grant, task)).toEqual({
      kind: "pending",
      retryAfterMs: 1000,
    });
    const reopened = createFleetDelegation(options);
    expect(await reopened.delegate(grant, task)).toEqual({
      kind: "pending",
      retryAfterMs: 1000,
    });
    expect(mail).toHaveLength(1);
    const accepted = await reopened.start(grant, task);
    const meta = { author: { id: "ada", name: "Ada" } };
    await host.agent
      .withMeta(meta)
      .session("main")
      .cancel({ commandId: accepted.commandId });
    await runtime.runNext();
    expect(await reopened.delegate(grant, task)).toMatchObject({
      kind: "settled",
      agentId: "child",
      sessionId: "main",
      terminal: { commandId: accepted.commandId, outcome: "cancelled", meta },
    });
    await expect(
      reopened.delegate(grant, { ...task, text: "different work" }),
    ).rejects.toThrow("request_conflict");
  } finally {
    await host.agent.close();
    childStore.close();
    directoryStore.close();
  }
});

it("sends replayable work through granted Sessions and refuses an expired grant before admission", async () => {
  const { createFleetSessions } = await import("../src/fleet.js");
  const { Shiitake } = await import("../src/index.js");
  const { compatibilityRunDependencies, createCompatibilityRuntimeHost } =
    await import("./fixtures/native-sqlite-owner.fixture.js");
  const store = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const agent = await Shiitake.make({
    ...compatibilityRunDependencies(),
    store: store.owner,
    runtime: runtime.runtime,
  });
  const fleet = createFleetSessions((_agentId, meta) =>
    Promise.resolve({
      agent: meta === undefined ? agent : agent.withMeta(meta),
      release: () => Promise.resolve(),
    }),
  );
  const grant: AgentGrant = {
    agents: ["moss"],
    actions: ["read", "send", "manage"],
    expiresAt: Date.now() + 60_000,
    provenance: { actor: "ada" },
    meta: { host: "ada" },
  };
  try {
    await expect(
      fleet.send({ ...grant, expiresAt: 0 }, "moss", undefined, {
        requestId: "first",
        text: "Hello",
      }),
    ).rejects.toThrow("expired");
    expect((await fleet.list(grant, "moss")).sessions).toEqual([]);
    const sent = await fleet.send(grant, "moss", undefined, {
      requestId: "first",
      text: "Hello",
    });
    expect(
      await fleet.send(grant, "moss", undefined, {
        requestId: "first",
        text: "Hello",
      }),
    ).toEqual(sent);
    await runtime.runNext();
    const snapshot = await fleet.read(grant, "moss", sent.sessionId);
    expect(snapshot.terminals).toContainEqual(
      expect.objectContaining({
        commandId: sent.commandId,
        outcome: "completed",
      }),
    );
    expect(
      snapshot.messages.find(({ value }) => value.role === "user")?.meta,
    ).toEqual(grant.meta);
  } finally {
    await agent.close();
    store.close();
  }
});

it("resolves an old settled delegation after its terminal leaves the page and the selected branch", async () => {
  const { createFleetDelegation } = await import("../src/fleet.js");
  const { makeHost } = await import("../src/index.js");
  const { compatibilityRunDependencies, createCompatibilityRuntimeHost } =
    await import("./fixtures/native-sqlite-owner.fixture.js");
  const Effect = await import("effect/Effect");
  const sender = createNativeSqliteOwnerFixture();
  const child = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const host = await makeHost(
    {
      ...compatibilityRunDependencies(),
      store: child.owner,
      runtime: runtime.runtime,
    },
    { fanOut: () => Effect.void, drainOutbox: () => Effect.void },
  );
  const grant: AgentGrant = {
    agents: ["parent", "child"],
    actions: ["delegate"],
    expiresAt: Date.now() + 60_000,
    provenance: {
      actor: "parent",
      session: { agentId: "parent", sessionId: "parent-session" },
    },
  };
  const task = {
    requestId: "old-work",
    agentId: "child",
    sessionId: "main",
    text: "Complete once",
  };
  const fleet = createFleetDelegation({
    storage: sender.owner,
    deliver: (mail) => host.acceptMail(mail),
    open: () =>
      Promise.resolve({ agent: host.agent, release: () => Promise.resolve() }),
    readSettlement: (_agentId, sessionId, commandId) =>
      host.readSettlement(sessionId, commandId),
  });
  try {
    await fleet.delegate(grant, task);
    await runtime.runNext();
    const settled = await fleet.delegate(grant, task);
    expect(settled).toMatchObject({
      kind: "settled",
      terminal: { outcome: "completed" },
    });
    const first = (await host.agent.session("main").read()).messages[0];
    if (!first) throw new Error("Missing command message");
    for (let i = 0; i < 201; i++)
      await host.agent.session("main").command({
        type: "operator",
        author: "operator",
        requestId: `operator-${String(i)}`,
        text: `Later line ${String(i)}`,
      });
    expect((await host.agent.session("main").read()).terminals).toEqual([]);
    expect(await fleet.delegate(grant, task)).toEqual(settled);
    await host.agent
      .session("main")
      .command({ type: "branch", entryId: first.id });
    expect((await host.agent.session("main").read()).terminals).toEqual([]);
    expect(await fleet.delegate(grant, task)).toEqual(settled);
  } finally {
    await host.agent.close();
    child.close();
    sender.close();
  }
});

it("refuses delegation back to an ancestor before delivering work", async () => {
  const { createFleetDelegation } = await import("../src/fleet.js");
  const store = createNativeSqliteOwnerFixture();
  const delivered: string[] = [];
  const fleet = createFleetDelegation({
    storage: store.owner,
    open: () => Promise.reject(new Error("An addressed Session needs no open")),
    readSettlement: () => Promise.resolve(undefined),
    deliver: (mail) => {
      delivered.push(mail.to.agentId);
      return Promise.resolve({ status: "accepted", commandId: "child-work" });
    },
  });
  const grant: AgentGrant = {
    agents: ["a", "b", "c"],
    actions: ["delegate"],
    expiresAt: Date.now() + 60_000,
    provenance: {
      actor: "b",
      session: { agentId: "b", sessionId: "b-session" },
    },
    meta: { delegation: { ancestors: ["a"], maxDepth: 8 } },
  };
  try {
    await expect(
      fleet.start(grant, {
        requestId: "b-back-to-a",
        agentId: "a",
        sessionId: "new-a-session",
        text: "Loop",
      }),
    ).rejects.toThrow("delegation_loop");
    expect(delivered).toEqual([]);
    await expect(
      fleet.start(grant, {
        requestId: "b-to-c",
        agentId: "c",
        sessionId: "c-session",
        text: "Continue",
      }),
    ).resolves.toEqual({ status: "accepted", commandId: "child-work" });
    expect(delivered).toEqual(["c"]);
  } finally {
    store.close();
  }
});

it("inherits the delegation depth cap across handoffs and refuses work at the limit", async () => {
  const { createFleetDelegation } = await import("../src/fleet.js");
  const store = createNativeSqliteOwnerFixture();
  const mails: import("@fungi.computer/bird-dog").BirdDogMail[] = [];
  const fleet = createFleetDelegation({
    storage: store.owner,
    open: () => Promise.reject(new Error("An addressed Session needs no open")),
    readSettlement: () => Promise.resolve(undefined),
    deliver: (mail) => {
      mails.push(mail);
      return Promise.resolve({ status: "accepted", commandId: "work" });
    },
  });
  const grant: AgentGrant = {
    agents: ["a", "b", "c", "d"],
    actions: ["delegate"],
    expiresAt: Date.now() + 60_000,
    provenance: {
      actor: "b",
      session: { agentId: "b", sessionId: "b-session" },
    },
    meta: { delegation: { ancestors: ["a"], maxDepth: 2 } },
  };
  try {
    await fleet.start(grant, {
      requestId: "within-limit",
      agentId: "c",
      sessionId: "c-session",
      text: "Last admitted hop",
    });
    expect(mails).toHaveLength(1);
    const admitted = mails[0];
    if (!admitted) throw new Error("Missing admitted mail");
    const reply: import("@fungi.computer/bird-dog").BirdDogMail = {
      key: `${admitted.key}:reply:0`,
      from: admitted.to,
      to: admitted.from,
      replyTo: [],
      content: "Work cancelled by Ada.",
      createdAt: Date.now(),
      reply: { commandId: "work", outcome: "cancelled" },
    };
    const reopened = createFleetDelegation({
      storage: store.owner,
      open: () =>
        Promise.reject(new Error("Reply context requires no transport")),
      readSettlement: () => Promise.resolve(undefined),
      deliver: () =>
        Promise.reject(new Error("Reply context requires no transport")),
    });
    // The reply is the child's: authored by the replying Session, correlated
    // to the send receipt, never by whoever cancelled it.
    expect(reopened.authenticateReply(reply)).toEqual({
      ancestors: ["a"],
      maxDepth: 2,
      reply: {
        agentId: "c",
        sessionId: "c-session",
        commandId: "work",
        outcome: "cancelled",
      },
    });
    const withoutFacts: import("@fungi.computer/bird-dog").BirdDogMail = {
      key: reply.key,
      from: reply.from,
      to: reply.to,
      replyTo: reply.replyTo,
      content: reply.content,
      createdAt: reply.createdAt,
    };
    const forgeries: import("@fungi.computer/bird-dog").BirdDogMail[] = [
      { ...reply, from: { ...reply.from, sessionId: "unrelated" } },
      { ...reply, reply: { commandId: "other-work", outcome: "completed" } },
      { ...reply, key: `${admitted.key}:reply:1` },
      withoutFacts,
    ];
    for (const forged of forgeries)
      expect(reopened.authenticateReply(forged)).toBeUndefined();
    await expect(
      fleet.start(
        agentGrantSchema.parse({
          ...grant,
          maxDepth: 8,
          provenance: {
            actor: "c",
            session: { agentId: "c", sessionId: "c-session" },
          },
          meta: admitted.meta,
        }),
        {
          requestId: "over-limit",
          agentId: "d",
          sessionId: "d-session",
          text: "Cannot widen inherited cap",
        },
      ),
    ).rejects.toThrow("delegation_depth");
    expect(mails).toHaveLength(1);
  } finally {
    store.close();
  }
});
