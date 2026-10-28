import { expect, it } from "vitest";
import { z } from "zod";
import { messageAuthor } from "@fungi.computer/agar/agents";
import {
  createAgentDirectory,
  createFleetCorrespondence,
  AgentGrantError,
  agentGrantSchema,
  agentIds,
  delegationReplyMeta,
  delegationSendMeta,
  settlementReadableBy,
  type AgentGrant,
} from "../src/fleet.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

it("createdBy is immutable creation identity, never the human credential owner", () => {
  const fixture = createNativeSqliteOwnerFixture();
  const grant: AgentGrant = {
    agents: "all",
    actions: ["create", "read", "manage"],
    expiresAt: Date.now() + 60_000,
    provenance: { actor: "human-owner" },
  };
  try {
    const directory = createAgentDirectory(fixture.owner.sql);
    const parent = directory.create(grant, {
      requestId: "parent",
      name: "Parent",
      configuration: "none",
    }).agent;
    expect(parent).toMatchObject({ createdBy: null });
    const input = {
      requestId: "child",
      name: "Child",
      configuration: "none",
      createdBy: parent.agentId,
    };
    const child = directory.create(grant, input).agent;
    expect(child).toMatchObject({ createdBy: parent.agentId });
    expect(directory.readCreator(grant, child.agentId)).toBe("human-owner");
    const reopened = createAgentDirectory(fixture.owner.sql);
    expect(reopened.create(grant, input)).toEqual({ agent: child, fact: null });
    expect(() => reopened.create(grant, { ...input, createdBy: null })).toThrow(
      "request_conflict",
    );
    expect(
      reopened.rename(grant, child.agentId, { name: "Renamed" }),
    ).toMatchObject({ createdBy: parent.agentId });
    expect(reopened.readCreator(grant, child.agentId)).toBe("human-owner");
  } finally {
    fixture.close();
  }
});

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
    {
      fanOut: () => Effect.void,
      postMail: () => Effect.void,
      drainOutbox: () => Effect.void,
      nextWakeAt: () => Effect.succeed(undefined),
    },
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
    if (accepted.status !== "accepted")
      throw new Error("recipient was expected to accept mail");
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
    {
      fanOut: () => Effect.void,
      postMail: () => Effect.void,
      drainOutbox: () => Effect.void,
      nextWakeAt: () => Effect.succeed(undefined),
    },
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

it("admits an authorized ancestor as a peer while refusing self-delegation", async () => {
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
        text: "Return work to a peer",
      }),
    ).resolves.toEqual({
      status: "accepted",
      commandId: "child-work",
      sessionId: "new-a-session",
    });
    expect(delivered).toEqual(["a"]);
    await expect(
      fleet.start(grant, {
        requestId: "b-to-self",
        agentId: "b",
        sessionId: "another-b-session",
        text: "Self-delegation",
      }),
    ).rejects.toThrow("delegation_loop");
    await expect(
      fleet.start(grant, {
        requestId: "b-to-c",
        agentId: "c",
        sessionId: "c-session",
        text: "Continue",
      }),
    ).resolves.toEqual({
      status: "accepted",
      commandId: "child-work",
      sessionId: "c-session",
    });
    expect(delivered).toEqual(["a", "c"]);
  } finally {
    store.close();
  }
});

it("bounds ancestor peer ping-pong at the inherited depth limit with a typed refusal", async () => {
  const a = createNativeSqliteOwnerFixture();
  const b = createNativeSqliteOwnerFixture();
  const delivered: import("@fungi.computer/bird-dog").BirdDogMail[] = [];
  const correspondence = (store: typeof a) =>
    createFleetCorrespondence({
      storage: store.owner,
      open: () => Promise.reject(new Error("The Session is already addressed")),
      deliver: (mail) => {
        delivered.push(mail);
        return Promise.resolve({
          status: "accepted",
          commandId: `work-${String(delivered.length)}`,
        });
      },
    });
  const fleets = { a: correspondence(a), b: correspondence(b) };
  let sender: "a" | "b" = "a";
  let meta: AgentGrant["meta"];
  const next = (index: number) => {
    const target: "a" | "b" = sender === "a" ? "b" : "a";
    let grant: AgentGrant = {
      agents: ["a", "b"],
      actions: ["delegate"],
      expiresAt: Date.now() + 60_000,
      provenance: {
        actor: sender,
        session: { agentId: sender, sessionId: `${sender}-session` },
      },
    };
    if (meta !== undefined) grant = { ...grant, meta };
    return {
      target,
      grant,
      task: {
        requestId: `handoff-${String(index)}`,
        agentId: target,
        sessionId: `${target}-session`,
        text: "Ordinary peer work",
      },
    };
  };
  try {
    // No caller-supplied depth increase: each hop inherits the actual frozen
    // mail from its peer, and the existing default is eight handoffs.
    for (let index = 0; index < 8; index += 1) {
      const { target, grant, task } = next(index);
      await expect(fleets[sender].start(grant, task)).resolves.toMatchObject({
        status: "accepted",
      });
      meta = z.json().parse(delivered.at(-1)?.meta);
      sender = target;
    }
    expect(delivered.map((mail) => mail.to.agentId)).toEqual([
      "b",
      "a",
      "b",
      "a",
      "b",
      "a",
      "b",
      "a",
    ]);
    const { grant, task } = next(8);
    const refusal = fleets[sender].start(grant, task);
    await expect(refusal).rejects.toBeInstanceOf(AgentGrantError);
    await expect(refusal).rejects.toMatchObject({ reason: "delegation_depth" });
    expect(delivered).toHaveLength(8);
  } finally {
    a.close();
    b.close();
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
      from: {
        kind: "agent",
        agentId: "c",
        sessionId: "c-session",
      },
      to: {
        kind: "agent",
        agentId: "b",
        sessionId: "b-session",
      },
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
      from: {
        kind: "agent",
        agentId: "c",
        sessionId: "c-session",
      },
      to: reply.to,
      replyTo: reply.replyTo,
      content: reply.content,
      createdAt: reply.createdAt,
    };
    const forgeries: import("@fungi.computer/bird-dog").BirdDogMail[] = [
      {
        ...reply,
        from: { kind: "agent", agentId: "c", sessionId: "unrelated" },
      },
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

const researcher = { id: "agent_research", name: "Research agent" };
const verifiedReply = {
  agentId: "agent_research",
  sessionId: "session_research",
  commandId: "command_research",
  outcome: "completed",
} as const;

it("stamps a delegated send as another Agent's message, with and without an inherited path (kind: agent)", () => {
  const inheriting = delegationSendMeta(researcher, {
    delegation: { ancestors: ["agent_root"], maxDepth: 4 },
  });
  const fresh = delegationSendMeta(researcher);
  expect(fresh).toEqual({ author: { ...researcher, kind: "agent" } });
  expect(inheriting.author).toEqual({ ...researcher, kind: "agent" });
  expect(inheriting.delegation).toEqual({
    ancestors: ["agent_root"],
    maxDepth: 4,
  });
  expect(messageAuthor(fresh)).toEqual({ ...researcher, kind: "agent" });
  expect(messageAuthor(inheriting)?.kind).toBe("agent");
});

it("stamps an authenticated delegation reply as another Agent's message (kind: agent)", () => {
  const meta = delegationReplyMeta(
    { ancestors: ["agent_root"], maxDepth: 4, reply: verifiedReply },
    "Research agent",
  );
  expect(meta.author).toEqual({ ...researcher, kind: "agent" });
  expect(messageAuthor(meta)).toEqual({ ...researcher, kind: "agent" });
});

it("a human line that says '↳ Research agent replied' and shares the Agent's name does not read as an Agent (human lines unchanged)", () => {
  // Only the host's metadata sets the kind; the line's text never does.
  const human = { author: { id: "visitor_ada", name: "Research agent" } };
  expect(messageAuthor(human)?.kind).toBeUndefined();
});

it("reads the existing correspondence receipt by request ID across reopen without sending again", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  let deliveries = 0;
  const options = {
    storage: fixture.owner,
    open: () => Promise.reject(new Error("an addressed Session needs no open")),
    deliver: () => {
      deliveries += 1;
      return Promise.resolve({
        status: "accepted" as const,
        commandId: "owner-issued-work",
      });
    },
  };
  const grant: AgentGrant = {
    agents: ["parent", "child"],
    actions: ["delegate"],
    expiresAt: Date.now() + 60_000,
    provenance: {
      actor: "parent",
      session: { agentId: "parent", sessionId: "parent-main" },
    },
  };
  const task = {
    requestId: "sender-request",
    agentId: "child",
    sessionId: "child-main",
    text: "Work once",
  };
  try {
    const correspondence = createFleetCorrespondence(options);
    // Reading an unknown key is read-only: it neither reserves nor fences it.
    expect(correspondence.receipt(task.requestId)).toBeUndefined();
    const accepted = await correspondence.start(grant, task);
    if (accepted.status !== "accepted")
      throw new Error("recipient was expected to accept mail");
    expect(correspondence.receipt(task.requestId)).toEqual(accepted);
    const reopened = createFleetCorrespondence(options);
    expect(reopened.receipt(task.requestId)).toEqual({
      status: "accepted",
      commandId: "owner-issued-work",
      sessionId: "child-main",
    });
    expect(reopened.receipt("other-request")).toBeUndefined();
    expect(await reopened.start(grant, task)).toEqual(accepted);
    expect(deliveries).toBe(1);
  } finally {
    fixture.close();
  }
});

it("a delegator reads only settlements no other Agent's mail admitted", () => {
  // Its own mail, or a goal, loop or person turn in the delegated Session.
  expect(
    settlementReadableBy(
      { acceptedSender: "first", delegator: "first" },
      "first",
    ),
  ).toBe(true);
  expect(
    settlementReadableBy({ acceptedSender: null, delegator: "first" }, "first"),
  ).toBe(true);
  // A later sender's correspondence stays with that sender.
  expect(
    settlementReadableBy(
      { acceptedSender: "later", delegator: "first" },
      "first",
    ),
  ).toBe(false);
  expect(
    settlementReadableBy(
      { acceptedSender: "later", delegator: "first" },
      "later",
    ),
  ).toBe(true);
  expect(
    settlementReadableBy({ acceptedSender: null, delegator: "first" }, "later"),
  ).toBe(false);
});
