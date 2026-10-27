import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import type { BirdDogMail } from "@fungi.computer/bird-dog";
import { acceptedNotice } from "@fungi.computer/bird-dog/effect";
import {
  createFleetCorrespondence,
  delegationReplyMeta,
  type AgentDelegationTask,
  type AgentGrant,
} from "../src/fleet.js";
import type { DelegationReceipt } from "../src/client.js";
import { createFleetDelegationStore } from "../src/internal/woodstock/fleet-store.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

// 5-C: Wiki computer-execution-backends, "Offline custody is not command
// acceptance". These are sender laws, not an implementation of the relay.
const cleanups: (() => void)[] = [];
afterEach(() => {
  for (const cleanup of cleanups.splice(0).reverse()) cleanup();
});

function unreachableTransport() {
  const directory = mkdtempSync(join(tmpdir(), "shiitake-fleet-relay-"));
  const database = join(directory, "sender.sqlite");
  let fixture = createNativeSqliteOwnerFixture(database);
  cleanups.push(() => {
    fixture.close();
    rmSync(directory, { recursive: true });
  });
  const opens: string[] = [];
  const deliveries: BirdDogMail[] = [];
  const correspondence = () =>
    createFleetCorrespondence({
      storage: fixture.owner,
      open: (agentId) => {
        opens.push(agentId);
        return Promise.reject(new Error("transport_unavailable"));
      },
      deliver: (mail) => {
        deliveries.push(structuredClone(mail));
        return Promise.reject(new Error("transport_unavailable"));
      },
    });
  return {
    opens,
    deliveries,
    correspondence,
    reopen: () => {
      fixture.close();
      fixture = createNativeSqliteOwnerFixture(database);
      return correspondence();
    },
  };
}

function grant(): AgentGrant {
  return {
    agents: ["parent", "child", "other-child"],
    actions: ["delegate"],
    expiresAt: Date.now() + 60_000,
    provenance: {
      actor: "parent",
      session: { agentId: "parent", sessionId: "parent-session" },
    },
  };
}

const task: AgentDelegationTask = {
  requestId: "one-submission",
  agentId: "child",
  text: "Work once",
};

it("records relay custody without claiming acceptance, and authenticates later replies without redelivery", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const sent: BirdDogMail[] = [];
  let deliveries = 0;
  const held = {
    status: "held" as const,
    reason: "recipient_offline" as const,
    message: "waiting: recipient offline" as const,
  };
  const sender = createFleetCorrespondence({
    storage: fixture.owner,
    open: () => Promise.reject(new Error("unused")),
    deliver: (mail) => {
      deliveries += 1;
      sent.push(mail);
      return Promise.resolve(held);
    },
  });
  const authority = grant();
  const addressed = { ...task, sessionId: "child-session" };

  expect(await sender.start(authority, addressed)).toEqual(held);
  expect(await sender.start(authority, addressed)).toEqual(held);
  expect(deliveries).toBe(1);
  const reopened = createFleetCorrespondence({
    storage: fixture.owner,
    open: () => Promise.reject(new Error("unused")),
    deliver: () => {
      deliveries += 1;
      return Promise.resolve({ status: "accepted", commandId: "unexpected" });
    },
  });
  expect(await reopened.start(authority, addressed)).toEqual(held);
  expect(deliveries).toBe(1);

  const original = sent[0];
  if (original === undefined) throw new Error("held mail was not recorded");
  const completed = {
    key: `${original.key}:reply:0`,
    from: {
      kind: "agent" as const,
      agentId: "child",
      sessionId: "child-session",
    },
    to: {
      kind: "agent" as const,
      agentId: "parent",
      sessionId: "parent-session",
    },
    replyTo: [],
    content: "Work completed",
    createdAt: Date.now(),
    reply: { commandId: "recipient-command", outcome: "completed" as const },
  };
  expect(sender.authenticateReply(completed)).toMatchObject({
    reply: {
      commandId: "recipient-command",
      outcome: "completed",
      agentId: "child",
      sessionId: "child-session",
    },
  });
  fixture.close();
});

it("freezes machine-home mail without a recipient Session when the Agent is offline", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const held: DelegationReceipt = {
    status: "held",
    reason: "recipient_offline",
    message: "waiting: recipient offline",
  };
  let opens = 0;
  const deliveries: BirdDogMail[] = [];
  const sender = createFleetCorrespondence({
    storage: fixture.owner,
    open: () => {
      opens += 1;
      throw new Error("session creation must wait for recipient acceptance");
    },
    deliver: (mail) => {
      deliveries.push(structuredClone(mail));
      return Promise.resolve(held);
    },
  });
  const authority = grant();

  expect(await sender.start(authority, task)).toEqual(held);
  expect(await sender.start(authority, task)).toEqual(held);
  expect(opens).toBe(0);
  expect(deliveries).toHaveLength(1);
  expect(deliveries[0]?.to).toEqual({
    kind: "agent",
    agentId: "child",
  });
  const original = deliveries[0];
  if (original === undefined) throw new Error("frozen mail was not delivered");
  if (original.from.kind !== "agent")
    throw new Error("frozen mail did not retain its Agent sender");
  const notice = acceptedNotice(original, "late-session", "command-1", 1);
  expect(sender.recordAcceptedNotice(notice)).toEqual({
    status: "accepted",
    commandId: "command-1",
    sessionId: "late-session",
  });
  expect(sender.recordAcceptedNotice(notice)).toEqual({
    status: "accepted",
    commandId: "command-1",
    sessionId: "late-session",
  });
  expect(sender.receipt(task.requestId)).toEqual({
    status: "accepted",
    commandId: "command-1",
    sessionId: "late-session",
  });
  expect(
    sender.authenticateReply({
      key: `${task.requestId}:reply:0`,
      to: original.from,
      from: { kind: "agent", agentId: "child", sessionId: "late-session" },
      replyTo: [],
      content: "Done",
      createdAt: 2,
      reply: { outcome: "completed", commandId: "command-1" },
    }),
  ).toMatchObject({ reply: { sessionId: "late-session" } });
  expect(
    sender.recordAcceptedNotice(
      acceptedNotice(original, "other-session", "command-2", 3),
    ),
  ).toBeUndefined();
  fixture.close();
});

it("admits only the frozen sender's typed undeliverable reply without a live child Session", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const deliveries: BirdDogMail[] = [];
  const held: DelegationReceipt = {
    status: "held",
    reason: "recipient_offline",
    message: "waiting: recipient offline",
  };
  const sender = createFleetCorrespondence({
    storage: fixture.owner,
    open: () => Promise.reject(new Error("unused")),
    deliver: (mail) => {
      deliveries.push(structuredClone(mail));
      return Promise.resolve(held);
    },
  });
  try {
    expect(
      await sender.start(grant(), { ...task, sessionId: "late-session" }),
    ).toEqual(held);
    const original = deliveries[0];
    if (original === undefined)
      throw new Error("frozen mail was not delivered");
    if (original.from.kind !== "agent")
      throw new Error("frozen mail did not retain its Agent sender");
    const reply: BirdDogMail = {
      key: `${original.key}:reply:0`,
      from: { kind: "system", systemId: "hub" },
      to: original.from,
      replyTo: [],
      content: "undeliverable",
      createdAt: Date.now(),
      reply: { outcome: "undeliverable", reason: "recipient_offline" },
    };

    const authenticated = sender.authenticateReply(reply);
    expect(authenticated).toMatchObject({
      reply: {
        agentId: "child",
        outcome: "undeliverable",
        reason: "recipient_offline",
      },
    });
    if (authenticated === undefined)
      throw new Error("held mail's undeliverable reply was not authenticated");
    const meta = delegationReplyMeta(authenticated, "Child");
    expect(meta.author).toEqual({ id: "hub/system", name: "Hub/system" });
    expect(meta.delegation.reply).toMatchObject({
      agentId: "child",
      outcome: "undeliverable",
      reason: "recipient_offline",
    });
    const recipientLeft = {
      ...reply,
      from: {
        kind: "agent" as const,
        agentId: "child",
        sessionId: "late-session",
      },
      reply: {
        outcome: "undeliverable" as const,
        reason: "recipient_left" as const,
      },
    };
    const authenticatedLeft = sender.authenticateReply(recipientLeft);
    expect(authenticatedLeft).toMatchObject({
      reply: {
        agentId: "child",
        outcome: "undeliverable",
        reason: "recipient_left",
      },
    });
    if (authenticatedLeft === undefined)
      throw new Error("recipient-left reply lost the existing Agent path");
    expect(delegationReplyMeta(authenticatedLeft, "Child").author).toEqual({
      id: "child",
      name: "Child",
      kind: "agent",
    });
    expect(sender.recordUndeliverableReply(original.key)).toBe(true);
    expect(sender.authenticateReply(reply)).toBeUndefined();
    expect(sender.recordUndeliverableReply(original.key)).toBe(false);
    expect(
      sender.authenticateReply({
        ...reply,
        key: "unknown-mail:reply:0",
      }),
    ).toBeUndefined();
    expect(
      sender.authenticateReply({
        ...reply,
        to: { ...original.from, agentId: "other-sender" },
      }),
    ).toBeUndefined();
  } finally {
    fixture.close();
  }
});

it("refuses an undeliverable reply unless its exact original mail is still held", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const sent: BirdDogMail[] = [];
  const sender = createFleetCorrespondence({
    storage: fixture.owner,
    open: () => Promise.reject(new Error("unused")),
    deliver: (mail) => {
      sent.push(structuredClone(mail));
      return Promise.reject(new Error("transport failed before Held custody"));
    },
  });
  try {
    await expect(sender.start(grant(), task)).rejects.toThrow(
      "transport failed before Held custody",
    );
    const original = sent[0];
    if (original === undefined) throw new Error("original mail was not sent");
    if (original.from.kind !== "agent")
      throw new Error("frozen mail did not retain its Agent sender");
    const reply: BirdDogMail = {
      key: `${original.key}:reply:0`,
      from: { kind: "system", systemId: "hub" },
      to: original.from,
      replyTo: [],
      content: "undeliverable",
      createdAt: Date.now(),
      reply: { outcome: "undeliverable", reason: "recipient_offline" },
    };
    expect(sender.authenticateReply(reply)).toBeUndefined();
    expect(
      sender.authenticateReply({
        ...reply,
        key: "different-original:reply:0",
      }),
    ).toBeUndefined();
  } finally {
    fixture.close();
  }
});

it("converges a sessionless acceptance receipt across a physical sender reopen", async () => {
  const directory = mkdtempSync(join(tmpdir(), "shiitake-accepted-reopen-"));
  const database = join(directory, "sender.sqlite");
  let fixture = createNativeSqliteOwnerFixture(database);
  const authority = grant();
  const request = {
    ...task,
    requestId: "sessionless-restart-convergence",
  };
  const deliveries: BirdDogMail[] = [];
  const held: DelegationReceipt = {
    status: "held",
    reason: "recipient_offline",
    message: "waiting: recipient offline",
  };
  const correspondence = (redeliver: boolean) =>
    createFleetCorrespondence({
      storage: fixture.owner,
      open: () => Promise.reject(new Error("unused")),
      deliver: (mail) => {
        if (!redeliver)
          throw new Error("Accepted mail must not be redelivered");
        deliveries.push(structuredClone(mail));
        return Promise.resolve(held);
      },
    });

  try {
    const sender = correspondence(true);
    expect(await sender.start(authority, request)).toEqual(held);
    expect(deliveries).toHaveLength(1);
    const original = deliveries[0];
    if (original === undefined)
      throw new Error("frozen mail was not delivered");
    if (original.from.kind !== "agent")
      throw new Error("frozen mail did not retain its Agent sender");
    expect(original.to).toEqual({ kind: "agent", agentId: "child" });
    const notice = acceptedNotice(
      original,
      "machine-created-session",
      "recipient-command",
      2,
    );
    const accepted = {
      status: "accepted" as const,
      commandId: "recipient-command",
      sessionId: "machine-created-session",
    };
    expect(sender.recordAcceptedNotice(notice)).toEqual(accepted);

    fixture.close();
    fixture = createNativeSqliteOwnerFixture(database);
    const reopened = correspondence(false);
    expect(reopened.receipt(request.requestId)).toEqual(accepted);
    expect(reopened.recordAcceptedNotice(notice)).toEqual(accepted);
    expect(await reopened.start(authority, request)).toEqual(accepted);
    expect(deliveries).toHaveLength(1);
    expect(
      reopened.authenticateReply({
        key: `${request.requestId}:reply:0`,
        to: original.from,
        from: {
          kind: "agent",
          agentId: "child",
          sessionId: "machine-created-session",
        },
        replyTo: [],
        content: "Completed after recipient restart",
        createdAt: 3,
        reply: { outcome: "completed", commandId: "recipient-command" },
      }),
    ).toMatchObject({
      reply: {
        commandId: "recipient-command",
        sessionId: "machine-created-session",
      },
    });
  } finally {
    fixture.close();
    rmSync(directory, { recursive: true, force: true });
  }
});

it("keeps the existing delegation rows unchanged and stores Held custody separately", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    fixture.owner.sql.exec(`CREATE TABLE shiitake_fleet_delegations (
      request_id TEXT PRIMARY KEY,
      input TEXT NOT NULL,
      mail TEXT NOT NULL,
      receipt TEXT
    ) STRICT`);
    fixture.owner.sql.exec(
      "INSERT INTO shiitake_fleet_delegations (request_id, input, mail, receipt) VALUES (?, ?, ?, ?)",
      "legacy-accepted",
      '{"input":"frozen"}',
      '{"key":"legacy-accepted"}',
      '{"status":"accepted","commandId":"legacy-command"}',
    );

    const store = createFleetDelegationStore(fixture.owner);
    const reopened = createFleetDelegationStore(fixture.owner);
    expect(store.read("legacy-accepted")).toMatchObject({
      state: "accepted",
      receipt: '{"status":"accepted","commandId":"legacy-command"}',
    });
    expect(reopened.sent()).toHaveLength(1);
    expect(
      fixture.owner.sql
        .exec("PRAGMA table_info(shiitake_fleet_delegations)")
        .toArray()
        .map(({ name }) => name),
    ).toEqual(["request_id", "input", "mail", "receipt"]);
    expect(
      fixture.owner.sql
        .exec("SELECT request_id FROM shiitake_fleet_held_delegations")
        .toArray(),
    ).toEqual([]);

    store.reserve("held-request", '{"input":"held"}', '{"key":"held-request"}');
    store.recordHeld("held-request");
    expect(store.read("held-request")).toMatchObject({
      state: "held",
      receipt: null,
    });
    expect(store.read("legacy-accepted")).toMatchObject({
      state: "accepted",
      receipt: '{"status":"accepted","commandId":"legacy-command"}',
    });
  } finally {
    fixture.close();
  }
});

it("5-C C2 retained control: an addressed send keeps its intent across a physical SQLite reopen", async () => {
  const sender = unreachableTransport();
  const authority = grant();
  const addressed = { ...task, sessionId: "known-session" };
  await expect(
    sender.correspondence().start(authority, addressed),
  ).rejects.toThrow("transport_unavailable");
  await expect(
    sender.reopen().start(authority, {
      ...addressed,
      text: "Different work",
    }),
  ).rejects.toThrow("request_conflict");
  expect(sender.opens).toEqual([]);
  expect(sender.deliveries).toHaveLength(1);
});
