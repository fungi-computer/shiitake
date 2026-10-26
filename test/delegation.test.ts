import { expect, it } from "vitest";
import {
  createAgentDirectory,
  createFleetCorrespondence,
  type AgentGrant,
} from "../src/fleet.js";
import {
  AgentDelegation,
  machineAgentCodecs,
  machineAgentReservations,
} from "../src/delegation.js";
import type { BirdDogMail } from "@fungi.computer/bird-dog";
import { BirdDogMailSchema } from "@fungi.computer/bird-dog/effect";
import * as Schema from "effect/Schema";
import type { AgentControlDirectory } from "../src/agent-control.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";
import { memoryHost } from "@fungi.computer/shiitake/testing";

it("lends the machine link Shiitake's own Session-creation and receipt grammar", () => {
  expect(machineAgentCodecs.sessionCreation({ requestId: "create-1" })).toEqual(
    { requestId: "create-1" },
  );
  expect(
    machineAgentCodecs.sessionCreation({ requestId: "create-1", extra: true }),
  ).toBeNull();
  expect(machineAgentCodecs.sessionCreation({ requestId: "" })).toBeNull();
  expect(() => {
    machineAgentCodecs.accepted({ status: "accepted", commandId: "work-1" });
  }).not.toThrow();
  expect(() => {
    machineAgentCodecs.accepted({
      status: "held",
      reason: "recipient_offline",
      message: "waiting: recipient offline",
    });
  }).not.toThrow();
  expect(() => {
    machineAgentCodecs.accepted({
      status: "held",
      reason: "recipient_offline",
    });
  }).toThrow();
  expect(() => {
    machineAgentCodecs.accepted({ status: "accepted", requestId: "caller-1" });
  }).toThrow();
  expect(() =>
    machineAgentCodecs.settlement('{"status":"accepted"}'),
  ).toThrow();
});

it("reserves one directory identity per enrollment receipt, created by the enrolling member", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    const reserve = machineAgentReservations(fixture.owner.sql);
    const reservation = {
      memberId: "member-1",
      requestId: "device-1:0",
      name: "Laptop Agent",
    };
    const first = reserve(reservation);
    expect(reserve(reservation)).toEqual(first);
    expect(() => reserve({ ...reservation, name: "Other" })).toThrow(
      "request_conflict",
    );
    const reader: AgentGrant = {
      agents: "all",
      actions: ["read"],
      expiresAt: Date.now() + 60_000,
      provenance: { actor: "auditor" },
    };
    const directory = createAgentDirectory(fixture.owner.sql);
    expect(directory.list(reader).map(({ agentId }) => agentId)).toEqual([
      first.agentId,
    ]);
    expect(
      directory.readCreator(
        reader,
        directory.list(reader)[0]?.agentId ?? first.agentId,
      ),
    ).toBe("member-1");
  } finally {
    fixture.close();
  }
});

it("accepts recipient_left from the frozen correspondent after directory removal, but refuses changed authors and departed completed replies", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const parent = { agentId: "parent", name: "Parent" };
  const child = { agentId: "child", name: "Child" };
  const mailboxes = new Map<
    string,
    import("@fungi.computer/bird-dog").BirdDogMail
  >();
  const sender = createFleetCorrespondence({
    storage: fixture.owner,
    open: () => Promise.reject(new Error("unused")),
    deliver: (mail) => {
      mailboxes.set(mail.key, mail);
      return Promise.resolve({
        status: "accepted",
        commandId: "accepted-command",
      });
    },
  });
  const grant: AgentGrant = {
    agents: ["parent", "child"],
    actions: ["delegate"],
    expiresAt: Date.now() + 60_000,
    provenance: {
      actor: "parent",
      session: { agentId: "parent", sessionId: "parent-session" },
    },
  };
  const original = await sender.start(grant, {
    requestId: "left-child-request",
    agentId: child.agentId,
    sessionId: "child-session",
    text: "Work",
  });
  if (original.status !== "accepted")
    throw new Error("recipient was expected to accept mail");
  const sent = mailboxes.get("left-child-request");
  expect(original.commandId).toBe("accepted-command");
  expect(sent).toBeDefined();
  if (sent === undefined) throw new Error("send was not recorded");

  let delivered = 0;
  const roster = [parent];
  const directory: AgentControlDirectory = {
    list: () => Promise.resolve(roster),
    open: () =>
      Promise.reject(new Error("no Agent should be opened for a reply")),
  };
  const receiver = new AgentDelegation({
    storage: fixture.owner,
    expiresAt: () => Date.now() + 60_000,
    identity: () => ({ scopeId: "scope", agentId: parent.agentId }),
    directory: () => directory,
    deliver: () => Promise.resolve(original),
    prepare: () => Promise.resolve(),
    stageAcceptedNotice: () => undefined,
    accept: () => {
      delivered += 1;
      return Promise.resolve(original);
    },
  });
  const envelope = (event: BirdDogMail) => ({
    key: event.key,
    kind: "agentMail" as const,
    address: { scopeId: "scope", agentId: parent.agentId },
    event,
  });

  const left = Schema.decodeUnknownSync(BirdDogMailSchema)({
    key: `${sent.key}:reply:0`,
    to: sent.from,
    from: sent.to,
    replyTo: [],
    content: "Not delivered: recipient left.",
    createdAt: Date.now(),
    reply: { outcome: "undeliverable", reason: "recipient_left" },
  });
  expect(await receiver.receive(envelope(left))).toMatchObject({
    status: "accepted",
  });
  expect(delivered).toBe(1);

  expect(
    await receiver.receive(
      envelope(
        Schema.decodeUnknownSync(BirdDogMailSchema)({
          ...left,
          from: { ...left.from, agentId: "another-agent" },
        }),
      ),
    ),
  ).toBe("refused");

  const completed = {
    ...left,
    from: {
      kind: "agent" as const,
      agentId: "child",
      sessionId: "child-session",
    },
    content: "Done",
    reply: { commandId: "accepted-command", outcome: "completed" as const },
  };
  expect(await receiver.receive(envelope(completed))).toBe("refused");
  fixture.close();
});

it("recalls accepted sessionless hosted work by its returned Session", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const target = await memoryHost();
  const sessionId = await target.agent.createSession("hosted-sessionless");
  const agents = [
    { agentId: "sender", name: "Sender", createdBy: null },
    { agentId: "recipient", name: "Recipient", createdBy: null },
  ];
  const directory: AgentControlDirectory = {
    list: () => Promise.resolve(agents),
    open: () =>
      Promise.resolve({
        agent: target.agent,
        release: () => Promise.resolve(),
      }),
  };
  const sender = new AgentDelegation({
    storage: fixture.owner,
    expiresAt: () => Date.now() + 60_000,
    identity: () => ({ scopeId: "team", agentId: "sender" }),
    directory: () => directory,
    deliver: async (mail) => {
      if (mail.to.sessionId !== undefined)
        throw new Error("This test sends sessionless work");
      const accepted = await target.agent
        .session(sessionId)
        .send(mail.content, {
          requestId: mail.key,
          delivery: "followUp",
        });
      return { ...accepted, sessionId };
    },
    prepare: () => Promise.resolve(),
    stageAcceptedNotice: () => undefined,
    accept: () => Promise.reject(new Error("This test receives no replies")),
  });
  try {
    const receipt = await sender.send(
      {
        agentId: "recipient",
        requestId: "hosted-sessionless-request",
        text: "Work once",
        delivery: "followUp",
      },
      "sender-session",
      new AbortController().signal,
      { toolCallId: "call-1", sendIndex: 0 },
    );
    if (receipt.status !== "accepted")
      throw new Error("Sessionless hosted work was not accepted");
    expect(receipt.sessionId).toBe(sessionId);
    expect(
      await sender.recall(
        {
          agentId: "recipient",
          sessionId,
          requestId: "hosted-sessionless-request",
        },
        "sender-session",
        new AbortController().signal,
      ),
    ).toMatchObject({ status: "accepted" });
    expect(
      (await target.agent.session(sessionId).read()).pending.prompts,
    ).toEqual([]);
  } finally {
    await target.close();
    fixture.close();
  }
});
