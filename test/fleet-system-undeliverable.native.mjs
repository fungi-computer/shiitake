import assert from "node:assert/strict";
import { test } from "node:test";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.ts";
import { createFleetCorrespondence } from "../src/fleet.ts";

const grant = () => ({
  agents: ["parent", "child"],
  actions: ["delegate"],
  expiresAt: Date.now() + 60_000,
  provenance: {
    actor: "parent",
    session: { agentId: "parent", sessionId: "parent-session" },
  },
});
const task = {
  requestId: "system-undeliverable-request",
  agentId: "child",
  text: "Work once",
};
const held = {
  status: "held",
  reason: "recipient_offline",
  message: "waiting: recipient offline",
};

await test("held sender mail admits only its exact Hub/system offline reply", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const sender = createFleetCorrespondence({
    storage: fixture.owner,
    open: () => Promise.reject(new Error("unused")),
    deliver: () => Promise.resolve(held),
  });
  try {
    assert.deepEqual(await sender.start(grant(), task), held);
    const reply = {
      key: `${task.requestId}:reply:0`,
      from: { kind: "system", systemId: "hub" },
      to: {
        kind: "agent",
        agentId: "parent",
        sessionId: "parent-session",
      },
      replyTo: [],
      content: "undeliverable",
      createdAt: Date.now(),
      reply: { outcome: "undeliverable", reason: "recipient_offline" },
    };

    assert.deepEqual(sender.authenticateReply(reply)?.reply, {
      agentId: "child",
      outcome: "undeliverable",
      reason: "recipient_offline",
    });
    assert.equal(
      sender.authenticateReply({
        ...reply,
        key: "not-the-sent-mail:reply:0",
      }),
      undefined,
    );
    assert.equal(
      sender.authenticateReply({
        ...reply,
        from: { kind: "agent", agentId: "child", sessionId: "child-session" },
      }),
      undefined,
      "an offline outcome is authored by Hub/system, never by the recipient",
    );
  } finally {
    fixture.close();
  }
});
