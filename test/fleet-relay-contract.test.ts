import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import type { BirdDogMail } from "@fungi.computer/bird-dog";
import {
  createFleetCorrespondence,
  type AgentDelegationTask,
  type AgentGrant,
} from "../src/fleet.js";
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
