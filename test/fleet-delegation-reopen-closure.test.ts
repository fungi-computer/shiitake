import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath, URL } from "node:url";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { fauxModel } from "@fungi.computer/mule/testing";
import * as Effect from "effect/Effect";
import { expect, it } from "vitest";

import { createFleetDelegation, type AgentGrant } from "../src/fleet.js";
import { makeHost, type ShiitakeHost } from "../src/index.js";
import {
  compatibilityRunDependencies,
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

type Round = Readonly<{
  fleet: ReturnType<typeof createFleetDelegation>;
  host: ShiitakeHost;
  runtime: ReturnType<typeof createCompatibilityRuntimeHost>;
}>;

it("reuses delegation admission and completed settlement across actual Agent and SQLite reopen", async () => {
  const parent = new URL(
    "../../../.botanical/helper-ownership/",
    import.meta.url,
  );
  mkdirSync(parent, { recursive: true });
  const directory = mkdtempSync(
    fileURLToPath(new URL("delegation-reopen-", parent)),
  );
  const senderPath = join(directory, "sender.sqlite");
  const targetPath = join(directory, "target.sqlite");
  const grant: AgentGrant = {
    provenance: {
      actor: "helper-reopen-test",
      session: { agentId: "caller", sessionId: "caller-main" },
    },
    expiresAt: Date.now() + 60_000,
    actions: ["delegate"],
    agents: ["caller", "target"],
  };
  const task = {
    agentId: "target",
    sessionId: "work",
    text: "Complete exactly once",
    requestId: "durable-delegation-request",
  };
  let deliveries = 0;
  let modelCalls = 0;

  async function inRound<Value>(
    use: (round: Round) => Promise<Value>,
  ): Promise<Value> {
    const sender = createNativeSqliteOwnerFixture(senderPath);
    const target = createNativeSqliteOwnerFixture(targetPath);
    const runtime = createCompatibilityRuntimeHost();
    const faux = fauxModel([]);
    faux.setResponses([
      () => {
        modelCalls += 1;
        return fauxAssistantMessage("Completed exactly once");
      },
    ]);
    const opening = makeHost(
      {
        ...compatibilityRunDependencies(),
        store: target.owner,
        runtime: runtime.runtime,
        model: {
          acquire: () =>
            Promise.resolve({
              model: faux.model,
              retryable: () => false,
              stream: faux.stream,
              release: () => Promise.resolve(),
            }),
        },
      },
      {
        fanOut: () => Effect.void,
        postMail: () => Effect.void,
        drainOutbox: () => Effect.void,
        nextWakeAt: () => Effect.succeed(undefined),
      },
    );
    try {
      const host = await opening;
      const fleet = createFleetDelegation({
        storage: sender.owner,
        open: (agentId) => {
          expect(agentId).toBe("target");
          return Promise.resolve({
            agent: host.agent,
            release: () => Promise.resolve(),
          });
        },
        deliver: (mail) => {
          expect(mail.to.agentId).toBe("target");
          deliveries += 1;
          return host.acceptMail(mail, mail.meta);
        },
        readSettlement: (agentId, sessionId, commandId) => {
          expect(agentId).toBe("target");
          return host.readSettlement(sessionId, commandId);
        },
      });
      return await use({ fleet, host, runtime });
    } finally {
      try {
        await (await opening).agent.close();
      } finally {
        try {
          target.close();
        } finally {
          sender.close();
        }
      }
    }
  }

  try {
    const accepted = await inRound(async ({ fleet, host }) => {
      const receipt = await fleet.start(grant, task);
      if (receipt.status !== "accepted")
        throw new Error("recipient was expected to accept mail");
      expect(await fleet.delegate(grant, task)).toMatchObject({
        kind: "pending",
      });
      expect(
        (await host.agent.session("work").read()).pending.prompts,
      ).toHaveLength(1);
      expect(deliveries).toBe(1);
      expect(modelCalls).toBe(0);
      return receipt;
    });

    // Both SQLite connections and the target Agent have closed before this
    // round constructs fresh owners over the same files.
    const settled = await inRound(async ({ fleet, host, runtime }) => {
      expect(await fleet.start(grant, task)).toEqual(accepted);
      expect(await fleet.delegate(grant, task)).toMatchObject({
        kind: "pending",
      });
      expect(deliveries).toBe(1);
      expect(modelCalls).toBe(0);
      await runtime.runNext();
      const result = await fleet.delegate(grant, task);
      expect(result).toMatchObject({
        kind: "settled",
        agentId: "target",
        sessionId: "work",
        terminal: {
          commandId: accepted.commandId,
          outcome: "completed",
        },
        content: "Completed exactly once",
      });
      expect((await host.agent.session("work").read()).terminals).toHaveLength(
        1,
      );
      expect(modelCalls).toBe(1);
      return result;
    });

    await inRound(async ({ fleet, host, runtime }) => {
      expect(await fleet.start(grant, task)).toEqual(accepted);
      expect(await fleet.delegate(grant, task)).toEqual(settled);
      if (runtime.snapshot().state === "pending") await runtime.runNext();
      expect(await fleet.delegate(grant, task)).toEqual(settled);
      const snapshot = await host.agent.session("work").read();
      expect(snapshot.terminals).toHaveLength(1);
      expect(snapshot.terminals[0]).toMatchObject({
        commandId: accepted.commandId,
        outcome: "completed",
      });
      expect(
        snapshot.messages.filter(({ value }) => value.role === "user"),
      ).toHaveLength(1);
      expect(deliveries).toBe(1);
      expect(modelCalls).toBe(1);
    });
  } finally {
    rmSync(directory, { recursive: true });
  }
});
