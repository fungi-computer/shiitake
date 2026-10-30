// Runtime for every law here: Node 24, `node:sqlite` (Woodstock's Local host),
// with the host's storage-pressure check supplied as a controllable fake.
import * as Effect from "effect/Effect";
import { describe, expect, it } from "vitest";

import { createShiitakeClient } from "../src/client.js";
import { ShiitakeError } from "../src/errors.js";
import { createShiitakeHttpRoutes } from "../src/http-routes.js";
import { Shiitake } from "../src/index.js";
import {
  openStorageAdmission,
  prepareSessionRun,
  prepareWoodstockSchema,
} from "../src/internal/woodstock/index.js";
import { writeLargeSession } from "./fixtures/large-session.fixture.js";
import {
  compatibilityRunDependencies,
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

function prompt(requestId: string, text: string) {
  return {
    type: "prompt" as const,
    requestId,
    text,
    delivery: "followUp" as const,
  };
}

const pressure = { boundary: "store", cause: "storage_pressure" } as const;

describe("storage refusal door", () => {
  it("refuses new work with the typed storage_pressure error while the host check reports pressure", () => {
    let pressured = false;
    const admission = openStorageAdmission({ underPressure: () => pressured });
    expect(() => {
      admission.admitNew();
    }).not.toThrow();
    pressured = true;
    expect(() => {
      admission.admitNew();
    }).toThrow(expect.objectContaining(pressure));
    pressured = false;
    expect(() => {
      admission.admitNew();
    }).not.toThrow();
  });

  it("carries the host check's own error through the door when it cannot measure", () => {
    const unreadable = new ShiitakeError("store", "unavailable", "unreadable");
    const admission = openStorageAdmission({
      underPressure: () => {
        throw unreadable;
      },
    });
    expect(() => {
      admission.admitNew();
    }).toThrow(unreadable);
  });

  it("admits everything when the host sets no check", () => {
    expect(() => {
      openStorageAdmission(undefined).admitNew();
    }).not.toThrow();
  });
});

describe("storage pressure backpressure (Node, node:sqlite)", () => {
  it("refuses new Sessions and prompts, finishes admitted work, then re-admits", async () => {
    const fixture = createNativeSqliteOwnerFixture();
    const runtime = createCompatibilityRuntimeHost();
    let used = 100;
    const agent = await Shiitake.make({
      ...compatibilityRunDependencies(),
      store: fixture.owner,
      runtime: runtime.runtime,
      storagePressure: { underPressure: () => used >= 800 },
    });
    const routes = createShiitakeHttpRoutes(
      (id) => agent.session(id),
      agent.listSessions,
      agent.createSession,
      agent.deleteSession,
      () => agent.extensions,
    );
    const client = createShiitakeClient({
      baseUrl: "http://shiitake.test",
      fetch: (input, init) =>
        Promise.resolve(routes.request(new Request(input, init))),
    });
    try {
      const sessionId = await agent.createSession("garden");
      const session = agent.session(sessionId);
      const first = await session.command(prompt("first", "water the fern"));
      const second = await session.command(prompt("second", "prune the moss"));

      used = 850;
      await expect(agent.createSession("another")).rejects.toMatchObject(
        pressure,
      );
      await expect(
        session.command(prompt("third", "plant the oak")),
      ).rejects.toMatchObject(pressure);
      // The typed refusal crosses the HTTP wire intact.
      await expect(client.createSession("remote")).rejects.toMatchObject(
        pressure,
      );
      // Replays of admitted requests are never refused.
      expect(await agent.createSession("garden")).toBe(sessionId);
      expect(await session.command(prompt("first", "water the fern"))).toEqual(
        first,
      );

      // Admitted work finishes under pressure: its writes bypass admission.
      while (runtime.snapshot().state === "pending") await runtime.runNext();
      const settled = await session.read();
      expect(
        settled.terminals.map(({ commandId, outcome }) => ({
          commandId,
          outcome,
        })),
      ).toEqual([
        { commandId: first.commandId, outcome: "completed" },
        { commandId: second.commandId, outcome: "completed" },
      ]);
      expect(settled.activity.queue.state).toBe("idle");

      // Headroom returned: admission resumes on its own.
      used = 700;
      const third = await session.command(prompt("third", "plant the oak"));
      expect(third.status).toBe("accepted");
      expect(await agent.createSession("another")).not.toBe(sessionId);
    } finally {
      await client.close();
      await agent.close();
      fixture.close();
    }
  });
});

describe("host archive admission (Node, node:sqlite)", () => {
  it("refuses only new Sessions and prompts through the same door, including the HTTP wire", async () => {
    const fixture = createNativeSqliteOwnerFixture();
    const runtime = createCompatibilityRuntimeHost();
    let archived = false;
    const agent = await Shiitake.make({
      ...compatibilityRunDependencies(),
      store: fixture.owner,
      runtime: runtime.runtime,
      admission: {
        assertOpen: () => {
          if (archived)
            throw new ShiitakeError(
              "contract",
              "archived",
              "Agent is archived; unarchive it to work with it",
            );
        },
      },
    });
    const routes = createShiitakeHttpRoutes(
      (id) => agent.session(id),
      agent.listSessions,
      agent.createSession,
      agent.deleteSession,
      () => agent.extensions,
    );
    const client = createShiitakeClient({
      baseUrl: "http://shiitake.test",
      fetch: (input, init) =>
        Promise.resolve(routes.request(new Request(input, init))),
    });
    try {
      const sessionId = await agent.createSession("garden");
      const accepted = await agent
        .session(sessionId)
        .command(prompt("first", "water the fern"));
      archived = true;
      const refusal = { boundary: "contract", cause: "archived" };
      await expect(agent.createSession("new")).rejects.toMatchObject(refusal);
      await expect(
        agent.session(sessionId).command(prompt("new", "plant the oak")),
      ).rejects.toMatchObject(refusal);
      await expect(client.createSession("remote")).rejects.toMatchObject(
        refusal,
      );
      expect(await agent.createSession("garden")).toBe(sessionId);
      expect(
        await agent
          .session(sessionId)
          .command(prompt("first", "water the fern")),
      ).toEqual(accepted);
    } finally {
      await client.close();
      await agent.close();
      fixture.close();
    }
  });
});

describe("stored context size (Node, node:sqlite)", () => {
  it("prepares a run over a large stored context without a byte ceiling", () => {
    const fixture = createNativeSqliteOwnerFixture();
    try {
      Effect.runSync(prepareWoodstockSchema(fixture.owner));
      // 120 uncompacted turns of two 56 KiB texts: about 13.5 MiB of context.
      // Context size is Pi's compaction threshold's concern, not the store's.
      const session = writeLargeSession(fixture.owner, {
        sessionId: "large",
        turns: 120,
        textBytes: 56 * 1_024,
      });
      const last = session.turns.at(-1);
      if (last === undefined) throw new Error("missing turn");
      const prepared = prepareSessionRun(fixture.owner, {
        sessionId: session.sessionId,
        commandFactId: last.commandId,
      });
      expect(prepared.path.length).toBeGreaterThan(240);
    } finally {
      fixture.close();
    }
  });
});
