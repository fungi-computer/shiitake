// Runtime for every law here: Node 24, `node:sqlite` (Woodstock's Local host),
// with the host's storage measurement supplied as a controllable budget.
import * as Effect from "effect/Effect";
import { describe, expect, it } from "vitest";

import { createShiitakeClient } from "../src/client.js";
import { ShiitakeError } from "../src/errors.js";
import { createShiitakeHttpRoutes } from "../src/http-routes.js";
import { Shiitake } from "../src/index.js";
import { readClientSnapshot } from "../src/internal/woodstock/client-projection.js";
import {
  contextByteLimit,
  openStorageAdmission,
  prepareSessionRun,
  prepareWoodstockSchema,
  readSelectedPath,
} from "../src/internal/woodstock/index.js";
import { idleActivity } from "./fixtures/client.fixture.js";
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

describe("storage admission thresholds", () => {
  it("refuses at 80%, keeps refusing until below 75%, then re-admits", () => {
    let used = 0;
    const admission = openStorageAdmission({
      usedBytes: () => used,
      budgetBytes: 1_000,
    });
    const admits = (bytes: number) => {
      used = bytes;
      try {
        admission.admitNew();
        return true;
      } catch (error) {
        expect(error).toBeInstanceOf(ShiitakeError);
        expect(error).toMatchObject(pressure);
        return false;
      }
    };
    expect(admits(790)).toBe(true);
    expect(admits(800)).toBe(false);
    expect(admits(780)).toBe(false);
    expect(admits(750)).toBe(false);
    expect(admits(749)).toBe(true);
    expect(admits(790)).toBe(true);
  });

  it("fails closed on an invalid budget or an unreadable measurement", () => {
    expect(() =>
      openStorageAdmission({ usedBytes: () => 0, budgetBytes: 0 }),
    ).toThrow(expect.objectContaining({ cause: "invalid" }));
    const admission = openStorageAdmission({
      usedBytes: () => Number.NaN,
      budgetBytes: 1_000,
    });
    expect(() => {
      admission.admitNew();
    }).toThrow(expect.objectContaining({ cause: "unavailable" }));
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
      storageBudget: { usedBytes: () => used, budgetBytes: 1_000 },
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

      // Hysteresis: dropping just under 80% does not re-admit yet.
      used = 780;
      await expect(
        session.command(prompt("third", "plant the oak")),
      ).rejects.toMatchObject(pressure);

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

describe("model context ceiling (Node, node:sqlite)", () => {
  // 120 uncompacted turns of two 56 KiB texts: about 13.5 MiB of context.
  const oversized = { turns: 120, textBytes: 56 * 1_024 } as const;

  it("refuses an oversized context read with a typed error before decoding it", () => {
    const fixture = createNativeSqliteOwnerFixture();
    try {
      Effect.runSync(prepareWoodstockSchema(fixture.owner));
      const session = writeLargeSession(fixture.owner, {
        sessionId: "oversized",
        ...oversized,
      });
      const last = session.turns.at(-1);
      if (last === undefined) throw new Error("missing turn");
      const stored = fixture.owner.sql
        .exec(
          "SELECT SUM(LENGTH(CAST(data_json AS BLOB))) AS bytes FROM session_entries",
        )
        .one().bytes;
      expect(Number(stored)).toBeGreaterThan(contextByteLimit);

      expect(() =>
        prepareSessionRun(fixture.owner, {
          sessionId: session.sessionId,
          commandFactId: last.commandId,
        }),
      ).toThrow(
        expect.objectContaining({
          boundary: "store",
          cause: "context_too_large",
        }),
      );
      // Scroll-back still pages the same history; only the model read refuses.
      const page = readClientSnapshot(
        fixture.owner,
        session.sessionId,
        { limit: 20 },
        { facts: idleActivity, progress: null },
      );
      expect(page.messages).toHaveLength(20);
      expect(page.hasMore).toBe(true);
    } finally {
      fixture.close();
    }
  });

  it("settles the run as failed and keeps the Session usable from an earlier entry", async () => {
    const fixture = createNativeSqliteOwnerFixture();
    const runtime = createCompatibilityRuntimeHost();
    const agent = await Shiitake.make({
      ...compatibilityRunDependencies(),
      store: fixture.owner,
      runtime: runtime.runtime,
    });
    try {
      const sessionId = await agent.createSession("oversized-run");
      const history = writeLargeSession(fixture.owner, {
        sessionId,
        ...oversized,
      });
      const session = agent.session(sessionId);
      const refused = await session.command(prompt("big", "one more"));
      while (runtime.snapshot().state === "pending") await runtime.runNext();
      const after = await session.read();
      expect(
        after.terminals.find(
          ({ commandId }) => commandId === refused.commandId,
        ),
      ).toMatchObject({ outcome: "failed" });
      expect(after.activity.queue.state).toBe("idle");

      // Branching to an earlier entry shrinks the context below the ceiling.
      const earlier = history.turns[10];
      if (earlier === undefined) throw new Error("missing turn");
      await session.command({ type: "branch", entryId: earlier.terminalId });
      const retried = await session.command(prompt("small", "start over"));
      while (runtime.snapshot().state === "pending") await runtime.runNext();
      expect(
        (await session.read()).terminals.find(
          ({ commandId }) => commandId === retried.commandId,
        ),
      ).toMatchObject({ outcome: "completed" });
      expect(
        readSelectedPath(fixture.owner, { sessionId }).length,
      ).toBeLessThan(history.entries);
    } finally {
      await agent.close();
      fixture.close();
    }
  });
});
