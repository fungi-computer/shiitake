import * as Effect from "effect/Effect";
import { expect, it } from "vitest";

import { Shiitake } from "../src/index.js";
import { readSessionSummaries } from "../src/effect.js";
import { prepareWoodstockSchema } from "../src/internal/woodstock/index.js";
import { sessionIds } from "../src/internal/woodstock/identities.js";
import * as woodstock from "../src/internal/woodstock/session-store.js";
import {
  compatibilityRunDependencies,
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
  type NativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

async function openAgent(fixture: NativeSqliteOwnerFixture) {
  const runtime = createCompatibilityRuntimeHost();
  const agent = await Shiitake.make({
    ...compatibilityRunDependencies(),
    store: fixture.owner,
    runtime: runtime.runtime,
  });
  return { agent, runtime };
}

/** Every owner title read: the inbox page and the cold stored-fact page. */
async function titles(
  agent: Awaited<ReturnType<typeof openAgent>>["agent"],
  fixture: NativeSqliteOwnerFixture,
  sessionId: string,
) {
  const inbox = (await agent.listSessions()).sessions.find(
    (row) => row.sessionId === sessionId,
  );
  const stored = (
    await Effect.runPromise(readSessionSummaries(fixture.owner))
  ).sessions.find((row) => row.sessionId === sessionId);
  return {
    inbox: inbox !== undefined && "title" in inbox ? inbox.title : undefined,
    stored: stored?.kind === "session" ? stored.title : undefined,
  };
}

it("overrides the derived title everywhere, survives restart and clears back", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  let host = await openAgent(fixture);
  try {
    const session = host.agent.session("named-session");
    await session.command({
      type: "prompt",
      requestId: "named-first",
      delivery: "followUp",
      text: "plant the fern",
    });
    while (host.runtime.snapshot().state === "pending")
      await host.runtime.runNext();
    const before = woodstock.readSelectedPath(fixture.owner, {
      sessionId: "named-session",
    });
    const activityBefore = (await host.agent.listSessions()).sessions.find(
      (row) => row.sessionId === "named-session",
    );
    expect(await titles(host.agent, fixture, "named-session")).toEqual({
      inbox: "plant the fern",
      stored: "plant the fern",
    });

    await session.command({ type: "title", title: "Fern notes" });
    await session.command({ type: "title", title: "Fern notes" });
    expect(await titles(host.agent, fixture, "named-session")).toEqual({
      inbox: "Fern notes",
      stored: "Fern notes",
    });
    // Display metadata only: history, the selected leaf and recency hold.
    expect(
      woodstock.readSelectedPath(fixture.owner, { sessionId: "named-session" }),
    ).toEqual(before);
    const activityAfter = (await host.agent.listSessions()).sessions.find(
      (row) => row.sessionId === "named-session",
    );
    expect(
      activityAfter !== undefined && "lastActivityAt" in activityAfter
        ? activityAfter.lastActivityAt
        : undefined,
    ).toBe(
      activityBefore !== undefined && "lastActivityAt" in activityBefore
        ? activityBefore.lastActivityAt
        : undefined,
    );

    // A later prompt keeps the user's title instead of re-deriving it.
    await session.command({
      type: "prompt",
      requestId: "named-second",
      delivery: "followUp",
      text: "water it",
    });
    while (host.runtime.snapshot().state === "pending")
      await host.runtime.runNext();
    expect((await titles(host.agent, fixture, "named-session")).inbox).toBe(
      "Fern notes",
    );

    await host.agent.close();
    host = await openAgent(fixture);
    expect(await titles(host.agent, fixture, "named-session")).toEqual({
      inbox: "Fern notes",
      stored: "Fern notes",
    });

    await host.agent
      .session("named-session")
      .command({ type: "title", title: null });
    expect(await titles(host.agent, fixture, "named-session")).toEqual({
      inbox: "plant the fern",
      stored: "plant the fern",
    });
  } finally {
    await host.agent.close();
    fixture.close();
  }
});

it("rejects an unbounded or multi-line title at the command door", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const host = await openAgent(fixture);
  try {
    const session = host.agent.session("bad-title");
    for (const title of ["", " padded ", "two\nlines", "x".repeat(121)])
      await expect(
        session.command({ type: "title", title }),
      ).rejects.toMatchObject({ boundary: "contract", cause: "invalid" });
    await expect(
      session.command({ type: "title", title: "é".repeat(120) }),
    ).resolves.toBeUndefined();
  } finally {
    await host.agent.close();
    fixture.close();
  }
});

it("derives the selected lineage's first prompt by bounded reads, even across branches", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const sessionId = "branched-title";
    const contract = { core: "a".repeat(64), extensions: [] } as const;
    const prompt = (content: string) =>
      woodstock.appendSessionEntry(fixture.owner, {
        sessionId,
        id: sessionIds.fact.generate(),
        entry: {
          kind: "command",
          workId: sessionIds.work.generate(),
          requestId: `request-${content}`,
          receiptId: sessionIds.receipt.generate(),
          message: { role: "user", content, timestamp: 1 },
          contract,
        },
      });
    const title = () =>
      woodstock.readStoredSessionSummary(fixture.owner, sessionId)?.title;
    const root = woodstock.setSessionModelPreference(
      fixture.owner,
      sessionId,
      null,
    );
    const first = prompt("first branch");
    prompt("later prompt");
    expect(title()).toBe("first branch");
    // A branch taken before the first prompt has no prompt until one enters.
    woodstock.selectSessionBranch(fixture.owner, {
      sessionId,
      entryId: root.id,
    });
    expect(title()).toBeUndefined();
    prompt("second branch");
    expect(title()).toBe("second branch");
    woodstock.selectSessionBranch(fixture.owner, {
      sessionId,
      entryId: first.id,
    });
    expect(title()).toBe("first branch");
  } finally {
    fixture.close();
  }
});
