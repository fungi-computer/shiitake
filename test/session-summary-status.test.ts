import { afterEach, expect, it, vi } from "vitest";

import { Shiitake } from "../src/index.js";
import { sessionIds } from "../src/internal/woodstock/identities.js";
import { readSessionInboxChanges } from "../src/internal/woodstock/session-inbox.js";
import * as woodstock from "../src/internal/woodstock/session-store.js";
import { admitWithoutCheck } from "../src/internal/woodstock/storage-admission.js";
import {
  compatibilityRunDependencies,
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
  type NativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

afterEach(() => {
  vi.useRealTimers();
});

async function openAgent(
  fixture: NativeSqliteOwnerFixture = createNativeSqliteOwnerFixture(),
  failingSessions: ReadonlySet<string> = new Set(),
) {
  const runtime = createCompatibilityRuntimeHost();
  const dependencies = compatibilityRunDependencies();
  const agent = await Shiitake.make({
    ...dependencies,
    model: {
      acquire: (input) =>
        failingSessions.has(input.sessionId)
          ? Promise.reject(new Error("provider rejected the run"))
          : dependencies.model.acquire(),
    },
    store: fixture.owner,
    runtime: runtime.runtime,
  });
  return {
    fixture,
    runtime,
    agent,
    close: async () => {
      await agent.close();
      fixture.close();
    },
  };
}

type Host = Awaited<ReturnType<typeof openAgent>>;

function prompt(requestId: string, text: string) {
  return {
    type: "prompt" as const,
    requestId,
    text,
    delivery: "followUp" as const,
  };
}

async function drain(host: Host) {
  while (host.runtime.snapshot().state === "pending")
    await host.runtime.runNext();
}

it("maps Watchdog head and outcome to one inbox state", async () => {
  const host = await openAgent(undefined, new Set(["summary-failed"]));
  try {
    const session = host.agent.session("summary-queued");
    await session.command(prompt("summary-first", "plant the fern"));
    const [queued] = (await host.agent.listSessions()).sessions;
    expect(queued).toMatchObject({
      sessionId: "summary-queued",
      state: "running",
    });
    expect(queued).not.toHaveProperty("settledAt");

    await drain(host);
    const [done] = (await host.agent.listSessions()).sessions;
    expect(Object.keys(done ?? {}).sort()).toEqual([
      "createdAt",
      "lastActivityAt",
      "sessionId",
      "settledAt",
      "state",
      "title",
    ]);
    expect(done).toMatchObject({
      sessionId: "summary-queued",
      title: "plant the fern",
      state: "done",
    });
    if (done?.state !== "done") throw new Error("Expected a done row");
    expect(done.settledAt).toBeGreaterThanOrEqual(done.createdAt);
    expect(done.lastActivityAt).toBeGreaterThanOrEqual(done.settledAt ?? 0);

    await host.agent
      .session("summary-failed")
      .command(prompt("summary-failed-request", "this run fails"));
    await drain(host);
    const rows = (await host.agent.listSessions()).sessions;
    const failed = rows.find((row) => row.sessionId === "summary-failed");
    expect(failed).toMatchObject({ state: "errored" });
    expect(failed).toHaveProperty("settledAt");

    await session.command(prompt("summary-second", "water the fern"));
    expect((await host.agent.listSessions()).sessions[0]).toMatchObject({
      sessionId: "summary-queued",
      state: "running",
    });
  } finally {
    await host.close();
  }
});

it("does not report an orphaned streaming entry as in-play work", async () => {
  const host = await openAgent();
  try {
    const sessionId = "summary-orphaned-stream";
    const session = host.agent.session(sessionId);
    await session.command(prompt("summary-orphan", "start the stream"));
    await drain(host);
    // An interrupted run can leave its placeholder behind after Watchdog settles.
    const run = { kind: "run", value: "orphan-run" } as const;
    const turn = { kind: "turn", value: "orphan-turn", parent: run } as const;
    const message = {
      kind: "message",
      value: "orphan-message",
      parent: turn,
    } as const;
    woodstock.appendSessionEntry(host.fixture.owner, {
      sessionId,
      id: message.value,
      state: "streaming",
      entry: {
        kind: "message_start",
        workId: sessionIds.work.generate(),
        run,
        turn,
        message,
        role: "assistant",
      },
    });

    expect((await host.agent.listSessions()).sessions).toEqual([
      expect.objectContaining({ sessionId, state: "done" }),
    ]);
  } finally {
    await host.close();
  }
});

it("pages past 800 Sessions newest first with one statement per page", async () => {
  vi.useFakeTimers({ toFake: ["Date"] });
  let now = 1_700_000_000_000;
  vi.setSystemTime(now);
  const host = await openAgent();
  try {
    for (let index = 0; index < 850; index += 1) {
      now += 1;
      vi.setSystemTime(now);
      woodstock.createStoredSession(
        host.fixture.owner,
        `bulk-${String(index)}`,
        admitWithoutCheck.admitNew,
      );
    }
    now += 1;
    vi.setSystemTime(now);
    const newest = await host.agent.createSession("newest-request");
    await host.agent
      .session(newest)
      .command(prompt("newest-prompt", "the newest conversation"));

    const before = host.fixture.ownerOperations();
    const firstPage = await host.agent.listSessions();
    expect(host.fixture.ownerOperations() - before).toBe(1);
    expect(firstPage.sessions).toHaveLength(200);
    expect(firstPage.sessions[0]).toMatchObject({
      sessionId: newest,
      state: "running",
    });

    const seen = new Set(firstPage.sessions.map((row) => row.sessionId));
    let next = firstPage.next;
    let previousActivity = Number.POSITIVE_INFINITY;
    while (next !== undefined) {
      const operations = host.fixture.ownerOperations();
      const page = await host.agent.listSessions(next);
      expect(host.fixture.ownerOperations() - operations).toBe(1);
      for (const row of page.sessions) {
        expect(seen.has(row.sessionId)).toBe(false);
        seen.add(row.sessionId);
        if (row.state === "unavailable") throw new Error("Unexpected row");
        expect(row.lastActivityAt).toBeLessThanOrEqual(previousActivity);
        previousActivity = row.lastActivityAt;
      }
      next = page.next;
    }
    expect(seen.size).toBe(851);
  } finally {
    await host.close();
  }
});

it("rejects a forged page cursor as invalid input", async () => {
  const host = await openAgent();
  try {
    await expect(host.agent.listSessions("not-a-cursor")).rejects.toMatchObject(
      { boundary: "contract", cause: "invalid" },
    );
  } finally {
    await host.close();
  }
});

it("reads coalesced inbox changes strictly after an owner revision", async () => {
  const host = await openAgent();
  try {
    const first = await host.agent.createSession("changes-first");
    const second = await host.agent.createSession("changes-second");
    const initial = readSessionInboxChanges(host.fixture.owner, 0, 64);
    expect(initial.rows.map((row) => row.sessionId)).toEqual([first, second]);
    expect(initial.through).toBe(initial.latest);

    await host.agent.session(first).command(prompt("changes-run", "go"));
    await drain(host);
    const later = readSessionInboxChanges(
      host.fixture.owner,
      initial.through,
      64,
    );
    // Many transitions of one Session coalesce to its latest row.
    expect(later.rows).toEqual([
      expect.objectContaining({ sessionId: first, state: "done", title: "go" }),
    ]);
    expect(later.through).toBe(later.latest);
    expect(later.latest).toBeGreaterThan(initial.latest + 1);

    const bounded = readSessionInboxChanges(host.fixture.owner, 0, 1);
    expect(bounded.rows.map((row) => row.sessionId)).toEqual([second]);
    expect(bounded.through).toBeLessThan(bounded.latest);
    expect(
      readSessionInboxChanges(host.fixture.owner, later.latest, 64).rows,
    ).toEqual([]);
  } finally {
    await host.close();
  }
});
