import { expect, it } from "vitest";

import { createShiitakeClient } from "../src/client.js";
import { createShiitakeHttpRoutes } from "../src/http-routes.js";
import { Shiitake } from "../src/index.js";
import { readSessionInboxChanges } from "../src/internal/woodstock/session-inbox.js";
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

it("deletes an idle Session's history and lets its creation request reserve a fresh one", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const agent = await Shiitake.make({
    ...compatibilityRunDependencies(),
    store: fixture.owner,
    runtime: runtime.runtime,
  });
  try {
    const sessionId = await agent.createSession("retained-visitor");
    const inboxRevision = readSessionInboxChanges(fixture.owner, 0, 64).latest;
    const session = agent.session(sessionId);
    await session.command(prompt("first", "plant the fern"));
    // Queued work makes deletion a conflict, never a silent loss.
    await expect(agent.deleteSession(sessionId)).rejects.toMatchObject({
      cause: "conflict",
    });
    await runtime.runNext();
    expect((await session.read()).activity.queue.state).toBe("idle");

    expect(await agent.deleteSession(sessionId)).toBe(true);
    expect(
      readSessionInboxChanges(fixture.owner, inboxRevision, 64).rows,
    ).toEqual([{ sessionId, deleted: true }]);
    expect(await agent.deleteSession(sessionId)).toBe(false);
    expect((await agent.listSessions()).sessions).toEqual([]);
    for (const table of [
      "session_entries",
      "session_command_admissions",
      "session_creation_receipts",
      "sessions",
    ])
      expect(
        fixture.owner.sql
          .exec(`SELECT COUNT(*) AS count FROM ${table}`)
          .toArray()[0],
      ).toEqual({ count: 0 });
    const fresh = await agent.createSession("retained-visitor");
    expect(fresh).not.toBe(sessionId);
    expect((await agent.session(fresh).read()).messages).toEqual([]);
  } finally {
    await agent.close();
    fixture.close();
  }
});

it("serves deletion as DELETE /sessions/:id through the remote client", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const agent = await Shiitake.make({
    ...compatibilityRunDependencies(),
    store: fixture.owner,
    runtime: runtime.runtime,
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
    const sessionId = await client.createSession("remote-delete");
    expect(await client.deleteSession(sessionId)).toBe(true);
    expect(await client.deleteSession(sessionId)).toBe(false);
  } finally {
    await client.close();
    await agent.close();
    fixture.close();
  }
});
