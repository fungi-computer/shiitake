import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";

import { Shiitake } from "../src/index.js";
import type { SqliteOwner } from "../src/internal/contracts.js";
import { readSessionInboxChanges } from "../src/internal/woodstock/session-inbox.js";
import {
  compatibilityRunDependencies,
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

async function withAgent<A>(
  path: string,
  use: (
    agent: Awaited<ReturnType<typeof Shiitake.make>>,
    owner: SqliteOwner,
  ) => Promise<A>,
): Promise<A> {
  const fixture = createNativeSqliteOwnerFixture(path);
  try {
    const runtime = createCompatibilityRuntimeHost();
    const agent = await Shiitake.make({
      ...compatibilityRunDependencies(),
      store: fixture.owner,
      runtime: runtime.runtime,
    });
    try {
      return await use(agent, fixture.owner);
    } finally {
      await agent.close();
    }
  } finally {
    fixture.close();
  }
}

it("replaying a committed title after SQLite reopen does not advance the inbox revision", async () => {
  const directory = await mkdtemp(join(tmpdir(), "shiitake-title-replay-"));
  const path = join(directory, "session.sqlite");
  try {
    const committed = await withAgent(path, async (agent, owner) => {
      const sessionId = await agent.createSession("title-replay");
      await agent
        .session(sessionId)
        .command({ type: "title", title: "Fern notes" });
      const baseline = readSessionInboxChanges(owner, 0, 64);
      expect(baseline.rows).toContainEqual(
        expect.objectContaining({ sessionId, title: "Fern notes" }),
      );
      return { sessionId, revision: baseline.latest };
    });

    await withAgent(path, async (agent, owner) => {
      const before = readSessionInboxChanges(owner, committed.revision, 64);
      expect(before).toEqual({
        rows: [],
        through: committed.revision,
        latest: committed.revision,
      });
      // The first response may have been lost before the host restarted.
      await agent.session(committed.sessionId).command({
        type: "title",
        title: "Fern notes",
      });
      expect(readSessionInboxChanges(owner, committed.revision, 64)).toEqual(
        before,
      );
    });
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
