import { expect, it } from "vitest";

import type { SessionWatch, SqliteOwner } from "../src/index.js";
import {
  appendSessionEntry,
  readSelectedPath,
} from "../src/internal/woodstock/session-store.js";
import { sessionIds } from "../src/internal/woodstock/identities.js";
import { memoryHost } from "../src/testing.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

it.each(["snapshot", "watch"] as const)(
  "keeps the %s context figure with its cursor when an append follows the atomic read",
  async (path) => {
    const fixture = createNativeSqliteOwnerFixture();
    let armed = false;
    let cursorRead = { seen: false };
    let injectedId: string | undefined;
    let sessionId: string;
    const owner: SqliteOwner = {
      transactionSync: (operation) => {
        const read = { seen: false };
        cursorRead = read;
        const result = fixture.owner.transactionSync(operation);
        // A second writer commits immediately after the cursor's read
        // transaction, before the caller can begin another stored read.
        if (armed && read.seen) {
          armed = false;
          injectedId = appendSessionEntry(fixture.owner, {
            sessionId,
            id: sessionIds.fact.generate(),
            entry: {
              kind: "operator",
              workId: null,
              requestId: "interleaved-line",
              author: "operator",
              message: {
                role: "user",
                content: "later context ".repeat(256),
                timestamp: 2,
              },
            },
          }).id;
        }
        return result;
      },
      sql: {
        exec: (statement, ...values) => {
          if (
            statement.includes(
              "SELECT tip.state AS state, tip.sequence AS newest_sequence",
            )
          )
            cursorRead.seen = true;
          return fixture.owner.sql.exec(statement, ...values);
        },
      },
    };
    const host = await memoryHost({ owner });
    let watch: SessionWatch | undefined;
    try {
      sessionId = await host.agent.createSession(`atomic-context-${path}`);
      const session = host.agent.session(sessionId);
      await session.command({
        type: "operator",
        requestId: "initial-line",
        author: "operator",
        text: "Initial context.",
      });
      watch = path === "watch" ? await session.watch() : undefined;
      const baseline = watch?.snapshot ?? (await session.read());
      if (baseline.context === undefined)
        throw new Error("Initial context figure is missing");

      // Advance the cursor without changing the prompt's tokens. Do not read
      // this cursor first: a warm context cache would conceal the torn read.
      await session.modelPreference.set(null);
      const target = readSelectedPath(fixture.owner, { sessionId }).at(-1);
      if (target === undefined) throw new Error("Branch target is missing");
      armed = true;
      const page =
        watch === undefined
          ? await session.read()
          : await watch.changes[Symbol.asyncIterator]()
              .next()
              .then((result) => {
                if (result.done || result.value.kind !== "update")
                  throw new Error("Expected the moving watch update");
                return result.value;
              });

      expect(injectedId).toBeDefined();
      expect(page.cursor).not.toBe(baseline.cursor);
      expect.soft(page.context).toEqual(baseline.context);

      // Revisit that exact cursor before any read can replace its cache entry.
      await session.command({ type: "branch", entryId: target.id });
      const revisited = await session.read();
      expect(revisited.cursor).toBe(page.cursor);
      expect.soft(revisited.context).toEqual(baseline.context);

      // The injected append really changed context, and later reads must see
      // it rather than retain the earlier transaction's ancestry or tokens.
      if (injectedId === undefined)
        throw new Error("Append did not interleave");
      await session.command({ type: "branch", entryId: injectedId });
      const later = await session.read();
      expect(later.cursor).not.toBe(page.cursor);
      expect(later.context?.tokens).toBeGreaterThan(baseline.context.tokens);
    } finally {
      await watch?.close();
      await host.close();
      fixture.close();
    }
  },
);
