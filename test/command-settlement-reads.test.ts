// Runtime: Node 24, node:sqlite, the Local owner's real storage representation.
import { expect, it } from "vitest";
import { memoryHost } from "../src/testing.js";
import { sessionIds } from "../src/internal/woodstock/identities.js";
import * as woodstock from "../src/internal/woodstock/session-store.js";
import { countMaterializedBytes } from "./fixtures/large-session.fixture.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

it("does not reread a live command's body before its terminal fact exists", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const measured = countMaterializedBytes(fixture.owner);
  const host = await memoryHost({ owner: measured.owner });
  try {
    const session = host.agent.session("child-session");
    const accepted = await session.send("work");
    const admission = woodstock.readSessionCommandAdmission(
      fixture.owner,
      "child-session",
      accepted.commandId,
    );
    if (admission === undefined) throw new Error("Expected accepted work");
    woodstock.appendSessionEntry(fixture.owner, {
      sessionId: "child-session",
      id: admission.id,
      entry: admission.entry,
    });
    const run = { kind: "run" as const, value: "child-run" };
    const turn = { kind: "turn" as const, value: "child-turn", parent: run };
    const largeAnswer = "x".repeat(128 * 1024);
    const usage = {
      input: 1,
      output: 1,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 2,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    };
    // The engine persists the completed assistant message before its terminal.
    const id = sessionIds.fact.generate();
    woodstock.appendSessionEntry(fixture.owner, {
      sessionId: "child-session",
      id,
      entry: {
        kind: "message",
        workId: admission.entry.workId,
        run,
        turn,
        message: { kind: "message", value: id, parent: turn },
        value: {
          role: "assistant",
          content: [{ type: "text", text: largeAnswer }],
          api: "anthropic-messages",
          provider: "anthropic",
          model: "fixture",
          usage,
          stopReason: "stop",
          timestamp: 1,
        },
      },
    });
    for (let tick = 0; tick < 3; tick += 1) {
      measured.take();
      expect(
        await session.readSettlement(accepted.commandId, { timeoutMs: 0 }),
      ).toBeUndefined();
      // A pending probe may read fixed admission/header facts, never the
      // existing assistant bodies. This bound is independent of work size.
      expect(measured.take()).toBeLessThan(4096);
    }
    woodstock.appendSessionEntry(fixture.owner, {
      sessionId: "child-session",
      id: sessionIds.fact.generate(),
      entry: {
        kind: "run_terminal",
        workId: admission.entry.workId,
        run,
        terminal: { kind: "terminal", value: "child-terminal", parent: run },
        outcome: "completed",
      },
    });
    const settled = await session.readSettlement(accepted.commandId, {
      timeoutMs: 0,
    });
    expect(settled?.terminal.outcome).toBe("completed");
    expect(settled?.content).toBe(largeAnswer);
    expect(settled?.usage).toHaveLength(1);
  } finally {
    await host.close();
    fixture.close();
  }
});
