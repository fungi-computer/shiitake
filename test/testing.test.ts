import { expect, it } from "vitest";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { memoryHost, fauxModelSource } from "../src/testing.js";

it("settles a durable Session with the credential-free default model", async () => {
  const host = await memoryHost();
  try {
    const accepted = await host.agent.session("example").send("Hello", {
      requestId: "example-prompt",
    });
    expect(accepted.status).toBe("accepted");
    const settled = await host.settle("example");
    expect(settled.activity.queue.state).toBe("idle");
    expect(settled.terminals).toMatchObject([{ outcome: "completed" }]);
    expect(settled.messages.at(-1)?.value).toMatchObject({
      role: "assistant",
      content: [{ type: "text", text: "complete" }],
    });
    expect(await host.agent.session("example").read()).toEqual(settled);
  } finally {
    await host.close();
  }
});

it("uses the supplied deterministic model source", async () => {
  const host = await memoryHost({
    model: fauxModelSource([fauxAssistantMessage("a supplied reply")]),
  });
  try {
    await host.agent.session("custom").send("Hello");
    expect((await host.settle("custom")).messages.at(-1)?.value).toMatchObject({
      role: "assistant",
      content: [{ type: "text", text: "a supplied reply" }],
    });
  } finally {
    await host.close();
  }
});

it("refuses settlement when its reconciliation tick budget is exhausted", async () => {
  const host = await memoryHost();
  try {
    await host.agent.session("bounded").send("Hello");
    await expect(host.settle("bounded", 1)).rejects.toThrow(
      "Memory host did not settle within 1 reconciliation ticks",
    );
    expect((await host.settle("bounded")).terminals).toMatchObject([
      { outcome: "completed" },
    ]);
  } finally {
    await host.close();
  }
});

it("closes its owned database exactly once", async () => {
  const host = await memoryHost();
  await host.close();
  await expect(host.close()).resolves.toBeUndefined();
  await expect(host.reconcile()).rejects.toThrow("closed");
});

it("refuses tick budgets outside the bounded integer range", async () => {
  const host = await memoryHost();
  try {
    await expect(host.settle("invalid-budget", 0)).rejects.toMatchObject({
      name: "ZodError",
      issues: [{ code: "too_small", minimum: 1, inclusive: true }],
    });
    await expect(host.settle("invalid-budget", 1_001)).rejects.toMatchObject({
      name: "ZodError",
      issues: [{ code: "too_big", maximum: 1_000, inclusive: true }],
    });
    for (const budget of [1.5, Infinity, NaN]) {
      await expect(host.settle("invalid-budget", budget)).rejects.toThrow();
    }
  } finally {
    await host.close();
  }
});

it("settles on a borrowed owner and leaves its database open after close", async () => {
  const { createNativeSqliteOwnerFixture } =
    await import("./fixtures/native-sqlite-owner.fixture.js");
  const fixture = createNativeSqliteOwnerFixture();
  try {
    const host = await memoryHost({ owner: fixture.owner });
    await host.agent.session("borrowed").send("Hello");
    expect((await host.settle("borrowed")).terminals).toMatchObject([
      { outcome: "completed" },
    ]);
    expect(
      fixture.owner.sql
        .exec("SELECT id FROM sessions WHERE id = ?", "borrowed")
        .one(),
    ).toEqual({ id: "borrowed" });
    await host.close();
    await host.close();
    expect(fixture.owner.sql.exec("SELECT 42 AS answer").one()).toEqual({
      answer: 42,
    });
    await expect(host.reconcile()).rejects.toThrow("closed");
  } finally {
    fixture.close();
  }
});
