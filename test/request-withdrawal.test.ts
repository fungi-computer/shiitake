import { expect, it } from "vitest";
import { Shiitake } from "../src/index.js";
import {
  compatibilityRunDependencies,
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

it("reports an unknown request honestly and retains its durable late-admission fence", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const options = {
    ...compatibilityRunDependencies(),
    store: fixture.owner,
    runtime: runtime.runtime,
  };
  let agent = await Shiitake.make(options);
  try {
    const session = agent.session("withdrawal");
    expect(await session.withdraw("late")).toEqual({ status: "unknown" });
    await agent.close();
    agent = await Shiitake.make({
      ...options,
      runtime: createCompatibilityRuntimeHost().runtime,
    });
    const reopened = agent.session("withdrawal");
    expect(await reopened.withdraw("late")).toEqual({ status: "unknown" });
    await expect(
      reopened.send("Never admit this late arrival", { requestId: "late" }),
    ).rejects.toThrow("Request was withdrawn");
    expect((await reopened.read()).pending.prompts).toEqual([]);
  } finally {
    await agent.close();
    fixture.close();
  }
});

it("returns the original admission without cancelling it, even after owner reopen", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  let agent = await Shiitake.make({
    ...compatibilityRunDependencies(),
    store: fixture.owner,
    runtime: createCompatibilityRuntimeHost().runtime,
  });
  try {
    const accepted = await agent.session("accepted").send("Keep this message", {
      requestId: "first",
      delivery: "followUp",
    });
    expect(await agent.session("accepted").withdraw("first")).toEqual(accepted);
    expect(
      (await agent.session("accepted").read()).pending.prompts,
    ).toContainEqual(
      expect.objectContaining({
        requestId: "first",
        commandId: accepted.commandId,
      }),
    );
    await agent.close();
    agent = await Shiitake.make({
      ...compatibilityRunDependencies(),
      store: fixture.owner,
      runtime: createCompatibilityRuntimeHost().runtime,
    });
    expect(await agent.session("accepted").withdraw("first")).toEqual(accepted);
    expect(await agent.session("other").withdraw("first")).toEqual({
      status: "unknown",
    });
  } finally {
    await agent.close();
    fixture.close();
  }
});

it("fences held operator replies too, and never withdraws an already recorded line", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const agent = await Shiitake.make({
    ...compatibilityRunDependencies(),
    store: fixture.owner,
    runtime: createCompatibilityRuntimeHost().runtime,
  });
  try {
    const id = await agent.createSession("operator-withdrawal");
    const session = agent.session(id);
    await session.command({
      type: "operator",
      requestId: "recorded",
      author: "operator",
      text: "Already recorded",
    });
    const message = (await session.read()).messages.find(
      ({ requestId }) => requestId === "recorded",
    );
    if (!message) throw new Error("Operator line missing");
    expect(await session.withdraw("recorded")).toEqual({
      status: "accepted",
      commandId: message.id,
    });
    expect(await session.withdraw("late-line")).toEqual({
      status: "unknown",
    });
    await expect(
      session.command({
        type: "operator",
        requestId: "late-line",
        author: "operator",
        text: "Too late",
      }),
    ).rejects.toThrow("Request was withdrawn");
  } finally {
    await agent.close();
    fixture.close();
  }
});

for (const withdrawFirst of [true, false]) {
  it(`serializes concurrent admission and withdrawal (${withdrawFirst ? "withdrawal" : "admission"} starts first)`, async () => {
    const fixture = createNativeSqliteOwnerFixture();
    const agent = await Shiitake.make({
      ...compatibilityRunDependencies(),
      store: fixture.owner,
      runtime: createCompatibilityRuntimeHost().runtime,
    });
    try {
      const session = agent.session("race");
      const send = () => session.send("One outcome", { requestId: "racing" });
      const withdraw = () => session.withdraw("racing");
      const [first, second] = await Promise.allSettled(
        withdrawFirst ? [withdraw(), send()] : [send(), withdraw()],
      );
      const resolution = await withdraw();
      if (resolution.status === "unknown") {
        expect(withdrawFirst ? first : second).toEqual({
          status: "fulfilled",
          value: { status: "unknown" },
        });
        expect(withdrawFirst ? second.status : first.status).toBe("rejected");
      } else {
        expect(first).toEqual({ status: "fulfilled", value: resolution });
        expect(second).toEqual({ status: "fulfilled", value: resolution });
      }
    } finally {
      await agent.close();
      fixture.close();
    }
  });
}
