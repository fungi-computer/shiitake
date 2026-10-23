import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { fauxModel } from "@fungi.computer/mule/testing";
import { Mycelium } from "@fungi.computer/mycelium";
import { directSandbox } from "@fungi.computer/mycelium/testing";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Stream from "effect/Stream";
import { expect, it } from "vitest";

import {
  createAgentControlSource,
  type AgentControlDirectory,
} from "../src/agent-control.js";
import type { ShiitakeAgent } from "../src/client.js";
import { Shiitake } from "../src/index.js";
import {
  compatibilityRunDependencies,
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

type AgentTools = NonNullable<
  Parameters<Parameters<typeof directSandbox>[0]>[0]["agents"]
>;

async function runControl(
  directory: AgentControlDirectory,
  run: (tools: AgentTools) => Promise<null>,
  signal?: AbortSignal,
) {
  const runtime = await Mycelium.make({
    sources: [createAgentControlSource(directory)],
    sandbox: directSandbox(async ({ agents }) => {
      if (agents === undefined) throw new Error("Agent controls missing");
      return run(agents);
    }),
  });
  const lease = await runtime.acquire();
  try {
    const prepared = await Effect.runPromise(
      lease.executeTool.prepare({
        type: "toolCall",
        id: "helper-ownership-control",
        name: "execute",
        arguments: { code: "return agents" },
      }),
    );
    return await Effect.runPromiseExit(Stream.runCollect(prepared.execute()), {
      signal,
    });
  } finally {
    await lease.release();
    await runtime.close();
  }
}

it("aborts only the public wait observation and cancels only the explicitly addressed queued command", async () => {
  const watching = await Effect.runPromise(Deferred.make<undefined>());
  const modelStarted = await Effect.runPromise(Deferred.make<undefined>());
  const finishModel = await Effect.runPromise(Deferred.make<undefined>());
  const controller = new AbortController();
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const faux = fauxModel([]);
  let modelSignal: AbortSignal | undefined;
  faux.setResponses([
    async (_context, options) => {
      modelSignal = options?.signal;
      await Effect.runPromise(Deferred.succeed(modelStarted, undefined));
      await Effect.runPromise(Deferred.await(finishModel));
      return fauxAssistantMessage("Observed work completed");
    },
    fauxAssistantMessage("Unrelated work completed"),
  ]);
  const opening = Shiitake.make({
    ...compatibilityRunDependencies(),
    store: fixture.owner,
    runtime: runtime.runtime,
    model: {
      acquire: () =>
        Promise.resolve({
          model: faux.model,
          retryable: () => false,
          stream: faux.stream,
          release: () => Promise.resolve(),
        }),
    },
  });
  let running: Promise<void> | undefined;
  let observing: ReturnType<typeof runControl> | undefined;
  let observationSettled = false;
  let opens = 0;
  let releases = 0;
  let watchCloses = 0;
  try {
    const agent = await opening;
    const session = agent.session("addressed-work");
    const first = await session.send("observe this work", {
      requestId: "observed",
      delivery: "followUp",
    });
    const cancelled = await session.send("cancel only this work", {
      requestId: "cancelled",
      delivery: "followUp",
    });
    const unrelated = await session.send("keep this unrelated work", {
      requestId: "unrelated",
      delivery: "followUp",
    });
    const borrowed: ShiitakeAgent = {
      ...agent,
      session: (id) => {
        const addressed = agent.session(id);
        return {
          ...addressed,
          watch: async () => {
            const watch = await addressed.watch();
            await Effect.runPromise(Deferred.succeed(watching, undefined));
            return {
              ...watch,
              close: async () => {
                await watch.close();
                watchCloses += 1;
              },
            };
          },
        };
      },
    };
    const directory: AgentControlDirectory = {
      list: () => Promise.resolve([{ agentId: "target", name: "Target" }]),
      open: (id) => {
        expect(id).toBe("target");
        opens += 1;
        return Promise.resolve({
          agent: borrowed,
          release: () => {
            releases += 1;
            return Promise.resolve();
          },
        });
      },
    };
    observing = runControl(
      directory,
      async ({ wait }) => {
        if (wait === undefined) throw new Error("Wait missing");
        await wait({
          agentId: "target",
          sessionId: "addressed-work",
          commandId: first.commandId,
          timeoutMs: 30000,
        });
        return null;
      },
      controller.signal,
    ).then((exit) => {
      observationSettled = true;
      return exit;
    });
    await Effect.runPromise(Deferred.await(watching));
    expect(opens).toBe(1);
    expect(releases).toBe(0);

    // A separate target owner advances while the control execution observes it.
    // This is not an Agent run spawning a nested child runtime.
    running = runtime.runNext();
    await Effect.runPromise(Deferred.await(modelStarted));
    expect((await session.read()).activity.queue.state).toBe("running");
    expect(observationSettled).toBe(false);
    const cancellation = await runControl(directory, async ({ cancel }) => {
      if (cancel === undefined) throw new Error("Cancel missing");
      expect(
        await cancel({
          agentId: "target",
          sessionId: "addressed-work",
          commandId: cancelled.commandId,
        }),
      ).toEqual({ status: "accepted", commandId: cancelled.commandId });
      return null;
    });
    expect(Exit.isSuccess(cancellation)).toBe(true);
    if (Exit.isSuccess(cancellation))
      expect(cancellation.value[0]).toMatchObject({
        type: "result",
        result: { isError: false },
      });
    expect(observationSettled).toBe(false);
    expect(opens).toBe(2);
    expect(releases).toBe(1);

    controller.abort();
    const stopped = await observing;
    expect(Exit.isFailure(stopped)).toBe(true);
    if (Exit.isFailure(stopped))
      expect(Cause.hasInterrupts(stopped.cause)).toBe(true);
    expect(watchCloses).toBe(1);
    expect(releases).toBe(opens);
    expect(modelSignal).toBeDefined();
    expect(modelSignal?.aborted).toBe(false);

    await Effect.runPromise(Deferred.succeed(finishModel, undefined));
    await running;
    running = undefined;
    await runtime.runNext();
    await runtime.runNext();
    const snapshot = await session.read();
    expect(snapshot.terminals).toHaveLength(3);
    expect(
      snapshot.terminals.find(({ commandId }) => commandId === first.commandId)
        ?.outcome,
    ).toBe("completed");
    expect(
      snapshot.terminals.find(
        ({ commandId }) => commandId === cancelled.commandId,
      )?.outcome,
    ).toBe("cancelled");
    expect(
      snapshot.terminals.find(
        ({ commandId }) => commandId === unrelated.commandId,
      )?.outcome,
    ).toBe("completed");
    expect(faux.state.callCount).toBe(2);
  } finally {
    controller.abort();
    await Effect.runPromise(Deferred.succeed(finishModel, undefined));
    try {
      await observing;
      await running;
      await (await opening).close();
    } finally {
      fixture.close();
    }
  }
});
