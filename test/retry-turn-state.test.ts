import { directSandbox } from "@fungi.computer/mycelium/testing";
import {
  fauxModel,
  type FauxResponseFactory,
} from "@fungi.computer/mule/testing";
import {
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { afterEach, expect, it, vi } from "vitest";
import { Shiitake } from "../src/index.js";
import {
  createCompatibilityRuntimeHost,
  compatibilityRunDependencies,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

afterEach(() => vi.useRealTimers());

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

it("leaves backoff when the next call starts and keeps retry cycles separate across tool turns", async () => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const faux = fauxModel([]);
  const resumed = deferred();
  const release = deferred();
  const error = (message: string) =>
    fauxAssistantMessage("", { stopReason: "error", errorMessage: message });
  const tool = () =>
    fauxAssistantMessage(fauxToolCall("execute", { code: "return 1" }), {
      stopReason: "toolUse",
    });
  faux.setResponses([
    error("servers overloaded"),
    async () => {
      resumed.resolve();
      await release.promise;
      return tool();
    },
    error("upstream connect error: connection termination"),
    tool(),
    fauxAssistantMessage("finished after both retry cycles"),
  ]);
  const agent = await Shiitake.make({
    ...compatibilityRunDependencies(),
    store: fixture.owner,
    runtime: runtime.runtime,
    model: {
      acquire: () =>
        Promise.resolve({
          model: faux.model,
          // Every scripted failure in this fixture is transient.
          retryable: () => true,
          stream: faux.stream,
          release: () => Promise.resolve(),
        }),
    },
    tools: { modules: [], sandbox: directSandbox(() => Promise.resolve(1)) },
  });
  const session = agent.session("retry-turns");
  let running: Promise<void> | undefined;
  try {
    await session.send("Use tools across two retries.", {
      requestId: "first",
      delivery: "followUp",
    });
    running = runtime.runNext();
    await vi.waitFor(async () => {
      expect((await session.read()).activity.retry).not.toBeNull();
    });
    await vi.advanceTimersByTimeAsync(2_000);
    await resumed.promise;
    const whileResumed = await session.read();
    release.resolve();
    await vi.waitFor(async () => {
      expect((await session.read()).activity.retry).not.toBeNull();
    });
    await vi.advanceTimersByTimeAsync(2_000);
    await running;
    const final = await session.read();
    expect(whileResumed.activity.retry).toBeNull();
    expect(final.terminals.at(-1)?.outcome).toBe("completed");
    expect(faux.state.callCount).toBe(5);
  } finally {
    release.resolve();
    await session.abort();
    await running;
    await agent.close();
    fixture.close();
  }
});

it.each(["backoff", "active retry"])(
  "cancels during %s and accepts the first following prompt",
  async (phase) => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    const fixture = createNativeSqliteOwnerFixture();
    const runtime = createCompatibilityRuntimeHost();
    const faux = fauxModel([]);
    let modelSignal: AbortSignal | undefined;
    faux.setResponses([
      fauxAssistantMessage("", {
        stopReason: "error",
        errorMessage: "servers overloaded",
      }),
      ...(phase === "active retry"
        ? [
            async (
              _context: Parameters<FauxResponseFactory>[0],
              options: Parameters<FauxResponseFactory>[1],
            ) => {
              modelSignal = options?.signal;
              await new Promise<void>((resolve) => {
                if (modelSignal?.aborted) resolve();
                else
                  modelSignal?.addEventListener(
                    "abort",
                    () => {
                      resolve();
                    },
                    {
                      once: true,
                    },
                  );
              });
              return fauxAssistantMessage("", { stopReason: "aborted" });
            },
          ]
        : []),
      fauxAssistantMessage("first following prompt completed"),
    ]);
    const agent = await Shiitake.make({
      ...compatibilityRunDependencies(),
      store: fixture.owner,
      runtime: runtime.runtime,
      model: {
        acquire: () =>
          Promise.resolve({
            model: faux.model,
            // Every scripted failure in this fixture is transient.
            retryable: () => true,
            stream: faux.stream,
            release: () => Promise.resolve(),
          }),
      },
    });
    const session = agent.session("cancel-retry");
    let running: Promise<void> | undefined;
    try {
      await session.send("Start and retry.", {
        requestId: "before-cancel",
        delivery: "followUp",
      });
      running = runtime.runNext();
      await vi.waitFor(async () => {
        expect((await session.read()).activity.retry).not.toBeNull();
      });
      if (phase === "active retry") {
        await vi.advanceTimersByTimeAsync(2_000);
        await vi.waitFor(() => {
          expect(modelSignal).toBeDefined();
        });
      }
      expect((await session.read()).activity.queue.state).toBe("running");
      await session.abort();
      await running;
      if (phase === "active retry") expect(modelSignal?.aborted).toBe(true);
      expect((await session.read()).terminals.at(-1)?.outcome).toBe(
        "cancelled",
      );
      await expect(
        session.send("New instruction.", {
          requestId: "after-cancel",
          delivery: "followUp",
        }),
      ).resolves.toMatchObject({ status: "accepted" });
      await runtime.runNext();
      const final = await session.read();
      expect(final.activity.queue.state).toBe("idle");
      expect(final.activity.retry).toBeNull();
      expect(final.terminals.at(-1)?.outcome).toBe("completed");
      expect(final.messages.at(-1)?.value).toMatchObject({
        content: [{ type: "text", text: "first following prompt completed" }],
      });
    } finally {
      await session.abort();
      await running;
      await agent.close();
      fixture.close();
    }
  },
);
