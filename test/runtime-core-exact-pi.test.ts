import { fauxModel } from "@fungi.computer/mule/testing";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { Context } from "@earendil-works/pi-ai";
import {
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import {
  Mycelium,
  type SandboxRequest,
  type CapabilitySession,
} from "@fungi.computer/mycelium";
import { expect, it } from "vitest";
import { requestMessages } from "./fixtures/model-context.js";
import { z } from "zod";

import {
  Shiitake,
  ShiitakeError,
  type SessionTerminal,
  type ShiitakeAgent,
  type ShiitakeOptions,
  type SqliteCursor,
  type SqliteOwner,
  type SqliteValue,
} from "../src/index.js";
import {
  createCompatibilityRuntimeHost,
  compatibilityRunDependencies,
  createNativeSqliteOwnerFixture,
  noExtensions,
} from "./fixtures/native-sqlite-owner.fixture.js";
import { requireAcceptedWorkId } from "./fixtures/accepted-work.fixture.js";
import { readSelectedPath } from "../src/internal/woodstock/session-store.js";
import { sessionIds } from "../src/internal/woodstock/identities.js";
import * as woodstock from "../src/internal/woodstock/session-store.js";

const firstPrompt = "remember the fern";
const secondPrompt = "what should you remember?";

it.each([
  {
    kind: "typed",
    expected:
      "model:rejected: OpenAI (openai) is not signed in. Run /login or choose another model with /model.",
  },
  { kind: "raw", expected: "model:rejected: Model acquisition failed" },
  {
    kind: "bounded",
    expected: `model:rejected: ${"Safe refusal. ".repeat(30)}`.slice(0, 200),
  },
])(
  "retains a safe bounded model refusal without its raw provider cause ($kind)",
  async ({ kind, expected }) => {
    const fixture = createNativeSqliteOwnerFixture();
    const runtime = createCompatibilityRuntimeHost();
    const safe =
      "OpenAI (openai) is not signed in. Run /login or choose another model with /model.";
    const dependencies = compatibilityRunDependencies();
    const agent = await Shiitake.make({
      ...dependencies,
      store: fixture.owner,
      runtime: runtime.runtime,
      model: {
        acquire: () =>
          Promise.reject(
            kind === "raw"
              ? new Error("synthetic-private-provider-detail")
              : new ShiitakeError(
                  "model",
                  "rejected",
                  kind === "bounded" ? "Safe refusal. ".repeat(30) : safe,
                  undefined,
                  new Error("synthetic-private-provider-detail"),
                ),
          ),
      },
    });
    try {
      const session = agent.session("model-refusal");
      const accepted = await session.send("Hello", { delivery: "followUp" });
      await runtime.runNext();
      const snapshot = await session.read();
      expect(snapshot.terminals).toContainEqual(
        expect.objectContaining({
          commandId: accepted.commandId,
          outcome: "failed",
          diagnostic: expected,
        }),
      );
      expect(JSON.stringify(snapshot)).not.toContain(
        "synthetic-private-provider-detail",
      );
    } finally {
      await agent.close();
      fixture.close();
    }
  },
);

it("acquires a model for the addressed Session only when its run begins", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const dependencies = compatibilityRunDependencies();
  const acquiredSessions: string[] = [];
  let releases = 0;
  const agent = await Shiitake.make({
    ...dependencies,
    store: fixture.owner,
    runtime: runtime.runtime,
    model: {
      acquire: async ({ sessionId, signal }) => {
        signal.throwIfAborted();
        acquiredSessions.push(sessionId);
        const acquired = await dependencies.model.acquire();
        return {
          ...acquired,
          release: async () => {
            releases += 1;
            await acquired.release();
          },
        };
      },
    },
  });
  try {
    for (const sessionId of ["first-conversation", "second-conversation"]) {
      await agent.session(sessionId).command({
        delivery: "followUp",

        type: "prompt",
        requestId: sessionId,
        text: "Complete this run.",
      });
    }
    expect(acquiredSessions).toEqual([]);
    await runtime.runNext();
    expect(runtime.snapshot().state).toBe("pending");
    await runtime.runNext();
    expect([...acquiredSessions].sort()).toEqual([
      "first-conversation",
      "second-conversation",
    ]);
    expect(releases).toBe(2);
    for (const sessionId of acquiredSessions) {
      const snapshot = await agent.session(sessionId).read();
      expect(snapshot.messages.at(-1)?.value.role).toBe("assistant");
    }
  } finally {
    await agent.close();
    fixture.close();
  }
});

it("acquires tool sources for the addressed Session of each run", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const dependencies = compatibilityRunDependencies();
  const toolSessions: (CapabilitySession | undefined)[] = [];
  const admitted: CapabilitySession[] = [];
  const meta = {
    author: { id: "a", name: "A" },
    delegation: { ancestors: ["a"], maxDepth: 8 },
  };
  const agent = await Shiitake.make({
    ...dependencies,
    store: fixture.owner,
    runtime: runtime.runtime,
    tools: {
      ...dependencies.tools,
      sources: [
        {
          id: "session-scoped",
          acquire: ({ session }) => {
            toolSessions.push(session);
            return Promise.resolve({
              snapshot: { version: "1", modules: [] },
              release: () => Promise.resolve(),
            });
          },
        },
      ],
    },
  });
  try {
    for (const sessionId of ["owner-session", "channel-session"]) {
      const receipt = await agent.withMeta(meta).session(sessionId).command({
        delivery: "followUp",
        type: "prompt",
        requestId: sessionId,
        text: "Complete this run.",
      });
      admitted.push({ sessionId, commandId: receipt.commandId, meta });
      await runtime.runNext();
    }
    expect(toolSessions).toEqual(admitted);
  } finally {
    await agent.close();
    fixture.close();
  }
});

it.each([
  {
    phase: "missing",
    message: "Instruction capabilities have no tool runtime",
  },
  { phase: "runtime", message: "Tool runtime failed" },
] as const)(
  "records a closed $phase tool-setup refusal before model dispatch",
  async ({ phase, message }) => {
    const fixture = createNativeSqliteOwnerFixture();
    const runtime = createCompatibilityRuntimeHost();
    const { tools, ...dependencies } = compatibilityRunDependencies();
    let modelCalls = 0;
    const options: ShiitakeOptions = {
      ...dependencies,
      store: fixture.owner,
      runtime: runtime.runtime,
      model: {
        acquire: async () => {
          const acquired = await dependencies.model.acquire();
          return {
            ...acquired,
            stream: (model, context, streamOptions) => {
              modelCalls += 1;
              return acquired.stream(model, context, streamOptions);
            },
          };
        },
      },
      instructions: {
        load: () =>
          Promise.resolve({
            revision: "tool-setup",
            text: "Use the supplied tools.",
            modules: [
              Mycelium.module({
                id: "fixture",
                name: "fixture",
                operations: {},
              }),
            ],
          }),
      },
    };
    const agent = await Shiitake.make(
      phase === "runtime"
        ? {
            ...options,
            tools: { ...tools, execution: { timeoutMs: -1 } },
          }
        : options,
    );
    try {
      const session = agent.session("setup-failure");
      const accepted = await session.send("Hello", { delivery: "followUp" });
      await runtime.runNext();
      expect((await session.read()).terminals).toContainEqual(
        expect.objectContaining({
          commandId: accepted.commandId,
          outcome: "failed",
          diagnostic: `runtime:rejected: ${message}`,
        }),
      );
      expect(modelCalls).toBe(0);
    } finally {
      await agent.close();
      fixture.close();
    }
  },
);

it("projects failed capability acquisition through public watch and cold reopen", async () => {
  const directory = await mkdtemp(
    join(tmpdir(), "shiitake-acquisition-failure-"),
  );
  const databasePath = join(directory, "agent.sqlite");
  let fixture = createNativeSqliteOwnerFixture(databasePath);
  let agent: ShiitakeAgent | undefined;
  let acquisitions = 0;
  let modelCalls = 0;
  const options = (
    runtime: ReturnType<typeof createCompatibilityRuntimeHost>,
  ): ShiitakeOptions => {
    const dependencies = compatibilityRunDependencies();
    return {
      ...dependencies,
      store: fixture.owner,
      runtime: runtime.runtime,
      model: {
        acquire: async () => {
          const acquired = await dependencies.model.acquire();
          return {
            ...acquired,
            stream: (model, context, streamOptions) => {
              modelCalls += 1;
              return acquired.stream(model, context, streamOptions);
            },
          };
        },
      },
      tools: {
        ...dependencies.tools,
        sources: [
          {
            id: "unavailable-home",
            acquire: () => {
              acquisitions += 1;
              return Promise.reject(new Error("Home acquisition unavailable"));
            },
          },
        ],
      },
    };
  };
  let expected: Pick<SessionTerminal, "commandId" | "outcome" | "diagnostic">[];
  try {
    const runtime = createCompatibilityRuntimeHost();
    agent = await Shiitake.make(options(runtime));
    const session = agent.session("acquisition-session");
    const watch = await session.watch();
    const observed: SessionTerminal[] = [];
    const consume = (async () => {
      for await (const frame of watch.changes) {
        if (frame.kind === "update") observed.push(...frame.terminals);
      }
    })();
    try {
      const accepted = await session.command({
        delivery: "followUp",

        type: "prompt",
        requestId: "failed-acquisition",
        text: "Read Home.",
      });
      expected = [
        {
          commandId: accepted.commandId,
          outcome: "failed",
          diagnostic: "runtime:rejected: Tool lease failed",
        },
      ];
      await runtime.runNext();
      const snapshot = await session.read();
      expect(snapshot.terminals).toMatchObject(expected);
      expect(JSON.stringify(snapshot)).not.toContain(
        "Home acquisition unavailable",
      );
    } finally {
      await watch.close();
      await consume;
    }
    expect(
      observed.map(({ commandId, outcome, diagnostic }) => ({
        commandId,
        outcome,
        diagnostic,
      })),
    ).toEqual(expect.arrayContaining(expected));
    expect(acquisitions).toBe(1);
    expect(modelCalls).toBe(0);
    expect(
      (await session.read()).messages.map(({ value }) => value.role),
    ).toEqual(["user"]);
    await agent.close();
    agent = undefined;
    fixture.close();

    fixture = createNativeSqliteOwnerFixture(databasePath);
    const reopenedRuntime = createCompatibilityRuntimeHost();
    agent = await Shiitake.make(options(reopenedRuntime));
    await reopenedRuntime.runNext();
    expect(
      (await agent.session("acquisition-session").read()).terminals,
    ).toMatchObject(expected);
    expect(acquisitions).toBe(1);
    expect(modelCalls).toBe(0);
    expect(
      fixture.owner.sql
        .exec(
          "SELECT COUNT(*) AS count FROM session_entries WHERE session_id = ? AND type = 'work_terminal'",
          "acquisition-session",
        )
        .one().count,
    ).toBe(1);
  } finally {
    await agent?.close();
    fixture.close();
    await rm(directory, { recursive: true, force: true });
  }
});

function terminalFailingOwner(source: SqliteOwner) {
  let armed = false;
  const owner: SqliteOwner = {
    sql: {
      exec: (statement: string, ...bindings: SqliteValue[]): SqliteCursor => {
        if (
          armed &&
          statement.includes("INSERT INTO session_entries") &&
          bindings[4] === "run_terminal"
        ) {
          armed = false;
          throw new Error("Injected terminal persistence failure");
        }
        return source.sql.exec(statement, ...bindings);
      },
    },
    transactionSync: <A>(operation: () => A): A =>
      source.transactionSync(operation),
  };
  return {
    owner,
    arm: () => {
      armed = true;
    },
  };
}

it("reconstructs the exact selected Pi lineage after a cold reopen", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const faux = fauxModel([], { tokenSize: { min: 1, max: 1 } });
  const requests: Context[] = [];
  const firstAssistant = fauxAssistantMessage("the fern", {
    responseId: "retained-provider-response",
    timestamp: 17,
  });
  faux.setResponses([
    (context) => {
      requests.push(structuredClone(context));
      return firstAssistant;
    },
    (context) => {
      requests.push(structuredClone(context));
      return fauxAssistantMessage("you asked me to remember the fern");
    },
  ]);

  const options = (
    runtime: ReturnType<typeof createCompatibilityRuntimeHost>,
  ): ShiitakeOptions => ({
    store: fixture.owner,
    extensions: noExtensions,
    runtime: runtime.runtime,
    instructions: {
      load: () =>
        Promise.resolve({
          revision: "runtime-core-exact-pi-v1",
          text: "Answer from the selected Session lineage.",
        }),
    },
    model: {
      acquire: () =>
        Promise.resolve({
          model: faux.model,
          stream: faux.stream,
          release: () => Promise.resolve(),
        }),
    },
  });

  let firstAgent: ShiitakeAgent | undefined;
  let secondAgent: ShiitakeAgent | undefined;
  try {
    const firstRuntime = createCompatibilityRuntimeHost();
    firstAgent = await Shiitake.make(options(firstRuntime));
    await firstAgent.session("exact-pi-session").command({
      requestId: "exact-pi-first",
      delivery: "followUp",

      type: "prompt",
      text: firstPrompt,
    });
    await firstRuntime.runNext();
    expect(
      (await firstAgent.session("exact-pi-session").read()).messages.map(
        ({ value }) => value.role,
      ),
    ).toEqual(["user", "assistant"]);
    await firstAgent.close();
    firstAgent = undefined;

    const secondRuntime = createCompatibilityRuntimeHost();
    secondAgent = await Shiitake.make(options(secondRuntime));
    await secondAgent.session("exact-pi-session").command({
      requestId: "exact-pi-second",
      delivery: "followUp",

      type: "prompt",
      text: secondPrompt,
    });
    await secondRuntime.runNext();

    expect(requests).toHaveLength(2);
    expect(requestMessages(requests[1])).toEqual([
      expect.objectContaining({ role: "user", content: firstPrompt }),
      expect.objectContaining({
        role: "assistant",
        content: [{ type: "text", text: "the fern" }],
        responseId: "retained-provider-response",
      }),
      expect.objectContaining({ role: "user", content: secondPrompt }),
    ]);
  } finally {
    await secondAgent?.close();
    await firstAgent?.close();
    fixture.close();
  }
});

it("does not admit an assistant message whose terminal fact failed", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const failing = terminalFailingOwner(fixture.owner);
  const faux = fauxModel([]);
  const requests: Context[] = [];
  faux.setResponses([
    (context) => {
      requests.push(structuredClone(context));
      return fauxAssistantMessage("uncommitted answer");
    },
    (context) => {
      requests.push(structuredClone(context));
      return fauxAssistantMessage("replacement answer");
    },
  ]);
  const runtime = createCompatibilityRuntimeHost();
  const agent = await Shiitake.make({
    store: failing.owner,
    extensions: noExtensions,
    runtime: runtime.runtime,
    instructions: {
      load: () =>
        Promise.resolve({ revision: "terminal-order-v1", text: "Answer." }),
    },
    model: {
      acquire: () =>
        Promise.resolve({
          model: faux.model,
          stream: faux.stream,
          release: () => Promise.resolve(),
        }),
    },
  });
  try {
    const first = await agent.session("terminal-order-session").command({
      requestId: "terminal-order-first",
      delivery: "followUp",

      type: "prompt",
      text: firstPrompt,
    });
    const firstWorkId = requireAcceptedWorkId(
      fixture.owner,
      "terminal-order-session",
      first.commandId,
    );
    failing.arm();
    await runtime.runNext();

    expect(
      fixture.owner.sql
        .exec(
          "SELECT state, outcome FROM watchdog_jobs WHERE job_id = ?",
          firstWorkId,
        )
        .one(),
    ).toEqual({ state: "settled", outcome: "failed" });

    await agent.session("terminal-order-session").command({
      requestId: "terminal-order-second",
      delivery: "followUp",

      type: "prompt",
      text: secondPrompt,
    });
    await runtime.runNext();

    expect(requests[1]?.messages).not.toContainEqual(
      expect.objectContaining({
        role: "assistant",
        content: [{ type: "text", text: "uncommitted answer" }],
      }),
    );
  } finally {
    await agent.close();
    fixture.close();
  }
});

it("rehydrates an ordinary Mycelium ui.* result without BirdDog", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const faux = fauxModel([]);
  const requests: Context[] = [];
  const openedUrls: string[] = [];
  faux.setResponses([
    fauxAssistantMessage(fauxToolCall("execute", { code: "ui.open" }), {
      stopReason: "toolUse",
    }),
    fauxAssistantMessage("tool completed"),
    (context) => {
      requests.push(structuredClone(context));
      return fauxAssistantMessage("the tool result survived");
    },
  ]);
  const ui = Mycelium.module({
    id: "runtime-ui",
    name: "ui",
    operations: {
      open: Mycelium.operation({
        input: z.object({ url: z.string() }),
        output: z.object({ opened: z.string() }),
        execute: ({ url }) => {
          openedUrls.push(url);
          return Promise.resolve({ opened: url });
        },
      }),
    },
  });
  let sourceAcquisitions = 0;
  let sourceReleases = 0;
  const executionBudgets: number[] = [];
  const tools = {
    execution: { timeoutMs: 900_000 },
    sources: [
      {
        id: "runtime-ui-source",
        acquire: () => {
          sourceAcquisitions += 1;
          return Promise.resolve({
            snapshot: { version: "runtime-ui-v1", modules: [ui] },
            release: () => {
              sourceReleases += 1;
              return Promise.resolve();
            },
          });
        },
      },
    ],
    sandbox: {
      execute: async (request: SandboxRequest) => {
        executionBudgets.push(request.timeoutMs);
        const open = request.bindings.ui?.open;
        if (open === undefined)
          throw new Error("Mycelium ui.open operation is unavailable");
        return open({ url: "/workspace" });
      },
    },
  } satisfies NonNullable<ShiitakeOptions["tools"]>;
  const options = (
    runtime: ReturnType<typeof createCompatibilityRuntimeHost>,
  ): ShiitakeOptions => ({
    store: fixture.owner,
    extensions: noExtensions,
    runtime: runtime.runtime,
    instructions: {
      load: () =>
        Promise.resolve({ revision: "tool-rehydrate-v1", text: "Use tools." }),
    },
    model: {
      acquire: () =>
        Promise.resolve({
          model: faux.model,
          stream: faux.stream,
          release: () => Promise.resolve(),
        }),
    },
    tools,
  });

  let firstAgent: ShiitakeAgent | undefined;
  let secondAgent: ShiitakeAgent | undefined;
  try {
    const firstRuntime = createCompatibilityRuntimeHost();
    firstAgent = await Shiitake.make(options(firstRuntime));
    await firstAgent.session("tool-rehydrate-session").command({
      requestId: "tool-rehydrate-first",
      delivery: "followUp",

      type: "prompt",
      text: "Open the workspace with the UI tool.",
    });
    await firstRuntime.runNext();
    await firstAgent.close();
    firstAgent = undefined;

    const secondRuntime = createCompatibilityRuntimeHost();
    secondAgent = await Shiitake.make(options(secondRuntime));
    await secondAgent.session("tool-rehydrate-session").command({
      requestId: "tool-rehydrate-second",
      delivery: "followUp",

      type: "prompt",
      text: "What did the UI tool open?",
    });
    await secondRuntime.runNext();

    expect(requestMessages(requests[0]).map(({ role }) => role)).toEqual([
      "user",
      "assistant",
      "toolResult",
      "assistant",
      "user",
    ]);
    expect(requestMessages(requests[0])[1]).toMatchObject({
      role: "assistant",
      content: [
        expect.objectContaining({
          type: "toolCall",
          name: "execute",
          arguments: { code: "ui.open" },
        }),
      ],
    });
    expect(sourceAcquisitions).toBe(2);
    expect(sourceReleases).toBe(2);
    expect(executionBudgets).toEqual([900_000]);
    expect(requestMessages(requests[0])[2]).toMatchObject({
      role: "toolResult",
      toolName: "execute",
      content: [{ type: "text", text: '{"opened":"/workspace"}' }],
      isError: false,
    });
    expect(openedUrls).toEqual(["/workspace"]);
  } finally {
    await secondAgent?.close();
    await firstAgent?.close();
    fixture.close();
  }
});

it("delivers one active steer from a second client at the Mule safe point and reopens it", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const faux = fauxModel([]);
  const requests: Context[] = [];
  let markStarted: () => void = () => undefined;
  let releaseFirst: () => void = () => undefined;
  const started = new Promise<void>((resolve) => {
    markStarted = resolve;
  });
  const held = new Promise<void>((resolve) => {
    releaseFirst = resolve;
  });
  faux.setResponses([
    async (context) => {
      requests.push(structuredClone(context));
      markStarted();
      await held;
      return fauxAssistantMessage("first answer before steering");
    },
    (context) => {
      requests.push(structuredClone(context));
      return fauxAssistantMessage("answer after steering");
    },
  ]);
  const options = (
    runtime: ReturnType<typeof createCompatibilityRuntimeHost>,
  ): ShiitakeOptions => ({
    ...compatibilityRunDependencies(),
    store: fixture.owner,
    runtime: runtime.runtime,
    model: {
      acquire: () =>
        Promise.resolve({
          model: faux.model,
          stream: faux.stream,
          release: () => Promise.resolve(),
        }),
    },
  });
  const runtime = createCompatibilityRuntimeHost();
  let agent: ShiitakeAgent | undefined;
  try {
    agent = await Shiitake.make(options(runtime));
    const firstClient = agent.session("two-client-steer");
    const secondClient = agent.session("two-client-steer");
    await firstClient.send("original request", {
      delivery: "followUp",
      requestId: "two-client-parent",
    });
    const running = runtime.runNext();
    await started;

    const steerAccepted = await secondClient.send("change direction", {
      delivery: "steer",
      requestId: "two-client-steer",
    });
    expect(steerAccepted).toMatchObject({ status: "accepted" });
    const steerCommandId = steerAccepted.commandId;
    expect((await firstClient.read()).pending.prompts).toContainEqual({
      delivery: "steer",
      commandId: steerCommandId,
      text: "change direction",
    });

    releaseFirst();
    await running;
    expect(requests).toHaveLength(2);
    expect(requests[1]?.messages).toContainEqual(
      expect.objectContaining({ role: "user", content: "change direction" }),
    );
    const settled = await firstClient.read();
    expect(settled.terminals).toContainEqual(
      expect.objectContaining({
        commandId: steerCommandId,
        outcome: "completed",
      }),
    );
    expect(settled.pending.prompts).toEqual([]);

    await agent.close();
    agent = undefined;
    const reopenedRuntime = createCompatibilityRuntimeHost();
    agent = await Shiitake.make(options(reopenedRuntime));
    const reopened = await agent.session("two-client-steer").read();
    expect(reopened.terminals).toContainEqual(
      expect.objectContaining({
        commandId: steerCommandId,
        outcome: "completed",
      }),
    );
    expect(
      reopened.messages.some(
        ({ value }) =>
          value.role === "user" && value.content === "change direction",
      ),
    ).toBe(true);
  } finally {
    releaseFirst();
    await agent?.close();
    fixture.close();
  }
});

it("signals active Pi work and keeps cancellation on the selected lineage", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const faux = fauxModel([]);
  let resolveStarted: () => void = () => undefined;
  const started = new Promise<void>((resolve) => {
    resolveStarted = resolve;
  });
  let modelSignal: AbortSignal | undefined;
  faux.setResponses([
    async (_context, options) => {
      modelSignal = options?.signal;
      resolveStarted();
      await new Promise<void>((resolve) => {
        if (modelSignal?.aborted === true) resolve();
        else
          modelSignal?.addEventListener(
            "abort",
            () => {
              resolve();
            },
            { once: true },
          );
      });
      return fauxAssistantMessage("discarded aborted response", {
        stopReason: "aborted",
      });
    },
    () => fauxAssistantMessage("follow-up survives abort"),
  ]);
  const runtime = createCompatibilityRuntimeHost();
  const agent = await Shiitake.make({
    store: fixture.owner,
    extensions: noExtensions,
    runtime: runtime.runtime,
    instructions: {
      load: () =>
        Promise.resolve({ revision: "active-cancel-v1", text: "Answer." }),
    },
    model: {
      acquire: () =>
        Promise.resolve({
          model: faux.model,
          stream: faux.stream,
          release: () => Promise.resolve(),
        }),
    },
  });
  let running: Promise<void> | undefined;
  try {
    const session = agent.session("active-cancel-session");
    const accepted = await session.command({
      requestId: "active-cancel-command",
      delivery: "followUp",

      type: "prompt",
      text: "Wait until I cancel this.",
    });
    const followUp = await session.send("Run after cancellation.", {
      requestId: "active-cancel-follow-up",
      delivery: "followUp",
    });
    const workId = requireAcceptedWorkId(
      fixture.owner,
      "active-cancel-session",
      accepted.commandId,
    );
    running = runtime.runNext();
    await started;

    await session.abort();
    // Repeating an active abort replays the committed cancellation while the
    // same Watchdog execution is still visible.
    await session.abort();
    await running;
    running = undefined;

    expect(modelSignal?.aborted).toBe(true);
    expect(
      fixture.owner.sql
        .exec(
          "SELECT state, outcome FROM watchdog_jobs WHERE job_id = ?",
          workId,
        )
        .one(),
    ).toEqual({ state: "settled", outcome: "cancelled" });
    expect(
      readSelectedPath(fixture.owner, {
        sessionId: "active-cancel-session",
      }).map(({ entry }) => entry.kind),
    ).toEqual([
      "command",
      "run_started",
      "turn_started",
      "model_prompt",
      "cancellation",
      "cancellation",
      "command",
      "run_terminal",
    ]);
    await runtime.runNext();
    expect((await session.read()).terminals).toMatchObject([
      { commandId: accepted.commandId, outcome: "cancelled" },
      { commandId: followUp.commandId, outcome: "cancelled" },
    ]);
  } finally {
    await running;
    await agent.close();
    fixture.close();
  }
});

it("interrupts a cooperatively held model acquisition before it is released", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const dependencies = compatibilityRunDependencies();
  type Acquired = Awaited<ReturnType<typeof dependencies.model.acquire>>;
  let resolveAcquisition!: (value: Acquired) => void;
  let rejectAcquisition!: (cause: unknown) => void;
  const acquisition = new Promise<Acquired>((resolve, reject) => {
    resolveAcquisition = resolve;
    rejectAcquisition = reject;
  });
  let resolveAcquisitionStarted!: () => void;
  const acquisitionStarted = new Promise<void>((resolve) => {
    resolveAcquisitionStarted = resolve;
  });
  let resolveSignalAbort!: () => void;
  const signalAborted = new Promise<void>((resolve) => {
    resolveSignalAbort = resolve;
  });
  let releases = 0;
  const agent = await Shiitake.make({
    ...dependencies,
    store: fixture.owner,
    extensions: noExtensions,
    runtime: runtime.runtime,
    model: {
      acquire: ({ signal }) => {
        resolveAcquisitionStarted();
        signal.addEventListener(
          "abort",
          () => {
            resolveSignalAbort();
            rejectAcquisition(new Error("cooperatively cancelled"));
          },
          { once: true },
        );
        return acquisition;
      },
    },
  });
  let running: Promise<void> | undefined;
  try {
    const session = agent.session("held-acquisition-cancel");
    await session.command({
      requestId: "held-acquisition-command",
      delivery: "followUp",
      type: "prompt",
      text: "Cancel while the model is still acquiring.",
    });
    running = runtime.runNext();
    await acquisitionStarted;

    const abort = session.abort();
    const promptly = await Promise.race([
      signalAborted.then(() => true),
      new Promise<false>((resolve) =>
        setTimeout(() => {
          resolve(false);
        }, 50),
      ),
    ]);
    if (!promptly) {
      // Keep the regression bounded if the old masked acquisition is present.
      const late = await dependencies.model.acquire();
      resolveAcquisition({
        ...late,
        release: async () => {
          releases += 1;
          await late.release();
        },
      });
    }
    await abort;
    await running;
    running = undefined;
    expect(promptly).toBe(true);
    expect(releases).toBe(0);
  } finally {
    rejectAcquisition(new Error("test cleanup"));
    await running;
    await agent.close();
    fixture.close();
  }
});

it("cancels one command without draining active or later follow-up work", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const faux = fauxModel([]);
  const calls: string[] = [];
  let resolveFirstStarted: () => void = () => undefined;
  const firstStarted = new Promise<void>((resolve) => {
    resolveFirstStarted = resolve;
  });
  let releaseFirst: () => void = () => undefined;
  const firstReleased = new Promise<void>((resolve) => {
    releaseFirst = resolve;
  });
  let firstSignal: AbortSignal | undefined;
  let resolveActiveStarted: () => void = () => undefined;
  const activeStarted = new Promise<void>((resolve) => {
    resolveActiveStarted = resolve;
  });
  let releaseActive: () => void = () => undefined;
  const activeReleased = new Promise<void>((resolve) => {
    releaseActive = resolve;
  });
  let activeSignal: AbortSignal | undefined;
  faux.setResponses([
    async (_context, options) => {
      calls.push("first");
      firstSignal = options?.signal;
      resolveFirstStarted();
      await firstReleased;
      return fauxAssistantMessage("first reply");
    },
    () => {
      calls.push("third");
      return fauxAssistantMessage("third reply");
    },
    async (_context, options) => {
      calls.push("fourth");
      activeSignal = options?.signal;
      resolveActiveStarted();
      await Promise.race([
        activeReleased,
        new Promise<void>((resolve) => {
          if (activeSignal?.aborted === true) resolve();
          else
            activeSignal?.addEventListener(
              "abort",
              () => {
                resolve();
              },
              { once: true },
            );
        }),
      ]);
      return fauxAssistantMessage("discarded active reply", {
        stopReason: "aborted",
      });
    },
    () => {
      calls.push("fifth");
      return fauxAssistantMessage("fifth reply");
    },
  ]);
  const runtime = createCompatibilityRuntimeHost();
  const agent = await Shiitake.make({
    store: fixture.owner,
    extensions: noExtensions,
    runtime: runtime.runtime,
    instructions: {
      load: () =>
        Promise.resolve({ revision: "targeted-cancel-v1", text: "Answer." }),
    },
    model: {
      acquire: () =>
        Promise.resolve({
          model: faux.model,
          stream: faux.stream,
          release: () => Promise.resolve(),
        }),
    },
  });
  let running: Promise<void> | undefined;
  try {
    const session = agent.session("targeted-cancel-session");
    const first = await session.send("first", {
      delivery: "followUp",
      requestId: "targeted-first-receipt",
    });
    const second = await session.send("second", {
      delivery: "followUp",
      requestId: "targeted-second-receipt",
    });
    const third = await session.send("third", {
      delivery: "followUp",
      requestId: "targeted-third-receipt",
    });

    running = runtime.runNext();
    await firstStarted;
    await expect(
      session.cancel({ commandId: second.commandId, onlyIfPending: true }),
    ).resolves.toEqual({
      status: "accepted",
      commandId: second.commandId,
    });
    expect(firstSignal?.aborted).toBe(false);
    releaseFirst();
    await running;
    running = undefined;
    await runtime.runNext();
    await runtime.runNext();

    let snapshot = await session.read();
    expect(snapshot.terminals).toMatchObject([
      { commandId: first.commandId, outcome: "completed" },
      { commandId: second.commandId, outcome: "cancelled" },
      { commandId: third.commandId, outcome: "completed" },
    ]);
    expect(calls).toEqual(["first", "third"]);

    const fourth = await session.send("fourth", {
      delivery: "followUp",
      requestId: "targeted-fourth-receipt",
    });
    const fifth = await session.send("fifth", {
      delivery: "followUp",
      requestId: "targeted-fifth-receipt",
    });
    running = runtime.runNext();
    await activeStarted;
    await expect(
      session.cancel({ commandId: fourth.commandId, onlyIfPending: true }),
    ).resolves.toEqual({ status: "too_late", commandId: fourth.commandId });
    expect(activeSignal?.aborted).toBe(false);
    await expect(
      session.cancel({ commandId: fourth.commandId }),
    ).resolves.toEqual({
      status: "accepted",
      commandId: fourth.commandId,
    });
    expect(activeSignal?.aborted).toBe(true);
    await running;
    running = undefined;
    await runtime.runNext();

    snapshot = await session.read();
    expect(snapshot.terminals).toMatchObject([
      { commandId: first.commandId, outcome: "completed" },
      { commandId: second.commandId, outcome: "cancelled" },
      { commandId: third.commandId, outcome: "completed" },
      { commandId: fourth.commandId, outcome: "cancelled" },
      { commandId: fifth.commandId, outcome: "completed" },
    ]);
    expect(calls).toEqual(["first", "third", "fourth", "fifth"]);
  } finally {
    releaseFirst();
    releaseActive();
    await running;
    await agent.close();
    fixture.close();
  }
});

it("branches and forks through the local Session facade", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const agent = await Shiitake.make({
    store: fixture.owner,
    runtime: runtime.runtime,
    ...compatibilityRunDependencies(),
  });
  const session = agent.session("public-branch-fork");

  try {
    await session.command({
      delivery: "followUp",

      type: "prompt",
      requestId: "public-first",
      text: "first",
    });
    await runtime.runNext();
    await session.command({
      delivery: "followUp",

      type: "prompt",
      requestId: "public-second",
      text: "second",
    });
    await runtime.runNext();

    const complete = await session.read();
    const firstMessageId = complete.messages[1]?.id;
    expect(firstMessageId).toEqual(expect.any(String));
    if (firstMessageId === undefined) throw new Error("Missing first message");

    const watch = await session.watch();
    const changes = watch.changes[Symbol.asyncIterator]();
    await expect(
      session.command({ type: "branch", entryId: firstMessageId }),
    ).resolves.toBeUndefined();
    await expect(changes.next()).resolves.toMatchObject({
      value: {
        kind: "reset",
        baseline: { messages: complete.messages.slice(0, 2) },
      },
    });
    await changes.return?.();
    await watch.close();

    const selected = await session.read();
    expect(selected.messages).toEqual(complete.messages.slice(0, 2));

    const forked = await session.command({
      type: "fork",
      entryId: firstMessageId,
    });
    expect(forked.sessionId).toEqual(expect.any(String));
    expect(forked.sessionId).not.toBe("public-branch-fork");
    const forkedMessages = (await agent.session(forked.sessionId).read())
      .messages;
    expect(forkedMessages.map(({ value }) => value)).toEqual(
      selected.messages.map(({ value }) => value),
    );
    expect(forkedMessages.map(({ id }) => id)).not.toEqual(
      selected.messages.map(({ id }) => id),
    );

    await session.command({
      delivery: "followUp",

      type: "prompt",
      requestId: "public-queued",
      text: "queued",
    });
    const queued = await session.read();
    const queuedWrites = fixture.ownerWrites();
    await expect(
      session.command({ type: "branch", entryId: firstMessageId }),
    ).rejects.toMatchObject({
      boundary: "runtime",
      cause: "conflict",
    });
    await expect(
      session.command({ type: "fork", entryId: firstMessageId }),
    ).rejects.toMatchObject({
      boundary: "runtime",
      cause: "conflict",
    });
    expect(await session.read()).toEqual(queued);
    expect(fixture.ownerWrites()).toBe(queuedWrites);
  } finally {
    await agent.close();
    fixture.close();
  }
});

it("decodes Session branch and fork input before owner lookup", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const agent = await Shiitake.make({
    store: fixture.owner,
    runtime: runtime.runtime,
    ...compatibilityRunDependencies(),
  });
  const validSession = agent.session("decode-before-lookup");
  const invalidSession = agent.session("");
  const operationsAfterMake = fixture.ownerOperations();
  try {
    await expect(
      invalidSession.command({
        type: "branch",
        entryId: "entry",
      }),
    ).rejects.toMatchObject({ boundary: "contract", cause: "invalid" });
    expect(fixture.ownerOperations()).toBe(operationsAfterMake);

    await expect(
      validSession.command({
        type: "branch",
        entryId: "",
      }),
    ).rejects.toMatchObject({ boundary: "contract", cause: "invalid" });
    expect(fixture.ownerOperations()).toBe(operationsAfterMake);

    const excessCommand = {
      type: "fork" as const,
      entryId: "entry",
      extra: true,
    };
    await expect(validSession.command(excessCommand)).rejects.toMatchObject({
      boundary: "contract",
      cause: "invalid",
    });
    expect(fixture.ownerOperations()).toBe(operationsAfterMake);
  } finally {
    await agent.close();
    fixture.close();
  }
});

it("reopens branched and forked histories from a native SQLite file", async () => {
  const directory = await mkdtemp(join(tmpdir(), "shiitake-branch-fork-"));
  const databasePath = join(directory, "session.sqlite");
  const fixture = createNativeSqliteOwnerFixture(databasePath);
  const runtime = createCompatibilityRuntimeHost();
  const faux = fauxModel([], { tokenSize: { min: 1, max: 1 } });
  const requests: Context[] = [];
  const makeAgent = () =>
    Shiitake.make({
      store: fixture.owner,
      extensions: noExtensions,
      runtime: runtime.runtime,
      instructions: {
        load: () =>
          Promise.resolve({ revision: "branch-fork-file-v1", text: "Answer." }),
      },
      model: {
        acquire: () =>
          Promise.resolve({
            model: faux.model,
            stream: faux.stream,
            release: () => Promise.resolve(),
          }),
      },
    });
  let agent: ShiitakeAgent | undefined;
  let fixtureClosed = false;
  try {
    faux.setResponses([
      fauxAssistantMessage("first answer"),
      fauxAssistantMessage("second answer"),
    ]);
    agent = await makeAgent();
    const source = agent.session("branch-fork-file");
    await source.command({
      delivery: "followUp",

      type: "prompt",
      requestId: "file-first",
      text: "first",
    });
    await runtime.runNext();
    await source.command({
      delivery: "followUp",

      type: "prompt",
      requestId: "file-second",
      text: "second",
    });
    await runtime.runNext();

    const complete = await source.read();
    const firstCommandId = complete.messages[0]?.id;
    if (firstCommandId === undefined) throw new Error("Missing first command");
    await source.command({ type: "branch", entryId: firstCommandId });
    const selected = await source.read();
    const forked = await source.command({
      type: "fork",
      entryId: firstCommandId,
    });
    const forkedBeforeClose = await agent.session(forked.sessionId).read();
    const selectedIds = selected.messages.map(({ id }) => id);
    const forkedIds = forkedBeforeClose.messages.map(({ id }) => id);
    expect(forkedIds).not.toEqual(selectedIds);

    await agent.close();
    agent = undefined;
    fixture.close();
    fixtureClosed = true;

    const reopenedFixture = createNativeSqliteOwnerFixture(databasePath);
    const reopenedRuntime = createCompatibilityRuntimeHost();
    faux.setResponses([
      (context) => {
        requests.push(structuredClone(context));
        return fauxAssistantMessage("reopened answer");
      },
    ]);
    const reopened = await Shiitake.make({
      store: reopenedFixture.owner,
      extensions: noExtensions,
      runtime: reopenedRuntime.runtime,
      instructions: {
        load: () =>
          Promise.resolve({ revision: "branch-fork-file-v1", text: "Answer." }),
      },
      model: {
        acquire: () =>
          Promise.resolve({
            model: faux.model,
            stream: faux.stream,
            release: () => Promise.resolve(),
          }),
      },
    });
    try {
      const reopenedSource = reopened.session("branch-fork-file");
      const reopenedSelected = await reopenedSource.read();
      const reopenedForked = await reopened.session(forked.sessionId).read();
      expect(reopenedSelected.messages.map(({ value }) => value)).toEqual(
        selected.messages.map(({ value }) => value),
      );
      expect(reopenedForked.messages.map(({ value }) => value)).toEqual(
        forkedBeforeClose.messages.map(({ value }) => value),
      );
      expect(reopenedForked.messages.map(({ id }) => id)).toEqual(forkedIds);

      await reopenedSource.command({
        delivery: "followUp",

        type: "prompt",
        requestId: "file-reopened",
        text: "reopened",
      });
      await reopenedRuntime.runNext();
      expect(requestMessages(requests[0])).toEqual([
        expect.objectContaining({ role: "user", content: "first" }),
        expect.objectContaining({ role: "user", content: "reopened" }),
      ]);
    } finally {
      await reopened.close();
      reopenedFixture.close();
    }
  } finally {
    await agent?.close();
    if (!fixtureClosed) fixture.close();
    await rm(directory, { recursive: true, force: true });
  }
});

it("rejects missing and incomplete branch or fork targets publicly", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const agent = await Shiitake.make({
    store: fixture.owner,
    runtime: runtime.runtime,
    ...compatibilityRunDependencies(),
  });
  const session = agent.session("invalid-branch-fork-target");
  try {
    for (const type of ["branch", "fork"] as const) {
      await expect(
        session.command({ type, entryId: "missing-entry" }),
      ).rejects.toMatchObject({
        boundary: "store",
        cause: "unavailable",
      });
    }

    const root = woodstock.appendSessionEntry(fixture.owner, {
      sessionId: "invalid-branch-fork-target",
      id: sessionIds.fact.generate(),
      entry: {
        kind: "command",
        workId: sessionIds.work.generate(),
        requestId: "invalid-target-root",
        receiptId: sessionIds.receipt.generate(),
        message: { role: "user", content: "root", timestamp: 1 },
        contract: {
          core: "a".repeat(64),
          extensions: [],
        },
      },
    });
    const run = { kind: "run", value: "invalid-target-run" } as const;
    const turn = {
      kind: "turn",
      value: "invalid-target-turn",
      parent: run,
    } as const;
    const message = {
      kind: "message",
      value: "invalid-target-message",
      parent: turn,
    } as const;
    if (root.entry.workId === null)
      throw new Error("Expected retained root work identity");
    const incomplete = woodstock.appendSessionEntry(fixture.owner, {
      sessionId: "invalid-branch-fork-target",
      id: message.value,
      state: "streaming",
      entry: {
        kind: "message_start",
        workId: root.entry.workId,
        run,
        turn,
        message,
        role: "assistant",
      },
    });
    expect(incomplete.state).toBe("streaming");

    for (const type of ["branch", "fork"] as const) {
      await expect(
        session.command({ type, entryId: incomplete.id }),
      ).rejects.toMatchObject({
        boundary: "store",
        cause: "unavailable",
      });
    }
  } finally {
    await agent.close();
    fixture.close();
  }
});

it("settles a pre-Mule acquisition failure and publishes idle status", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  let resolveAcquiring: () => void = () => undefined;
  let rejectAcquisition: () => void = () => undefined;
  const acquiring = new Promise<void>((resolve) => {
    resolveAcquiring = resolve;
  });
  const acquisition = new Promise<never>((_resolve, reject) => {
    rejectAcquisition = () => {
      reject(new Error("Model acquisition failed"));
    };
  });
  const agent = await Shiitake.make({
    store: fixture.owner,
    extensions: noExtensions,
    runtime: runtime.runtime,
    instructions: {
      load: () =>
        Promise.resolve({ revision: "pre-mule-failure-v1", text: "Answer." }),
    },
    model: {
      acquire: () => {
        resolveAcquiring();
        return acquisition;
      },
    },
  });
  const session = agent.session("pre-mule-failure-session");
  const watch = await session.watch();
  const changes = watch.changes[Symbol.asyncIterator]();
  let running: Promise<void> | undefined;
  try {
    const accepted = await session.command({
      requestId: "pre-mule-failure-command",
      delivery: "followUp",

      type: "prompt",
      text: "Fail before Mule starts.",
    });
    await expect(session.read()).resolves.toMatchObject({
      messages: [],
      activity: { queue: { state: "queued" } },
    });

    await expect(changes.next()).resolves.toMatchObject({
      done: false,
      value: { kind: "update", activity: { queue: { state: "queued" } } },
    });
    const activeFrame = changes.next();
    running = runtime.runNext();
    await acquiring;
    // The claim observation precedes loading the command into the lineage.
    await expect(activeFrame).resolves.toMatchObject({
      done: false,
      value: {
        kind: "update",
        messages: [],
        activity: { queue: { state: "running" } },
      },
    });
    // Model acquisition happens after lineage preparation; inspect that phase.
    await expect(session.read()).resolves.toMatchObject({
      messages: [
        {
          commandId: accepted.commandId,
          value: { role: "user", content: "Fail before Mule starts." },
        },
      ],
      activity: { queue: { state: "running" } },
    });

    await expect(changes.next()).resolves.toMatchObject({
      done: false,
      value: {
        kind: "update",
        messages: [
          {
            commandId: accepted.commandId,
            value: { role: "user", content: "Fail before Mule starts." },
          },
        ],
        activity: { queue: { state: "running" } },
      },
    });
    const idleFrame = (async () => {
      for (;;) {
        const frame = await changes.next();
        if (
          frame.done ||
          (frame.value.kind !== "reset" &&
            frame.value.activity.queue.state === "idle")
        )
          return frame;
      }
    })();
    rejectAcquisition();
    await running;
    running = undefined;
    // Watch updates contain only entries after their cursor, not a new baseline.
    await expect(idleFrame).resolves.toMatchObject({
      done: false,
      value: {
        kind: "update",
        messages: [],
        activity: { queue: { state: "idle" } },
      },
    });
    // Settlement must preserve the user message in durable Session history.
    await expect(session.read()).resolves.toMatchObject({
      messages: [
        {
          commandId: accepted.commandId,
          value: { role: "user", content: "Fail before Mule starts." },
        },
      ],
      activity: { queue: { state: "idle" } },
    });

    const workId = requireAcceptedWorkId(
      fixture.owner,
      "pre-mule-failure-session",
      accepted.commandId,
    );
    expect(
      fixture.owner.sql
        .exec(
          "SELECT state, outcome FROM watchdog_jobs WHERE job_id = ?",
          workId,
        )
        .one(),
    ).toEqual({ state: "settled", outcome: "failed" });
  } finally {
    rejectAcquisition();
    await running;
    await watch.close();
    await agent.close();
    fixture.close();
  }
});

it("preserves queued prompt lineage and correlation through the public Session", async () => {
  const directory = await mkdtemp(join(tmpdir(), "shiitake-overlap-"));
  const databasePath = join(directory, "agent.sqlite");
  let fixture = createNativeSqliteOwnerFixture(databasePath);
  const faux = fauxModel([]);
  let releaseA: () => void = () => undefined;
  const aHeld = new Promise<void>((resolve) => {
    releaseA = resolve;
  });
  let aStarted: () => void = () => undefined;
  const started = new Promise<void>((resolve) => {
    aStarted = resolve;
  });
  const contexts: Context[] = [];
  faux.setResponses([
    async () => {
      aStarted();
      await aHeld;
      return fauxAssistantMessage("A completed reply");
    },
    (context) => {
      contexts.push(structuredClone(context));
      return fauxAssistantMessage("B completed reply");
    },
  ]);
  const options = (
    runtime: ReturnType<typeof createCompatibilityRuntimeHost>,
  ): ShiitakeOptions => ({
    store: fixture.owner,
    runtime: runtime.runtime,
    ...compatibilityRunDependencies(),
    model: {
      acquire: () =>
        Promise.resolve({
          model: faux.model,
          stream: faux.stream,
          release: () => Promise.resolve(),
        }),
    },
  });
  let runtime = createCompatibilityRuntimeHost();
  let agent: ShiitakeAgent | undefined = await Shiitake.make(options(runtime));
  try {
    const session = agent.session("overlap-public");
    const a = await session.command({
      delivery: "followUp",

      type: "prompt",
      requestId: "overlap-A",
      text: "A prompt",
    });
    const running = runtime.runNext();
    await started;
    const b = await session.command({
      delivery: "followUp",

      type: "prompt",
      requestId: "overlap-B",
      text: "B prompt",
    });
    await expect(
      session.command({
        delivery: "followUp",

        type: "prompt",
        requestId: "overlap-B",
        text: "B prompt",
      }),
    ).resolves.toEqual(b);
    expect((await session.read()).pending).toEqual({
      total: 1,
      prompts: [
        { commandId: b.commandId, text: "B prompt", delivery: "followUp" },
      ],
      hasMore: false,
    });
    const beforeBDispatch = (await session.read()).messages;
    expect(beforeBDispatch).toHaveLength(1);
    expect(beforeBDispatch[0]?.value).toMatchObject({
      role: "user",
      content: "A prompt",
    });
    const bWorkId = requireAcceptedWorkId(
      fixture.owner,
      "overlap-public",
      b.commandId,
    );
    expect(
      fixture.owner.sql
        .exec(
          "SELECT COUNT(*) AS count FROM session_entries WHERE session_id = ? AND work_id = ? AND type = 'command'",
          "overlap-public",
          bWorkId,
        )
        .one().count,
    ).toBe(0);
    releaseA();
    await running;

    await agent.close();
    agent = undefined;
    fixture.close();
    fixture = createNativeSqliteOwnerFixture(databasePath);
    runtime = createCompatibilityRuntimeHost();
    agent = await Shiitake.make(options(runtime));

    expect((await agent.session("overlap-public").read()).pending).toEqual({
      total: 1,
      prompts: [
        { commandId: b.commandId, text: "B prompt", delivery: "followUp" },
      ],
      hasMore: false,
    });
    await runtime.runNext();
    const reopened = agent.session("overlap-public");
    const writesAfterDispatch = fixture.ownerWrites();
    await expect(
      reopened.command({
        delivery: "followUp",

        type: "prompt",
        requestId: "overlap-B",
        text: "B prompt",
      }),
    ).resolves.toEqual(b);
    expect(fixture.ownerWrites()).toBe(writesAfterDispatch);
    const read = await reopened.read();
    expect(read.pending).toEqual({ total: 0, prompts: [], hasMore: false });
    expect(read.messages.map(({ value }) => value)).toMatchObject([
      { role: "user", content: "A prompt" },
      {
        role: "assistant",
        content: [{ type: "text", text: "A completed reply" }],
      },
      { role: "user", content: "B prompt" },
      {
        role: "assistant",
        content: [{ type: "text", text: "B completed reply" }],
      },
    ]);
    expect(read.terminals.map(({ commandId }) => commandId)).toEqual([
      a.commandId,
      b.commandId,
    ]);
    expect(
      fixture.owner.sql
        .exec(
          "SELECT COUNT(*) AS count FROM session_entries WHERE session_id = ? AND work_id = ? AND type = 'command'",
          "overlap-public",
          bWorkId,
        )
        .one().count,
    ).toBe(1);
    expect(requestMessages(contexts[0])).toMatchObject([
      { role: "user", content: "A prompt" },
      {
        role: "assistant",
        content: [{ type: "text", text: "A completed reply" }],
      },
      { role: "user", content: "B prompt" },
    ]);

    await agent.close();
    agent = undefined;
    fixture.close();
    fixture = createNativeSqliteOwnerFixture(databasePath);
    runtime = createCompatibilityRuntimeHost();
    agent = await Shiitake.make(options(runtime));
    expect(
      (await agent.session("overlap-public").read()).terminals.map(
        ({ commandId }) => commandId,
      ),
    ).toEqual([a.commandId, b.commandId]);
  } finally {
    releaseA();
    await agent?.close();
    fixture.close();
    await rm(directory, { recursive: true, force: true });
  }
});

it("cancels a queued prompt and retains its text when abort arrives before execution", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const agent = await Shiitake.make({
    store: fixture.owner,
    runtime: runtime.runtime,
    ...compatibilityRunDependencies(),
  });
  try {
    const session = agent.session("queued-cancellation");
    const accepted = await session.command({
      delivery: "followUp",

      type: "prompt",
      requestId: "queued-cancellation-request",
      text: "Keep this prompt for resubmission.",
    });
    expect((await session.read()).messages).toEqual([]);

    await session.abort();
    expect((await session.read()).pending).toEqual({
      total: 0,
      prompts: [],
      hasMore: false,
    });
    await runtime.runNext();

    expect((await session.read()).messages).toMatchObject([
      {
        value: { role: "user", content: "Keep this prompt for resubmission." },
      },
    ]);
    expect((await session.read()).pending).toEqual({
      total: 0,
      prompts: [],
      hasMore: false,
    });
    expect((await session.read()).terminals).toMatchObject([
      {
        commandId: accepted.commandId,
        outcome: "cancelled",
      },
    ]);
  } finally {
    await agent.close();
    fixture.close();
  }
});

it("projects failed work before a queued prompt enters the conversation", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const agent = await Shiitake.make({
    store: fixture.owner,
    runtime: runtime.runtime,
    ...compatibilityRunDependencies(),
  });
  try {
    const session = agent.session("queued-failure");
    const accepted = await session.command({
      delivery: "followUp",

      type: "prompt",
      requestId: "queued-failure-request",
      text: "Fail before entering this prompt.",
    });
    const workId = requireAcceptedWorkId(
      fixture.owner,
      "queued-failure",
      accepted.commandId,
    );
    fixture.owner.sql.exec(
      "UPDATE watchdog_jobs SET state = 'settled', outcome = 'failed' WHERE job_id = ? AND state = 'queued'",
      workId,
    );

    await runtime.runNext();

    expect((await session.read()).messages).toEqual([]);
    expect((await session.read()).pending).toEqual({
      total: 0,
      prompts: [],
      hasMore: false,
    });
    expect((await session.read()).terminals).toMatchObject([
      { commandId: workId, outcome: "failed" },
    ]);
  } finally {
    await agent.close();
    fixture.close();
  }
});
