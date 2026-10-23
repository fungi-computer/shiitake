import { directSandbox } from "@fungi.computer/mycelium/testing";
import { idleActivity } from "./fixtures/client.fixture.js";
import { Mycelium } from "@fungi.computer/mycelium";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import type { AssistantMessage } from "@earendil-works/pi-ai";
import { expect, it } from "vitest";
import { z } from "zod";
import type {
  Accepted,
  AbortCommand,
  BranchCommand,
  OperatorCommand,
  TitleCommand,
  CancelCommand,
  ForkCommand,
  ForkedSession,
  CompactCommand,
  PromptCommand,
  SessionCommand,
  SessionMessage,
  SessionSnapshot,
  SessionTerminal,
  SendOptions,
  ShiitakeAgent,
  ShiitakeSession,
} from "../src/client.js";
import {
  createAgentControlSource,
  waitForAgentTerminal,
} from "../src/agent-control.js";
import { ShiitakeError } from "../src/errors.js";
import { decodeClientReadOptions } from "../src/internal/client-boundary.js";
import { readClientSnapshot } from "../src/internal/woodstock/client-projection.js";
import { prepareWoodstockSchema } from "../src/internal/woodstock/index.js";
import { writeLargeSession } from "./fixtures/large-session.fixture.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

const baseline: SessionSnapshot = {
  sessionId: "session-a",
  cursor: null,
  hasMore: false,
  messages: [],
  terminals: [],
  pending: { total: 0, prompts: [], hasMore: false },
  activity: idleActivity,
  toolOutput: [],
  progress: null,
  extensions: {},
  attention: [],
};

function testSession(
  onSend?: (text: string, options: SendOptions | undefined) => void,
  watchOverride?: ShiitakeSession["watch"],
): ShiitakeSession {
  async function command(
    input: PromptCommand | CompactCommand | CancelCommand,
  ): Promise<Accepted>;
  async function command(
    input: AbortCommand | BranchCommand | OperatorCommand | TitleCommand,
  ): Promise<undefined>;
  async function command(input: ForkCommand): Promise<ForkedSession>;
  async function command(
    input: SessionCommand,
  ): Promise<Accepted | ForkedSession | undefined>;
  function command(input: SessionCommand) {
    if (input.type === "prompt")
      return Promise.resolve({
        status: "accepted" as const,
        commandId: `command-${input.requestId}`,
      });
    if (input.type === "fork") return Promise.resolve({ sessionId: "forked" });
    if (input.type === "cancel")
      return Promise.resolve({
        status: "accepted" as const,
        commandId: input.commandId,
      });
    return Promise.resolve(undefined);
  }
  const session: ShiitakeSession = {
    address: "session-a",
    command,
    withdraw: () => Promise.resolve({ status: "unknown" }),
    abort: () => Promise.resolve(),
    send: (text, options) => {
      onSend?.(text, options);
      return Promise.resolve({
        status: "accepted" as const,
        commandId: text,
      });
    },
    cancel: ({ commandId }) =>
      Promise.resolve({ status: "accepted" as const, commandId }),
    latestPending: () => Promise.resolve(null),
    extension: () => ({ call: () => Promise.reject(new Error("unused")) }),
    modelPreference: {
      read: () => Promise.resolve(null),
      set: () => Promise.resolve(),
    },
    read: () => Promise.resolve(baseline),
    subscribe: () => ({
      async *[Symbol.asyncIterator]() {
        await Promise.resolve();
        yield {
          kind: "reset" as const,
          reason: "history_unavailable" as const,
          cursor: null,
          baseline,
        };
      },
    }),
    watch:
      watchOverride ??
      (() =>
        Promise.resolve({
          snapshot: baseline,
          changes: {
            async *[Symbol.asyncIterator]() {
              await Promise.resolve();
              yield {
                kind: "reset" as const,
                reason: "history_unavailable" as const,
                cursor: null,
                baseline,
              };
              yield {
                kind: "update" as const,
                delivery: "live" as const,
                cursor: "cursor-1",
                pending: baseline.pending,
                messages: [],
                terminals: [
                  {
                    commandId: "target",
                    outcome: "completed" as const,
                    sequence: 1,
                    diagnostic: null,
                  },
                ],
                activity: idleActivity,
                extensions: baseline.extensions,
                attention: [],
              };
            },
          },
          close: () => Promise.resolve(),
        })),
  };
  return session;
}

it("executes Agent list, read, wait and send through the public Mycelium door", async () => {
  const sends: { text: string; options: SendOptions | undefined }[] = [];
  const createdSessions: string[] = [];
  const createdAgents: unknown[] = [];
  const modelChanges: unknown[] = [];
  const session = testSession((text, options) => {
    sends.push({ text, options });
  });
  const agent: ShiitakeAgent = {
    createSession: (requestId) => {
      createdSessions.push(requestId);
      return Promise.resolve("session-new");
    },
    deleteSession: () => Promise.resolve(false),
    listSessions: () =>
      Promise.resolve({
        sessions: [
          {
            sessionId: "session-a",
            createdAt: 1,
            lastActivityAt: 2,
            title: "Garden",
            state: "done",
          },
        ],
        next: undefined,
      }),
    extensions: {
      list: () => Promise.resolve([]),
      resume: () => Promise.reject(new Error("unused")),
      call: () => Promise.reject(new Error("unused")),
    },
    session: () => session,
    close: () => Promise.resolve(),
  };
  const runtime = await Mycelium.make({
    session: { sessionId: "fixture-parent" },
    sources: [
      createAgentControlSource({
        send: ({ text, requestId, delivery }) =>
          session.send(text, { requestId, delivery }),
        list: () => Promise.resolve([{ agentId: "agent-a", name: "Ada" }]),
        create: (input) => {
          createdAgents.push(input);
          return Promise.resolve({ agentId: "agent-new", name: "New Agent" });
        },
        setModel: (input) => {
          modelChanges.push(input);
          return Promise.resolve();
        },
        open: (agentId) => {
          if (agentId !== "agent-a") throw new Error("Agent is not authorized");
          return Promise.resolve({ agent, release: () => Promise.resolve() });
        },
      }),
    ],
    sandbox: directSandbox(async (bindings) => {
      const tools = bindings.agents;
      if (tools === undefined) throw new Error("Agent tools missing");
      const { list, create, createSession, read, send, cancel, wait } = tools;
      if (
        list === undefined ||
        create === undefined ||
        createSession === undefined ||
        read === undefined ||
        send === undefined ||
        cancel === undefined ||
        wait === undefined
      )
        throw new Error("Agent operations missing");
      const listed = await list({});
      const createdAgent = await create({
        requestId: "agent-request",
        model: "openai/gpt-6-astra",
      });
      if (tools.setModel === undefined)
        throw new Error("Model control missing");
      await tools.setModel({ agentId: "agent-a", model: "openai/gpt-6-sol" });
      const createdSession = await createSession({
        agentId: "agent-a",
        requestId: "session-request",
      });
      let oversizedRequestRejected = false;
      try {
        await createSession({
          agentId: "agent-a",
          requestId: "é".repeat(200),
        });
      } catch {
        oversizedRequestRejected = true;
      }
      const readResult = await read({
        agentId: "agent-a",
        sessionId: "session-a",
      });
      const sent = await send({
        agentId: "agent-a",
        sessionId: "session-a",
        requestId: "caller-send-request",
        text: "hello",
        delivery: "steer",
      });
      let missingDeliveryRejected = false;
      try {
        await send({
          agentId: "agent-a",
          sessionId: "session-a",
          requestId: "missing-delivery-request",
          text: "missing delivery",
        });
      } catch {
        missingDeliveryRejected = true;
      }
      const cancelled = await cancel({
        agentId: "agent-a",
        sessionId: "session-a",
        commandId: "send-1",
      });
      const waited = await wait({
        agentId: "agent-a",
        sessionId: "session-a",
        commandId: "target",
        timeoutMs: 1000,
      });
      if (tools.goal !== undefined)
        throw new Error("Agent control still names a feature operation");
      return {
        listed,
        createdAgent,
        createdSession,
        oversizedRequestRejected,
        read: readResult,
        sent,
        missingDeliveryRejected,
        cancelled,
        waited,
      };
    }),
  });
  const lease = await runtime.acquire();
  try {
    const prepared = await Effect.runPromise(
      lease.executeTool.prepare({
        type: "toolCall",
        id: "agent-control-1",
        name: "execute",
        arguments: { code: "return agents" },
      }),
    );
    const parts = await Effect.runPromise(
      Stream.runCollect(prepared.execute()),
    );
    expect(parts[0]).toMatchObject({
      type: "result",
      result: { isError: false },
    });
    expect(JSON.stringify(parts[0])).toContain('\\"commandId\\":\\"target\\"');
    expect(JSON.stringify(parts[0])).toContain(
      '\\"missingDeliveryRejected\\":true',
    );
    expect(sends).toEqual([
      {
        text: "hello",
        options: { delivery: "steer", requestId: "caller-send-request" },
      },
    ]);
    expect(createdAgents).toEqual([
      {
        requestId: "agent-request",
        model: { providerId: "openai", modelId: "gpt-6-astra" },
      },
    ]);
    expect(modelChanges).toEqual([
      {
        agentId: "agent-a",
        model: { providerId: "openai", modelId: "gpt-6-sol" },
      },
    ]);
    expect(createdSessions).toEqual(["session-request"]);
    expect(JSON.stringify(parts[0])).toContain(
      '\\"oversizedRequestRejected\\":true',
    );
  } finally {
    await lease.release();
    await runtime.close();
  }
});

it("omits Agent creation when the host does not authorize it", async () => {
  const runtime = await Mycelium.make({
    sources: [
      createAgentControlSource({
        list: () => Promise.resolve([]),
        open: () => Promise.reject(new Error("No Agent is authorized")),
      }),
    ],
    sandbox: directSandbox((bindings) => {
      const tools = bindings.agents;
      if (tools === undefined) throw new Error("Agent tools missing");
      if (tools.create !== undefined)
        throw new Error("Agent creation was exposed without host authority");
      if (tools.setModel !== undefined)
        throw new Error("Model control was exposed without host authority");
      return Promise.resolve({
        hasCreateSession: tools.createSession !== undefined,
      });
    }),
  });
  const lease = await runtime.acquire();
  try {
    const prepared = await Effect.runPromise(
      lease.executeTool.prepare({
        type: "toolCall",
        id: "agent-control-without-create",
        name: "execute",
        arguments: { code: "return agents" },
      }),
    );
    const parts = await Effect.runPromise(
      Stream.runCollect(prepared.execute()),
    );
    expect(parts[0]).toMatchObject({
      type: "result",
      result: { isError: false },
    });
    expect(JSON.stringify(parts[0])).toContain('\\"hasCreateSession\\":true');
  } finally {
    await lease.release();
    await runtime.close();
  }
});

it("keeps a resolved watch open until delayed terminal observation", async () => {
  let closed = false;
  const session = testSession(undefined, () =>
    Promise.resolve({
      snapshot: baseline,
      changes: {
        async *[Symbol.asyncIterator]() {
          await new Promise<void>((resolve) => {
            setTimeout(resolve, 0);
          });
          if (closed)
            throw new Error("Watch closed before terminal observation");
          yield {
            kind: "update" as const,
            delivery: "live" as const,
            cursor: "cursor-1",
            pending: baseline.pending,
            messages: [],
            terminals: [
              {
                commandId: "target",
                outcome: "completed" as const,
                sequence: 1,
                diagnostic: null,
              },
            ],
            activity: idleActivity,
            extensions: baseline.extensions,
            attention: [],
          };
        },
      },
      close: () => {
        closed = true;
        return Promise.resolve();
      },
    }),
  );

  await expect(
    waitForAgentTerminal(
      session,
      "target",
      1_000,
      new AbortController().signal,
    ),
  ).resolves.toMatchObject({ commandId: "target", outcome: "completed" });
  expect(closed).toBe(true);
});

it.each([true, false])(
  "delegation uses only the host's invoking Session (present: %s)",
  async (hasSession) => {
    const parents: string[] = [];
    const sends: unknown[] = [];
    const options = {
      sources: [
        createAgentControlSource({
          list: () => Promise.resolve([]),
          open: () =>
            Promise.reject(new Error("Delegation must use the host mail door")),
          send: (input, parentSessionId) => {
            parents.push(parentSessionId);
            sends.push(input);
            return Promise.resolve({
              status: "accepted",
              commandId: "owner-issued",
            });
          },
        }),
      ],
      sandbox: directSandbox(async (bindings) => {
        const send = bindings.agents?.send;
        if (!send) throw new Error("Missing send");
        const input = {
          agentId: "child",
          sessionId: "child-session",
          text: "Task",
          delivery: "followUp",
          requestId: "caller-delegation",
        };
        await expect(
          send({ ...input, sourceSessionId: "forged-parent" }),
        ).rejects.toThrow();
        await expect(
          send({ ...input, delegation: { ancestors: [], maxDepth: 8 } }),
        ).rejects.toThrow();
        if (!hasSession) {
          await expect(send(input)).rejects.toThrow(
            "Agent sends require an invoking Session",
          );
          return { refused: true };
        }
        return send(input);
      }),
    };
    const runtime = await Mycelium.make(
      hasSession
        ? { ...options, session: { sessionId: "trusted-parent" } }
        : options,
    );
    const lease = await runtime.acquire();
    try {
      const prepared = await Effect.runPromise(
        lease.executeTool.prepare({
          type: "toolCall",
          id: "delegate",
          name: "execute",
          arguments: { code: "agents.send" },
        }),
      );
      const result = await Effect.runPromise(
        Stream.runCollect(prepared.execute()),
      );
      expect(result[0]).toMatchObject({
        type: "result",
        result: { isError: false },
      });
      expect(parents).toEqual(hasSession ? ["trusted-parent"] : []);
      expect(sends).toEqual(
        hasSession
          ? [
              {
                agentId: "child",
                sessionId: "child-session",
                text: "Task",
                delivery: "followUp",
                requestId: "caller-delegation",
              },
            ]
          : [],
      );
    } finally {
      await lease.release();
      await runtime.close();
    }
  },
);

it("refuses a send before acceptance and carries a model diagnostic through wait", async () => {
  const diagnostic =
    "model:rejected: openai is not signed in. Run /login or choose another model with /model.";
  let sent = false;
  const session = testSession(
    () => {
      sent = true;
    },
    () =>
      Promise.resolve({
        snapshot: {
          ...baseline,
          terminals: [
            {
              commandId: "rejected",
              outcome: "failed",
              diagnostic,
              sequence: 1,
            },
          ],
        },
        changes: {
          async *[Symbol.asyncIterator]() {
            await Promise.resolve();
            yield {
              kind: "reset" as const,
              reason: "history_unavailable" as const,
              cursor: null,
              baseline,
            };
          },
        },
        close: () => Promise.resolve(),
      }),
  );
  const runtime = await Mycelium.make({
    sources: [
      createAgentControlSource({
        list: () => Promise.resolve([]),
        checkModel: () => Promise.reject(new Error(diagnostic)),
        send: () =>
          Promise.reject(new Error("preflight must refuse before sending")),
        open: () =>
          Promise.resolve({
            agent: {
              session: () => session,
              createSession: () =>
                Promise.reject(new Error("Unexpected creation")),
              deleteSession: () =>
                Promise.reject(new Error("Unexpected deletion")),
              listSessions: () =>
                Promise.resolve({ sessions: [], next: undefined }),
              extensions: {
                list: () => Promise.resolve([]),
                call: () => Promise.reject(new Error("Unexpected extension")),
                resume: () => Promise.reject(new Error("Unexpected extension")),
              },
              close: () => Promise.resolve(),
            },
            release: () => Promise.resolve(),
          }),
      }),
    ],
    sandbox: directSandbox(async (bindings) => {
      const tools = bindings.agents;
      if (tools?.send === undefined || tools.wait === undefined)
        throw new Error("Agent tools missing");
      await expect(
        tools.send({
          agentId: "agent-a",
          sessionId: "session-a",
          text: "Hello",
          delivery: "followUp",
          requestId: "model-refused-request",
        }),
      ).rejects.toThrow(diagnostic);
      const result = await tools.wait({
        agentId: "agent-a",
        sessionId: "session-a",
        commandId: "rejected",
        timeoutMs: 100,
      });
      expect(result).toEqual({
        commandId: "rejected",
        outcome: "failed",
        diagnostic,
      });
      return result;
    }),
  });
  const lease = await runtime.acquire();
  try {
    const prepared = await Effect.runPromise(
      lease.executeTool.prepare({
        type: "toolCall",
        id: "model-refusal",
        name: "execute",
        arguments: { code: "agents.send" },
      }),
    );
    const parts = await Effect.runPromise(
      Stream.runCollect(prepared.execute()),
    );
    expect(parts[0]).toMatchObject({
      type: "result",
      result: { isError: false },
    });
    expect(sent).toBe(false);
  } finally {
    await lease.release();
    await runtime.close();
  }
});

async function runAgentCode(
  directory: Parameters<typeof createAgentControlSource>[0],
  run: (
    tools: NonNullable<
      Parameters<Parameters<typeof directSandbox>[0]>[0]["agents"]
    >,
  ) => Promise<null>,
  sourceSessionId?: string,
) {
  let options: Parameters<typeof Mycelium.make>[0] = {
    sources: [createAgentControlSource(directory)],
    sandbox: directSandbox(async (bindings) => {
      const tools = bindings.agents;
      if (tools === undefined) throw new Error("Agent tools missing");
      return await run(tools);
    }),
  };
  if (sourceSessionId !== undefined)
    options = { ...options, session: { sessionId: sourceSessionId } };
  const runtime = await Mycelium.make(options);
  const lease = await runtime.acquire();
  try {
    const prepared = await Effect.runPromise(
      lease.executeTool.prepare({
        type: "toolCall",
        id: "agent-control-model",
        name: "execute",
        arguments: { code: "return agents" },
      }),
    );
    return await Effect.runPromise(Stream.runCollect(prepared.execute()));
  } finally {
    await lease.release();
    await runtime.close();
  }
}

const unregistered = () =>
  Promise.reject(
    new ShiitakeError(
      "model",
      "rejected",
      "Local model is not registered: openai/luna",
    ),
  );

const rejectionSchema = z.object({
  _tag: z.string().optional(),
  namespace: z.string().optional(),
  operation: z.string().optional(),
  cause: z.object({ kind: z.string(), message: z.string() }).optional(),
});
type Rejection = z.infer<typeof rejectionSchema>;

async function captureRejection(pending: Promise<unknown>): Promise<Rejection> {
  const [settled] = await Promise.allSettled([pending]);
  if (settled.status === "fulfilled")
    throw new Error("Expected the operation to fail");
  return rejectionSchema.parse(settled.reason);
}

it.each(["create", "setModel"] as const)(
  "%s admits a host model refusal as a typed capability failure",
  async (operation) => {
    const failures: Rejection[] = [];
    const parts = await runAgentCode(
      {
        list: () => Promise.resolve([]),
        create: unregistered,
        setModel: unregistered,
        open: () => Promise.reject(new Error("unused")),
      },
      async (tools) => {
        const call = tools[operation];
        if (call === undefined) throw new Error("Model operation missing");
        failures.push(
          await captureRejection(
            call(
              operation === "create"
                ? { requestId: "agent-request", model: "openai/luna" }
                : { agentId: "agent-a", model: "openai/luna" },
            ),
          ),
        );
        return null;
      },
    );
    expect(parts[0]).toMatchObject({ result: { isError: false } });
    expect(failures).toMatchObject([
      {
        _tag: "CapabilityFailed",
        namespace: "agents",
        operation,
        cause: {
          kind: "OperationFailed",
          message: "model:rejected: Local model is not registered: openai/luna",
        },
      },
    ]);
  },
);

it("keeps unexpected setModel failures out of the model refusal path", async () => {
  const failures: Rejection[] = [];
  await runAgentCode(
    {
      list: () => Promise.resolve([]),
      setModel: () => Promise.reject(new Error("store exploded")),
      open: () => Promise.reject(new Error("unused")),
    },
    async (tools) => {
      const setModel = tools.setModel;
      if (setModel === undefined) throw new Error("Model control missing");
      failures.push(
        await captureRejection(
          setModel({ agentId: "agent-a", model: "openai/gpt-6-luna" }),
        ),
      );
      return null;
    },
  );
  expect(failures).toHaveLength(1);
  expect(failures[0]).not.toMatchObject({ _tag: "CapabilityFailed" });
});

it("lists only host-offered models and keeps valid setModel a null result", async () => {
  const changes: unknown[] = [];
  const results: z.core.util.JSONType[] = [];
  const parts = await runAgentCode(
    {
      list: () => Promise.resolve([]),
      models: () =>
        Promise.resolve([
          { address: "openai/gpt-6-luna", name: "GPT-6 Luna" },
          { address: "openai/gpt-6-sol", name: "GPT-6 Sol" },
        ]),
      setModel: (input) => {
        changes.push(input);
        return Promise.resolve();
      },
      open: () => Promise.reject(new Error("unused")),
    },
    async (tools) => {
      const { models, setModel } = tools;
      if (models === undefined || setModel === undefined)
        throw new Error("Model operations missing");
      results.push(
        z.json().parse(await models({})),
        z
          .json()
          .parse(
            await setModel({ agentId: "agent-a", model: "openai/gpt-6-luna" }),
          ),
      );
      return null;
    },
  );
  expect(parts[0]).toMatchObject({ result: { isError: false } });
  expect(results).toEqual([
    {
      models: [
        { address: "openai/gpt-6-luna", name: "GPT-6 Luna" },
        { address: "openai/gpt-6-sol", name: "GPT-6 Sol" },
      ],
      truncated: false,
    },
    null,
  ]);
  expect(changes).toEqual([
    {
      agentId: "agent-a",
      model: { providerId: "openai", modelId: "gpt-6-luna" },
    },
  ]);
});

function assistantMessage(
  commandId: string,
  sequence: number,
  content: AssistantMessage["content"],
): SessionMessage {
  return {
    id: `message-${String(sequence)}`,
    sequence,
    commandId,
    value: {
      role: "assistant",
      content,
      api: "openai-responses",
      provider: "openai",
      model: "gpt-6-luna",
      usage: {
        input: 0,
        output: 0,
        cacheRead: 0,
        cacheWrite: 0,
        totalTokens: 0,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
      },
      stopReason: "stop",
      timestamp: sequence,
    },
  };
}

async function waitWithHistory(
  outcome: SessionTerminal["outcome"],
  messages: readonly SessionMessage[],
) {
  const terminal: SessionTerminal = {
    commandId: "target",
    outcome,
    sequence: 10,
    diagnostic: outcome === "failed" ? "provider refused" : null,
  };
  const snapshot: SessionSnapshot = {
    ...baseline,
    messages,
    terminals: [terminal],
  };
  const session: ShiitakeSession = {
    ...testSession(undefined, () =>
      Promise.resolve({
        snapshot,
        changes: {
          async *[Symbol.asyncIterator]() {
            // The terminal is already in the snapshot; no change follows.
          },
        },
        close: () => Promise.resolve(),
      }),
    ),
    read: () => Promise.resolve(snapshot),
  };
  const agent: ShiitakeAgent = {
    createSession: () => Promise.reject(new Error("unused")),
    deleteSession: () => Promise.resolve(false),
    listSessions: () => Promise.resolve({ sessions: [], next: undefined }),
    extensions: {
      list: () => Promise.resolve([]),
      resume: () => Promise.reject(new Error("unused")),
      call: () => Promise.reject(new Error("unused")),
    },
    session: () => session,
    close: () => Promise.resolve(),
  };
  const results: z.core.util.JSONType[] = [];
  await runAgentCode(
    {
      list: () => Promise.resolve([]),
      open: () => Promise.resolve({ agent, release: () => Promise.resolve() }),
    },
    async (tools) => {
      const wait = tools.wait;
      if (wait === undefined) throw new Error("Agent wait missing");
      results.push(
        z.json().parse(
          await wait({
            agentId: "agent-a",
            sessionId: "session-a",
            commandId: "target",
            timeoutMs: 1000,
          }),
        ),
      );
      return null;
    },
  );
  return results[0];
}

it("wait returns a completed command's final assistant text", async () => {
  await expect(
    waitWithHistory("completed", [
      assistantMessage("target", 1, [{ type: "text", text: "Working." }]),
      assistantMessage("other", 2, [{ type: "text", text: "Not mine." }]),
      assistantMessage("target", 3, [
        { type: "thinking", thinking: "private" },
        { type: "text", text: "Done: " },
        { type: "text", text: "three files." },
      ]),
    ]),
  ).resolves.toEqual({
    commandId: "target",
    outcome: "completed",
    text: "Done: three files.",
    truncated: false,
  });
});

it.each(["failed", "cancelled"] as const)(
  "wait returns a %s outcome without assistant text",
  async (outcome) => {
    const result = await waitWithHistory(outcome, [
      assistantMessage("target", 1, [{ type: "text", text: "partial" }]),
    ]);
    expect(result).toEqual(
      outcome === "failed"
        ? { commandId: "target", outcome, diagnostic: "provider refused" }
        : { commandId: "target", outcome },
    );
  },
);

it("wait truncates long final text and flags it", async () => {
  const result = z
    .object({ text: z.string(), truncated: z.boolean() })
    .parse(
      await waitWithHistory("completed", [
        assistantMessage("target", 1, [
          { type: "text", text: "a".repeat(20_000) },
        ]),
      ]),
    );
  expect(result.truncated).toBe(true);
  expect(result.text).toBe("a".repeat(16_384));
});

// Runtime: Node 24, `node:sqlite`, through the public Mycelium door.
it("read pages back with before through a full Woodstock history", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const history = writeLargeSession(fixture.owner, {
      sessionId: "session-a",
      turns: 160,
      compactEvery: 50,
      keepTurns: 5,
    });
    const reads: unknown[] = [];
    const session: ShiitakeSession = {
      ...testSession(),
      read: (options) => {
        reads.push(options);
        return Promise.resolve(
          readClientSnapshot(
            fixture.owner,
            "session-a",
            decodeClientReadOptions(options),
            { facts: idleActivity, toolOutput: [], progress: null },
          ),
        );
      },
    };
    const agent: ShiitakeAgent = {
      createSession: () => Promise.reject(new Error("unused")),
      deleteSession: () => Promise.resolve(false),
      listSessions: () => Promise.resolve({ sessions: [], next: undefined }),
      extensions: {
        list: () => Promise.resolve([]),
        resume: () => Promise.reject(new Error("unused")),
        call: () => Promise.reject(new Error("unused")),
      },
      session: () => session,
      close: () => Promise.resolve(),
    };
    const page = z.object({
      hasMore: z.boolean(),
      oldestCursor: z.string().nullable(),
      messages: z.array(z.object({ id: z.string() })),
    });
    const pages: z.infer<typeof page>[] = [];
    await runAgentCode(
      {
        list: () => Promise.resolve([]),
        open: () =>
          Promise.resolve({ agent, release: () => Promise.resolve() }),
      },
      async (tools) => {
        const read = tools.read;
        if (read === undefined) throw new Error("Agent read missing");
        let before: string | null = null;
        for (;;) {
          const current = page.parse(
            await read({ agentId: "agent-a", sessionId: "session-a", before }),
          );
          pages.push(current);
          if (!current.hasMore) break;
          before = current.oldestCursor;
        }
        return null;
      },
    );

    const ids = [...pages]
      .reverse()
      .flatMap(({ messages }) => messages.map(({ id }) => id));
    expect(ids).toEqual(
      history.turns.flatMap(({ commandId, assistantId }) => [
        commandId,
        assistantId,
      ]),
    );
    expect(ids.length).toBeGreaterThan(200);
    expect(pages.length).toBeGreaterThan(1);
    expect(reads[0]).toEqual({});
    expect(reads[1]).toEqual({ before: pages[0]?.oldestCursor });
  } finally {
    fixture.close();
  }
});

it("preserves the typed unknown cancellation result through the public Mycelium door", async () => {
  const session: ShiitakeSession = {
    ...testSession(),
    cancel: () => Promise.resolve({ status: "unknown" }),
  };
  const agent: ShiitakeAgent = {
    createSession: () => Promise.reject(new Error("unused")),
    deleteSession: () => Promise.resolve(false),
    listSessions: () => Promise.resolve({ sessions: [], next: undefined }),
    extensions: {
      list: () => Promise.resolve([]),
      resume: () => Promise.reject(new Error("unused")),
      call: () => Promise.reject(new Error("unused")),
    },
    session: () => session,
    close: () => Promise.resolve(),
  };
  const results: unknown[] = [];
  const parts = await runAgentCode(
    {
      list: () => Promise.resolve([]),
      open: () => Promise.resolve({ agent, release: () => Promise.resolve() }),
    },
    async ({ cancel }) => {
      if (cancel === undefined) throw new Error("Agent cancellation missing");
      results.push(
        await cancel({
          agentId: "agent-a",
          sessionId: "session-a",
          commandId: "missing",
        }),
      );
      return null;
    },
  );
  expect(parts[0]).toMatchObject({
    type: "result",
    result: { isError: false },
  });
  expect(results).toEqual([{ status: "unknown" }]);
});

it.each([true, false])(
  "host recall receives only the trusted invoking Session (present: %s)",
  async (hasSession) => {
    const calls: unknown[] = [];
    const options = {
      sources: [
        createAgentControlSource({
          list: () => Promise.resolve([]),
          open: () =>
            Promise.reject(
              new Error(
                "Host-scoped recall must not use the raw client request ID",
              ),
            ),
          recall: (task, sourceSessionId) => {
            calls.push({ task, sourceSessionId });
            return Promise.resolve({ status: "unknown" });
          },
        }),
      ],
      sandbox: directSandbox(async ({ agents }) => {
        const recall = agents?.recall;
        if (recall === undefined) throw new Error("Recall missing");
        const task = {
          agentId: "child",
          sessionId: "child-main",
          requestId: "caller-request",
        };
        await expect(
          recall({ ...task, sourceSessionId: "forged-parent" }),
        ).rejects.toThrow();
        await expect(
          recall({ ...task, author: "forged-author" }),
        ).rejects.toThrow();
        await expect(
          recall({ ...task, onlyIfPending: false }),
        ).rejects.toThrow();
        if (!hasSession) {
          await expect(recall(task)).rejects.toThrow(
            "Recall requires an invoking Session",
          );
          return null;
        }
        return recall(task);
      }),
    };
    const runtime = await Mycelium.make(
      hasSession
        ? { ...options, session: { sessionId: "trusted-parent" } }
        : options,
    );
    const lease = await runtime.acquire();
    try {
      const prepared = await Effect.runPromise(
        lease.executeTool.prepare({
          type: "toolCall",
          id: "host-recall",
          name: "execute",
          arguments: { code: "agents.recall" },
        }),
      );
      const result = await Effect.runPromise(
        Stream.runCollect(prepared.execute()),
      );
      expect(result[0]).toMatchObject({
        type: "result",
        result: { isError: false },
      });
      expect(calls).toEqual(
        hasSession
          ? [
              {
                task: {
                  agentId: "child",
                  sessionId: "child-main",
                  requestId: "caller-request",
                },
                sourceSessionId: "trusted-parent",
              },
            ]
          : [],
      );
    } finally {
      await lease.release();
      await runtime.close();
    }
  },
);

it("requires host request ownership before advertising send and recall", async () => {
  const advertised: boolean[] = [];
  await runAgentCode(
    {
      list: () => Promise.resolve([]),
      open: () => Promise.reject(new Error("no request owner was supplied")),
    },
    (tools) => {
      advertised.push(tools.send !== undefined, tools.recall !== undefined);
      return Promise.resolve(null);
    },
    "trusted-source-session",
  );
  expect(advertised).toEqual([false, false]);
});
