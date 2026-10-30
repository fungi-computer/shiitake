import { directSandbox } from "@fungi.computer/mycelium/testing";
import { Mycelium } from "@fungi.computer/mycelium";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { expect, it } from "vitest";
import { z } from "zod";
import {
  AgentArchiveRefused,
  AgentCreateRefused,
  createAgentControlSource,
  type AgentControlAgent,
  type AgentControlDirectory,
} from "../src/agent-control.js";

const refusing = () => Promise.reject(new Error("unused"));
const project = "/work/project";

type Bindings = Parameters<Parameters<typeof directSandbox>[0]>[0];

async function inSandbox(
  directory: AgentControlDirectory,
  run: (bindings: Bindings) => Promise<z.core.util.JSONType>,
) {
  const outcomes: unknown[] = [];
  const runtime = await Mycelium.make({
    sources: [createAgentControlSource(directory)],
    sandbox: directSandbox(async (bindings) => {
      outcomes.push(await run(bindings));
      return null;
    }),
  });
  const lease = await runtime.acquire();
  try {
    const prepared = await Effect.runPromise(
      lease.executeTool.prepare({
        type: "toolCall",
        id: "subagent-friction",
        name: "execute",
        arguments: { code: "return agents" },
      }),
    );
    const parts = await Effect.runPromise(
      Stream.runCollect(prepared.execute()),
    );
    expect(parts[0]).toMatchObject({ result: { isError: false } });
  } finally {
    await lease.release();
    await runtime.close();
  }
  return z.json().parse(outcomes[0]);
}

const hostDirectory: AgentControlDirectory = {
  list: () =>
    Promise.resolve([
      {
        agentId: "agent-a",
        name: "reviewer",
        model: {
          providerId: "openai",
          modelId: "gpt-6-sol",
          reasoning: "high",
        },
        workingDirectory: project,
      },
    ]),
  create: ({ name, model }) => {
    const agent: AgentControlAgent = {
      agentId: "agent-new",
      name: name ?? "agent-new",
      workingDirectory: project,
    };
    return Promise.resolve(model === undefined ? agent : { ...agent, model });
  },
  open: () =>
    Promise.resolve({
      agent: {
        createSession: () => Promise.resolve("session-new"),
        deleteSession: refusing,
        listSessions: refusing,
        extensions: { list: refusing, resume: refusing, call: refusing },
        session: () => {
          throw new Error("unused");
        },
        close: () => Promise.resolve(),
      },
      release: () => Promise.resolve(),
    }),
};

it("finds the Agent operations by the words subagent and spawn", async () => {
  const found = await inSandbox(hostDirectory, async ({ forage }) => {
    const search = forage?.search;
    if (search === undefined) throw new Error("forage.search missing");
    return {
      subagent: JSON.stringify(await search({ query: "subagent" })),
      spawn: JSON.stringify(await search({ query: "spawn" })),
    };
  });
  for (const query of ["subagent", "spawn"] as const) {
    const hits = z.object({ [query]: z.string() }).parse(found)[query];
    expect(hits).toContain("agents.create");
    expect(hits).toContain("agents.send");
    expect(hits).toContain("agents.wait");
  }
});

it("labels Agents on create and lists name, model and working directory", async () => {
  const seen = await inSandbox(hostDirectory, async ({ agents }) => {
    if (agents?.create === undefined || agents.list === undefined)
      throw new Error("Agent operations missing");
    return {
      created: await agents.create({ requestId: "r", name: "scout" }),
      listed: await agents.list({}),
    };
  });
  expect(seen).toEqual({
    created: {
      agentId: "agent-new",
      sessionId: "session-new",
      name: "scout",
      workingDirectory: project,
    },
    listed: [
      {
        agentId: "agent-a",
        name: "reviewer",
        model: "openai/gpt-6-sol",
        reasoning: "high",
        workingDirectory: project,
      },
    ],
  });
});

it("accepts a thinking level on create and setModel and echoes the requested model", async () => {
  const changes: unknown[] = [];
  const seen = await inSandbox(
    {
      ...hostDirectory,
      setModel: (input) => {
        changes.push(input);
        return Promise.resolve();
      },
    },
    async ({ agents }) => {
      if (
        agents?.create === undefined ||
        agents.setModel === undefined ||
        agents.createSession === undefined
      )
        throw new Error("Agent operations missing");
      const created = await agents.create({
        requestId: "r",
        model: "openai/gpt-6-astra",
        reasoning: "xhigh",
      });
      await agents.setModel({
        agentId: "agent-a",
        model: "openai/gpt-6-sol",
        reasoning: "minimal",
      });
      let orphanReasoning = "accepted";
      try {
        await agents.create({ requestId: "r2", reasoning: "high" });
      } catch {
        orphanReasoning = "rejected";
      }
      return {
        created,
        session: await agents.createSession({
          agentId: "agent-a",
          requestId: "s",
        }),
        orphanReasoning,
      };
    },
  );
  expect(seen).toEqual({
    created: {
      agentId: "agent-new",
      sessionId: "session-new",
      name: "agent-new",
      model: "openai/gpt-6-astra",
      reasoning: "xhigh",
      workingDirectory: project,
    },
    session: {
      sessionId: "session-new",
    },
    orphanReasoning: "rejected",
  });
  expect(changes).toEqual([
    {
      agentId: "agent-a",
      model: {
        providerId: "openai",
        modelId: "gpt-6-sol",
        reasoning: "minimal",
      },
    },
  ]);
});

it("describes the wait bound and the working directory", async () => {
  const described = await inSandbox(hostDirectory, async ({ forage }) => {
    const describe = forage?.describe;
    if (describe === undefined) throw new Error("forage.describe missing");
    return {
      wait: JSON.stringify(await describe({ path: "agents.wait" })),
      create: JSON.stringify(await describe({ path: "agents.create" })),
    };
  });
  const text = z
    .object({ wait: z.string(), create: z.string() })
    .parse(described);
  expect(text.wait).toContain("30000");
  expect(text.wait).toMatch(/maximum/u);
  expect(text.create).toContain("workingDirectory");
  expect(text.create).toMatch(/project directory/u);
});

it("passes the extensions an Agent asks to give a new Agent through to the host", async () => {
  const requests: unknown[] = [];
  await inSandbox(
    {
      ...hostDirectory,
      create: (request, signal) => {
        requests.push(request);
        return hostDirectory.create?.(request, signal) ?? refusing();
      },
    },
    async ({ agents }) => {
      if (agents?.create === undefined) throw new Error("create missing");
      await agents.create({ requestId: "with", extensions: ["counter"] });
      await agents.create({ requestId: "without" });
      return null;
    },
  );
  expect(requests).toEqual([
    { requestId: "with", extensions: ["counter"] },
    { requestId: "without" },
  ]);
});

it("reports an Agent creating an Agent that the host refused as a typed create failure", async () => {
  const seen = await inSandbox(
    {
      ...hostDirectory,
      create: () =>
        Promise.reject(
          new AgentCreateRefused(
            "exceeds_creator",
            "the creating Agent does not hold counter",
          ),
        ),
    },
    async ({ agents }) => {
      if (agents?.create === undefined) throw new Error("create missing");
      try {
        await agents.create({ requestId: "r", extensions: ["counter"] });
        return "created";
      } catch (cause) {
        return String(cause instanceof Error ? cause.message : cause);
      }
    },
  );
  expect(seen).toContain("create:exceeds_creator");
  expect(seen).toContain("does not hold counter");
});

it("archives an Agent through the typed operation, lists archived ones on request, and surfaces a refusal as archive:<reason>", async () => {
  const calls: string[] = [];
  const directory: AgentControlDirectory = {
    ...hostDirectory,
    list: (_signal, options) => {
      calls.push(options?.archived === true ? "list archived" : "list");
      return Promise.resolve(
        options?.archived === true
          ? [{ agentId: "agent-old", name: "old" }]
          : [],
      );
    },
    archive: ({ agentId }) => {
      calls.push("archive " + agentId);
      return agentId === "agent-busy"
        ? Promise.reject(
            new AgentArchiveRefused("running", "command command_9 is running"),
          )
        : Promise.resolve();
    },
    unarchive: ({ agentId }) => {
      calls.push("unarchive " + agentId);
      return Promise.resolve();
    },
  };
  const seen = await inSandbox(directory, async ({ agents }) => {
    if (
      agents?.archive === undefined ||
      agents.unarchive === undefined ||
      agents.list === undefined
    )
      throw new Error("Agent operations missing");
    const archived = await agents.list({ archived: true });
    await agents.archive({ agentId: "agent-a" });
    await agents.unarchive({ agentId: "agent-a" });
    const refusal = await agents.archive({ agentId: "agent-busy" }).then(
      () => "archived",
      (cause: unknown) => String(cause),
    );
    return { archived, refusal };
  });
  expect(seen).toMatchObject({
    archived: [{ agentId: "agent-old", name: "old" }],
  });
  expect(JSON.stringify(seen)).toContain("archive:running");
  expect(JSON.stringify(seen)).toContain("command_9");
  expect(calls).toEqual([
    "list archived",
    "archive agent-a",
    "unarchive agent-a",
    "archive agent-busy",
  ]);
});

it("refuses agents.send to an archived Agent with a typed archive:archived failure", async () => {
  const directory: AgentControlDirectory = {
    ...hostDirectory,
    send: refusing,
    checkModel: () =>
      Promise.reject(new AgentArchiveRefused("archived", "Agent is archived")),
  };
  const seen = await inSandbox(directory, async ({ agents }) => {
    if (agents?.send === undefined) throw new Error("send missing");
    return agents
      .send({
        agentId: "agent-a",
        requestId: "archived-send",
        sessionId: "main",
        text: "hello",
        delivery: "followUp",
      })
      .then(
        () => "sent",
        (cause: unknown) => String(cause),
      );
  });
  expect(seen).toContain("archive:archived");
});

it("offers archive only when the host does", async () => {
  const seen = await inSandbox(hostDirectory, ({ agents }) =>
    Promise.resolve({
      archive: agents?.archive === undefined,
      unarchive: agents?.unarchive === undefined,
    }),
  );
  expect(seen).toEqual({ archive: true, unarchive: true });
});

it("offers the archived input on agents.list only when the host can archive", async () => {
  const lists: unknown[] = [];
  const archiving: AgentControlDirectory = {
    ...hostDirectory,
    list: (_signal, options) => {
      lists.push(options);
      return Promise.resolve([]);
    },
    archive: () => Promise.resolve(),
  };
  const seen = await inSandbox(archiving, async ({ agents }) => {
    if (agents?.list === undefined) throw new Error("list missing");
    await agents.list({ archived: true });
    await agents.list({});
    return null;
  });
  expect(seen).toBeNull();
  expect(lists).toEqual([{ archived: true }, { archived: false }]);
  // Hub-like host: no archive, so no archived input; it is refused, never ignored.
  const refused = await inSandbox(hostDirectory, async ({ agents }) => {
    if (agents?.list === undefined) throw new Error("list missing");
    return agents.list({ archived: true }).then(
      () => "listed",
      (cause: unknown) => String(cause),
    );
  });
  expect(refused).not.toBe("listed");
});
