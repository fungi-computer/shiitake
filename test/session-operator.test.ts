import { fauxModel } from "@fungi.computer/mule/testing";
import type { Context } from "@earendil-works/pi-ai";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { expect, it } from "vitest";
import { requestMessages } from "./fixtures/model-context.js";

import { createShiitakeClient } from "../src/client.js";
import { createShiitakeHttpRoutes } from "../src/http-routes.js";
import { Shiitake } from "../src/index.js";
import {
  compatibilityRunDependencies,
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

it("refuses caller identity in HTTP operator commands; only host metadata attributes a line", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const sam = { author: { id: "user_sam", name: "Sam" } };
  const agent = await Shiitake.make({
    ...compatibilityRunDependencies(),
    store: fixture.owner,
    runtime: runtime.runtime,
  });
  const routes = createShiitakeHttpRoutes(
    (id) => agent.withMeta(sam).session(id),
    agent.listSessions,
    agent.createSession,
    agent.deleteSession,
    () => agent.extensions,
  );
  const client = createShiitakeClient({
    baseUrl: "http://shiitake.test",
    fetch: (input, init) =>
      Promise.resolve(routes.request(new Request(input, init))),
  });
  try {
    const sessionId = await client.createSession("forged-operator");
    for (const segment of ["command", "%63ommand", "comm%61nd"]) {
      const response = await routes.request(
        new Request(`http://shiitake.test/sessions/${sessionId}/${segment}`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            command: {
              type: "operator",
              requestId: segment,
              author: "operator",
              identity: { id: "user_ada", name: "Ada" },
              text: "forged encoded reply",
            },
          }),
        }),
      );
      expect(response.status).toBe(400);
    }
    await client.session(sessionId).command({
      type: "operator",
      requestId: "attributed",
      author: "operator",
      text: "hello",
    });
    expect(
      (await client.session(sessionId).read()).messages.map(({ meta }) => meta),
    ).toEqual([sam]);
  } finally {
    await client.close();
    await agent.close();
    fixture.close();
  }
});

it("records operator and party lines without a run, idempotently, as context for the next run", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const contexts: Context[] = [];
  const dependencies = compatibilityRunDependencies();
  const sam = { author: { id: "user_sam", name: "Sam" } };
  const agent = await Shiitake.make({
    ...dependencies,
    model: {
      acquire: () => {
        const faux = fauxModel([]);
        faux.setResponses([fauxAssistantMessage("back on duty")]);
        return Promise.resolve({
          model: faux.model,
          retryable: () => false,
          stream: (model, context, options) => {
            contexts.push({ ...context, messages: [...context.messages] });
            return faux.stream(model, context, options);
          },
          release: () => Promise.resolve(),
        });
      },
    },
    store: fixture.owner,
    runtime: runtime.runtime,
  });
  try {
    const sessionId = await agent.createSession("takeover");
    const session = agent.session(sessionId);
    const asSam = agent.withMeta(sam).session(sessionId);
    const party = {
      type: "operator" as const,
      requestId: "party-1",
      author: "party" as const,
      text: "is anyone there?",
    };
    const operator = {
      type: "operator" as const,
      requestId: "operator-1",
      author: "operator" as const,
      text: "Hi, this is Sam from support.",
    };
    await session.command(party);
    await asSam.command(operator);
    // The same line under the same request id is a replay, not a new entry.
    await asSam.command(operator);
    await expect(
      asSam.command({ ...operator, text: "something else" }),
    ).rejects.toMatchObject({ cause: "conflict" });
    await expect(
      agent
        .withMeta({ author: { id: "user_ada", name: "Ada" } })
        .session(sessionId)
        .command(operator),
    ).rejects.toMatchObject({ cause: "conflict" });
    await expect(runtime.runNext()).resolves.toBeUndefined();

    const read = await session.read();
    expect(read.activity.queue.state).toBe("idle");
    expect(read.terminals).toEqual([]);
    expect(read.messages.at(-1)?.meta).toEqual(sam);
    expect(
      read.messages.map(({ commandId, value }) => [commandId, value]),
    ).toMatchObject([
      [null, { role: "user", content: "is anyone there?" }],
      [
        null,
        {
          role: "assistant",
          provider: "operator",
          content: [{ type: "text", text: "Hi, this is Sam from support." }],
        },
      ],
    ]);
    expect(contexts).toEqual([]);

    await session.command({
      type: "prompt",
      requestId: "after-release",
      text: "thanks",
      delivery: "followUp",
    });
    await runtime.runNext();
    expect(contexts).toHaveLength(1);
    // A human's words reach the model as a labelled user note, never as its
    // own assistant output.
    expect(
      requestMessages(contexts[0]).map(({ role, content }) => [role, content]),
    ).toEqual([
      ["user", "is anyone there?"],
      [
        "user",
        "[A human operator replied to the user on your behalf]\nHi, this is Sam from support.",
      ],
      ["user", "thanks"],
    ]);
  } finally {
    await agent.close();
    fixture.close();
  }
});

it("refuses an operator line while a run is queued and serves it over HTTP", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const agent = await Shiitake.make({
    ...compatibilityRunDependencies(),
    store: fixture.owner,
    runtime: runtime.runtime,
  });
  const routes = createShiitakeHttpRoutes(
    (id) => agent.session(id),
    agent.listSessions,
    agent.createSession,
    agent.deleteSession,
    () => agent.extensions,
  );
  const client = createShiitakeClient({
    baseUrl: "http://shiitake.test",
    fetch: (input, init) =>
      Promise.resolve(routes.request(new Request(input, init))),
  });
  try {
    const sessionId = await client.createSession("remote-operator");
    const session = client.session(sessionId);
    await session.command({
      type: "prompt",
      requestId: "queued",
      text: "hello",
      delivery: "followUp",
    });
    await expect(
      session.command({
        type: "operator",
        requestId: "too-early",
        author: "operator",
        text: "hold on",
      }),
    ).rejects.toMatchObject({ cause: "conflict" });
    await runtime.runNext();
    await expect(
      session.command({
        type: "operator",
        requestId: "in-time",
        author: "operator",
        text: "hold on",
      }),
    ).resolves.toBeUndefined();
    expect((await session.read()).messages.at(-1)?.value).toMatchObject({
      role: "assistant",
      provider: "operator",
    });
  } finally {
    await client.close();
    await agent.close();
    fixture.close();
  }
});
