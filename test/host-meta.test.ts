import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as Schema from "effect/Schema";
import { fauxModel } from "@fungi.computer/mule/testing";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { Hono } from "hono";
import { expect, it } from "vitest";

import {
  createShiitakeClient,
  parseSessionSnapshot,
  type PromptCommand,
  type SendOptions,
} from "../src/client.js";
import {
  createShiitakeHttpRoutes,
  type ShiitakeHttpEnv,
} from "../src/http-routes.js";
import { Shiitake, type ShiitakeOptions } from "../src/index.js";
import {
  compatibilityRunDependencies,
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
  noExtensions,
} from "./fixtures/native-sqlite-owner.fixture.js";

/** A host's own metadata contract; Shiitake only checks values against it. */
const HostMeta = Schema.Struct({
  author: Schema.Struct({ id: Schema.String, name: Schema.String }),
  channel: Schema.optionalKey(Schema.Literals(["web", "slack"])),
});

async function openAgent(
  meta?: ShiitakeOptions["meta"],
  databasePath?: string,
) {
  const fixture = createNativeSqliteOwnerFixture(databasePath);
  const runtime = createCompatibilityRuntimeHost();
  const options: ShiitakeOptions = {
    ...compatibilityRunDependencies(),
    store: fixture.owner,
    runtime: runtime.runtime,
  };
  const agent = await Shiitake.make(
    meta === undefined ? options : { ...options, meta },
  );
  return {
    agent,
    /** Settle every queued run so accepted prompts reach the transcript. */
    drain: async () => {
      for (let pass = 0; pass < 20; pass += 1) {
        if (runtime.snapshot().state !== "pending") return;
        await runtime.runNext();
      }
      throw new Error("Runs did not settle");
    },
    close: async () => {
      await agent.close();
      fixture.close();
    },
  };
}

const ada = { author: { id: "user_ada", name: "Ada" }, channel: "web" };

it("selects the newest own pending prompt beyond the oldest-first page, including images and steers", async () => {
  const { agent, close } = await openAgent({
    schema: HostMeta,
    identityPath: "$.author.id",
  });
  try {
    const session = agent.withMeta(ada).session("recall-newest");
    for (let index = 0; index < 205; index += 1)
      await session.send(`older ${String(index)}`, {
        requestId: `older-${String(index)}`,
        delivery: "followUp",
      });
    const images: SendOptions["images"] = [
      { type: "image", data: "aGVsbG8=", mimeType: "image/png" },
    ];
    const last = await session.send("newest", {
      requestId: "last",
      delivery: "steer",
      images,
    });
    await agent
      .withMeta({ author: { id: "user_eve", name: "Ada" } })
      .session("recall-newest")
      .send("not yours", { requestId: "eve", delivery: "followUp" });
    expect((await session.read()).pending.hasMore).toBe(true);
    expect(await session.latestPending()).toEqual({
      commandId: last.commandId,
      requestId: "last",
      text: "newest",
      images,
      delivery: "steer",
      meta: ada,
    });
    expect(
      await session.cancel({ commandId: last.commandId, onlyIfPending: true }),
    ).toEqual(last);
    expect((await session.latestPending())?.text).toBe("older 204");
  } finally {
    await close();
  }
});

it("stores the host's metadata beside a prompt and returns it on read", async () => {
  const { agent, drain, close } = await openAgent({ schema: HostMeta });
  try {
    const sessionId = await agent.createSession("meta-read");
    await agent
      .withMeta(ada)
      .session(sessionId)
      .send("hello", { requestId: "with-meta" });
    await agent.session(sessionId).send("plain", { requestId: "plain" });
    await drain();
    const messages = (await agent.session(sessionId).read()).messages.filter(
      ({ value }) => value.role === "user",
    );
    expect(messages.map(({ meta }) => meta)).toEqual([ada, undefined]);
    // A host decodes it with its own schema; Shiitake returns JSON.
    expect(
      Schema.decodeUnknownSync(HostMeta)(messages[0]?.meta).author.name,
    ).toBe("Ada");
  } finally {
    await close();
  }
});

it("checks immutable authorship on the raw command door and on cancellation replay, not display labels", async () => {
  const directory = mkdtempSync(join(tmpdir(), "recall-author-"));
  const databasePath = join(directory, "agent.sqlite");
  const eve = { author: { id: "user_eve", name: "Ada" } };
  let host = await openAgent(
    { schema: HostMeta, identityPath: "$.author.id" },
    databasePath,
  );
  try {
    const sessionId = "raw-cancel";
    const accepted = await host.agent
      .withMeta(ada)
      .session(sessionId)
      .send("private own work", {
        requestId: "own-work",
        delivery: "followUp",
      });
    const other = host.agent.withMeta(eve).session(sessionId);
    expect(await other.latestPending()).toBeNull();
    await expect(
      other.command({
        type: "cancel",
        commandId: accepted.commandId,
        onlyIfPending: true,
      }),
    ).rejects.toMatchObject({ boundary: "runtime", cause: "rejected" });
    await expect(
      other.cancel({ commandId: accepted.commandId }),
    ).rejects.toMatchObject({ boundary: "runtime", cause: "rejected" });
    expect((await host.agent.session(sessionId).read()).pending.total).toBe(1);
    const own = host.agent.withMeta(ada).session(sessionId);
    expect(
      await own.cancel({ commandId: accepted.commandId, onlyIfPending: true }),
    ).toEqual(accepted);
    await host.close();
    host = await openAgent(
      { schema: HostMeta, identityPath: "$.author.id" },
      databasePath,
    );
    await expect(
      host.agent
        .withMeta(eve)
        .session(sessionId)
        .cancel({ commandId: accepted.commandId, onlyIfPending: true }),
    ).rejects.toMatchObject({ boundary: "runtime", cause: "rejected" });
    expect(
      await host.agent
        .withMeta(ada)
        .session(sessionId)
        .cancel({ commandId: accepted.commandId, onlyIfPending: true }),
    ).toEqual(accepted);
  } finally {
    await host.close();
    rmSync(directory, { recursive: true });
  }
});

it("refuses metadata the host's schema rejects, or that exceeds the cap, writing nothing", async () => {
  const { agent, close } = await openAgent({ schema: HostMeta });
  try {
    const sessionId = await agent.createSession("meta-invalid");
    await expect(
      agent
        .withMeta({ author: "Ada" })
        .session(sessionId)
        .send("hello", { requestId: "bad-shape" }),
    ).rejects.toMatchObject({ boundary: "contract", cause: "invalid" });
    await expect(
      agent
        .withMeta({
          author: { id: "user_ada", name: "x".repeat(5000) },
        })
        .session(sessionId)
        .send("hello", { requestId: "too-large" }),
    ).rejects.toMatchObject({ boundary: "contract", cause: "invalid" });
    expect((await agent.session(sessionId).read()).messages).toEqual([]);
  } finally {
    await close();
  }
});

it("caps metadata even when the host supplies no schema", async () => {
  const { agent, drain, close } = await openAgent();
  try {
    const sessionId = await agent.createSession("meta-cap");
    await agent
      .withMeta({ note: "small" })
      .session(sessionId)
      .send("ok", { requestId: "small" });
    await expect(
      agent
        .withMeta({ note: "x".repeat(4097) })
        .session(sessionId)
        .send("hello", { requestId: "large" }),
    ).rejects.toMatchObject({ boundary: "contract", cause: "invalid" });
    await drain();
    expect(
      (await agent.session(sessionId).read()).messages
        .filter(({ value }) => value.role === "user")
        .map(({ meta }) => meta),
    ).toEqual([{ note: "small" }]);
  } finally {
    await close();
  }
});

it("replays a retried request only with the same metadata", async () => {
  const { agent, close } = await openAgent({ schema: HostMeta });
  try {
    const sessionId = await agent.createSession("meta-replay");
    const first = await agent
      .withMeta(ada)
      .session(sessionId)
      .send("hello", { requestId: "retry" });
    const replay = await agent
      .withMeta(ada)
      .session(sessionId)
      .send("hello", { requestId: "retry" });
    expect(replay.commandId).toBe(first.commandId);
    await expect(
      agent
        .withMeta({ author: { id: "user_eve", name: "Eve" } })
        .session(sessionId)
        .send("hello", { requestId: "retry" }),
    ).rejects.toMatchObject({ boundary: "contract", cause: "conflict" });
  } finally {
    await close();
  }
});

it("carries metadata on operator lines", async () => {
  const { agent, close } = await openAgent({ schema: HostMeta });
  try {
    const sessionId = await agent.createSession("meta-operator");
    await agent.withMeta(ada).session(sessionId).command({
      type: "operator",
      requestId: "line",
      author: "operator",
      text: "I'll take it from here",
    });
    expect(
      (await agent.session(sessionId).read()).messages.at(-1)?.meta,
    ).toEqual(ada);
  } finally {
    await close();
  }
});

it("takes metadata over HTTP only from host middleware, never from a request body", async () => {
  const { agent, drain, close } = await openAgent({
    schema: HostMeta,
    identityPath: "$.author.id",
  });
  const routes = createShiitakeHttpRoutes(
    (id, meta) =>
      (meta === undefined ? agent : agent.withMeta(meta)).session(id),
    agent.listSessions,
    agent.createSession,
    agent.deleteSession,
    () => agent.extensions,
  );
  const host = new Hono<ShiitakeHttpEnv>()
    .use(async (context, next) => {
      context.set("shiitakeMeta", ada);
      await next();
    })
    .route("/", routes);
  const client = createShiitakeClient({
    baseUrl: "http://shiitake.test",
    fetch: (input, init) =>
      Promise.resolve(host.request(new Request(input, init))),
  });
  try {
    const sessionId = await client.createSession("meta-http");
    const accepted = await client
      .session(sessionId)
      .send("hello", { requestId: "http" });
    expect(await client.session(sessionId).latestPending()).toMatchObject({
      commandId: accepted.commandId,
      text: "hello",
      meta: ada,
    });
    await drain();
    expect((await client.session(sessionId).read()).messages[0]?.meta).toEqual(
      ada,
    );
    const forged = await host.request(
      new Request(`http://shiitake.test/sessions/${sessionId}/command`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          command: {
            type: "prompt",
            delivery: "followUp",
            requestId: "forged",
            text: "hello",
            meta: { author: { id: "user_eve", name: "Eve" } },
          },
        }),
      }),
    );
    expect(forged.status).toBe(400);
    await drain();
    expect(
      (await client.session(sessionId).read()).messages.filter(
        ({ value }) => value.role === "user",
      ),
    ).toHaveLength(1);
  } finally {
    await client.close();
    await close();
  }
});

it("stores a cancel's metadata on its entry, through a restart, as the terminal's", async () => {
  const directory = mkdtempSync(join(tmpdir(), "shiitake-cancel-meta-"));
  const path = join(directory, "agent.sqlite");
  try {
    const first = await openAgent({ schema: HostMeta }, path);
    const sessionId = await first.agent.createSession("cancel-meta");
    const accepted = await first.agent
      .session(sessionId)
      .send("Long task", { requestId: "long" });
    // The cancel goes down the same command path, carrying who sent it.
    await expect(
      first.agent
        .withMeta(ada)
        .session(sessionId)
        .cancel({ commandId: accepted.commandId }),
    ).resolves.toMatchObject({ status: "accepted" });
    await first.close();
    const reopened = await openAgent({ schema: HostMeta }, path);
    try {
      await reopened.drain();
      const { terminals } = await reopened.agent.session(sessionId).read();
      expect(terminals).toMatchObject([
        { commandId: accepted.commandId, outcome: "cancelled", meta: ada },
      ]);
    } finally {
      await reopened.close();
    }
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

it("refuses metadata in a cancel body, and checks a cancel's metadata against the schema", async () => {
  const { agent, close } = await openAgent({ schema: HostMeta });
  const routes = createShiitakeHttpRoutes(
    (id, meta) =>
      (meta === undefined ? agent : agent.withMeta(meta)).session(id),
    agent.listSessions,
    agent.createSession,
    agent.deleteSession,
    () => agent.extensions,
  );
  try {
    const sessionId = await agent.createSession("cancel-body");
    const accepted = await agent
      .session(sessionId)
      .send("Work", { requestId: "work" });
    const forged = await routes.request(
      new Request(`http://shiitake.test/sessions/${sessionId}/command`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          command: {
            type: "cancel",
            commandId: accepted.commandId,
            meta: { author: { id: "user_eve", name: "Eve" } },
          },
        }),
      }),
    );
    expect(forged.status).toBe(400);
    await expect(
      agent
        .withMeta({ author: "Eve" })
        .session(sessionId)
        .cancel({ commandId: accepted.commandId }),
    ).rejects.toMatchObject({ boundary: "contract", cause: "invalid" });
  } finally {
    await close();
  }
});

it("stores an abort's metadata on its cancellation, through a restart", async () => {
  const directory = mkdtempSync(join(tmpdir(), "shiitake-abort-meta-"));
  const path = join(directory, "agent.sqlite");
  const faux = fauxModel([]);
  let started: () => void = () => undefined;
  const running = new Promise<void>((resolve) => {
    started = resolve;
  });
  faux.setResponses([
    async (_context, options) => {
      started();
      // Hold the run until the abort's signal ends it.
      await new Promise<void>((resolve) => {
        if (options?.signal?.aborted === true) resolve();
        else
          options?.signal?.addEventListener(
            "abort",
            () => {
              resolve();
            },
            {
              once: true,
            },
          );
      });
      return fauxAssistantMessage("aborted", { stopReason: "aborted" });
    },
  ]);
  const open = async () => {
    const fixture = createNativeSqliteOwnerFixture(path);
    const runtime = createCompatibilityRuntimeHost();
    const agent = await Shiitake.make({
      store: fixture.owner,
      extensions: noExtensions,
      runtime: runtime.runtime,
      meta: { schema: HostMeta },
      instructions: {
        load: () => Promise.resolve({ revision: "abort-meta", text: "Wait." }),
      },
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
    return { agent, runtime, fixture };
  };
  try {
    const first = await open();
    const sessionId = await first.agent.createSession("abort-meta");
    const accepted = await first.agent
      .session(sessionId)
      .send("Wait for me", { requestId: "wait" });
    const run = first.runtime.runNext();
    await running;
    await first.agent.withMeta(ada).session(sessionId).abort();
    await run;
    await first.agent.close();
    first.fixture.close();

    const reopened = await open();
    try {
      const { terminals } = await reopened.agent.session(sessionId).read();
      expect(terminals).toMatchObject([
        { commandId: accepted.commandId, outcome: "cancelled", meta: ada },
      ]);
    } finally {
      await reopened.agent.close();
      reopened.fixture.close();
    }
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

it("returns admitted host metadata on a pending prompt before execution", async () => {
  const { agent, close } = await openAgent({ schema: HostMeta });
  try {
    const sessionId = await agent.createSession("pending-meta");
    const command: PromptCommand = {
      type: "prompt",
      requestId: "pending-meta",
      delivery: "followUp",
      text: "hello",
      images: [{ type: "image", data: "aGk=", mimeType: "image/png" }],
    };
    const accepted = await agent
      .withMeta(ada)
      .session(sessionId)
      .command(command);
    const snapshot = await agent.session(sessionId).read();
    const expected = [
      {
        commandId: accepted.commandId,
        requestId: command.requestId,
        text: command.text,
        delivery: command.delivery,
        images: command.images,
        meta: ada,
      },
    ];
    expect(snapshot.pending.prompts).toEqual(expected);
    expect(
      parseSessionSnapshot(JSON.stringify(snapshot)).pending.prompts,
    ).toEqual(expected);
    expect(snapshot.messages).toEqual([]);
  } finally {
    await close();
  }
});
