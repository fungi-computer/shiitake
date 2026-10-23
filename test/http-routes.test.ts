import { expect, it } from "vitest";

import type {
  AgentExtensionControls,
  AbortCommand,
  Accepted,
  BranchCommand,
  OperatorCommand,
  TitleCommand,
  CancelCommand,
  ForkCommand,
  ForkedSession,
  CompactCommand,
  PromptCommand,
  SessionCommand,
  SessionFrame,
  SessionSnapshot,
  ShiitakeSession,
  SessionExtensionSlots,
} from "../src/client.js";
import { parseShiitakeError } from "../src/client.js";
import type { Json } from "../src/extensions.js";
import { ShiitakeError } from "../src/errors.js";
import {
  createShiitakeHttpRoutes,
  type ShiitakeSessionResolver,
} from "../src/http-routes.js";
import { idleActivity, sessionSnapshot } from "./fixtures/client.fixture.js";

const accepted: Accepted = {
  status: "accepted",
  commandId: "command-1",
};

const unusedExtensions: AgentExtensionControls = {
  list: () => Promise.resolve([]),
  resume: () => Promise.reject(new Error("Extensions are unused here")),
  call: () => Promise.reject(new Error("Extensions are unused here")),
};

const snapshot: SessionSnapshot = sessionSnapshot({
  sessionId: "demo-session",
});

const reset: SessionFrame = {
  kind: "reset",
  reason: "history_unavailable",
  cursor: "opaque-cursor-2",
  baseline: snapshot,
};

function commandResult(
  input: PromptCommand | CompactCommand | CancelCommand,
): Promise<Accepted>;
function commandResult(
  input: AbortCommand | BranchCommand | OperatorCommand | TitleCommand,
): Promise<undefined>;
function commandResult(input: ForkCommand): Promise<ForkedSession>;
function commandResult(
  input: SessionCommand,
): Promise<Accepted | ForkedSession | undefined>;
function commandResult(
  input: SessionCommand,
): Promise<Accepted | ForkedSession | undefined> {
  return Promise.resolve(
    input.type === "prompt"
      ? accepted
      : input.type === "cancel"
        ? { status: "accepted", commandId: input.commandId }
        : input.type === "fork"
          ? { sessionId: "forked-session" }
          : undefined,
  );
}

async function* frames(
  values: readonly SessionFrame[],
): AsyncGenerator<SessionFrame> {
  await Promise.resolve();
  yield* values;
}

async function* framesThenFail(
  values: readonly SessionFrame[],
  error: ShiitakeError,
): AsyncGenerator<SessionFrame> {
  await Promise.resolve();
  yield* values;
  throw error;
}

async function* framesAfter(
  completion: Promise<void>,
): AsyncGenerator<SessionFrame> {
  await completion;
  yield* frames([]);
}

function makeSession(
  overrides: Partial<ShiitakeSession> = {},
): ShiitakeSession {
  return {
    address: "test://shiitake/sessions/demo-session",
    command: overrides.command ?? commandResult,
    withdraw:
      overrides.withdraw ?? (() => Promise.resolve({ status: "unknown" })),
    abort: overrides.abort ?? (() => Promise.resolve()),
    send:
      overrides.send ??
      (() => Promise.resolve({ status: "accepted", commandId: "test" })),
    cancel:
      overrides.cancel ??
      ((input) =>
        Promise.resolve({ status: "accepted", commandId: input.commandId })),
    latestPending: overrides.latestPending ?? (() => Promise.resolve(null)),
    modelPreference: overrides.modelPreference ?? {
      read: () => Promise.resolve(null),
      set: () => Promise.resolve(),
    },
    extension:
      overrides.extension ??
      (() => ({ call: () => Promise.reject(new Error("extension unused")) })),
    read: overrides.read ?? (() => Promise.resolve(snapshot)),
    subscribe: overrides.subscribe ?? (() => frames([])),
    watch:
      overrides.watch ??
      (() =>
        Promise.resolve({
          snapshot,
          changes: frames([]),
          close: () => Promise.resolve(),
        })),
  };
}

it("Hono carries abort through the same typed command route", async () => {
  let observed: SessionCommand | undefined;
  function observeCommand(
    input: PromptCommand | CompactCommand | CancelCommand,
  ): Promise<Accepted>;
  function observeCommand(
    input: AbortCommand | BranchCommand | OperatorCommand | TitleCommand,
  ): Promise<undefined>;
  function observeCommand(input: ForkCommand): Promise<ForkedSession>;
  function observeCommand(
    input: SessionCommand,
  ): Promise<Accepted | ForkedSession | undefined>;
  function observeCommand(input: SessionCommand) {
    observed = input;
    return commandResult(input);
  }
  const session = makeSession({
    command: observeCommand,
  });
  const routes = fixtureRoutes(() => session);

  const response = await routes.request(
    "http://shiitake.test/sessions/demo-session/command",
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ command: { type: "abort" } }),
    },
  );

  expect(response.status).toBe(204);
  expect(observed).toEqual({ type: "abort" });
});

it("Hono routes generic extension commands, lists and resumes without naming a feature", async () => {
  const calls: [string, string, Json][] = [];
  const extensions: AgentExtensionControls = {
    list: () =>
      Promise.resolve([
        {
          id: "fixture",
          version: "1.0.0",
          revision: "sha256:fixture",
          status: "stopped",
          reason: "fixture stopped",
          stoppedAt: 1,
        },
      ]),
    resume: (id) =>
      Promise.resolve({
        id,
        version: "1.0.0",
        revision: "sha256:fixture",
        status: "running",
        reason: null,
        stoppedAt: null,
      }),
    call: (id, command, input) => {
      calls.push([`agent:${id}`, command, input]);
      return Promise.resolve({ ok: true });
    },
  };
  const routes = createShiitakeHttpRoutes(
    () =>
      makeSession({
        extension: (id) => ({
          call: (command, input) => {
            calls.push([`session:${id}`, command, input]);
            return Promise.resolve([1, "two"]);
          },
        }),
      }),
    () => Promise.resolve({ sessions: [], next: undefined }),
    () => Promise.resolve("fresh-session"),
    () => Promise.resolve(false),
    () => extensions,
  );
  const post = (path: string, body: string) =>
    routes.request(`http://shiitake.test${path}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body,
    });

  const sessionCall = await post(
    "/sessions/demo-session/extensions/fixture/commands/pause",
    "{}",
  );
  await expect(sessionCall.json()).resolves.toEqual([1, "two"]);
  const agentCall = await post(
    "/extensions/fixture/commands/list",
    '{"after":"a"}',
  );
  await expect(agentCall.json()).resolves.toEqual({ ok: true });
  expect(calls).toEqual([
    ["session:fixture", "pause", {}],
    ["agent:fixture", "list", { after: "a" }],
  ]);
  expect(
    (await post("/extensions/fixture/commands/list", "not json")).status,
  ).toBe(400);
  await expect(
    (await routes.request("http://shiitake.test/extensions")).json(),
  ).resolves.toMatchObject([{ id: "fixture", status: "stopped" }]);
  await expect(
    (await post("/extensions/fixture/resume", "")).json(),
  ).resolves.toMatchObject({ id: "fixture", status: "running" });
});

it("Hono carries branch and fork through the existing command route", async () => {
  const observed: SessionCommand[] = [];
  function observeCommand(
    input: PromptCommand | CompactCommand | CancelCommand,
  ): Promise<Accepted>;
  function observeCommand(
    input: AbortCommand | BranchCommand | OperatorCommand | TitleCommand,
  ): Promise<undefined>;
  function observeCommand(input: ForkCommand): Promise<ForkedSession>;
  function observeCommand(
    input: SessionCommand,
  ): Promise<Accepted | ForkedSession | undefined>;
  function observeCommand(
    input: SessionCommand,
  ): Promise<Accepted | ForkedSession | undefined> {
    observed.push(input);
    return commandResult(input);
  }
  const routes = fixtureRoutes(() => makeSession({ command: observeCommand }));

  const branch = await routes.request(
    "http://shiitake.test/sessions/demo-session/command",
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ command: { type: "branch", entryId: "entry-1" } }),
    },
  );
  expect(branch.status).toBe(204);

  const fork = await routes.request(
    "http://shiitake.test/sessions/demo-session/command",
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ command: { type: "fork", entryId: "entry-1" } }),
    },
  );
  expect(fork.status).toBe(200);
  await expect(fork.json()).resolves.toEqual({
    sessionId: "forked-session",
  });
  expect(observed).toEqual([
    { type: "branch", entryId: "entry-1" },
    { type: "fork", entryId: "entry-1" },
  ]);

  for (const command of [
    { type: "branch" },
    { type: "fork", entryId: "entry-1", extra: true },
  ]) {
    const malformed = await routes.request(
      "http://shiitake.test/sessions/demo-session/command",
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ command }),
      },
    );
    expect(malformed.status).toBe(400);
  }
});

it("Hono streams one bounded snapshot followed by live frames", async () => {
  let closeCount = 0;
  const session = makeSession({
    watch: () =>
      Promise.resolve({
        snapshot,
        changes: frames([reset]),
        close: () => {
          closeCount += 1;
          return Promise.resolve();
        },
      }),
  });
  const routes = fixtureRoutes(() => session);

  const response = await routes.request(
    "http://shiitake.test/sessions/demo-session/events",
  );
  const body = await response.text();

  expect(response.status).toBe(200);
  expect(response.headers.get("content-type")).toContain("text/event-stream");
  expect(body).toBe(
    `event: snapshot\ndata: ${JSON.stringify(snapshot)}\n\n` +
      `event: frame\ndata: ${JSON.stringify(reset)}\nid: opaque-cursor-2\n\n`,
  );
  expect(closeCount).toBe(1);
});

it("Hono watch carries an extension-only committed update without a new message", async () => {
  const goalSnapshot = sessionSnapshot({
    sessionId: "demo-session",
    cursor: "opaque-cursor-1",
    extensions: fixtureSlot({ state: "active" }),
  });
  const goalUpdate: SessionFrame = {
    pending: { total: 0, prompts: [], hasMore: false },
    kind: "update",
    delivery: "live",
    cursor: "opaque-cursor-1",
    messages: [],
    terminals: [],
    activity: idleActivity,
    extensions: fixtureSlot({ state: "paused" }),
    attention: [],
  };
  const routes = fixtureRoutes(() =>
    makeSession({
      watch: () =>
        Promise.resolve({
          snapshot: goalSnapshot,
          changes: frames([goalUpdate]),
          close: () => Promise.resolve(),
        }),
    }),
  );

  const response = await routes.request(
    "http://shiitake.test/sessions/demo-session/events",
  );
  const body = await response.text();

  expect(body).toContain(
    `event: snapshot\ndata: ${JSON.stringify(goalSnapshot)}\nid: opaque-cursor-1\n\n`,
  );
  expect(body).toContain(
    `event: frame\ndata: ${JSON.stringify(goalUpdate)}\nid: opaque-cursor-1\n\n`,
  );
});

it("disconnecting the Hono SSE response closes its Session watch once", async () => {
  let finishChanges = (): void => undefined;
  const changesFinished = new Promise<void>((resolve) => {
    finishChanges = resolve;
  });
  let observeClose = (): void => undefined;
  const closed = new Promise<void>((resolve) => {
    observeClose = resolve;
  });
  let closeCount = 0;
  const session = makeSession({
    watch: () =>
      Promise.resolve({
        snapshot,
        changes: framesAfter(changesFinished),
        close: () => {
          closeCount += 1;
          finishChanges();
          observeClose();
          return Promise.resolve();
        },
      }),
  });
  const routes = fixtureRoutes(() => session);

  const response = await routes.request(
    "http://shiitake.test/sessions/demo-session/events",
  );
  const reader = response.body?.getReader();
  if (reader === undefined) throw new Error("SSE response body is unavailable");
  await reader.read();
  await reader.cancel();
  await closed;

  expect(closeCount).toBe(1);
});

it("Last-Event-ID resumes strictly after one opaque Shiitake cursor", async () => {
  let observedAfter: string | undefined;
  const session = makeSession({
    subscribe: (input) => {
      observedAfter = input.after;
      return frames([reset]);
    },
  });
  const routes = fixtureRoutes(() => session);

  const response = await routes.request(
    "http://shiitake.test/sessions/demo-session/events",
    { headers: { "Last-Event-ID": "opaque-cursor-1" } },
  );
  const body = await response.text();

  expect(observedAfter).toBe("opaque-cursor-1");
  expect(body).toBe(
    `event: frame\ndata: ${JSON.stringify(reset)}\nid: opaque-cursor-2\n\n`,
  );
});

it("a reset to an empty baseline clears the native EventSource cursor", async () => {
  const emptyReset: SessionFrame = {
    ...reset,
    cursor: null,
  };
  const routes = fixtureRoutes(() =>
    makeSession({ subscribe: () => frames([emptyReset]) }),
  );

  const response = await routes.request(
    "http://shiitake.test/sessions/demo-session/events?mode=subscribe",
  );

  await expect(response.text()).resolves.toBe(
    `event: frame\ndata: ${JSON.stringify(emptyReset)}\nid: \n\n`,
  );
});

it("Hono emits a canonical runtime failure after SSE headers begin", async () => {
  let closeCount = 0;
  const session = makeSession({
    watch: () =>
      Promise.resolve({
        snapshot,
        changes: framesThenFail(
          [],
          new ShiitakeError("runtime", "unavailable", "Session stream failed"),
        ),
        close: () => {
          closeCount += 1;
          return Promise.resolve();
        },
      }),
  });
  const routes = fixtureRoutes(() => session);

  const response = await routes.request(
    "http://shiitake.test/sessions/demo-session/events",
  );
  const body = await response.text();

  expect(response.status).toBe(200);
  expect(body).toContain("event: snapshot\n");
  expect(body).toContain(
    `event: runtime-error\ndata: ${JSON.stringify({
      _tag: "ShiitakeError",
      boundary: "runtime",
      cause: "unavailable",
      message: "Session stream failed",
    })}\n\n`,
  );
  expect(closeCount).toBe(1);
});

it("Hono emits a canonical runtime failure when opening the watch fails", async () => {
  const session = makeSession({
    watch: () =>
      Promise.reject(
        new ShiitakeError(
          "runtime",
          "unavailable",
          "Session watch could not open",
        ),
      ),
  });
  const routes = fixtureRoutes(() => session);

  const response = await routes.request(
    "http://shiitake.test/sessions/demo-session/events",
  );

  expect(response.status).toBe(200);
  await expect(response.text()).resolves.toBe(
    `event: runtime-error\ndata: ${JSON.stringify({
      _tag: "ShiitakeError",
      boundary: "runtime",
      cause: "unavailable",
      message: "Session watch could not open",
    })}\n\n`,
  );
});

it("Hono rejects a malformed command through Shiitake's parser", async () => {
  const routes = fixtureRoutes(() => makeSession());
  const response = await routes.request(
    "http://shiitake.test/sessions/demo-session/command",
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ command: { type: "prompt" } }),
    },
  );

  expect(response.status).toBe(400);
  expect(parseShiitakeError(await response.text())).toMatchObject({
    _tag: "ShiitakeError",
    boundary: "contract",
    cause: "invalid",
    message: "Command is invalid",
  });
});

/** One running extension slot carrying a small encoded value. */
function fixtureSlot(value: Json): SessionExtensionSlots {
  return {
    fixture: { status: "running", revision: "sha256:fixture", value },
  };
}

function fixtureRoutes(resolveSession: ShiitakeSessionResolver) {
  return createShiitakeHttpRoutes(
    resolveSession,
    () => Promise.resolve({ sessions: [], next: undefined }),
    () => Promise.resolve("fresh-session"),
    () => Promise.resolve(false),
    () => unusedExtensions,
  );
}

it("requires and forwards the Session creation request ID", async () => {
  const received: string[] = [];
  const routes = createShiitakeHttpRoutes(
    () => makeSession(),
    () => Promise.resolve({ sessions: [], next: undefined }),
    (requestId) => {
      received.push(requestId);
      return Promise.resolve("created-session");
    },
    () => Promise.resolve(false),
    () => unusedExtensions,
  );
  const create = (body: string) =>
    routes.request("http://shiitake.test/sessions", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body,
    });

  expect((await create("{}")).status).toBe(400);
  expect(
    (await create(JSON.stringify({ requestId: "model-create-session-1" })))
      .status,
  ).toBe(201);
  expect(
    (await create(JSON.stringify({ requestId: "model-create-session-1" })))
      .status,
  ).toBe(201);
  expect(received).toEqual([
    "model-create-session-1",
    "model-create-session-1",
  ]);
});

it("lists saved Sessions without resolving or creating a Session", async () => {
  const observed: (string | undefined)[] = [];
  const routes = createShiitakeHttpRoutes(
    () => {
      throw new Error("Discovery must not address a Session");
    },
    (after) => {
      observed.push(after);
      return Promise.resolve({
        sessions: [
          {
            sessionId: "saved",
            createdAt: 1,
            lastActivityAt: 2,
            title: "Saved task",
            state: "done",
          },
        ],
        next: undefined,
      });
    },
    () => Promise.resolve("fresh-session"),
    () => Promise.resolve(false),
    () => unusedExtensions,
  );
  const response = await routes.request(
    "http://shiitake.test/sessions?after=earlier",
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toEqual({
    sessions: [
      {
        sessionId: "saved",
        createdAt: 1,
        lastActivityAt: 2,
        title: "Saved task",
        state: "done",
      },
    ],
  });
  expect(observed).toEqual(["earlier"]);
});

it("rejects an invalid discovery cursor before reading the owner", async () => {
  let reads = 0;
  const routes = createShiitakeHttpRoutes(
    () => makeSession(),
    () => {
      reads += 1;
      return Promise.resolve({ sessions: [], next: undefined });
    },
    () => Promise.resolve("fresh-session"),
    () => Promise.resolve(false),
    () => unusedExtensions,
  );
  const response = await routes.request("http://shiitake.test/sessions?after=");
  expect(response.status).toBe(400);
  expect(reads).toBe(0);
});
