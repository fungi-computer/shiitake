import * as Schema from "effect/Schema";
import { afterEach, expect, it, vi } from "vitest";

import {
  createShiitakeClient,
  type SessionFrame,
  type SessionSnapshot,
} from "../src/client.js";
import { Shiitake } from "../src/index.js";
import { ShiitakeError, type ShiitakeErrorWire } from "../src/errors.js";
import {
  compatibilityRunDependencies,
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";
import { createShiitakeHttpRoutes } from "../src/http-routes.js";
import {
  idleActivity,
  runningActivity,
  sessionSnapshot,
} from "./fixtures/client.fixture.js";

type SseListener = (event: Event) => void;
type SsePayload = SessionFrame | SessionSnapshot | ShiitakeErrorWire;

class FakeEventSource {
  static current: FakeEventSource | undefined;
  readonly CONNECTING = 0;
  readonly OPEN = 1;
  readonly CLOSED = 2;
  readonly listeners = new Map<string, SseListener>();
  closeCount = 0;
  onerror: (() => void) | null = null;
  readyState = this.CONNECTING;

  constructor(readonly url: string) {
    FakeEventSource.current = this;
  }

  static created(): FakeEventSource {
    const source = FakeEventSource.current;
    if (source === undefined)
      throw new Error("EventSource was not constructed");
    return source;
  }

  static pending(): FakeEventSource | undefined {
    return this.current;
  }

  addEventListener(name: string, listener: SseListener): void {
    this.listeners.set(name, listener);
  }

  emit(name: string, value: SsePayload): void {
    this.emitSerialized(name, JSON.stringify(value));
  }

  emitSerialized(name: string, data: string): void {
    this.listeners.get(name)?.(new MessageEvent(name, { data }));
  }

  emitError(readyState: number): void {
    this.readyState = readyState;
    this.listeners.get("error")?.(new Event("error"));
  }

  close(): void {
    this.closeCount += 1;
  }
}

afterEach(() => {
  vi.unstubAllGlobals();
});

/** One running extension slot with a small encoded value. */
function fixtureSlot(state: string) {
  return {
    fixture: {
      status: "running" as const,
      revision: "sha256:fixture",
      value: { state },
    },
  };
}

it("neutral client sends one addressed command and parses its acceptance", async () => {
  let observed: Request | undefined;
  vi.stubGlobal(
    "fetch",
    (input: string | URL | Request, init?: RequestInit) => {
      observed = new Request(
        input instanceof Request
          ? input
          : new URL(input.toString(), "http://localhost"),
        init,
      );
      return Promise.resolve(
        Response.json({
          status: "accepted",
          commandId: "command-1",
        }),
      );
    },
  );

  expect(() => createShiitakeClient({ baseUrl: "/api/" })).not.toThrow();
  const agent = createShiitakeClient({ baseUrl: "/api/" });
  const accepted = await agent.session("demo-session").command({
    delivery: "followUp",

    type: "prompt",
    text: "Hello",
    requestId: "request-1",
  });

  expect(accepted).toEqual({
    status: "accepted",
    commandId: "command-1",
  });
  expect(observed?.url).toBe(
    "http://localhost/api/sessions/demo-session/command",
  );
  expect(observed?.method).toBe("POST");
  await expect(observed?.json()).resolves.toEqual({
    command: {
      delivery: "followUp",

      type: "prompt",
      text: "Hello",
      requestId: "request-1",
    },
  });
  await agent.close();
});

it("send exposes one generated request id when the response is lost", async () => {
  let attempt = 0;
  let observed: Request | undefined;
  vi.stubGlobal(
    "fetch",
    (input: string | URL | Request, init?: RequestInit) => {
      observed = new Request(
        input instanceof Request
          ? input
          : new URL(input.toString(), "http://localhost"),
        init,
      );
      attempt += 1;
      return attempt === 1
        ? Promise.reject(new TypeError("response lost"))
        : Promise.resolve(
            Response.json({ status: "accepted", commandId: "retry-id" }),
          );
    },
  );

  const agent = createShiitakeClient({ baseUrl: "/api" });
  const session = agent.session("demo-session");
  let requestId: string | undefined;
  try {
    await session.send("Retry this request");
  } catch (error) {
    if (!(error instanceof ShiitakeError)) throw error;
    expect(error).toMatchObject({ boundary: "runtime", cause: "unavailable" });
    requestId = error.requestId;
  }
  expect(requestId).toEqual(expect.any(String));
  expect(await observed?.json()).toMatchObject({
    command: { requestId, delivery: "steer", type: "prompt" },
  });
  if (requestId === undefined)
    throw new Error("send did not return a request id");

  await expect(
    session.send("Retry this request", { requestId }),
  ).resolves.toEqual({ status: "accepted", commandId: "retry-id" });
  await agent.close();
});

it("neutral client reads one bounded Session snapshot", async () => {
  let observed: Request | undefined;
  vi.stubGlobal(
    "fetch",
    (input: string | URL | Request, init?: RequestInit) => {
      observed = new Request(
        input instanceof Request
          ? input
          : new URL(input.toString(), "http://localhost"),
        init,
      );
      return Promise.resolve(
        Response.json(sessionSnapshot({ sessionId: "demo-session" })),
      );
    },
  );

  const agent = createShiitakeClient({ baseUrl: "/api" });
  const snapshot = await agent.session("demo-session").read({ limit: 20 });

  expect(snapshot).toEqual(sessionSnapshot({ sessionId: "demo-session" }));
  expect(observed?.url).toBe(
    "http://localhost/api/sessions/demo-session?limit=20",
  );
  expect(observed?.method).toBe("GET");
  await agent.close();
});

it("neutral client calls Session and Agent extension commands through generic routes", async () => {
  const observed: Readonly<{ url: string; method: string; body: string }>[] =
    [];
  vi.stubGlobal(
    "fetch",
    async (input: string | URL | Request, init?: RequestInit) => {
      const request = new Request(
        input instanceof Request
          ? input
          : new URL(input.toString(), "http://localhost"),
        init,
      );
      observed.push({
        url: request.url,
        method: request.method,
        body: await request.text(),
      });
      return Response.json({ echoed: true });
    },
  );

  const agent = createShiitakeClient({ baseUrl: "/api" });
  await expect(
    agent.session("demo-session").extension("fixture").call("pause", {}),
  ).resolves.toEqual({ echoed: true });
  await expect(
    agent.extensions.call("fixture", "list", { limit: 2 }),
  ).resolves.toEqual({ echoed: true });
  expect(observed).toEqual([
    {
      url: "http://localhost/api/sessions/demo-session/extensions/fixture/commands/pause",
      method: "POST",
      body: "{}",
    },
    {
      url: "http://localhost/api/extensions/fixture/commands/list",
      method: "POST",
      body: '{"limit":2}',
    },
  ]);
  await agent.close();
});

it("neutral client aborts one addressed Session", async () => {
  let observed: Request | undefined;
  vi.stubGlobal(
    "fetch",
    (input: string | URL | Request, init?: RequestInit) => {
      observed = new Request(
        input instanceof Request
          ? input
          : new URL(input.toString(), "http://localhost"),
        init,
      );
      return Promise.resolve(new Response(null, { status: 204 }));
    },
  );

  const agent = createShiitakeClient({ baseUrl: "/api" });
  await agent.session("demo-session").abort();
  expect(observed?.url).toBe(
    "http://localhost/api/sessions/demo-session/command",
  );
  expect(observed?.method).toBe("POST");
  await expect(observed?.json()).resolves.toEqual({
    command: { type: "abort" },
  });
  await agent.close();
});

it("neutral client acknowledges targeted cancellation as acceptance", async () => {
  let observed: Request | undefined;
  vi.stubGlobal(
    "fetch",
    (input: string | URL | Request, init?: RequestInit) => {
      observed = new Request(
        input instanceof Request
          ? input
          : new URL(input.toString(), "http://localhost"),
        init,
      );
      return Promise.resolve(
        Response.json({ status: "accepted", commandId: "target-command" }),
      );
    },
  );

  const agent = createShiitakeClient({ baseUrl: "/api" });
  await expect(
    agent.session("demo-session").cancel({ commandId: "target-command" }),
  ).resolves.toEqual({ status: "accepted", commandId: "target-command" });
  await expect(observed?.json()).resolves.toEqual({
    command: { type: "cancel", commandId: "target-command" },
  });
  await agent.close();
});

it("neutral client carries branch and fork through the existing command route", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const local = await Shiitake.make({
    store: fixture.owner,
    runtime: runtime.runtime,
    ...compatibilityRunDependencies(),
  });
  const routes = createShiitakeHttpRoutes(
    (sessionId) => local.session(sessionId),
    local.listSessions,
    local.createSession,
    local.deleteSession,
    () => local.extensions,
  );
  vi.stubGlobal("fetch", (input: string | URL | Request, init?: RequestInit) =>
    routes.request(
      new Request(
        input instanceof Request
          ? input
          : new URL(input.toString(), "http://shiitake.test"),
        init,
      ),
    ),
  );

  const remote = createShiitakeClient({ baseUrl: "http://shiitake.test" });
  const source = local.session("transport-branch-fork");
  try {
    await source.command({
      delivery: "followUp",

      type: "prompt",
      requestId: "transport-first",
      text: "first",
    });
    await runtime.runNext();
    await source.command({
      delivery: "followUp",

      type: "prompt",
      requestId: "transport-second",
      text: "second",
    });
    await runtime.runNext();
    const complete = await source.read();
    const firstMessageId = complete.messages[1]?.id;
    if (firstMessageId === undefined) throw new Error("Missing first message");

    await expect(
      remote.session("transport-branch-fork").command({
        type: "branch",
        entryId: firstMessageId,
      }),
    ).resolves.toBeUndefined();
    const selected = await remote.session("transport-branch-fork").read();
    expect(selected.messages).toEqual(complete.messages.slice(0, 2));

    const forked = await remote.session("transport-branch-fork").command({
      type: "fork",
      entryId: firstMessageId,
    });
    expect(forked.sessionId).toEqual(expect.any(String));
    const forkedSnapshot = await remote.session(forked.sessionId).read();
    expect(forkedSnapshot.messages.map(({ value }) => value)).toEqual(
      selected.messages.map(({ value }) => value),
    );
    expect(forkedSnapshot.messages.map(({ id }) => id)).not.toEqual(
      selected.messages.map(({ id }) => id),
    );
  } finally {
    await remote.close();
    await local.close();
    fixture.close();
  }
});

it("neutral client preserves one canonical Shiitake HTTP failure", async () => {
  vi.stubGlobal("fetch", () =>
    Promise.resolve(
      Response.json(
        {
          _tag: "ShiitakeError",
          boundary: "model",
          cause: "rejected",
          message: "Model rejected this run",
        },
        { status: 503 },
      ),
    ),
  );

  const agent = createShiitakeClient({ baseUrl: "/api" });
  await expect(
    agent.session("demo-session").command({
      delivery: "followUp",

      type: "prompt",
      text: "Hello",
      requestId: "request-1",
    }),
  ).rejects.toMatchObject({
    _tag: "ShiitakeError",
    boundary: "model",
    cause: "rejected",
    message: "Model rejected this run",
  });
  await agent.close();
});

it("neutral subscribe resumes strictly after one opaque cursor", async () => {
  FakeEventSource.current = undefined;
  vi.stubGlobal("EventSource", FakeEventSource);

  const agent = createShiitakeClient({ baseUrl: "/api/" });
  const subscription = agent
    .session("demo-session")
    .subscribe({ after: "opaque-cursor" });
  const iterator = subscription[Symbol.asyncIterator]();
  const next = iterator.next();

  await vi.waitFor(() => {
    expect(FakeEventSource.pending()).toBeDefined();
  });
  const source = FakeEventSource.created();
  expect(source.url).toBe(
    "/api/sessions/demo-session/events?mode=subscribe&after=opaque-cursor",
  );
  source.emit("frame", {
    kind: "reset",
    reason: "history_unavailable",
    cursor: null,
    baseline: sessionSnapshot({ sessionId: "demo-session" }),
  });

  await expect(next).resolves.toMatchObject({
    done: false,
    value: { kind: "reset", reason: "history_unavailable" },
  });
  await iterator.return?.();
  expect(source.closeCount).toBe(1);
  await agent.close();
});

it("neutral watch yields a parsed snapshot and live frames", async () => {
  FakeEventSource.current = undefined;
  vi.stubGlobal("EventSource", FakeEventSource);

  const agent = createShiitakeClient({ baseUrl: "/api/" });
  const pending = agent.session("demo-session").watch();

  await vi.waitFor(() => {
    expect(FakeEventSource.pending()).toBeDefined();
  });
  const pendingSource = FakeEventSource.created();
  pendingSource.emit(
    "snapshot",
    sessionSnapshot({ sessionId: "demo-session" }),
  );
  const watch = await pending;

  const source = FakeEventSource.created();
  expect(source.url).toBe("/api/sessions/demo-session/events");
  expect(watch.snapshot).toEqual(
    sessionSnapshot({ sessionId: "demo-session" }),
  );

  const next = watch.changes[Symbol.asyncIterator]().next();
  source.emit("frame", {
    kind: "reset",
    reason: "history_unavailable",
    cursor: null,
    baseline: sessionSnapshot({ sessionId: "demo-session" }),
  });
  await expect(next).resolves.toEqual({
    done: false,
    value: {
      kind: "reset",
      reason: "history_unavailable",
      cursor: null,
      baseline: sessionSnapshot({ sessionId: "demo-session" }),
    },
  });

  await watch.close();
  await watch.close();
  expect(source.closeCount).toBe(1);
  await agent.close();
});

it("neutral watch parses an extension-only update at the existing cursor", async () => {
  FakeEventSource.current = undefined;
  vi.stubGlobal("EventSource", FakeEventSource);

  const agent = createShiitakeClient({ baseUrl: "/api" });
  const pendingWatch = agent.session("demo-session").watch();
  await vi.waitFor(() => {
    expect(FakeEventSource.pending()).toBeDefined();
  });
  const source = FakeEventSource.created();
  source.emit(
    "snapshot",
    sessionSnapshot({
      sessionId: "demo-session",
      cursor: "opaque-cursor-1",
      extensions: fixtureSlot("active"),
    }),
  );
  const watch = await pendingWatch;
  const next = watch.changes[Symbol.asyncIterator]().next();
  source.emit("frame", {
    pending: { total: 0, prompts: [], hasMore: false },
    kind: "update",
    delivery: "live",
    cursor: "opaque-cursor-1",
    messages: [],
    terminals: [],
    activity: idleActivity,
    extensions: fixtureSlot("paused"),
    attention: [],
  });

  await expect(next).resolves.toEqual({
    done: false,
    value: {
      pending: { total: 0, prompts: [], hasMore: false },
      kind: "update",
      delivery: "live",
      cursor: "opaque-cursor-1",
      messages: [],
      terminals: [],
      activity: idleActivity,
      extensions: fixtureSlot("paused"),
      attention: [],
    },
  });
  await watch.close();
  await agent.close();
});

it("neutral watch turns a cursorless native reconnect snapshot into a reset", async () => {
  FakeEventSource.current = undefined;
  vi.stubGlobal("EventSource", FakeEventSource);

  const agent = createShiitakeClient({ baseUrl: "/api" });
  const pendingWatch = agent.session("demo-session").watch();
  await vi.waitFor(() => {
    expect(FakeEventSource.pending()).toBeDefined();
  });
  const source = FakeEventSource.created();
  const initial = sessionSnapshot({ sessionId: "demo-session" });
  source.emit("snapshot", initial);
  const watch = await pendingWatch;

  const next = watch.changes[Symbol.asyncIterator]().next();
  source.emit("snapshot", initial);

  await expect(next).resolves.toEqual({
    done: false,
    value: {
      kind: "reset",
      reason: "history_unavailable",
      cursor: null,
      baseline: initial,
    },
  });
  await watch.close();
  expect(source.closeCount).toBe(1);
  await agent.close();
});

it("neutral watch surfaces a canonical runtime failure after SSE starts", async () => {
  FakeEventSource.current = undefined;
  vi.stubGlobal("EventSource", FakeEventSource);

  const agent = createShiitakeClient({ baseUrl: "/api" });
  const pendingWatch = agent.session("demo-session").watch();
  await vi.waitFor(() => {
    expect(FakeEventSource.pending()).toBeDefined();
  });
  const source = FakeEventSource.created();
  source.emit("snapshot", sessionSnapshot({ sessionId: "demo-session" }));
  const watch = await pendingWatch;
  const resultPromise = watch.changes[Symbol.asyncIterator]().next();

  source.emit("runtime-error", {
    _tag: "ShiitakeError",
    boundary: "runtime",
    cause: "unavailable",
    message: "Session stream failed",
  });
  await expect(
    Promise.race([
      resultPromise,
      new Promise<never>((_resolve, reject) => {
        setTimeout(() => {
          reject(new Error("Session runtime error did not settle promptly"));
        }, 100);
      }),
    ]),
  ).rejects.toMatchObject({
    _tag: "ShiitakeError",
    boundary: "runtime",
    cause: "unavailable",
    message: "Session stream failed",
  });
  await watch.close();
  expect(source.closeCount).toBe(1);
  await agent.close();
});

it("neutral watch survives reconnecting errors and fails after permanent EventSource closure", async () => {
  FakeEventSource.current = undefined;
  vi.stubGlobal("EventSource", FakeEventSource);

  const agent = createShiitakeClient({ baseUrl: "/api" });
  const pendingWatch = agent.session("demo-session").watch();
  await vi.waitFor(() => {
    expect(FakeEventSource.pending()).toBeDefined();
  });
  const source = FakeEventSource.created();
  source.emit("snapshot", sessionSnapshot({ sessionId: "demo-session" }));
  const watch = await pendingWatch;
  const changes = watch.changes[Symbol.asyncIterator]();

  const afterReconnectableError = changes.next();
  source.emitError(source.CONNECTING);
  source.emit("frame", {
    kind: "progress",
    activity: runningActivity,
    progress: {
      messageId: "message-1",
      revision: 1,
      text: "still connected",
      thinking: "",
      textTruncated: false,
      thinkingTruncated: false,
    },
  });
  await expect(afterReconnectableError).resolves.toMatchObject({
    done: false,
    value: { kind: "progress", progress: { messageId: "message-1" } },
  });

  const afterPermanentError = changes.next();
  source.emitError(source.CLOSED);
  try {
    await expect(
      Promise.race([
        afterPermanentError,
        new Promise<never>((_resolve, reject) => {
          setTimeout(() => {
            reject(new Error("Permanent EventSource closure did not settle"));
          }, 100);
        }),
      ]),
    ).rejects.toMatchObject({
      _tag: "ShiitakeError",
      boundary: "runtime",
      cause: "unavailable",
    });
  } finally {
    await watch.close();
    expect(source.closeCount).toBe(1);
    await agent.close();
  }
});

it("neutral watch rejects a malformed SSE representation", async () => {
  FakeEventSource.current = undefined;
  vi.stubGlobal("EventSource", FakeEventSource);

  const agent = createShiitakeClient({ baseUrl: "/api" });
  const pendingWatch = agent.session("demo-session").watch();
  await vi.waitFor(() => {
    expect(FakeEventSource.pending()).toBeDefined();
  });
  const source = FakeEventSource.created();
  source.emitSerialized("snapshot", "not-json");

  await expect(pendingWatch).rejects.toMatchObject({
    _tag: "ShiitakeError",
    boundary: "runtime",
    cause: "invalid",
    message: "Session snapshot is invalid",
  });
  expect(source.closeCount).toBe(1);
  await agent.close();
});

it("closing a neutral watch settles one pending read and closes once", async () => {
  FakeEventSource.current = undefined;
  vi.stubGlobal("EventSource", FakeEventSource);

  const agent = createShiitakeClient({ baseUrl: "/api" });
  const pendingWatch = agent.session("demo-session").watch();
  await vi.waitFor(() => {
    expect(FakeEventSource.pending()).toBeDefined();
  });
  const source = FakeEventSource.created();
  source.emit("snapshot", sessionSnapshot({ sessionId: "demo-session" }));
  const watch = await pendingWatch;
  const pendingRead = watch.changes[Symbol.asyncIterator]().next();

  await watch.close();
  await expect(pendingRead).resolves.toEqual({
    done: true,
    value: undefined,
  });
  await watch.close();
  expect(source.closeCount).toBe(1);
  await agent.close();
});

it("neutral discovery decodes a page and rejects reads after client close", async () => {
  const requests: string[] = [];
  vi.stubGlobal("fetch", (url: string) => {
    requests.push(url);
    return Promise.resolve(
      Response.json({
        sessions: [
          {
            sessionId: "saved",
            title: "Retained task",
            createdAt: 1,
            lastActivityAt: 2,
            state: "done",
          },
        ],
      }),
    );
  });
  const client = createShiitakeClient({ baseUrl: "http://shiitake.test" });
  expect(await client.listSessions("earlier")).toEqual({
    sessions: [
      {
        sessionId: "saved",
        title: "Retained task",
        createdAt: 1,
        lastActivityAt: 2,
        state: "done",
      },
    ],
    next: undefined,
  });
  expect(requests).toEqual(["http://shiitake.test/sessions?after=earlier"]);
  await client.close();
  await expect(client.listSessions()).rejects.toMatchObject({
    cause: "closed",
  });
  expect(requests).toHaveLength(1);
});

it("neutral discovery rejects malformed summaries rather than displaying them", async () => {
  vi.stubGlobal("fetch", () =>
    Promise.resolve(
      Response.json({
        sessions: [
          {
            sessionId: "bad",
            title: 42,
            createdAt: 1,
            lastActivityAt: 2,
            state: "done",
          },
        ],
      }),
    ),
  );
  const client = createShiitakeClient({ baseUrl: "http://shiitake.test" });
  try {
    await expect(client.listSessions()).rejects.toMatchObject({
      cause: "invalid",
    });
  } finally {
    await client.close();
  }
});

it("persists a new conversation before its first prompt and discovers it without read writes", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const local = await Shiitake.make({
    store: fixture.owner,
    runtime: runtime.runtime,
    ...compatibilityRunDependencies(),
  });
  const routes = createShiitakeHttpRoutes(
    (id) => local.session(id),
    local.listSessions,
    local.createSession,
    local.deleteSession,
    () => local.extensions,
  );
  vi.stubGlobal("fetch", (input: RequestInfo | URL, init?: RequestInit) =>
    routes.fetch(new Request(input, init)),
  );
  const remote = createShiitakeClient({ baseUrl: "http://shiitake.test" });
  try {
    expect((await remote.listSessions()).sessions).toEqual([]);
    const changes = () =>
      Schema.decodeUnknownSync(Schema.Struct({ count: Schema.Number }))(
        fixture.owner.sql.exec("SELECT total_changes() AS count").one(),
      ).count;
    const beforeCreation = changes();
    const selectedId = await remote.createSession("discovery-create-selected");
    const replayedId = await remote.createSession("discovery-create-selected");
    const otherId = await remote.createSession("discovery-create-other");
    expect(replayedId).toBe(selectedId);
    expect(selectedId).not.toBe(otherId);
    // Two Sessions, each with its receipt, inbox row and inbox revision.
    expect(changes()).toBe(beforeCreation + 8);
    expect((await remote.listSessions()).sessions).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ sessionId: selectedId, title: "" }),
        expect.objectContaining({ sessionId: otherId, title: "" }),
      ]),
    );
    await remote.session(selectedId).command({
      delivery: "followUp",

      type: "prompt",
      requestId: "discovery-task",
      text: "Continue the repository task",
    });
    await runtime.runNext();
    const before = changes();
    const page = await remote.listSessions();
    expect(page.sessions).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          sessionId: selectedId,
          title: "Continue the repository task",
        }),
        expect.objectContaining({ sessionId: otherId, title: "" }),
      ]),
    );
    expect(changes()).toBe(before);
    await local.close();
    await expect(
      remote.createSession("closed-create-request"),
    ).rejects.toMatchObject({
      cause: "closed",
    });
    await expect(local.listSessions()).rejects.toMatchObject({
      cause: "closed",
    });
  } finally {
    await remote.close();
    await local.close();
    fixture.close();
  }
});
