import { expect, it, vi } from "vitest";
import { BrowserSessions } from "../src/internal/browser-sessions.js";

function request(session: string, route = "events") {
  return new Request(
    `http://owner/browser/sessions/${encodeURIComponent(session)}/browser/${route}`,
  );
}

function hostFactory() {
  const cancelled = vi.fn();
  const open = vi.fn((sessionId: string) => ({
    modules: [],
    fetch: (request: Request) => {
      if (new URL(request.url).pathname !== "/browser/events")
        return Promise.resolve(new Response(sessionId));
      return Promise.resolve(
        new Response(
          new ReadableStream<Uint8Array>({
            start: (controller) => {
              controller.enqueue(new TextEncoder().encode(sessionId));
            },
            cancel: cancelled,
          }),
        ),
      );
    },
  }));
  return { open, cancelled };
}

it("shares concurrent browser connections only within their persisted Session and releases idle hosts", async () => {
  const { open, cancelled } = hostFactory();
  const sessions = new BrowserSessions(open, (id) =>
    Promise.resolve(id !== "missing"),
  );
  expect((await sessions.fetch(request("missing"))).status).toBe(404);
  expect((await sessions.fetch(request("one", "claims"))).status).toBe(404);
  expect(open).not.toHaveBeenCalled();
  const [first, second, other] = await Promise.all([
    sessions.fetch(request("one")),
    sessions.fetch(request("one")),
    sessions.fetch(request("two")),
  ]);
  expect(open.mock.calls).toEqual([["one"], ["two"]]);
  await first.body?.cancel();
  expect(await (await sessions.fetch(request("one", "claims"))).text()).toBe(
    "one",
  );
  await second.body?.cancel();
  expect((await sessions.fetch(request("one", "claims"))).status).toBe(404);
  expect(await (await sessions.fetch(request("two", "results"))).text()).toBe(
    "two",
  );
  const reconnected = await sessions.fetch(request("one"));
  expect(open).toHaveBeenCalledTimes(3);
  const reconnectedReader = reconnected.body?.getReader();
  const otherReader = other.body?.getReader();
  await reconnectedReader?.read();
  await otherReader?.read();
  await sessions.clear();
  expect(cancelled).toHaveBeenCalledTimes(4);
  expect(await reconnectedReader?.read()).toEqual({
    done: true,
    value: undefined,
  });
  expect(await otherReader?.read()).toEqual({
    done: true,
    value: undefined,
  });
});

it("a run lease retains its browser host after SSE disconnect, with idempotent finalization", async () => {
  const { open } = hostFactory();
  const sessions = new BrowserSessions(open, () => Promise.resolve(true));
  const first = sessions.acquire("one");
  const second = sessions.acquire("one");
  const connection = await sessions.fetch(request("one"));
  await connection.body?.cancel();
  first.release();
  first.release();
  expect(await (await sessions.fetch(request("one", "results"))).text()).toBe(
    "one",
  );
  second.release();
  expect((await sessions.fetch(request("one", "claims"))).status).toBe(404);
  const next = sessions.acquire("one");
  expect(open).toHaveBeenCalledTimes(2);
  next.release();
});

it("failed stream admission releases the ephemeral host so reconnect can retry", async () => {
  const open = vi
    .fn()
    .mockReturnValueOnce({
      modules: [],
      fetch: () => Promise.reject(new Error("host unavailable")),
    })
    .mockReturnValue({
      modules: [],
      fetch: () => Promise.resolve(new Response(null, { status: 503 })),
    });
  const sessions = new BrowserSessions(open, () => Promise.resolve(true));
  await expect(sessions.fetch(request("one"))).rejects.toThrow(
    "host unavailable",
  );
  expect((await sessions.fetch(request("one", "claims"))).status).toBe(404);
  expect((await sessions.fetch(request("one"))).status).toBe(503);
  expect((await sessions.fetch(request("one", "claims"))).status).toBe(404);
  expect(open).toHaveBeenCalledTimes(2);
});

it("teardown refuses and cancels an event stream that finishes opening late", async () => {
  let complete: (response: Response) => void = () => {
    throw new Error("opening not started");
  };
  const cancelled = vi.fn();
  const open = vi.fn(() => ({
    modules: [],
    fetch: () =>
      new Promise<Response>((resolve) => {
        complete = resolve;
      }),
  }));
  const sessions = new BrowserSessions(open, () => Promise.resolve(true));
  const pending = sessions.fetch(request("one"));
  await vi.waitFor(() => {
    expect(open).toHaveBeenCalledOnce();
  });
  await sessions.clear();
  complete(new Response(new ReadableStream<Uint8Array>({ cancel: cancelled })));
  expect((await pending).status).toBe(404);
  expect(cancelled).toHaveBeenCalledOnce();
  expect((await sessions.fetch(request("one", "claims"))).status).toBe(404);
});

it("teardown cannot reopen a host from a pending Session lookup", async () => {
  let complete: (exists: boolean) => void = () => {
    throw new Error("lookup not started");
  };
  const { open } = hostFactory();
  const sessions = new BrowserSessions(
    open,
    () =>
      new Promise<boolean>((resolve) => {
        complete = resolve;
      }),
  );
  const pending = sessions.fetch(request("one"));
  await sessions.clear();
  complete(true);
  const response = await pending;
  try {
    expect(response.status).toBe(404);
    expect(open).not.toHaveBeenCalled();
  } finally {
    await response.body?.cancel();
  }
});
