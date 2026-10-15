import { afterEach, expect, it, vi } from "vitest";

import { createShiitakeClient } from "../src/client.js";

afterEach(() => {
  vi.unstubAllGlobals();
});

function deferred<A>() {
  let resolve!: (value: A) => void;
  const promise = new Promise<A>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

it("closes a held request through its fetch AbortSignal", async () => {
  const response = deferred<Response>();
  let signal: AbortSignal | undefined;
  vi.stubGlobal(
    "fetch",
    (_input: string | URL | Request, init?: RequestInit) => {
      signal = init?.signal ?? undefined;
      return response.promise;
    },
  );

  const agent = createShiitakeClient({ baseUrl: "/api" });
  const pending = agent.session("demo-session").command({
    type: "prompt",
    delivery: "steer",
    text: "Hello",
    requestId: "request-1",
  });
  await vi.waitFor(() => {
    expect(signal).toBeDefined();
  });

  await agent.close();

  await expect(pending).rejects.toMatchObject({
    _tag: "ShiitakeError",
    cause: "closed",
  });
  expect(signal?.aborted).toBe(true);
  response.resolve(
    Response.json({ status: "accepted", commandId: "request-1" }),
  );
});

it("closes a response body held after fetch headers arrive", async () => {
  const body = deferred<string>();
  let signal: AbortSignal | undefined;
  vi.stubGlobal(
    "fetch",
    (_input: string | URL | Request, init?: RequestInit) => {
      signal = init?.signal ?? undefined;
      return Promise.resolve({
        ok: true,
        text: () => body.promise,
      });
    },
  );

  const agent = createShiitakeClient({ baseUrl: "/api" });
  const pending = agent.session("demo-session").read();
  await vi.waitFor(() => {
    expect(signal).toBeDefined();
  });

  await agent.close();

  await expect(pending).rejects.toMatchObject({
    _tag: "ShiitakeError",
    cause: "closed",
  });
  expect(signal?.aborted).toBe(true);
  body.resolve("{}");
});

it("closes only its own requests without sending a cancellation command", async () => {
  const first = deferred<Response>();
  const second = deferred<Response>();
  const requests: Request[] = [];
  vi.stubGlobal(
    "fetch",
    (input: string | URL | Request, init?: RequestInit) => {
      const request = new Request(
        input instanceof Request
          ? input
          : new URL(input.toString(), "http://localhost"),
        init,
      );
      requests.push(request);
      return requests.length === 1 ? first.promise : second.promise;
    },
  );

  const closedAgent = createShiitakeClient({ baseUrl: "/api" });
  const openAgent = createShiitakeClient({ baseUrl: "/api" });
  const closedPending = closedAgent.session("demo-session").command({
    type: "prompt",
    delivery: "steer",
    text: "Hello",
    requestId: "request-1",
  });
  const openPending = openAgent.session("demo-session").command({
    type: "prompt",
    delivery: "steer",
    text: "Hello",
    requestId: "request-2",
  });
  await vi.waitFor(() => {
    expect(requests).toHaveLength(2);
  });

  await closedAgent.close();
  await expect(closedPending).rejects.toMatchObject({ cause: "closed" });
  expect(requests.map(({ method }) => method)).toEqual(["POST", "POST"]);

  second.resolve(Response.json({ status: "accepted", commandId: "request-2" }));
  await expect(openPending).resolves.toEqual({
    status: "accepted",
    commandId: "request-2",
  });
  await openAgent.close();
  first.resolve(Response.json({ status: "accepted", commandId: "request-1" }));
});
