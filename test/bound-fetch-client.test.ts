import { grantAll, recorderExtension } from "./fixtures/fixture-extensions.js";
import { expect, it, vi } from "vitest";
import { createShiitakeClient } from "../src/client.js";
import { Shiitake } from "../src/index.js";
import { createShiitakeHttpRoutes } from "../src/http-routes.js";
import {
  compatibilityRunDependencies,
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

it("uses the host-bound fetch for ordinary Agent and Session HTTP operations", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const owner = await Shiitake.make({
    store: fixture.owner,
    runtime: runtime.runtime,
    ...compatibilityRunDependencies(),
  });
  const routes = createShiitakeHttpRoutes(
    owner.session,
    owner.listSessions,
    owner.createSession,
    owner.deleteSession,
    () => owner.extensions,
  );
  const globalFetch = vi.fn(() =>
    Promise.reject(new Error("Public network is forbidden")),
  );
  vi.stubGlobal("fetch", globalFetch);
  const requests: string[] = [];
  const client = createShiitakeClient({
    baseUrl: "https://agent.internal",
    fetch: (input, init) => {
      const request = new Request(input, init);
      requests.push(new URL(request.url).pathname);
      return Promise.resolve(routes.fetch(request));
    },
  });
  try {
    const sessionId = await client.createSession("bound-fetch-create-session");
    expect((await client.listSessions()).sessions).toEqual([
      expect.objectContaining({ sessionId, title: "", state: "done" }),
    ]);
    const session = client.session(sessionId);
    expect((await session.read()).sessionId).toBe(sessionId);
    await expect(
      session.extension("unrostered").call("read", {}),
    ).rejects.toMatchObject({ boundary: "extensions", cause: "unavailable" });
    expect(globalFetch).not.toHaveBeenCalled();
    expect(requests).toEqual([
      "/sessions",
      "/sessions",
      `/sessions/${sessionId}`,
      `/sessions/${sessionId}/extensions/unrostered/commands/read`,
    ]);
  } finally {
    vi.unstubAllGlobals();
    await client.close();
    await owner.close();
    fixture.close();
  }
});

it("calls, lists and resumes rostered extensions through the existing client route", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const owner = await Shiitake.make({
    ...compatibilityRunDependencies(),
    store: fixture.owner,
    runtime: runtime.runtime,
    extensions: grantAll([recorderExtension()]),
  });
  const routes = createShiitakeHttpRoutes(
    owner.session,
    owner.listSessions,
    owner.createSession,
    owner.deleteSession,
    () => owner.extensions,
  );
  const client = createShiitakeClient({
    baseUrl: "https://agent.internal",
    fetch: (input, init) =>
      Promise.resolve(routes.fetch(new Request(input, init))),
  });
  try {
    await expect(
      client
        .session("remote-extension-session")
        .extension("test-recorder")
        .call("arm", { key: "later", dueAt: Date.now() + 60_000 }),
    ).resolves.toEqual({ armed: true });
    await expect(
      client.extensions.call("test-recorder", "seen", {}),
    ).resolves.toEqual({ count: 0 });
    await expect(client.extensions.list()).resolves.toMatchObject([
      { id: "test-recorder", status: "running" },
    ]);
    await expect(
      client.extensions.resume("test-recorder"),
    ).resolves.toMatchObject({ id: "test-recorder", status: "running" });
    await expect(
      client.extensions.call("test-recorder", "missing", {}),
    ).rejects.toMatchObject({ boundary: "contract", cause: "invalid" });
  } finally {
    await client.close();
    await owner.close();
    fixture.close();
  }
});
