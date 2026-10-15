import { createShiitakeClient } from "../src/client.js";
import { createShiitakeHttpRoutes } from "../src/http-routes.js";
import { expect, it, vi } from "vitest";
import { Shiitake, type ModelPreference } from "../src/index.js";
import {
  createCompatibilityRuntimeHost,
  compatibilityRunDependencies,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

type Deferred<T> = {
  promise: Promise<T>;
  resolve: (value: T | PromiseLike<T>) => void;
};

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}

it("freezes the prepared preference while queued runs use later changes and clearing", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const dependencies = compatibilityRunDependencies();
  const entered = deferred<undefined>();
  const release = deferred<undefined>();
  const acquired: (ModelPreference | null)[] = [];
  let loads = 0;
  const agent = await Shiitake.make({
    ...dependencies,
    store: fixture.owner,
    runtime: runtime.runtime,
    instructions: {
      load: async () => {
        if (loads++ === 0) {
          entered.resolve(undefined);
          await release.promise;
        }
        return dependencies.instructions.load();
      },
    },
    model: {
      acquire: ({ preference }) => {
        acquired.push(preference);
        return dependencies.model.acquire();
      },
    },
  });
  let running: Promise<void> | undefined;
  try {
    const sessionId = "preference-runs";
    const session = agent.session(sessionId);
    const first = { providerId: "faux", modelId: "first" };
    const second = { providerId: "faux", modelId: "second" };
    await session.modelPreference.set(first);
    expect(await session.modelPreference.read()).toEqual(first);
    await session.command({
      delivery: "followUp",

      type: "prompt",
      requestId: "first",
      text: "First run",
    });
    await session.command({
      delivery: "followUp",

      type: "prompt",
      requestId: "second",
      text: "Queued run",
    });
    running = runtime.runNext();
    await entered.promise;
    await session.modelPreference.set(second);
    expect(acquired).toEqual([]);
    release.resolve(undefined);
    await running;
    running = undefined;
    expect(acquired).toEqual([first]);
    await runtime.runNext();
    expect(acquired).toEqual([first, second]);
    await session.modelPreference.set(null);
    expect(await session.modelPreference.read()).toBeNull();
    await session.command({
      delivery: "followUp",

      type: "prompt",
      requestId: "third",
      text: "Inherit again",
    });
    await runtime.runNext();
    expect(acquired).toEqual([first, second, null]);
  } finally {
    release.resolve(undefined);
    await running;
    await agent.close();
    fixture.close();
  }
});

it("shares preference set, read and clear over HTTP without replacing the owner", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const agent = await Shiitake.make({
    ...compatibilityRunDependencies(),
    store: fixture.owner,
    runtime: runtime.runtime,
  });
  const app = createShiitakeHttpRoutes(
    (id) => agent.session(id),
    agent.listSessions,
    agent.createSession,
    agent.deleteSession,
    () => agent.extensions,
  );
  vi.stubGlobal("fetch", (input: RequestInfo | URL, init?: RequestInit) =>
    app.fetch(new Request(input, init)),
  );
  const remote = createShiitakeClient({ baseUrl: "http://shiitake.test" });
  try {
    const session = remote.session("preferences-http");
    expect(await session.modelPreference.read()).toBeNull();
    const preference = { providerId: "faux", modelId: "selected" };
    await session.modelPreference.set(preference);
    expect(await session.modelPreference.read()).toEqual(preference);
    expect(
      await agent.session("preferences-http").modelPreference.read(),
    ).toEqual(preference);
    const invalid = await app.request(
      "/sessions/preferences-http/model-preference",
      {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ providerId: "", modelId: "selected" }),
      },
    );
    expect(invalid.status).toBe(400);
    expect(await session.modelPreference.read()).toEqual(preference);
    await session.modelPreference.set(null);
    expect(await session.modelPreference.read()).toBeNull();
    expect((await session.read()).messages).toEqual([]);
  } finally {
    await remote.close();
    vi.unstubAllGlobals();
    await agent.close();
    fixture.close();
  }
});
