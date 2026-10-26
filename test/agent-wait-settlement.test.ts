import { Mycelium } from "@fungi.computer/mycelium";
import { directSandbox } from "@fungi.computer/mycelium/testing";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { isString } from "effect/Predicate";
import { expect, it } from "vitest";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { fauxModel } from "@fungi.computer/mule/testing";

import { createAgentControlSource } from "../src/agent-control.js";
import { createShiitakeClient } from "../src/client.js";
import { createShiitakeHttpRoutes } from "../src/http-routes.js";
import type { SqliteOwner } from "../src/index.js";
import { memoryHost } from "../src/testing.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

it.each([false, true])(
  "160 in-flight agents.wait calls avoid observation work (HTTP: %s)",
  async (remote) => {
    async function measure(waiting: boolean) {
      const fixture = createNativeSqliteOwnerFixture();
      let contextReads = 0;
      let snapshotReads = 0;
      let progressFrames = 0;
      let waitStarts = 0;
      const owner: SqliteOwner = {
        transactionSync: (operation) =>
          fixture.owner.transactionSync(operation),
        sql: {
          exec: (statement, ...values) => {
            if (
              statement.includes("SELECT id FROM walk") &&
              statement.includes("state != 'streaming'")
            )
              contextReads += 1;
            return fixture.owner.sql.exec(statement, ...values);
          },
        },
      };
      const faux = fauxModel(
        Array.from({ length: 160 }, () => fauxAssistantMessage("answer")),
        { tokenSize: { min: 1, max: 1 }, tokensPerSecond: 2_000 },
      );
      const host = await memoryHost({
        owner,
        model: {
          acquire: () =>
            Promise.resolve({
              model: faux.model,
              stream: faux.stream,
              retryable: () => false,
              release: () => Promise.resolve(),
            }),
        },
      });
      const observedAgent = {
        ...host.agent,
        session: (id: string) => {
          const session = host.agent.session(id);
          return {
            ...session,
            read: (input: Parameters<typeof session.read>[0]) => {
              snapshotReads += 1;
              return session.read(input);
            },
            readSettlement: (
              ...args: Parameters<typeof session.readSettlement>
            ) => {
              waitStarts += 1;
              return session.readSettlement(...args);
            },
            watch: async (input: Parameters<typeof session.watch>[0]) => {
              snapshotReads += 1;
              const watch = await session.watch(input);
              waitStarts += 1;
              return {
                ...watch,
                changes: (async function* () {
                  for await (const frame of watch.changes) {
                    if (frame.kind === "progress") progressFrames += 1;
                    yield frame;
                  }
                })(),
              };
            },
          };
        },
      };
      const routes = createShiitakeHttpRoutes(
        observedAgent.session,
        host.agent.listSessions,
        host.agent.createSession,
        host.agent.deleteSession,
        () => host.agent.extensions,
      );
      // The old watch path uses native EventSource over this real HTTP endpoint.
      // Run the base comparison with NODE_OPTIONS=--experimental-eventsource.
      const server = createServer((request, response) => {
        const abort = new AbortController();
        const closed = once(response, "close");
        response.on("close", () => {
          abort.abort();
        });
        void (async () => {
          const result = await routes.fetch(
            new Request(new URL(request.url ?? "/", "http://localhost"), {
              signal: abort.signal,
            }),
          );
          response.writeHead(result.status, Object.fromEntries(result.headers));
          if (result.body !== null) {
            const reader = result.body.getReader();
            try {
              await Promise.race([
                closed,
                (async () => {
                  for (;;) {
                    const chunk = await reader.read();
                    if (chunk.done) return;
                    response.write(chunk.value);
                  }
                })(),
              ]);
            } finally {
              await reader.cancel();
            }
          }
          response.end();
        })().catch((error: unknown) => {
          response.destroy(
            error instanceof Error ? error : new Error(String(error)),
          );
        });
      });
      server.listen(0, "127.0.0.1");
      await once(server, "listening");
      const address = server.address();
      if (address === null || isString(address))
        throw new Error("Missing HTTP listener address");
      const requests: string[] = [];
      const client = createShiitakeClient({
        baseUrl: `http://127.0.0.1:${String(address.port)}`,
        fetch: (input, init) => {
          const request = new Request(input, init);
          requests.push(new URL(request.url).pathname);
          return Promise.resolve(routes.fetch(request));
        },
      });
      const runtime = await Mycelium.make({
        sources: [
          createAgentControlSource({
            list: () => Promise.resolve([]),
            open: () =>
              Promise.resolve({
                agent: remote ? client : observedAgent,
                release: () => Promise.resolve(),
              }),
          }),
        ],
        sandbox: directSandbox(async ({ agents }) => {
          if (agents?.wait === undefined)
            throw new Error("Missing agents.wait");
          const wait = agents.wait;
          for (let cycle = 0; cycle < 20; cycle += 1) {
            const targets = [];
            for (let index = 0; index < 8; index += 1) {
              const sessionId = `helper-${String(index)}`;
              const accepted = await host.agent
                .session(sessionId)
                .send("Answer.");
              targets.push({ sessionId, commandId: accepted.commandId });
            }
            const waits = waiting
              ? Promise.all(
                  targets.map(async ({ sessionId, commandId }) => {
                    expect(
                      await wait({
                        agentId: "helper",
                        sessionId,
                        commandId,
                        timeoutMs: 30_000,
                      }),
                    ).toEqual({
                      commandId,
                      outcome: "completed",
                      text: "answer",
                      truncated: false,
                    });
                  }),
                )
              : Promise.resolve([]);
            if (waiting)
              await expect.poll(() => waitStarts).toBe((cycle + 1) * 8);
            // No reconciliation has run before all eight waits enter the owner.
            // Drive the identical workload without waits as the execution control.
            await host.settle("helper-0");
            await waits;
          }
          return null;
        }),
      });
      const lease = await runtime.acquire();
      try {
        const prepared = await Effect.runPromise(
          lease.executeTool.prepare({
            type: "toolCall",
            id: "settlement-waits",
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
        if (remote && waiting && snapshotReads === 0)
          expect(
            requests.filter((path) => path.endsWith("/settlement")),
          ).toHaveLength(160);
        return { snapshotReads, contextReads, progressFrames };
      } finally {
        await lease.release();
        await runtime.close();
        await client.close();
        await host.close();
        fixture.close();
        server.closeAllConnections();
        await new Promise<void>((resolve, reject) =>
          server.close((error) => {
            if (error) reject(error);
            else resolve();
          }),
        );
      }
    }
    const control = await measure(false);
    const observed = await measure(true);
    // The executor necessarily reads its input context. Only the extra reads
    // caused by waits belong to S6's zero-observation-work bar.
    expect({
      snapshotReads: observed.snapshotReads - control.snapshotReads,
      contextReads: observed.contextReads - control.contextReads,
      progressFrames: observed.progressFrames - control.progressFrames,
    }).toEqual({ snapshotReads: 0, contextReads: 0, progressFrames: 0 });
  },
  60_000,
);

it.each([
  { remote: false, outcome: "completed" },
  { remote: true, outcome: "completed" },
  { remote: false, outcome: "failed" },
  { remote: true, outcome: "failed" },
  { remote: false, outcome: "cancelled" },
  { remote: true, outcome: "cancelled" },
])(
  "settlement read carries $outcome after a hint (HTTP: $remote)",
  async ({ remote, outcome }) => {
    const faux = fauxModel(
      [
        fauxAssistantMessage("Exact command answer.", {
          stopReason: outcome === "failed" ? "error" : "stop",
        }),
      ],
      { tokenSize: { min: 1, max: 1 } },
    );
    const host = await memoryHost({
      model: {
        acquire: () =>
          Promise.resolve({
            model: faux.model,
            stream: faux.stream,
            retryable: () => false,
            release: () => Promise.resolve(),
          }),
      },
    });
    const requests: string[] = [];
    const routes = createShiitakeHttpRoutes(
      host.agent.session,
      host.agent.listSessions,
      host.agent.createSession,
      host.agent.deleteSession,
      () => host.agent.extensions,
    );
    const client = createShiitakeClient({
      baseUrl: "https://agent.internal",
      fetch: (input, init) => {
        const request = new Request(input, init);
        requests.push(new URL(request.url).pathname);
        return Promise.resolve(routes.fetch(request));
      },
    });
    const session = (remote ? client : host.agent).session("settlement");
    try {
      const accepted = await session.send("Work.");
      // Exceed the registration ceiling cumulatively: timeouts must release it.
      for (let cycle = 0; cycle < 70; cycle += 1)
        expect(
          await session.readSettlement(accepted.commandId, {
            timeoutMs: 1,
          }),
        ).toBeUndefined();
      const waiting = session.readSettlement(accepted.commandId, {
        timeoutMs: 2_000,
      });
      if (outcome === "cancelled")
        await session.cancel({ commandId: accepted.commandId });
      const snapshot = await host.settle("settlement");
      const settled = await waiting;
      expect(settled?.terminal).toMatchObject({
        commandId: accepted.commandId,
        outcome,
      });
      expect(settled?.content).toBe(
        outcome === "cancelled" ? "" : "Exact command answer.",
      );
      if (outcome === "failed")
        expect(settled?.terminal.diagnostic).toEqual(expect.any(String));
      if (outcome === "completed") {
        expect(settled?.usage).toHaveLength(1);
        const first = snapshot.messages[0];
        if (first === undefined) throw new Error("Missing command row");
        await session.command({ type: "branch", entryId: first.id });
        // Exact settlement is still available when the selected branch no
        // longer contains its answer or terminal.
        expect(await session.readSettlement(accepted.commandId)).toEqual(
          settled,
        );
      }
      expect(settled?.toolCalls).toBe(0);
      expect(requests.some((path) => path.endsWith("/events"))).toBe(false);
      if (remote)
        expect(
          requests.filter((path) => path.endsWith("/settlement")).length,
        ).toBeGreaterThanOrEqual(71);
    } finally {
      await client.close();
      await host.close();
    }
  },
);

it("aborted and closed HTTP settlement reads release registrations without cancelling work", async () => {
  const host = await memoryHost();
  const routes = createShiitakeHttpRoutes(
    host.agent.session,
    host.agent.listSessions,
    host.agent.createSession,
    host.agent.deleteSession,
    () => host.agent.extensions,
  );
  const client = createShiitakeClient({
    baseUrl: "https://agent.internal",
    fetch: (input, init) =>
      Promise.resolve(routes.fetch(new Request(input, init))),
  });
  try {
    const session = client.session("abort");
    const { commandId } = await session.send("Survive abandoned observers.");
    for (let index = 0; index < 70; index += 1) {
      const controller = new AbortController();
      const waiting = session.readSettlement(commandId, {
        timeoutMs: 30_000,
        signal: controller.signal,
      });
      const rejected = expect(waiting).rejects.toThrow();
      controller.abort();
      await rejected;
    }
    const waiting = session.readSettlement(commandId, { timeoutMs: 30_000 });
    const rejected = expect(waiting).rejects.toThrow("closed");
    await client.close();
    await rejected;
    await host.settle("abort");
    expect(
      (await host.agent.session("abort").readSettlement(commandId))?.terminal,
    ).toMatchObject({ outcome: "completed" });
  } finally {
    await client.close();
    await host.close();
  }
});
import { createServer } from "node:http";
import { once } from "node:events";
