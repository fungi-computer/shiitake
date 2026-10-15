import { fauxModel } from "@fungi.computer/mule/testing";
import { cloudflareStorageFixture } from "./fixtures/cloudflare-storage.fixture.js";
import {
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { GuestThrew } from "@fungi.computer/mycelium";
import { afterEach, expect, it, vi } from "vitest";

import { Shiitake, type ShiitakeAgent } from "../src/index.js";
import { makeCloudflareShiitake } from "../src/cloudflare.js";
import { createShiitakeHttpRoutes } from "../src/http-routes.js";
import { createShiitakeClient } from "../src/remote-client.js";
import {
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
  noExtensions,
} from "./fixtures/native-sqlite-owner.fixture.js";

const cases = [
  {
    name: "preserves a sandbox-admitted guest diagnostic",
    failure: new GuestThrew({
      name: "Error",
      message: "guest operation is unavailable",
    }),
    expected: "Error: guest operation is unavailable",
  },
  {
    name: "keeps an unclassified host diagnostic private",
    failure: new Error("private host credential canary"),
    expected: "The sandbox failed for an unrecognized reason",
  },
] as const;

afterEach(() => vi.restoreAllMocks());

for (const transport of ["promise", "cloudflare"] as const) {
  it.each(cases)(
    `${transport} $name in the model and Session`,
    async (test) => {
      // Operator logs are separate from model/Session messages; keep this
      // intentional host-failure fixture out of the test runner's console.
      vi.spyOn(console, "log").mockImplementation(() => undefined);
      const fixture = createNativeSqliteOwnerFixture();
      const runtime = createCompatibilityRuntimeHost();
      const tasks: Promise<unknown>[] = [];
      const faux = fauxModel([]);
      const modelResults: unknown[] = [];
      const modelContexts: unknown[] = [];
      faux.setResponses([
        fauxAssistantMessage(
          fauxToolCall("execute", { code: "throw new Error('guest failure')" }),
          { stopReason: "toolUse" },
        ),
        (context) => {
          modelContexts.push(context);
          modelResults.push(
            ...context.messages.filter(
              (message) => message.role === "toolResult",
            ),
          );
          return fauxAssistantMessage("I can report the failed operation.");
        },
      ]);
      const tools = {
        modules: [],
        sandbox: { execute: () => Promise.reject(test.failure) },
      };
      const acquisition = {
        model: faux.model,
        stream: faux.stream,
        release: () => Promise.resolve(),
      };
      let agent: ShiitakeAgent | undefined;
      try {
        agent =
          transport === "promise"
            ? await Shiitake.make({
                store: fixture.owner,
                extensions: noExtensions,
                runtime: runtime.runtime,
                instructions: {
                  load: () =>
                    Promise.resolve({
                      revision: "sandbox-error-v1",
                      text: "Report failures.",
                    }),
                },
                model: { acquire: () => Promise.resolve(acquisition) },
                tools,
              })
            : await makeCloudflareShiitake(
                {
                  storage: cloudflareStorageFixture(fixture.owner),
                  waitUntil: (task) => tasks.push(task),
                },
                {
                  extensions: noExtensions,
                  model: { acquire: () => Promise.resolve(acquisition) },
                  instructions: {
                    load: () =>
                      Promise.resolve({
                        revision: "failure-test-v1",
                        text: "Report failures.",
                      }),
                  },
                  tools,
                },
              );
        const session = agent.session("sandbox-error-session");
        await session.command({
          delivery: "followUp",

          type: "prompt",
          requestId: "sandbox-error-command",
          text: "Try the operation and report what happened.",
        });
        if (transport === "promise") await runtime.runNext();
        else for (const task of tasks) await task;

        const expectedText: unknown = expect.stringContaining(test.expected);
        const expectedResult = {
          role: "toolResult",
          toolName: "execute",
          isError: true,
          content: [{ type: "text", text: expectedText }],
        };
        expect(modelResults).toHaveLength(1);
        expect(modelResults[0]).toMatchObject(expectedResult);
        expect(JSON.stringify(await session.read())).not.toContain(
          "private host credential canary",
        );
        const snapshot = await session.read({ includeSandboxFailures: true });
        expect(JSON.stringify(modelContexts)).not.toContain(
          "private host credential canary",
        );
        expect(snapshot.sandboxFailures).toHaveLength(1);
        expect(snapshot.sandboxFailures?.[0]).toMatchObject({
          kind: "sandbox_failure",
          tag: test.failure instanceof GuestThrew ? "GuestThrew" : "Unknown",
        });
        const selectedAgent = agent;
        const routes = createShiitakeHttpRoutes(
          (sessionId) => selectedAgent.session(sessionId),
          agent.listSessions,
          agent.createSession,
          agent.deleteSession,
          () => selectedAgent.extensions,
        );
        const remote = createShiitakeClient({
          baseUrl: "http://shiitake.test",
          fetch: async (input, init) =>
            await routes.fetch(new Request(input, init)),
        });
        try {
          const remoteSession = remote.session("sandbox-error-session");
          expect((await remoteSession.read()).sandboxFailures).toBeUndefined();
          expect(
            (await remoteSession.read({ includeSandboxFailures: true }))
              .sandboxFailures,
          ).toEqual(snapshot.sandboxFailures);
          const normal = await session.watch();
          const operator = await session.watch({
            includeSandboxFailures: true,
          });
          try {
            expect(normal.snapshot.sandboxFailures).toBeUndefined();
            expect(operator.snapshot.sandboxFailures).toEqual(
              snapshot.sandboxFailures,
            );
          } finally {
            await normal.close();
            await operator.close();
          }
        } finally {
          await remote.close();
        }
        const storedResults = snapshot.messages.filter(
          ({ value }) => value.role === "toolResult",
        );
        expect(storedResults).toHaveLength(1);
        expect(storedResults[0]?.value).toMatchObject(expectedResult);
      } finally {
        await agent?.close();
        fixture.close();
      }
    },
  );
}
