import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { rejectingSandbox } from "@fungi.computer/mycelium/testing";
import { openNodeShiitake } from "@fungi.computer/shiitake/node";
import { fauxModelSource } from "@fungi.computer/shiitake/testing";
import { createShiitakeClient } from "@fungi.computer/shiitake/client";
import { Hono } from "hono";
import type { ShiitakeHttpEnv } from "@fungi.computer/shiitake/http";

/**
 * Open a durable Agent in a SQLite file, serve its Session routes behind
 * middleware that names the caller, and wait for one reply.
 */
export async function nodeAgent(path: string) {
  const host = await openNodeShiitake({
    path,
    extensions: { roster: [], grants: [] },
    instructions: {
      load: () => Promise.resolve({ revision: "v1", text: "Reply briefly." }),
    },
    model: fauxModelSource([fauxAssistantMessage("Ready on Node.")]),
    tools: { sandbox: rejectingSandbox("This Agent has no tools") },
  });
  // Only your middleware sets `shiitakeMeta`; request bodies cannot.
  const app = new Hono<ShiitakeHttpEnv>()
    .use(async (context, next) => {
      context.set("shiitakeMeta", { author: { id: "ada", name: "Ada" } });
      await next();
      return undefined;
    })
    .route("/", host.routes);
  // Any HTTP client reaches the same routes; here, Shiitake's own client.
  const client = createShiitakeClient({
    baseUrl: "http://agent.local",
    fetch: (input, init) =>
      Promise.resolve(app.request(new Request(input, init))),
  });
  try {
    const sessionId = await client.createSession("first");
    await client
      .session(sessionId)
      .send("Say hello.", { requestId: "hello", delivery: "followUp" });
    // The Node event loop runs the accepted prompt; poll until it settles.
    for (let attempt = 0; attempt < 200; attempt += 1) {
      const snapshot = await host.agent.session(sessionId).read();
      if (snapshot.terminals.length > 0) return snapshot;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    throw new Error("The prompt did not settle");
  } finally {
    await client.close();
    await host.close();
  }
}
