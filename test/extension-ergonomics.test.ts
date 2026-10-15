import { Mycelium } from "@fungi.computer/mycelium";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { expect, it } from "vitest";
import { z } from "zod";

import {
  defineCollection,
  defineExtension,
  type CommandsV0,
} from "../src/extensions.js";
import { callTool, echoSystemPrompt, memoryHost } from "../src/testing.js";

const assistantText = z.object({
  role: z.literal("assistant"),
  content: z.array(z.object({ type: z.literal("text"), text: z.string() })),
});

function lastText(value: unknown): string {
  return assistantText
    .parse(value)
    .content.map((part) => part.text)
    .join("");
}

it("defines an extension from a bare id and a test host grants what it requests", async () => {
  const houseStyle = defineExtension("house-style").define({
    prompt: () => [{ name: "voice", text: "Answer in one short sentence." }],
  });
  expect(houseStyle.manifest).toMatchObject({
    id: "house-style",
    version: "0.0.0",
    requests: ["prompt"],
  });
  const host = await memoryHost({
    extensions: [houseStyle],
    script: [echoSystemPrompt()],
  });
  try {
    await host.agent.session("style").send("Hi", { requestId: "hi" });
    const snapshot = await host.settle("style");
    expect(lastText(snapshot.messages.at(-1)?.value)).toContain(
      "[extension:house-style.voice]\nAnswer in one short sentence.",
    );
  } finally {
    await host.close();
  }
});

it("keeps an explicit version and refuses an invalid id", () => {
  expect(
    defineExtension({ id: "notes", version: "1.2.3" }).define({}).manifest
      .version,
  ).toBe("1.2.3");
  expect(() => defineExtension("Not An Id")).toThrow();
});

it("scripts a model tool call without a hand-written sandbox", async () => {
  const seen: string[] = [];
  const links = defineExtension("links").define({
    tools: () =>
      Mycelium.module({
        id: "example.links",
        name: "links",
        operations: {
          check: Mycelium.operation({
            input: Schema.Struct({ url: Schema.String }),
            output: Schema.Struct({ host: Schema.String }),
            execute: ({ url }) => {
              seen.push(url);
              return Promise.resolve({ host: new URL(url).host });
            },
          }),
        },
      }),
  });
  const host = await memoryHost({
    extensions: [links],
    script: [
      callTool("links.check", { url: "https://shiit.app/" }),
      "Checked.",
    ],
  });
  try {
    await host.agent.session("links").send("Check it", { requestId: "c" });
    const snapshot = await host.settle("links");
    expect(seen).toEqual(["https://shiit.app/"]);
    expect(lastText(snapshot.messages.at(-1)?.value)).toBe("Checked.");
  } finally {
    await host.close();
  }
});

const Link = Schema.Struct({ url: Schema.String, title: Schema.String });
const Bookmark = Schema.Struct({ ...Link.fields, sessionId: Schema.String });

it("declares state by collections alone, reads it synchronously and projects a plain value", async () => {
  const bookmarks = defineExtension("bookmarks")
    .state({
      bookmarks: defineCollection(Bookmark, {
        key: (b) => JSON.stringify([b.sessionId, b.url]),
        index: (b) => b.sessionId,
      }),
    })
    .define({
      prompt: (ctx, run) => {
        const rows = ctx.state.read((s) =>
          s.bookmarks.page({ index: run.sessionId, limit: 32 }),
        );
        return [
          {
            name: "saved",
            text: `Saved: ${rows.map((b) => b.title).join(", ") || "nothing"}`,
          },
        ];
      },
      tools: (ctx, run) =>
        Mycelium.module({
          id: "example.bookmarks",
          name: "bookmarks",
          operations: {
            add: Mycelium.operation({
              input: Link,
              output: Schema.Struct({}),
              execute: (link) =>
                Promise.resolve(
                  ctx.state.update((tx) => {
                    tx.bookmarks.put({ ...link, sessionId: run.sessionId });
                    tx.observe(run.sessionId);
                    return {};
                  }),
                ),
            }),
          },
        }),
      projection: {
        schema: Schema.Array(Bookmark),
        read: (ctx, sessionId) =>
          ctx.state.bookmarks.peekPage({ index: sessionId, limit: 32 }),
      },
    });
  const host = await memoryHost({
    extensions: [bookmarks],
    script: [
      callTool("bookmarks.add", {
        url: "https://shiitake.shiit.app/",
        title: "Shiitake docs",
      }),
      echoSystemPrompt(),
    ],
  });
  try {
    await host.agent.session("reading").send("Save it", { requestId: "s" });
    const snapshot = await host.settle("reading");
    expect(lastText(snapshot.messages.at(-1)?.value)).toContain(
      "Saved: Shiitake docs",
    );
    expect(snapshot.extensions.bookmarks).toMatchObject({
      status: "running",
      value: [{ title: "Shiitake docs", sessionId: "reading" }],
    });
  } finally {
    await host.close();
  }
});

it("exposes a command to the model as a tool that runs the same handler", async () => {
  const commands = {
    add: { scope: "session", input: Link, output: Bookmark },
  } satisfies CommandsV0;
  const bookmarks = defineExtension("bookmarks")
    .state({
      bookmarks: defineCollection(Bookmark, {
        key: (b) => JSON.stringify([b.sessionId, b.url]),
        index: (b) => b.sessionId,
      }),
    })
    .define({
      commands: {
        specs: commands,
        handlers: (ctx) => ({
          add: ({ sessionId, input }) =>
            Effect.gen(function* () {
              const row = { ...input, sessionId };
              yield* ctx.state.bookmarks.put(row);
              yield* ctx.observe(sessionId);
              return row;
            }),
        }),
      },
      tools: (_ctx, _run, commands) =>
        Mycelium.module({
          id: "example.bookmarks",
          name: "bookmarks",
          operations: {
            add: commands.tool("add", {
              description: "Save a link in this Session.",
            }),
          },
        }),
      projection: {
        schema: Schema.Array(Bookmark),
        read: (ctx, sessionId) =>
          ctx.state.bookmarks.peekPage({ index: sessionId, limit: 32 }),
      },
    });
  const host = await memoryHost({
    extensions: [bookmarks],
    script: [
      callTool("bookmarks.add", { url: "https://a.example/", title: "A" }),
      "Saved.",
    ],
  });
  try {
    const session = host.agent.session("cmd");
    await session.send("Save A", { requestId: "a" });
    await host.settle("cmd");
    await session.extension(bookmarks.manifest.id).call("add", {
      url: "https://b.example/",
      title: "B",
    });
    const snapshot = await session.read();
    expect(snapshot.extensions.bookmarks).toMatchObject({
      status: "running",
      value: [
        { title: "A", sessionId: "cmd" },
        { title: "B", sessionId: "cmd" },
      ],
    });
  } finally {
    await host.close();
  }
});

const Note = Schema.Struct({ id: Schema.String });

it("refuses the model's command tool when the host withholds the commands grant", async () => {
  let handled = 0;
  const notes = defineExtension("notes")
    .state({ notes: defineCollection(Note, { key: (n) => n.id }) })
    .define({
      commands: {
        specs: { add: { scope: "session", input: Note, output: Note } },
        handlers: (ctx) => ({
          add: ({ input }) =>
            Effect.gen(function* () {
              handled += 1;
              yield* ctx.state.notes.put(input);
              return input;
            }),
        }),
      },
      tools: (_ctx, _run, commands) =>
        Mycelium.module({
          id: "example.notes",
          name: "notes",
          operations: { add: commands.tool("add", { description: "Add" }) },
        }),
    });
  const host = await memoryHost({
    extensions: {
      roster: [notes],
      grants: [{ id: notes.manifest.id, capabilities: ["state", "run_tools"] }],
    },
    script: [callTool("notes.add", { id: "n1" }), "Done."],
  });
  try {
    await host.agent.session("n").send("Add", { requestId: "n" });
    await host.settle("n");
    expect(handled).toBe(0);
  } finally {
    await host.close();
  }
});

it("gives state.read a view without writes", async () => {
  const seen: string[][] = [];
  const notes = defineExtension("notes")
    .state({ notes: defineCollection(Note, { key: (n) => n.id }) })
    .define({
      prompt: (ctx) => {
        seen.push(
          ctx.state.read((s) => [
            ...Object.keys(s.notes).sort(),
            String(Object.isFrozen(s) && Object.isFrozen(s.notes)),
          ]),
        );
        return [];
      },
    });
  const host = await memoryHost({ extensions: [notes] });
  try {
    await host.agent.session("n").send("Hi", { requestId: "n" });
    await host.settle("n");
    expect(seen).toEqual([["get", "page", "true"]]);
  } finally {
    await host.close();
  }
});

it("refuses collections that would shadow state methods or the state declaration", async () => {
  const shadow = defineExtension("shadow")
    .state({ read: defineCollection(Note, { key: (n) => n.id }) })
    .define({});
  await expect(memoryHost({ extensions: [shadow] })).rejects.toThrow(
    "Extension collection name is invalid",
  );
  expect(() =>
    defineExtension("ambiguous").state({
      collections: defineCollection(Note, { key: (n) => n.id }),
    }),
  ).toThrow(
    "A collection named collections, version or migrations needs the full state declaration",
  );
});

it("reports a failing scripted call as the guest exception a real sandbox reports", async () => {
  const failing = defineExtension("failing").define({
    tools: () =>
      Mycelium.module({
        id: "example.failing",
        name: "failing",
        operations: {
          run: Mycelium.operation({
            input: Schema.Struct({}),
            execute: () => Promise.reject<null>(new Error("operation refused")),
          }),
        },
      }),
  });
  const host = await memoryHost({
    extensions: [failing],
    script: [callTool("failing.run"), "Recovered."],
  });
  try {
    await host.agent.session("f").send("Run", { requestId: "f" });
    const snapshot = await host.settle("f");
    const result = JSON.stringify(snapshot.messages.at(-2)?.value);
    expect(result).toContain('"isError":true');
    expect(result).not.toContain("unrecognized reason");
  } finally {
    await host.close();
  }
});

it("reports a scripted call to an ungranted operation as a guest error", async () => {
  const host = await memoryHost({
    script: [callTool("missing.run"), "Recovered."],
  });
  try {
    await host.agent.session("m").send("Run", { requestId: "m" });
    const snapshot = await host.settle("m");
    const result = JSON.stringify(snapshot.messages.at(-2)?.value);
    expect(result).toContain("missing.run is not available to this run");
  } finally {
    await host.close();
  }
});
