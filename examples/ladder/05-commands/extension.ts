import { Mycelium } from "@fungi.computer/mycelium";
import {
  defineCollection,
  defineExtension,
} from "@fungi.computer/shiitake/extensions";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

const Link = Schema.Struct({ url: Schema.String, title: Schema.String });
const Bookmark = Schema.Struct({ ...Link.fields, sessionId: Schema.String });

/** People and the model save links through one command. */
export const bookmarks = defineExtension("bookmarks")
  .state({
    bookmarks: defineCollection(Bookmark, {
      key: (b) => JSON.stringify([b.sessionId, b.url]),
      index: (b) => b.sessionId,
    }),
  })
  .define({
    prompt: (ctx, run) => {
      const saved = ctx.state.read((s) =>
        s.bookmarks.page({ index: run.sessionId, limit: 32 }),
      );
      const titles = saved.map((b) => b.title).join(", ");
      return [{ name: "saved", text: `Saved links: ${titles || "none"}` }];
    },
    commands: {
      specs: { add: { scope: "session", input: Link, output: Bookmark } },
      handlers: (ctx) => ({
        add: ({ sessionId, input }) =>
          Effect.gen(function* () {
            const bookmark = { ...input, sessionId };
            yield* ctx.state.bookmarks.put(bookmark);
            yield* ctx.observe(sessionId);
            return bookmark;
          }),
      }),
    },
    tools: (_ctx, _run, commands) =>
      Mycelium.module({
        id: "example.bookmarks",
        name: "bookmarks",
        operations: {
          add: commands.tool("add", {
            description: "Save a link; saving the same URL replaces it.",
          }),
        },
      }),
    projection: {
      schema: Schema.Array(Bookmark),
      read: (ctx, sessionId) =>
        ctx.state.bookmarks.peekPage({ index: sessionId, limit: 32 }),
    },
  });
