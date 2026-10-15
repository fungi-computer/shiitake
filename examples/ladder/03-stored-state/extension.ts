import { Mycelium } from "@fungi.computer/mycelium";
import {
  defineCollection,
  defineExtension,
} from "@fungi.computer/shiitake/extensions";
import * as Schema from "effect/Schema";

const Link = Schema.Struct({ url: Schema.String, title: Schema.String });
const Bookmark = Schema.Struct({ ...Link.fields, sessionId: Schema.String });

/** The model saves links; the next prompt lists what this Session saved. */
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
    tools: (ctx, run) =>
      Mycelium.module({
        id: "example.bookmarks",
        name: "bookmarks",
        operations: {
          add: Mycelium.operation({
            replay: "safe",
            description: "Save a link; saving the same URL replaces it.",
            input: Link,
            execute: (link) =>
              Promise.resolve(
                ctx.state.update((s) => {
                  s.bookmarks.put({ ...link, sessionId: run.sessionId });
                  return null;
                }),
              ),
          }),
        },
      }),
  });
