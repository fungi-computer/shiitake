import { Mycelium } from "@fungi.computer/mycelium";
import { defineExtension } from "@fungi.computer/shiitake/extensions";
import * as Schema from "effect/Schema";

const isWebLink = (url: string) =>
  URL.canParse(url) && ["http:", "https:"].includes(new URL(url).protocol);

/** One stateless tool: the model checks a link before it shares it. */
export const bookmarks = defineExtension("bookmarks").define({
  prompt: () => [
    { name: "guide", text: "Check a link with bookmarks.check first." },
  ],
  tools: () =>
    Mycelium.module({
      id: "example.bookmarks",
      name: "bookmarks",
      operations: {
        check: Mycelium.operation({
          replay: "safe",
          description: "Check that a link is an http(s) URL; return its host.",
          input: Schema.Struct({ url: Schema.String }),
          output: Schema.Struct({ ok: Schema.Boolean, host: Schema.String }),
          execute: ({ url }) =>
            Promise.resolve(
              isWebLink(url)
                ? { ok: true, host: new URL(url).host }
                : { ok: false, host: "" },
            ),
        }),
      },
    }),
});
