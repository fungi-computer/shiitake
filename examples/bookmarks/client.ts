import { defineClientEntry } from "@fungi.computer/shiitake/client-entry";
import { bookmarkContract } from "./contract.js";

/** Panel rows carry at most 1,024 characters of detail. */
const shortened = (url: string) =>
  url.length <= 1_024 ? url : `${url.slice(0, 1_023)}…`;

/** One panel row per saved link, from the decoded projection only. */
export default defineClientEntry(bookmarkContract, {
  contribute: (view) => ({
    chips: [],
    panel:
      view.status === "running" && view.value !== null
        ? view.value.slice(0, 16).map((bookmark, index) => ({
            id: `bookmark-${String(index)}`,
            title: bookmark.title,
            detail: shortened(bookmark.url),
          }))
        : [],
    timeline: { cells: [], omitted: 0 },
    waiting: null,
  }),
});
