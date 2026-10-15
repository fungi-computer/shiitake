import {
  ExtensionId,
  type CommandsV0,
} from "@fungi.computer/shiitake/extensions";
import * as Schema from "effect/Schema";

const Link = Schema.Struct({ url: Schema.String, title: Schema.String });
export const Bookmark = Schema.Struct({
  ...Link.fields,
  sessionId: Schema.String,
});
const commands = {
  add: { scope: "session", input: Link, output: Bookmark },
} satisfies CommandsV0;

/** What a client may know about Bookmarks. It imports no host code. */
export const bookmarkContract = {
  id: ExtensionId.make("bookmarks"),
  commands,
  projection: { schema: Schema.Array(Bookmark) },
};
