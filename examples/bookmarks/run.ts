import type { ExtensionRosterV0 } from "@fungi.computer/shiitake/extensions";
import { callTool, memoryHost } from "@fungi.computer/shiitake/testing";
import client from "./client.js";
import { bookmarks } from "./extension.js";
import { bookmarkRoster } from "./roster.js";

/** The model saves a link, a person saves another, the client draws both. */
export async function bookmarkRoundTrip(
  extensions: ExtensionRosterV0 = bookmarkRoster,
) {
  const host = await memoryHost({
    extensions,
    script: [
      callTool("bookmarks.add", {
        url: "https://docs.fungi.computer/shiitake/",
        title: "Shiitake docs",
      }),
      "Saved.",
    ],
  });
  try {
    const session = host.agent.session("reading");
    await session.send("Save the Shiitake docs.", { requestId: "save" });
    await host.settle("reading");
    await session.extension(bookmarks.manifest.id).call("add", {
      url: "https://fungi.computer/",
      title: "Fungi",
    });
    const snapshot = await session.read();
    return client.contribute?.(snapshot.extensions.bookmarks).panel ?? [];
  } finally {
    await host.close();
  }
}
