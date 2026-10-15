import type { ExtensionRosterV0 } from "@fungi.computer/shiitake/extensions";
import { memoryHost } from "@fungi.computer/shiitake/testing";
import { bookmarks } from "./extension.js";

/** A host that lets clients read bookmarks but nobody save them. */
export const readOnlyRoster: ExtensionRosterV0 = {
  roster: [bookmarks],
  grants: [
    {
      id: bookmarks.manifest.id,
      capabilities: ["state", "prompt", "projection"],
    },
  ],
};

/** A person tries to save a link on the read-only host. */
export async function readOnlySave() {
  const host = await memoryHost({ extensions: readOnlyRoster });
  try {
    const session = host.agent.session("reading");
    await session.send("Start.", { requestId: "start" });
    await host.settle("reading");
    return await session.extension(bookmarks.manifest.id).call("add", {
      url: "https://fungi.computer/",
      title: "Fungi",
    });
  } finally {
    await host.close();
  }
}
