import { callTool, memoryHost } from "@fungi.computer/shiitake/testing";
import { bookmarks } from "./extension.js";

/** The model saves one link through the tool; a person saves another. */
export async function commandsRun() {
  const host = await memoryHost({
    extensions: [bookmarks],
    script: [
      callTool("bookmarks.add", {
        url: "https://shiitake.shiit.app/",
        title: "Shiitake docs",
      }),
      "Saved.",
    ],
  });
  try {
    const session = host.agent.session("reading");
    await session.send("Save the Shiitake docs.", { requestId: "save" });
    await host.settle("reading");
    const added = await session.extension(bookmarks.manifest.id).call("add", {
      url: "https://fungi.computer/",
      title: "Fungi",
    });
    return { added, snapshot: await session.read() };
  } finally {
    await host.close();
  }
}
