import { callTool, memoryHost } from "@fungi.computer/shiitake/testing";
import { bookmarks } from "./extension.js";

/** The model saves a link; read the Session as a client would. */
export async function projectionRun() {
  const host = await memoryHost({
    extensions: [bookmarks],
    script: [
      callTool("bookmarks.add", {
        url: "https://docs.fungi.computer/shiitake/",
        title: "Shiitake docs",
      }),
      "Saved.",
    ],
  });
  try {
    await host.agent
      .session("reading")
      .send("Save the Shiitake docs.", { requestId: "save" });
    return await host.settle("reading");
  } finally {
    await host.close();
  }
}
