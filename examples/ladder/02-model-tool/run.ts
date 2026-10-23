import { callTool, memoryHost } from "@fungi.computer/shiitake/testing";
import { bookmarks } from "./extension.js";

/** A scripted model calls bookmarks.check, then replies. */
export async function modelToolRun() {
  const host = await memoryHost({
    extensions: [bookmarks],
    script: [
      callTool("bookmarks.check", {
        url: "https://docs.fungi.computer/shiitake/",
      }),
      "That link is fine.",
    ],
  });
  try {
    await host.agent
      .session("links")
      .send("Is this link OK?", { requestId: "check" });
    return await host.settle("links");
  } finally {
    await host.close();
  }
}
