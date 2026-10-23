import {
  callTool,
  echoSystemPrompt,
  memoryHost,
} from "@fungi.computer/shiitake/testing";
import { bookmarks } from "./extension.js";

/** The model saves a link; its next request shows the saved list. */
export async function storedStateRun() {
  const host = await memoryHost({
    extensions: [bookmarks],
    script: [
      callTool("bookmarks.add", {
        url: "https://docs.fungi.computer/shiitake/",
        title: "Shiitake docs",
      }),
      echoSystemPrompt(),
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
