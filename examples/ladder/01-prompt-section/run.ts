import { echoSystemPrompt, memoryHost } from "@fungi.computer/shiitake/testing";
import { houseStyle } from "./extension.js";

/** Send one prompt to a stand-in model that echoes its system prompt. */
export async function promptSectionRun() {
  const host = await memoryHost({
    extensions: [houseStyle],
    script: [echoSystemPrompt()],
  });
  try {
    await host.agent.session("style").send("Hi", { requestId: "hi" });
    return await host.settle("style");
  } finally {
    await host.close();
  }
}
