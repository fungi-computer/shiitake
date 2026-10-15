import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { memoryHost, fauxModelSource } from "@fungi.computer/shiitake/testing";

/** Accept and settle a real durable Session using the public test host. */
export async function firstSession() {
  const host = await memoryHost({
    model: fauxModelSource([fauxAssistantMessage("Ready.")]),
  });
  try {
    const session = host.agent.session("first-session");
    const receipt = await session.send("Say hello.", {
      requestId: "first-session-prompt",
    });
    const snapshot = await host.settle("first-session");
    return { receipt, snapshot };
  } finally {
    await host.close();
  }
}
