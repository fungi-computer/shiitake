import { memoryHost } from "@fungi.computer/shiitake/testing";

// A real durable Session with a scripted model: no keys, no timers.
const host = await memoryHost({ script: ["Hello! I'm ready to work."] });
try {
  const session = host.agent.session("first-session");
  const receipt = await session.send("Say hello.", { requestId: "prompt-1" });
  const snapshot = await host.settle("first-session");
  console.log(receipt.status, snapshot.terminals.at(-1)?.outcome);
} finally {
  await host.close();
}
