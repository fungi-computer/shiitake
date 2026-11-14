import { appendFileSync } from "node:fs";
import { fauxModel } from "@fungi.computer/mule/testing";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { Shiitake } from "../../src/index.js";
import {
  compatibilityRunDependencies,
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./native-sqlite-owner.fixture.js";

// A child Agent process that streams a long reply slowly. `crash-partial`
// reports once the reply text reached SQLite (the parent then SIGKILLs it);
// `finish-partial` reopens, answers briefly, and reports what the restarted
// turn could read of the lost reply before its own model call returned.
const [databasePath, mode] = process.argv.slice(2);
if (databasePath === undefined) throw new Error("Database path required");
const fixture = createNativeSqliteOwnerFixture(databasePath);
const longReply = "the first answer streams ".repeat(40);

const faux = fauxModel([], {
  tokenSize: { min: 4, max: 4 },
  tokensPerSecond: 200,
});
let seenAtRestart: string | null = null;
faux.setResponses(
  mode === "crash-partial"
    ? [fauxAssistantMessage(longReply)]
    : [
        async () => {
          appendFileSync(databasePath + ".requests", "request\n");
          const progress = (await agent.session("restart").read()).progress;
          seenAtRestart = progress === null ? null : progress.text;
          return fauxAssistantMessage("the restarted answer");
        },
      ],
);
const runtime = createCompatibilityRuntimeHost();
const dependencies = compatibilityRunDependencies();
const agent = await Shiitake.make({
  ...dependencies,
  store: fixture.owner,
  runtime: runtime.runtime,
  model: {
    acquire: async () => ({
      ...(await dependencies.model.acquire()),
      model: faux.model,
      retryable: () => false,
      stream: faux.stream,
    }),
  },
});
if (mode === "crash-partial") {
  await agent.session("restart").command({
    type: "prompt",
    delivery: "followUp",
    requestId: "restart-command",
    text: "Answer at length",
  });
  const running = runtime.runNext();
  // Report as soon as the streaming placeholder holds reply text.
  for (;;) {
    const flushed = fixture.owner.sql
      .exec(
        "SELECT instr(data_json, 'streams the first') AS at FROM session_entries WHERE state = 'streaming' AND type = 'message_start'",
      )
      .toArray()
      .some(({ at }) => at !== 0);
    if (flushed) {
      process.send?.("partial-flushed");
      await new Promise<never>(() => undefined);
    }
    await Promise.race([
      running,
      new Promise((resolve) => setTimeout(resolve, 10)),
    ]);
  }
}
await runtime.runNext();
const snapshot = await agent.session("restart").read();
await agent.close();
fixture.close();
process.send?.({ snapshot, seenAtRestart });
process.disconnect();
