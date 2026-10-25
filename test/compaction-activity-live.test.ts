import { fauxModel } from "@fungi.computer/mule/testing";
// Confirms the live `SessionActivity.compaction` fact is set for the
// durable commit's whole window and cleared once it settles, whichever way
// it ends: Pi's faux provider plus a compaction handoff that reads the fact
// mid-commit, following the manual-compaction scenario in
// compaction-real-caller.test.ts.
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { expect, it } from "vitest";

import { defineExtension, ExtensionId } from "../src/extensions.js";
import { Shiitake, type BorrowedCompactionViewV0 } from "../src/index.js";
import { readProcessCompaction } from "../src/internal/woodstock/compaction-activity.js";
import {
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

const sessionId = "compaction-activity-live";

function compactionExtension(
  consume: (view: BorrowedCompactionViewV0) => Promise<{
    readonly type: "discarded_by_policy";
  }>,
) {
  return defineExtension({
    id: ExtensionId.make("compaction-activity-live-handoff"),
    version: "1.0.0",
  }).define({ compaction: () => ({ consume }) });
}

it("shows a running manual compaction live and clears it once committed", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const faux = fauxModel([], {
    models: [{ id: "faux-1", contextWindow: 1_000_000, maxTokens: 100_000 }],
    tokenSize: { min: 100, max: 100 },
  });
  const seenDuringHandoff: unknown[] = [];
  const extension = compactionExtension((view) => {
    void view;
    seenDuringHandoff.push(readProcessCompaction(fixture.owner, sessionId));
    return Promise.resolve({ type: "discarded_by_policy" as const });
  });
  const runtime = createCompatibilityRuntimeHost();
  const agent = await Shiitake.make({
    store: fixture.owner,
    runtime: runtime.runtime,
    extensions: {
      roster: [extension],
      grants: [
        { id: extension.manifest.id, capabilities: ["compaction_handoff"] },
      ],
    },
    instructions: {
      load: () =>
        Promise.resolve({ revision: "activity-live-v1", text: "Answer." }),
    },
    model: {
      acquire: () => {
        const model = faux.model;
        return Promise.resolve({
          model,
          retryable: () => false,
          stream: (model, context, options) =>
            faux.stream(model, context, options),
          release: () => Promise.resolve(),
        });
      },
    },
  });
  try {
    const session = agent.session(sessionId);

    faux.setResponses([fauxAssistantMessage("x".repeat(100_000))]);
    await session.command({
      requestId: "activity-live-history",
      delivery: "followUp",
      type: "prompt",
      text: "Build history.",
    });
    await runtime.runNext();

    expect(readProcessCompaction(fixture.owner, sessionId)).toBeNull();
    expect((await session.read()).activity.compaction).toBeNull();

    faux.setResponses([fauxAssistantMessage("manual summary")]);
    await session.command({
      type: "compact",
      requestId: "activity-live-manual",
    });
    for (
      let pass = 0;
      pass < 10 && runtime.snapshot().state === "pending";
      pass++
    )
      await runtime.runNext();

    // The handoff ran inside the commit's window, while the fact was live.
    expect(seenDuringHandoff).toEqual([{ trigger: "manual" }]);
    // The commit has settled: the live fact is cleared again.
    expect(readProcessCompaction(fixture.owner, sessionId)).toBeNull();
    expect((await session.read()).activity.compaction).toBeNull();
  } finally {
    await agent.close();
    fixture.close();
  }
});
