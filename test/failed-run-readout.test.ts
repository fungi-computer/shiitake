import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fauxModel } from "@fungi.computer/mule/testing";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { expect, it } from "vitest";
import { Shiitake } from "../src/index.js";
import { readRecentFailedRuns } from "../src/internal/woodstock/session-store.js";
import {
  createNativeSqliteOwnerFixture,
  createCompatibilityRuntimeHost,
  noExtensions,
} from "./fixtures/native-sqlite-owner.fixture.js";

it("reads failed run terminals for support without any prompt, reply or tool content", async () => {
  const directory = mkdtempSync(join(tmpdir(), "failed-run-readout-"));
  const fixture = createNativeSqliteOwnerFixture(
    join(directory, "agent.sqlite"),
  );
  const runtime = createCompatibilityRuntimeHost();
  const faux = fauxModel([]);
  faux.setResponses([
    fauxAssistantMessage("PRIVATE-REPLY-TEXT", {
      stopReason: "error",
      errorMessage: "PRIVATE-PROVIDER-BODY",
    }),
  ]);
  try {
    const agent = await Shiitake.make({
      store: fixture.owner,
      extensions: noExtensions,
      runtime: runtime.runtime,
      instructions: {
        load: () => Promise.resolve({ revision: "v1", text: "Reply." }),
      },
      model: {
        acquire: () =>
          Promise.resolve({
            model: faux.model,
            retryable: () => false,
            stream: faux.stream,
            release: () => Promise.resolve(),
          }),
      },
    });
    await agent
      .session("support")
      .send("PRIVATE-PROMPT-TEXT", { requestId: "support-1" });
    await runtime.runNext();
    const failed = readRecentFailedRuns(fixture.owner, 5);
    expect(failed).toHaveLength(1);
    expect(failed[0]?.sessionId).toBe("support");
    expect(failed[0]?.at).toBeGreaterThan(0);
    const wire = JSON.stringify(failed);
    for (const secret of [
      "PRIVATE-PROMPT-TEXT",
      "PRIVATE-REPLY-TEXT",
      "PRIVATE-PROVIDER-BODY",
    ])
      expect(wire).not.toContain(secret);
    expect(Object.keys(failed[0] ?? {}).sort()).toEqual([
      "at",
      "diagnostic",
      "sessionId",
      "workId",
    ]);
    expect(readRecentFailedRuns(fixture.owner, 0)).toHaveLength(1);
    await agent.close();
  } finally {
    fixture.close();
    rmSync(directory, { recursive: true, force: true });
  }
}, 20_000);
