import { inspect } from "node:util";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import { afterEach, expect, it, vi } from "vitest";
import { make } from "../src/effect.js";
import { ShiitakeError } from "../src/errors.js";
import { manualExtensionRuntime } from "./fixtures/extension-host.fixture.js";
import {
  createNativeSqliteOwnerFixture,
  noExtensions,
} from "./fixtures/native-sqlite-owner.fixture.js";

afterEach(() => vi.restoreAllMocks());

it("reports a failed accepted run with its redacted original model cause", async () => {
  const marker = "synthetic-model-secret-marker";
  const original = new Error(`Model I/O failed ${marker}`, {
    cause: new Error(`Nested failure ${marker}`),
  });
  const failure = new ShiitakeError(
    "model",
    "unavailable",
    "Model unavailable",
    undefined,
    original,
  );
  const report = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = manualExtensionRuntime();
  try {
    const agent = await Effect.runPromise(
      make({
        store: fixture.owner,
        extensions: noExtensions,
        runtime: runtime.runtime,
        instructions: {
          load: () => Effect.succeed({ revision: "v1", text: "Reply." }),
        },
        model: { acquire: () => Effect.fail(failure) },
      }),
    );
    try {
      const session = agent.session("failure-report");
      await Effect.runPromise(
        session.send("fixture", { requestId: "report-failure" }),
      );
      await runtime.drain();
      const retained = report.mock.calls
        .flatMap((args: unknown[]) => args)
        .filter(Redacted.isRedacted)
        .map(Redacted.value);
      expect(retained).toContain(failure);
      expect(retained.find((value) => value === failure)).toBe(failure);
      expect(JSON.stringify(report.mock.calls)).toContain(
        "shiitake_run_failed",
      );
      expect(JSON.stringify(report.mock.calls)).toContain("model:unavailable");
      expect(JSON.stringify(report.mock.calls)).not.toContain(marker);
      expect(inspect(report.mock.calls, { depth: null })).not.toContain(marker);
      expect(
        (await Effect.runPromise(session.read())).terminals.at(-1)?.outcome,
      ).toBe("failed");
    } finally {
      await Effect.runPromise(agent.close);
    }
  } finally {
    fixture.close();
  }
});
