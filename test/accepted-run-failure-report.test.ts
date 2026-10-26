import { inspect } from "node:util";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fauxModel } from "@fungi.computer/mule/testing";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import { afterEach, expect, it, vi } from "vitest";
import { make } from "../src/effect.js";
import { ShiitakeError } from "../src/errors.js";
import { Shiitake } from "../src/index.js";
import { waitForAgentSettlement } from "../src/agent-control.js";
import { manualExtensionRuntime } from "./fixtures/extension-host.fixture.js";
import {
  createNativeSqliteOwnerFixture,
  createCompatibilityRuntimeHost,
  noExtensions,
} from "./fixtures/native-sqlite-owner.fixture.js";

afterEach(() => vi.restoreAllMocks());

it.each([
  undefined,
  "synthetic-returned-private-provider-body",
  "x".repeat(220),
])(
  "settles a returned failure once, without trusting provider text, through control reads and reopen (%s)",
  async (errorMessage) => {
    const directory = mkdtempSync(join(tmpdir(), "failed-run-diagnostic-"));
    const databasePath = join(directory, "agent.sqlite");
    let announceRelease: (() => void) | undefined;
    let allowRelease: (() => void) | undefined;
    const releaseStarted = new Promise<void>((resolve) => {
      announceRelease = resolve;
    });
    const releaseAllowed = new Promise<void>((resolve) => {
      allowRelease = resolve;
    });
    const faux = fauxModel([]);
    // Pi can end with an error message that supplies no errorMessage.
    const response = fauxAssistantMessage("", { stopReason: "error" });
    if (errorMessage !== undefined) response.errorMessage = errorMessage;
    faux.setResponses([response]);
    const open = async () => {
      const fixture = createNativeSqliteOwnerFixture(databasePath);
      const runtime = createCompatibilityRuntimeHost();
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
              release: () => {
                announceRelease?.();
                return releaseAllowed;
              },
            }),
        },
      });
      return { fixture, runtime, agent };
    };
    let current = await open();
    let running: Promise<void> | undefined;
    try {
      const session = current.agent.session("returned-failure");
      const accepted = await session.send("fixture", {
        requestId: "returned-failure",
      });
      const firstWait = waitForAgentSettlement(
        session,
        accepted.commandId,
        1000,
        new AbortController().signal,
      ).then(({ terminal }) => terminal);
      running = current.runtime.runNext();
      // Hold actual acquired-model cleanup open: Mule has finished, but
      // Watchdog settlement cannot yet add or repair a terminal diagnostic.
      await releaseStarted;
      const early = await firstWait;
      expect(early).toMatchObject({ outcome: "failed" });
      expect(early.diagnostic).toContain("reason_unavailable");
      expect(early.diagnostic).toContain("run=mule_run_");
      expect(early.diagnostic).toContain("provider=faux");
      expect(early.diagnostic).toContain(`model=${faux.model.id}`);
      expect(early.diagnostic).not.toContain(
        "synthetic-returned-private-provider-body",
      );
      allowRelease?.();
      await running;
      const terminals = (await session.read()).terminals.filter(
        (terminal) => terminal.commandId === accepted.commandId,
      );
      const terminal = terminals[0];
      expect(terminal).toMatchObject({
        commandId: accepted.commandId,
        outcome: "failed",
      });
      expect(terminal?.diagnostic).toContain("reason_unavailable");
      expect(terminal?.diagnostic).toContain("run=mule_run_");
      expect(terminal?.diagnostic).toContain("provider=faux");
      expect(terminal?.diagnostic).toContain(`model=${faux.model.id}`);
      expect(terminal?.diagnostic).not.toContain(
        "synthetic-returned-private-provider-body",
      );
      expect(
        current.fixture.owner.sql
          .exec(
            "SELECT failure_reason FROM watchdog_jobs WHERE job_id = ?",
            accepted.commandId,
          )
          .one().failure_reason,
      ).toBe(terminal?.diagnostic);
      expect(terminals).toHaveLength(1);
      expect(await firstWait).toMatchObject({
        diagnostic: terminal?.diagnostic,
      });
      expect(
        await waitForAgentSettlement(
          session,
          accepted.commandId,
          1000,
          new AbortController().signal,
        ).then(({ terminal }) => terminal),
      ).toMatchObject({ diagnostic: terminal?.diagnostic });
      await current.agent.close();
      current.fixture.close();
      current = await open();
      expect(
        (
          await current.agent.session("returned-failure").read()
        ).terminals.filter((value) => value.commandId === accepted.commandId),
      ).toEqual([terminal]);
      const settled = await waitForAgentSettlement(
        current.agent.session("returned-failure"),
        accepted.commandId,
        1000,
        new AbortController().signal,
      ).then(({ terminal }) => terminal);
      expect(settled).toMatchObject({ diagnostic: terminal?.diagnostic });
    } finally {
      allowRelease?.();
      await running;
      await current.agent.close();
      current.fixture.close();
      rmSync(directory, { recursive: true, force: true });
    }
  },
);

it("keeps Mule's safe model-phase reason after its failed run terminal", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const faux = fauxModel([]);
  const privateMarker = "synthetic-private-provider-body";
  const agent = await Shiitake.make({
    store: fixture.owner,
    runtime: runtime.runtime,
    extensions: noExtensions,
    instructions: {
      load: () => Promise.resolve({ revision: "v1", text: "Reply." }),
    },
    model: {
      acquire: () =>
        Promise.resolve({
          model: faux.model,
          retryable: () => false,
          stream: () => {
            throw new Error(privateMarker);
          },
          release: () => Promise.resolve(),
        }),
    },
  });
  try {
    const session = agent.session("stream-failure");
    const accepted = await session.send("fixture", {
      requestId: "stream-failure",
    });
    const firstWait = waitForAgentSettlement(
      session,
      accepted.commandId,
      1000,
      new AbortController().signal,
    ).then(({ terminal }) => terminal);
    await runtime.runNext();
    const settled = await waitForAgentSettlement(
      session,
      accepted.commandId,
      1000,
      new AbortController().signal,
    ).then(({ terminal }) => terminal);
    expect(settled).toMatchObject({
      outcome: "failed",
      diagnostic: "model:rejected: Pi model stream could not start",
    });
    expect(await firstWait).toMatchObject({
      diagnostic: "model:rejected: Pi model stream could not start",
    });
    expect((await session.read()).terminals).toHaveLength(1);
    expect(JSON.stringify(await session.read())).not.toContain(privateMarker);
  } finally {
    await agent.close();
    fixture.close();
  }
});

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
