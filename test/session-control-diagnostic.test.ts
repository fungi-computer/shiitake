import { fauxModel } from "@fungi.computer/mule/testing";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";

import { Shiitake, type SessionWatch } from "../src/index.js";
import {
  compatibilityRunDependencies,
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

it("publishes a delivered steer's first terminal with its failed parent's safe diagnostic", async () => {
  const directory = mkdtempSync(join(tmpdir(), "control-diagnostic-"));
  const databasePath = join(directory, "agent.sqlite");
  const privateMarker = "synthetic-private-stream-failure";
  const safeReason = "model:rejected: Pi model stream could not start";
  let markStarted: () => void = () => undefined;
  let releaseFirst: () => void = () => undefined;
  const started = new Promise<void>((resolve) => {
    markStarted = resolve;
  });
  const held = new Promise<void>((resolve) => {
    releaseFirst = resolve;
  });
  const faux = fauxModel([]);
  faux.setResponses([
    async () => {
      markStarted();
      await held;
      return fauxAssistantMessage("first answer before steering");
    },
  ]);
  let streamCalls = 0;
  const open = async () => {
    const fixture = createNativeSqliteOwnerFixture(databasePath);
    const runtime = createCompatibilityRuntimeHost();
    const agent = await Shiitake.make({
      ...compatibilityRunDependencies(),
      store: fixture.owner,
      runtime: runtime.runtime,
      model: {
        acquire: () =>
          Promise.resolve({
            model: faux.model,
            retryable: () => false,
            stream: (...args: Parameters<typeof faux.stream>) => {
              streamCalls += 1;
              // The second turn exists only after Mule delivers the steer.
              // Exercise its real untrusted stream-start boundary, not a
              // diagnostic supplied directly to the settlement helper.
              if (streamCalls === 2) throw new Error(privateMarker);
              return faux.stream(...args);
            },
            release: () => Promise.resolve(),
          }),
      },
    });
    return { fixture, runtime, agent };
  };
  let current = await open();
  let running: Promise<void> | undefined;
  let watch: SessionWatch | undefined;
  try {
    const session = current.agent.session("control-diagnostic");
    const parent = await session.send("original request", {
      requestId: "parent",
      delivery: "followUp",
    });
    running = current.runtime.runNext();
    await started;
    const control = await session.send("change direction", {
      requestId: "steer",
      delivery: "steer",
    });
    expect(
      current.fixture.owner.sql
        .exec(
          "SELECT json_extract(data_json, '$.targetWorkId') AS target FROM session_command_admissions WHERE session_id = ? AND work_id = ?",
          "control-diagnostic",
          control.commandId,
        )
        .one().target,
    ).toBe(parent.commandId);
    watch = await session.watch();
    expect(watch.snapshot.terminals).toEqual([]);
    const changes = watch.changes;
    const firstControlTerminal = (async () => {
      for await (const frame of changes) {
        const terminals =
          frame.kind === "update"
            ? frame.terminals
            : frame.kind === "reset"
              ? frame.baseline.terminals
              : [];
        const terminal = terminals.find(
          (value) => value.commandId === control.commandId,
        );
        if (terminal !== undefined) return terminal;
      }
      return undefined;
    })();
    releaseFirst();
    await running;
    const first = await firstControlTerminal;
    const snapshot = await session.read();
    expect(streamCalls).toBe(2);
    expect(snapshot.terminals).toContainEqual(
      expect.objectContaining({
        commandId: parent.commandId,
        outcome: "failed",
        diagnostic: safeReason,
      }),
    );
    // This must fail on the landed predecessor's original null diagnostic,
    // not pass by checking a later replacement terminal.
    expect(first).toMatchObject({
      commandId: control.commandId,
      outcome: "failed",
      diagnostic: safeReason,
    });
    expect(
      snapshot.terminals.filter(
        (value) => value.commandId === control.commandId,
      ),
    ).toEqual([first]);
    expect(
      current.fixture.owner.sql
        .exec("SELECT job_id FROM watchdog_jobs ORDER BY job_id")
        .toArray()
        .map((row) => row.job_id),
    ).toEqual([parent.commandId]);
    expect(
      (
        await session.send("change direction", {
          requestId: "steer",
          delivery: "steer",
        })
      ).commandId,
    ).toBe(control.commandId);
    expect((await session.read()).terminals).toEqual(snapshot.terminals);
    expect(JSON.stringify(snapshot)).not.toContain(privateMarker);
    await watch.close();
    watch = undefined;
    await current.agent.close();
    current.fixture.close();
    current = await open();
    expect(
      (await current.agent.session("control-diagnostic").read()).terminals,
    ).toEqual(snapshot.terminals);
  } finally {
    releaseFirst();
    await running;
    await watch?.close();
    await current.agent.close();
    current.fixture.close();
    rmSync(directory, { recursive: true, force: true });
  }
});
