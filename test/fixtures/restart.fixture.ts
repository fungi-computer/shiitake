import { fork } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect } from "vitest";
import * as Schema from "effect/Schema";
import { SessionSnapshotSchema } from "../../src/client-wire.js";

function start(databasePath: string, mode: string, fixture: string) {
  const child = fork(
    new URL(`./${fixture}.fixture.ts`, import.meta.url).pathname,
    [databasePath, mode],
    { execArgv: ["--import", "tsx"], silent: true },
  );
  let errors = "";
  child.stderr?.on("data", (data: Buffer) => {
    errors += data.toString();
  });
  const exited = once(child, "exit");
  const message = new Promise<unknown>((resolve, reject) => {
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      reject(new Error(`Child checkpoint timed out: ${errors}`));
    }, 15_000);
    child.once("message", (value) => {
      clearTimeout(timer);
      resolve(value);
    });
    child.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.once("exit", (code) => {
      clearTimeout(timer);
      reject(
        new Error(`Child exited ${String(code)} before message: ${errors}`),
      );
    });
  });
  return {
    message,
    exited,
    stop: async () => {
      if (child.exitCode === null && child.signalCode === null)
        child.kill("SIGKILL");
      await exited;
    },
  };
}

export async function killed(
  databasePath: string,
  mode: string,
  checkpoint: string,
  fixture = "restart-process",
) {
  const execution = start(databasePath, mode, fixture);
  try {
    expect(await execution.message).toBe(checkpoint);
  } finally {
    await execution.stop();
  }
}

export async function finished(
  databasePath: string,
  mode = "finish",
  fixture = "restart-process",
) {
  const execution = start(databasePath, mode, fixture);
  try {
    const snapshot = await execution.message;
    expect(await execution.exited).toEqual([0, null]);
    return Schema.decodeUnknownSync(SessionSnapshotSchema)(snapshot);
  } finally {
    await execution.stop();
  }
}

export async function withDatabase(test: (path: string) => Promise<void>) {
  const directory = await mkdtemp(join(tmpdir(), "shiitake-restart-"));
  try {
    await test(join(directory, "agent.sqlite"));
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

export function expectOutcome(
  snapshot: Awaited<ReturnType<typeof finished>>,
  outcome: string,
) {
  expect(snapshot.terminals.map((terminal) => terminal.outcome)).toContain(
    outcome,
  );
}
export function expectVerifiedAnswer(
  snapshot: Awaited<ReturnType<typeof finished>>,
) {
  const text = snapshot.messages.flatMap(({ value }) =>
    value.role === "assistant"
      ? value.content.flatMap((part) =>
          part.type === "text" ? [part.text] : [],
        )
      : [],
  );
  expect(text).toContain("verified recovered context");
}
export function toolResult(
  snapshot: Awaited<ReturnType<typeof finished>>,
  callId: string,
) {
  return snapshot.messages
    .map(({ value }) => value)
    .find(
      (value) => value.role === "toolResult" && value.toolCallId === callId,
    );
}
