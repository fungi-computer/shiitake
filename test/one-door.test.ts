import { readdirSync, readFileSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import * as Effect from "effect/Effect";
import { expect, it } from "vitest";

import * as Schema from "effect/Schema";

import {
  defineExtension,
  ExtensionCommandError,
  ExtensionId,
} from "../src/extensions.js";
import { Shiitake } from "../src/index.js";
import { prepareSessionStore } from "../src/internal/woodstock/index.js";
import {
  readSessionCommandAdmission,
  readSessionCommandAdmissionByRequest,
  type SessionQueueHead,
} from "../src/internal/woodstock/session-store.js";
import { openTestAdmissionDoor } from "./fixtures/admission-door.fixture.js";
import {
  compatibilityRunDependencies,
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

const sourceRoot = join(dirname(fileURLToPath(import.meta.url)), "../src");

function sourceFiles(directory: string): readonly string[] {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) return sourceFiles(path);
    return entry.name.endsWith(".ts") ? [path] : [];
  });
}

function filesMatching(pattern: RegExp): readonly string[] {
  return sourceFiles(sourceRoot)
    .filter((path) => pattern.test(readFileSync(path, "utf8")))
    .map((path) => relative(sourceRoot, path))
    .sort();
}

const decodeAdmitted = Schema.decodeUnknownSync(
  Schema.Struct({ commandId: Schema.String, delivery: Schema.String }),
);

/** A test-only producer whose one Agent command sends through the door. */
const producer = defineExtension({
  id: ExtensionId.make("test-producer"),
  version: "1.0.0",
}).define({
  commands: {
    specs: {
      send: {
        scope: "agent",
        input: Schema.Struct({
          sessionId: Schema.String,
          text: Schema.String,
          requestId: Schema.String,
          correlation: Schema.String,
        }),
        output: Schema.Struct({
          commandId: Schema.String,
          delivery: Schema.Literals(["new", "replay"]),
        }),
      },
    },
    handlers: (ctx) => ({
      send: ({ input }) =>
        ctx
          .send({
            sessionId: input.sessionId,
            when: "now",
            correlation: input.correlation,
            prompt: {
              text: input.text,
              requestId: input.requestId,
              delivery: "followUp",
            },
          })
          .pipe(
            Effect.flatMap((admitted) =>
              admitted._tag === "Accepted"
                ? Effect.succeed({
                    commandId: admitted.commandId,
                    delivery: admitted.delivery,
                  })
                : Effect.fail(
                    new ExtensionCommandError({
                      reason: "conflict",
                      message: "Session was not ready",
                    }),
                  ),
            ),
          ),
    }),
  },
});

async function openAgent() {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const agent = await Shiitake.make({
    ...compatibilityRunDependencies(),
    store: fixture.owner,
    runtime: runtime.runtime,
    extensions: {
      roster: [producer],
      grants: [
        {
          id: ExtensionId.make("test-producer"),
          capabilities: ["send", "commands"],
        },
      ],
    },
  });
  return {
    agent,
    owner: fixture.owner,
    send: (input: {
      sessionId: string;
      text: string;
      requestId: string;
      correlation: string;
    }) => agent.extensions.call("test-producer", "send", input),
    close: async () => {
      await agent.close();
      fixture.close();
    },
  };
}

it("lets only the accepted-command door write accepted commands", () => {
  // The one Woodstock write is called by the door and nothing else.
  expect(filesMatching(/\bacceptSessionCommandOwned\b/u)).toEqual([
    "internal/admission-door.ts",
    "internal/woodstock/session-store.ts",
  ]);
  expect(
    filesMatching(
      /INSERT\s+(?:OR\s+\w+\s+)?INTO\s+session_command_admissions/u,
    ),
  ).toEqual(["internal/woodstock/session-store.ts"]);
  const store = readFileSync(
    join(sourceRoot, "internal/woodstock/session-store.ts"),
    "utf8",
  );
  expect(
    store.match(/INSERT\s+(?:OR\s+\w+\s+)?INTO\s+session_command_admissions/gu),
  ).toHaveLength(1);
  // Producers reach the door, never the Woodstock write or the Watchdog enqueue.
  for (const producerFile of [
    "internal/coordinator.ts",
    "internal/extension-runtime.ts",
  ]) {
    const text = readFileSync(join(sourceRoot, producerFile), "utf8");
    expect(text, producerFile).not.toMatch(
      /enqueueAcceptedJob|EnqueueAcceptedWork|acceptSessionCommandOwned/u,
    );
  }
});

it("records who admitted each command: a user or an extension with its correlation", async () => {
  const opened = await openAgent();
  try {
    const session = opened.agent.session("origin-session");
    const human = await session.command({
      type: "prompt",
      requestId: "origin-human",
      delivery: "followUp",
      text: "A human prompt",
    });
    expect(
      readSessionCommandAdmission(
        opened.owner,
        "origin-session",
        human.commandId,
      )?.origin,
    ).toEqual({ kind: "user" });

    const produced = await opened.send({
      sessionId: "origin-session",
      text: "An extension prompt",
      requestId: "origin-extension",
      correlation: "origin-correlation",
    });
    expect(
      readSessionCommandAdmission(
        opened.owner,
        "origin-session",
        decodeAdmitted(produced).commandId,
      )?.origin,
    ).toEqual({
      kind: "extension",
      extensionId: "test-producer",
      correlation: "origin-correlation",
    });
  } finally {
    await opened.close();
  }
});

it("replays an exact retry and returns a typed conflict for any changed payload", async () => {
  const opened = await openAgent();
  try {
    const session = opened.agent.session("replay-session");
    const request = {
      sessionId: "replay-session",
      text: "  ship it  ",
      requestId: "replay-producer",
      correlation: "replay",
    };
    const first = await opened.send(request);
    // No decoder trims: the stored prompt is the exact text.
    expect(
      readSessionCommandAdmissionByRequest(
        opened.owner,
        "replay-session",
        "replay-producer",
      )?.entry,
    ).toMatchObject({ message: { content: "  ship it  " } });
    await expect(opened.send(request)).resolves.toEqual({
      commandId: decodeAdmitted(first).commandId,
      delivery: "replay",
    });
    await expect(
      opened.send({ ...request, text: "ship it" }),
    ).rejects.toMatchObject({ boundary: "contract", cause: "conflict" });

    await session.command({
      type: "prompt",
      requestId: "replay-delivery",
      delivery: "followUp",
      text: "Same text",
    });
    await expect(
      session.command({
        type: "prompt",
        requestId: "replay-delivery",
        delivery: "steer",
        text: "Same text",
      }),
    ).rejects.toMatchObject({
      _tag: "ShiitakeError",
      boundary: "contract",
      cause: "conflict",
    });
    // The same request id from a different producer is a different command.
    await expect(
      opened.send({
        sessionId: "replay-session",
        text: "Same text",
        requestId: "replay-delivery",
        correlation: "replay",
      }),
    ).rejects.toMatchObject({ boundary: "contract", cause: "conflict" });
  } finally {
    await opened.close();
  }
});

it("binds extension prompts by the same 256 KiB grammar as human prompts", async () => {
  const opened = await openAgent();
  try {
    const session = opened.agent.session("grammar-session");
    const tooLong = "é".repeat(200_000);
    await expect(
      opened.send({
        sessionId: "grammar-session",
        text: tooLong,
        requestId: "grammar-producer",
        correlation: "grammar",
      }),
    ).rejects.toMatchObject({ boundary: "contract", cause: "invalid" });
    await expect(
      session.command({
        type: "prompt",
        requestId: "grammar-human",
        delivery: "followUp",
        text: tooLong,
      }),
    ).rejects.toMatchObject({ boundary: "contract", cause: "invalid" });
    await expect(
      opened.send({
        sessionId: "grammar-session",
        text: "x".repeat(5_000),
        requestId: "grammar-long",
        correlation: "grammar",
      }),
    ).resolves.toMatchObject({ delivery: "new" });
  } finally {
    await opened.close();
  }
});

it("admits an idle-only prompt only once the Session queue is idle", () => {
  const fixture = createNativeSqliteOwnerFixture();
  const enqueued: string[] = [];
  let head: SessionQueueHead = {
    state: "running",
    jobId: "busy-work",
    cancellationRequested: false,
  };
  let wakes = 0;
  try {
    prepareSessionStore(fixture.owner);
    const door = openTestAdmissionDoor(fixture.owner, {
      enqueue: ({ jobId }) => enqueued.push(jobId),
      queueHead: () => head,
      requestWake: Effect.sync(() => {
        wakes += 1;
      }),
    });
    const request = {
      sessionId: "idle-session",
      command: {
        type: "prompt",
        requestId: "idle-continuation",
        delivery: "followUp",
        text: "Continue when idle",
      },
      when: "idle",
      origin: {
        kind: "extension",
        extensionId: "idle-extension",
        correlation: "idle-correlation",
      },
    } as const;

    expect(Effect.runSync(door.admit(request))).toEqual({
      status: "not_ready",
    });
    expect(
      readSessionCommandAdmissionByRequest(
        fixture.owner,
        "idle-session",
        "idle-continuation",
      ),
    ).toBeUndefined();
    expect(enqueued).toEqual([]);
    expect(wakes).toBe(0);

    head = { state: "idle" };
    const accepted = Effect.runSync(door.admit(request));
    expect(accepted).toMatchObject({ status: "accepted", delivery: "new" });
    expect(enqueued).toHaveLength(1);
    expect(wakes).toBe(1);

    // A retry of admitted work replays even while the queue is busy again.
    head = {
      state: "queued",
      jobId: "later-work",
      cancellationRequested: false,
    };
    expect(Effect.runSync(door.admit(request))).toEqual({
      ...accepted,
      delivery: "replay",
    });
    expect(enqueued).toHaveLength(1);
  } finally {
    fixture.close();
  }
});
