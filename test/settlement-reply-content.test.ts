import {
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import type { BirdDogMail } from "@fungi.computer/bird-dog";
import { make as makeBirdDog } from "@fungi.computer/bird-dog/effect";
import { fauxModel } from "@fungi.computer/mule/testing";
import * as Effect from "effect/Effect";
import { expect, it } from "vitest";

import {
  defineExtension,
  ExtensionId,
  type ExtensionRosterV0,
} from "../src/extensions.js";
import { openRuntimeCore } from "../src/internal/runtime-core.js";
import {
  completingRunOptions,
  manualExtensionRuntime,
} from "./fixtures/extension-host.fixture.js";
import {
  createNativeSqliteOwnerFixture,
  noExtensions,
} from "./fixtures/native-sqlite-owner.fixture.js";

const parent = {
  kind: "agent",
  agentId: "parent-agent",
  sessionId: "parent-session",
} as const;

async function replyFixture(
  responses: Parameters<typeof fauxModel>[0],
  extensions: ExtensionRosterV0 = noExtensions,
) {
  const fixture = createNativeSqliteOwnerFixture();
  const host = manualExtensionRuntime();
  const faux = fauxModel(responses);
  const posted: BirdDogMail[] = [];
  const room = Effect.runSync(
    makeBirdDog({
      stage: (mail) =>
        Effect.sync(() => {
          posted.push(...mail);
        }),
      now: () => 100,
    }),
  );
  const core = await Effect.runPromise(
    openRuntimeCore(
      {
        ...completingRunOptions(fixture.owner, host.runtime, extensions),
        model: {
          acquire: () =>
            Effect.succeed({
              model: faux.model,
              retryable: () => false,
              stream: faux.stream,
              release: Effect.void,
            }),
        },
      },
      room,
    ),
  );
  return {
    core,
    host,
    posted,
    start: () =>
      Effect.runPromise(
        core.acceptMail(
          {
            key: "delegation",
            to: { kind: "agent", agentId: "child-agent" },
            from: parent,
            replyTo: [parent],
            content: "Do the work",
            createdAt: 42,
          },
          undefined,
          undefined,
          () => undefined,
        ),
      ),
    close: async () => {
      await Effect.runPromise(core.agent.close);
      fixture.close();
    },
  };
}

it("a work whose last message is empty replies with its earlier text", async () => {
  const fixture = await replyFixture([
    fauxAssistantMessage(
      [
        { type: "text", text: "READY: verified the work" },
        fauxToolCall("unavailable", {}),
      ],
      { stopReason: "toolUse" },
    ),
    fauxAssistantMessage(""),
  ]);
  try {
    await fixture.start();
    await fixture.host.drain();
    expect(fixture.posted).toHaveLength(1);
    expect(fixture.posted[0]?.content).toBe("READY: verified the work");
  } finally {
    await fixture.close();
  }
});

const continuerId = ExtensionId.make("reply-content-continuer");
const continuer = defineExtension({ id: continuerId, version: "1.0.0" }).define(
  {
    hooks: {
      onSettled: (ctx, page) =>
        Effect.forEach(
          page,
          (run) => {
            if (run.outcome !== "completed") return Effect.void;
            const correlation =
              run.origin === null
                ? "one"
                : run.origin.correlation === "one"
                  ? "two"
                  : undefined;
            if (correlation === undefined) return Effect.void;
            return ctx
              .send({
                sessionId: run.sessionId,
                requireExistingSession: true,
                when: "idle",
                correlation,
                prompt: {
                  text: "Continue the goal",
                  requestId: `continue-${run.workId}`,
                  delivery: "followUp",
                },
              })
              .pipe(Effect.asVoid);
          },
          { discard: true },
        ),
    },
  },
);
const continuing: ExtensionRosterV0 = {
  roster: [continuer],
  grants: [{ id: continuerId, capabilities: ["settled", "send"] }],
};

it("a goal stretch whose last turn says nothing reports the READY from an earlier turn", async () => {
  const fixture = await replyFixture(
    [
      fauxAssistantMessage("Working"),
      fauxAssistantMessage("READY: goal verified"),
      fauxAssistantMessage(""),
    ],
    continuing,
  );
  try {
    await fixture.start();
    await fixture.host.drain();
    expect(fixture.posted.map(({ content }) => content)).toEqual([
      "Working",
      "READY: goal verified",
    ]);
  } finally {
    await fixture.close();
  }
});

it.each([
  [
    "includes the initiating mail's answer",
    ["READY: initiating work", "", ""],
    "READY: initiating work",
  ],
  [
    "prefers newer text over an earlier READY",
    ["READY: old", "Newer result", " \n "],
    "Newer result",
  ],
  [
    "uses the final turn when it has text",
    ["READY: old", "", "Final result"],
    "Final result",
  ],
  [
    "keeps the no-text sentence for an empty stretch",
    ["", "", ""],
    "The delegated work ended completed without reply text.",
  ],
] as const)("a stretch %s", async (_law, texts, expected) => {
  const fixture = await replyFixture(
    texts.map((text) => fauxAssistantMessage(text)),
    continuing,
  );
  try {
    await fixture.start();
    await fixture.host.drain();
    expect(fixture.posted).toHaveLength(2);
    expect(fixture.posted[1]?.content).toBe(expected);
  } finally {
    await fixture.close();
  }
});

it("a work with no text at all still says 'ended completed without reply text'", async () => {
  const fixture = await replyFixture([fauxAssistantMessage("")]);
  try {
    await fixture.start();
    await fixture.host.drain();
    expect(fixture.posted.map(({ content }) => content)).toEqual([
      "The delegated work ended completed without reply text.",
    ]);
  } finally {
    await fixture.close();
  }
});

it.each(["person", "mail"] as const)(
  "a new %s work starts a fresh stretch",
  async (producer) => {
    const fixture = await replyFixture(
      [
        fauxAssistantMessage("READY: previous stretch"),
        ...Array.from({ length: 5 }, () => fauxAssistantMessage("")),
      ],
      continuing,
    );
    try {
      const created = await fixture.start();
      if (created.sessionId === undefined)
        throw new Error("Missing created Session");
      await fixture.host.drain();
      expect(fixture.posted[1]?.content).toBe("READY: previous stretch");
      if (producer === "person") {
        await Effect.runPromise(
          fixture.core.agent
            .session(created.sessionId)
            .send("New task", { requestId: "new-task" }),
        );
      } else {
        await Effect.runPromise(
          fixture.core.acceptMail({
            key: "new-task",
            to: {
              kind: "agent",
              agentId: "child-agent",
              sessionId: created.sessionId,
            },
            from: parent,
            replyTo: [],
            content: "New task",
            createdAt: 43,
          }),
        );
      }
      await fixture.host.drain();
      expect(fixture.posted).toHaveLength(3);
      expect(fixture.posted[2]?.content).toBe(
        "The delegated work ended completed without reply text.",
      );
    } finally {
      await fixture.close();
    }
  },
);
