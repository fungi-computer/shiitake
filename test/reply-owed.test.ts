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
  const requests: Parameters<typeof faux.stream>[1][] = [];
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
              stream: (model, context, options) => {
                requests.push({ ...context, messages: [...context.messages] });
                return faux.stream(model, context, options);
              },
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
    requests,
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

it("asks a textless delegated work for its reply once, in the same run", async () => {
  const fixture = await replyFixture([
    fauxAssistantMessage([fauxToolCall("unavailable", {})], {
      stopReason: "toolUse",
    }),
    fauxAssistantMessage(""),
    fauxAssistantMessage("READY: verified the work"),
  ]);
  try {
    const accepted = await fixture.start();
    if (accepted.sessionId === undefined) throw new Error("Missing Session");
    await fixture.host.drain();
    const snapshot = await Effect.runPromise(
      fixture.core.agent.session(accepted.sessionId).read(),
    );
    expect(fixture.requests).toHaveLength(3);
    expect(fixture.posted.map(({ content }) => content)).toEqual([
      "READY: verified the work",
    ]);
    expect(fixture.requests[2]?.messages.at(-1)).toMatchObject({
      role: "user",
      content:
        "Your turn ended without a reply to parent-agent. Reply now with your result in at most 5 lines.",
    });
    expect(snapshot.terminals).toHaveLength(1);
    expect(snapshot.terminals[0]?.outcome).toBe("completed");
    expect(
      snapshot.messages.some(
        ({ value }) =>
          value.role === "assistant" &&
          value.content.some(
            (part) =>
              part.type === "text" && part.text === "READY: verified the work",
          ),
      ),
    ).toBe(true);
  } finally {
    await fixture.close();
  }
});

it("ends after a second empty answer without asking again", async () => {
  const fixture = await replyFixture([
    fauxAssistantMessage(""),
    fauxAssistantMessage(" \n "),
    fauxAssistantMessage("Must not be requested"),
  ]);
  try {
    await fixture.start();
    await fixture.host.drain();
    expect(fixture.requests).toHaveLength(2);
    expect(fixture.posted.map(({ content }) => content)).toEqual([
      "The delegated work ended completed without reply text.",
    ]);
  } finally {
    await fixture.close();
  }
});

it.each(["undelegated", "person in a delegated Session"] as const)(
  "does not nudge %s work",
  async (producer) => {
    const delegated = producer === "person in a delegated Session";
    const fixture = await replyFixture([
      ...(delegated ? [fauxAssistantMessage("Previous result")] : []),
      fauxAssistantMessage(""),
      fauxAssistantMessage("Must not be requested"),
    ]);
    try {
      let sessionId = "person-session";
      if (delegated) {
        const accepted = await fixture.start();
        if (accepted.sessionId === undefined)
          throw new Error("Missing Session");
        sessionId = accepted.sessionId;
        await fixture.host.drain();
      }
      await Effect.runPromise(
        fixture.core.agent
          .session(sessionId)
          .send("A person asks", { requestId: "person-work" }),
      );
      await fixture.host.drain();
      expect(fixture.requests).toHaveLength(delegated ? 2 : 1);
    } finally {
      await fixture.close();
    }
  },
);

it("keeps earlier text from this work instead of asking again", async () => {
  const fixture = await replyFixture([
    fauxAssistantMessage(
      [
        { type: "text", text: "READY: already answered" },
        fauxToolCall("unavailable", {}),
      ],
      { stopReason: "toolUse" },
    ),
    fauxAssistantMessage(""),
    fauxAssistantMessage("Must not be requested"),
  ]);
  try {
    await fixture.start();
    await fixture.host.drain();
    expect(fixture.requests).toHaveLength(2);
    expect(fixture.posted.map(({ content }) => content)).toEqual([
      "READY: already answered",
    ]);
  } finally {
    await fixture.close();
  }
});

it("does not nudge a failed work", async () => {
  const fixture = await replyFixture([
    fauxAssistantMessage("", {
      stopReason: "error",
      errorMessage: "Provider unavailable",
    }),
    fauxAssistantMessage("Must not be requested"),
  ]);
  try {
    const accepted = await fixture.start();
    if (accepted.sessionId === undefined) throw new Error("Missing Session");
    await fixture.host.drain();
    const snapshot = await Effect.runPromise(
      fixture.core.agent.session(accepted.sessionId).read(),
    );
    expect(fixture.requests).toHaveLength(1);
    expect(snapshot.terminals.at(-1)?.outcome).toBe("failed");
  } finally {
    await fixture.close();
  }
});

const continuerId = ExtensionId.make("reply-owed-continuer");
const continuer = defineExtension({ id: continuerId, version: "1.0.0" }).define(
  {
    hooks: {
      onSettled: (ctx, page) =>
        Effect.forEach(
          page,
          (run) =>
            run.outcome === "completed" && run.origin === null
              ? ctx
                  .send({
                    sessionId: run.sessionId,
                    requireExistingSession: true,
                    when: "idle",
                    correlation: "continuation",
                    prompt: {
                      text: "Continue",
                      requestId: `continue-${run.workId}`,
                      delivery: "followUp",
                    },
                  })
                  .pipe(Effect.asVoid)
              : Effect.void,
          { discard: true },
        ),
    },
  },
);
const continuing: ExtensionRosterV0 = {
  roster: [continuer],
  grants: [{ id: continuerId, capabilities: ["settled", "send"] }],
};

it("asks an empty extension continuation to reply to its recorded parent", async () => {
  const fixture = await replyFixture(
    [
      fauxAssistantMessage("Initial result"),
      fauxAssistantMessage(""),
      fauxAssistantMessage("READY: continued work"),
    ],
    continuing,
  );
  try {
    await fixture.start();
    await fixture.host.drain();
    expect(fixture.requests).toHaveLength(3);
    expect(fixture.requests[2]?.messages.at(-1)).toMatchObject({
      role: "user",
      content:
        "Your turn ended without a reply to parent-agent. Reply now with your result in at most 5 lines.",
    });
    expect(fixture.posted.map(({ content }) => content)).toEqual([
      "Initial result",
      "READY: continued work",
    ]);
  } finally {
    await fixture.close();
  }
});
