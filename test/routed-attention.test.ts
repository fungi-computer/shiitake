import { createMemoryOutbox } from "@fungi.computer/carrier/testing";
import { fauxModel } from "@fungi.computer/mule/testing";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import type { BirdDogMail } from "@fungi.computer/bird-dog";
import { make as makeBirdDog } from "@fungi.computer/bird-dog/effect";
import { Mycelium } from "@fungi.computer/mycelium";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { expect, it } from "vitest";

import type { ShiitakeEffectOptions } from "../src/effect.js";
import {
  AttentionRecipientSchema,
  defineExtension,
  ExtensionId,
} from "../src/extensions.js";
import { openRuntimeCore } from "../src/internal/runtime-core.js";
import { readSessionInboxPage } from "../src/internal/woodstock/index.js";
import { grantAll } from "./fixtures/fixture-extensions.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

const parent = {
  kind: "agent",
  agentId: "agent-shiitake",
  sessionId: "session-shiitake",
} as const;
const child = {
  kind: "agent",
  agentId: "agent-scout",
  sessionId: "session-scout",
} as const;
const sender = {
  agentId: parent.agentId,
  sessionId: parent.sessionId,
  name: "Shiitake",
};

const probeId = ExtensionId.make("routed-attention-probe");
const probe = defineExtension({ id: probeId, version: "1.0.0" }).define({
  attention: true,
  commands: {
    specs: {
      /** Raise one item for a run, waiting on whoever sent that run. */
      raise: {
        scope: "session",
        input: Schema.Struct({ workId: Schema.String, itemId: Schema.String }),
        output: Schema.NullOr(AttentionRecipientSchema),
      },
    },
    handlers: (ctx) => ({
      raise: ({ sessionId, input }) =>
        Effect.gen(function* () {
          const waitingOn = yield* ctx.workSender({
            sessionId,
            workId: input.workId,
          });
          const item = {
            id: input.itemId,
            kind: "question",
            summary: "Which branch?",
            since: 1,
          };
          yield* ctx.attention.set(
            sessionId,
            waitingOn === null ? item : { ...item, waitingOn },
          );
          return waitingOn;
        }),
    }),
  },
  // Reading a run's sender is a run-tools grant, like the question tool's.
  tools: () =>
    Mycelium.module({
      id: "routed.probe",
      name: "probe",
      description: "Holds the run_tools grant.",
      operations: {},
    }),
});

function manualRuntime() {
  let reconcile: Effect.Effect<void, Error> | undefined;
  const runtime: ShiitakeEffectOptions["runtime"] = {
    bind: (input) =>
      Effect.sync(() => {
        reconcile = input.reconcile;
        return {
          commit: (mutation) => mutation,
          requestWake: Effect.void,
          close: Effect.void,
        };
      }),
  };
  return {
    runtime,
    reconcile: () => {
      if (reconcile === undefined) throw new Error("Runtime is unavailable");
      return Effect.runPromise(reconcile);
    },
  };
}

async function openChild(
  replies: readonly string[] = ["Asked; waiting on the answer."],
) {
  const fixture = createNativeSqliteOwnerFixture();
  const faux = fauxModel([]);
  faux.setResponses(replies.map((text) => fauxAssistantMessage(text)));
  const posted: BirdDogMail[] = [];
  const rows = createMemoryOutbox();
  const room = Effect.runSync(
    makeBirdDog({
      outbox: {
        read: (key) => Effect.promise(() => rows.read(key)),
        put: (key, value) => Effect.promise(() => rows.put(key, value)),
        list: (limit, afterKey) =>
          Effect.promise(() => rows.list(limit, afterKey)),
        delete: (key) => Effect.promise(() => rows.delete(key)),
      },
      post: (mail) =>
        Effect.sync(() => {
          posted.push(mail);
          return true;
        }),
      scheduleWake: () => Effect.void,
      now: () => 100,
    }),
  );
  const runtime = manualRuntime();
  const core = await Effect.runPromise(
    openRuntimeCore(
      {
        store: fixture.owner,
        extensions: grantAll([probe]),
        runtime: runtime.runtime,
        instructions: {
          load: () =>
            Effect.succeed({ revision: "routed-v1", text: "Do the work." }),
        },
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
  const mail = (key: string): BirdDogMail => ({
    key,
    to: child,
    from: parent,
    replyTo: [parent],
    content: "Ask me which branch, then echo it",
    createdAt: 1,
    meta: { author: { id: parent.agentId, name: "Shiitake", kind: "agent" } },
  });
  return {
    fixture,
    core,
    posted,
    reconcile: runtime.reconcile,
    mail,
    close: async () => {
      await Effect.runPromise(core.agent.close);
      fixture.close();
    },
  };
}

it("an item raised during an Agent-sent run waits on that sender, calmly, and rides the reply", async () => {
  const open = await openChild();
  try {
    const mail = open.mail("scout-1");
    const accepted = await Effect.runPromise(
      open.core.acceptMail(mail, mail.meta),
    );
    const session = open.core.agent.session(child.sessionId);
    const raised = await Effect.runPromise(
      session.extension(probeId).call("raise", {
        workId: accepted.commandId,
        itemId: "ask-1",
      }),
    );
    expect(raised).toEqual(sender);
    const snapshot = await Effect.runPromise(session.read());
    expect(snapshot.attention).toEqual([
      expect.objectContaining({
        id: "ask-1",
        waitingOn: sender,
        // Core, not the extension, keeps a routed item out of every inbox.
        inbox: false,
      }),
    ]);
    expect(
      readSessionInboxPage(open.fixture.owner, undefined).sessions[0]?.state,
    ).not.toBe("needs_you");
    await open.reconcile();
    await open.reconcile();
    const waitingOnYou = [
      { itemId: "ask-1", kind: "question", summary: "Which branch?" },
    ];
    expect(open.posted).toEqual([
      expect.objectContaining({
        to: parent,
        reply: {
          commandId: accepted.commandId,
          outcome: "completed",
          waitingOnYou,
        },
      }),
    ]);
    // agents.wait reads the same fact from the exact command.
    const settlement = await Effect.runPromise(
      open.core.readSettlement(child.sessionId, accepted.commandId),
    );
    expect(settlement?.waitingOnYou).toEqual(waitingOnYou);
  } finally {
    await open.close();
  }
});

it("an escalated item is no longer named to the sender, and a run no Agent sent has no sender", async () => {
  const open = await openChild();
  try {
    const mail = open.mail("scout-2");
    const accepted = await Effect.runPromise(
      open.core.acceptMail(mail, mail.meta),
    );
    const session = open.core.agent.session(child.sessionId);
    const raise = (workId: string, itemId: string) =>
      Effect.runPromise(
        session.extension(probeId).call("raise", { workId, itemId }),
      );
    await raise(accepted.commandId, "ask-2");
    // Raising it again without a sender is the escalated shape.
    await raise("a-run-no-agent-sent", "ask-2");
    const [item] = (await Effect.runPromise(session.read())).attention;
    expect(item).not.toHaveProperty("waitingOn");
    expect(item).not.toHaveProperty("inbox");
    expect(
      readSessionInboxPage(open.fixture.owner, undefined).sessions[0]?.state,
    ).toBe("needs_you");
    await open.reconcile();
    await open.reconcile();
    expect(open.posted[0]?.reply).toEqual({
      commandId: accepted.commandId,
      outcome: "completed",
    });
  } finally {
    await open.close();
  }
});

it("a later turn's report to the delegator names what still waits on it", async () => {
  const open = await openChild(["Asked.", "Still waiting on the branch."]);
  try {
    const mail = open.mail("scout-3");
    const accepted = await Effect.runPromise(
      open.core.acceptMail(mail, mail.meta),
    );
    const session = open.core.agent.session(child.sessionId);
    await Effect.runPromise(
      session.extension(probeId).call("raise", {
        workId: accepted.commandId,
        itemId: "ask-3",
      }),
    );
    await open.reconcile();
    await open.reconcile();
    // A turn no Agent's mail started, such as a goal turn, has no sender of its own.
    const later = await Effect.runPromise(
      session.send("Keep going", { requestId: "later-turn" }),
    );
    await open.reconcile();
    await open.reconcile();
    expect(open.posted.map(({ key }) => key)).toEqual([
      "scout-3:reply:0",
      `scout-3:report:${later.commandId}:0`,
    ]);
    expect(open.posted[1]?.reply).toEqual({
      commandId: later.commandId,
      outcome: "completed",
      waitingOnYou: [
        { itemId: "ask-3", kind: "question", summary: "Which branch?" },
      ],
    });
  } finally {
    await open.close();
  }
});
