import { expect, it } from "vitest";
import * as Effect from "effect/Effect";
import { fauxModel } from "@fungi.computer/mule/testing";
import type { BirdDogMail } from "@fungi.computer/bird-dog";
import { openRuntimeCore } from "../src/internal/runtime-core.js";
import { readSessionDelegator } from "../src/internal/woodstock/session-delegator.js";
import { createStoredSession } from "../src/internal/woodstock/session-store.js";
import { admitWithoutCheck } from "../src/internal/woodstock/storage-admission.js";
import {
  createNativeSqliteOwnerFixture,
  noExtensions,
} from "./fixtures/native-sqlite-owner.fixture.js";

const sender = {
  kind: "agent",
  agentId: "worker",
  sessionId: "worker-session",
} as const;
const recipient = { kind: "agent", agentId: "coordinator" } as const;
const mail = (key: string, sessionId?: string): BirdDogMail => ({
  key,
  from: sender,
  to: sessionId === undefined ? recipient : { ...recipient, sessionId },
  replyTo: [sender],
  content: "Status report",
  createdAt: 1,
});

it("records only the mail that created a Session as its delegator", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const faux = fauxModel([]);
  const core = await Effect.runPromise(
    openRuntimeCore({
      store: fixture.owner,
      extensions: noExtensions,
      runtime: {
        bind: () =>
          Effect.succeed({
            commit: <A, E>(mutation: Effect.Effect<A, E>) => mutation,
            requestWake: Effect.void,
            close: Effect.void,
          }),
      },
      instructions: {
        load: () => Effect.succeed({ revision: "test", text: "Reply." }),
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
    }),
  );
  try {
    const existing = createStoredSession(
      fixture.owner,
      "created-by-user",
      admitWithoutCheck.admitNew,
    );
    await Effect.runPromise(core.acceptMail(mail("worker-report", existing)));
    expect(readSessionDelegator(fixture.owner, existing)).toBeUndefined();

    const created = await Effect.runPromise(
      core.acceptMail(
        mail("creator-mail"),
        undefined,
        undefined,
        () => undefined,
      ),
    );
    if (created.sessionId === undefined)
      throw new Error("Missing created Session");
    expect(
      readSessionDelegator(fixture.owner, created.sessionId),
    ).toMatchObject({
      key: "creator-mail",
      replyTo: [sender],
    });
  } finally {
    await Effect.runPromise(core.agent.close);
    fixture.close();
  }
});
