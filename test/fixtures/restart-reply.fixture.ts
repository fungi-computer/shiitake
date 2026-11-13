import { appendFileSync, readFileSync } from "node:fs";
import { fauxModel } from "@fungi.computer/mule/testing";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { make as makeBirdDog } from "@fungi.computer/bird-dog/effect";
import * as Effect from "effect/Effect";
import type { ShiitakeEffectOptions } from "../../src/effect.js";
import { openRuntimeCore } from "../../src/internal/runtime-core.js";
import {
  createNativeSqliteOwnerFixture,
  noExtensions,
} from "./native-sqlite-owner.fixture.js";

// A child Agent process that answers one delegated mail and stages its reply in
// durable rows of the same SQLite file. `crash-reply` stops it after the work
// settled and before the reply is staged; `finish-reply` reopens and reports.
const [databasePath, mode] = process.argv.slice(2);
if (databasePath === undefined) throw new Error("Database path required");
const fixture = createNativeSqliteOwnerFixture(databasePath);
fixture.owner.sql.exec(
  "CREATE TABLE IF NOT EXISTS reply_rows(key TEXT PRIMARY KEY, to_session TEXT NOT NULL, content TEXT NOT NULL)",
);
const parent = {
  kind: "agent",
  agentId: "parent-agent",
  sessionId: "parent-session",
} as const;
const child = {
  kind: "agent",
  agentId: "child-agent",
  sessionId: "child-session",
} as const;

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
const room = Effect.runSync(
  makeBirdDog({
    stage: (mail) =>
      Effect.sync(() => {
        for (const letter of mail) {
          const known = fixture.owner.sql
            .exec("SELECT 1 AS one FROM reply_rows WHERE key = ?", letter.key)
            .toArray();
          if (known.length > 0) continue;
          if (mode === "crash-reply") {
            process.send?.("reply-staging");
            process.kill(process.pid, "SIGSTOP");
          }
          fixture.owner.sql.exec(
            "INSERT INTO reply_rows(key, to_session, content) VALUES (?, ?, ?)",
            letter.key,
            letter.to.sessionId ?? "",
            letter.content,
          );
        }
      }),
    now: () => 100,
  }),
);
const faux = fauxModel([]);
faux.setResponses([
  () => {
    appendFileSync(databasePath + ".requests", "request\n");
    return fauxAssistantMessage("The request is fixed");
  },
]);
const core = await Effect.runPromise(
  openRuntimeCore(
    {
      store: fixture.owner,
      extensions: noExtensions,
      runtime,
      instructions: {
        load: () => Effect.succeed({ revision: "reply-v1", text: "Answer." }),
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
const reconcileOnce = () => {
  if (reconcile === undefined) throw new Error("Runtime is unavailable");
  return Effect.runPromise(reconcile);
};
if (mode === "crash-reply") {
  await Effect.runPromise(
    core.acceptMail({
      key: "mail-1",
      to: child,
      from: parent,
      replyTo: [parent],
      content: "Investigate the failing request",
      createdAt: 42,
    }),
  );
  await reconcileOnce();
}
if (mode === "finish-reply") {
  await reconcileOnce();
  await reconcileOnce();
  const rows = fixture.owner.sql
    .exec("SELECT key, to_session, content FROM reply_rows ORDER BY key")
    .toArray();
  const requests = readFileSync(databasePath + ".requests", "utf8")
    .split("\n")
    .filter((line) => line !== "").length;
  const settled = fixture.owner.sql
    .exec("SELECT state FROM watchdog_jobs")
    .toArray();
  await Effect.runPromise(core.agent.close);
  fixture.close();
  process.send?.({ rows, requests, settled });
  process.disconnect();
}
