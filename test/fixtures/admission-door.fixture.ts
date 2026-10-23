import type { Json } from "../../src/extensions.js";
import * as Effect from "effect/Effect";

import type { SqliteOwner } from "../../src/effect.js";
import {
  openAdmissionDoor,
  type AdmissionDoor,
  type AdmissionOrigin,
  type AdmitRequest,
} from "../../src/internal/admission-door.js";
import { admitWithoutCheck } from "../../src/internal/woodstock/storage-admission.js";
import {
  readSessionCommandAdmission,
  type AcceptedSessionCommand,
  type EnqueueAcceptedWork,
  type RunContract,
  type SessionQueueHead,
} from "../../src/internal/woodstock/session-store.js";

const digest = "a".repeat(64);
const testContract: RunContract = {
  core: digest,
  extensions: [],
};

/** The production door over a test owner, idle queue and no-op host by default. */
export function openTestAdmissionDoor(
  owner: SqliteOwner,
  options: Readonly<{
    contract?: RunContract;
    enqueue?: EnqueueAcceptedWork;
    queueHead?: (sessionId: string) => SessionQueueHead;
    requestWake?: Effect.Effect<void>;
    admitNew?: () => void;
  }> = {},
): AdmissionDoor {
  return openAdmissionDoor({
    owner,
    admitNew: options.admitNew ?? admitWithoutCheck.admitNew,
    contract: () => options.contract ?? testContract,
    enqueue: (work) => options.enqueue?.(work),
    readQueueHead: options.queueHead ?? (() => ({ state: "idle" })),
    commit: (mutation) => mutation,
    requestWake: options.requestWake ?? Effect.void,
  });
}

/**
 * Admit one user prompt through the door and return the stored admission, for
 * Woodstock tests that inspect the durable command it wrote.
 */
export function admitStoredCommand(
  owner: SqliteOwner,
  sessionId: string,
  work: AdmitRequest["command"],
  options: Parameters<typeof openTestAdmissionDoor>[1] &
    Readonly<{
      origin?: AdmissionOrigin;
      when?: AdmitRequest["when"];
      meta?: Json;
    }> = {},
): AcceptedSessionCommand {
  const result = Effect.runSync(
    openTestAdmissionDoor(owner, options).admit(
      options.meta === undefined
        ? {
            sessionId,
            command: work,
            when: options.when ?? "now",
            origin: options.origin ?? { kind: "user" },
          }
        : {
            sessionId,
            command: work,
            when: options.when ?? "now",
            origin: options.origin ?? { kind: "user" },
            meta: options.meta,
          },
    ),
  );
  if (result.status !== "accepted")
    throw new Error("Door did not admit the command");
  const command = readSessionCommandAdmission(
    owner,
    sessionId,
    result.commandId,
  );
  if (command === undefined) throw new Error("Admitted command is missing");
  return {
    accepted: {
      sessionId,
      requestId: work.requestId,
      commandId: result.commandId,
    },
    command,
    delivery: result.delivery,
  };
}
