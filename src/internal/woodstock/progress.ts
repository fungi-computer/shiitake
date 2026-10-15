import type { MuleEvent } from "@fungi.computer/mule";
import * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";

import type { SessionProgress } from "../../client.js";
import { ShiitakeError } from "../../errors.js";
import type { SqliteOwner } from "../contracts.js";
import { publishOwnerObservation } from "../observation-kernel.js";
import type { AcceptedCommand } from "./session-store.js";

type ProcessProgress = Readonly<{
  workId: string;
  value: SessionProgress;
}>;

const progressByOwner = new WeakMap<
  SqliteOwner,
  Map<string, ProcessProgress>
>();

function progressFor(owner: SqliteOwner): Map<string, ProcessProgress> {
  const existing = progressByOwner.get(owner);
  if (existing !== undefined) return existing;
  const created = new Map<string, ProcessProgress>();
  progressByOwner.set(owner, created);
  return created;
}

function rejected(message: string): ShiitakeError {
  return new ShiitakeError("runtime", "rejected", message);
}

/** Read the current process-local progress value for one addressed Session. */
export function readProcessProgress(
  owner: SqliteOwner,
  sessionId: string,
): SessionProgress | null {
  return progressFor(owner).get(sessionId)?.value ?? null;
}

/** Read progress only when it belongs to the current durable work head. */
export function readProcessProgressForWork(
  owner: SqliteOwner,
  sessionId: string,
  workId: string,
): SessionProgress | null {
  const current = progressFor(owner).get(sessionId);
  return current?.workId === workId ? current.value : null;
}

export type ProgressMessageStart = Extract<
  MuleEvent,
  { type: "message_start" }
>;
export type ProgressMessageUpdate = Extract<
  MuleEvent,
  { type: "message_update" }
>;
export type ProgressMessageEnd = Extract<MuleEvent, { type: "message_end" }>;

export type ProgressController = Readonly<{
  started: (event: ProgressMessageStart) => Effect.Effect<void, ShiitakeError>;
  update: (event: ProgressMessageUpdate) => Effect.Effect<void, ShiitakeError>;
  terminal: (event: ProgressMessageEnd) => Effect.Effect<void, ShiitakeError>;
}>;

function textFor(event: ProgressMessageUpdate) {
  let text = "";
  let thinking = "";
  for (const content of event.value.content) {
    if (content.type === "text") text += content.text;
    if (content.type === "thinking") thinking += content.thinking;
  }
  return { text, thinking };
}

/**
 * Keep live model progress in the current process only.
 *
 * Woodstock durably owns one empty assistant placeholder and the exact Pi
 * message at completion. Updates deliberately perform no SQLite writes.
 */
export function openProgress(
  owner: SqliteOwner,
  accepted: AcceptedCommand,
  complete: (event: ProgressMessageEnd) => Effect.Effect<void, ShiitakeError>,
): Effect.Effect<ProgressController, never, Scope.Scope> {
  return Effect.gen(function* () {
    let started: ProgressMessageStart | undefined;

    const clear = Effect.sync(() => {
      if (progressFor(owner).delete(accepted.sessionId))
        publishOwnerObservation(owner, accepted.sessionId, "progress");
      started = undefined;
    });
    yield* Effect.addFinalizer(() => clear);

    return Object.freeze({
      started: (event) =>
        Effect.sync(() => {
          started = event;
          progressFor(owner).delete(accepted.sessionId);
        }),
      update: (event) =>
        Effect.suspend(() => {
          const messageStart = started;
          if (messageStart === undefined)
            return Effect.fail(
              rejected("Mule progress arrived before its message start"),
            );
          if (
            messageStart.run.value !== event.run.value ||
            messageStart.turn.value !== event.turn.value ||
            messageStart.message.value !== event.message.value
          )
            return Effect.fail(rejected("Mule progress identity conflicts"));

          const previous = progressFor(owner).get(accepted.sessionId);
          const content = textFor(event);
          if (
            previous?.workId === accepted.commandId &&
            previous.value.text === content.text &&
            previous.value.thinking === content.thinking
          )
            return Effect.void;

          progressFor(owner).set(
            accepted.sessionId,
            Object.freeze({
              workId: accepted.commandId,
              value: Object.freeze({
                messageId: event.message.value,
                revision:
                  (previous?.workId === accepted.commandId
                    ? previous.value.revision
                    : 0) + 1,
                text: content.text,
                thinking: content.thinking,
                textTruncated: false,
                thinkingTruncated: false,
              }),
            }),
          );
          publishOwnerObservation(owner, accepted.sessionId, "progress");
          return Effect.void;
        }),
      terminal: (event) => clear.pipe(Effect.andThen(complete(event))),
    });
  });
}
