import type { MuleEvent } from "@fungi.computer/mule";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Result from "effect/Result";
import type * as Scope from "effect/Scope";

import type { SessionProgress, SessionToolOutput } from "../../client.js";
import { ShiitakeError } from "../../errors.js";
import type { SqliteOwner } from "../contracts.js";
import { publishOwnerObservation } from "../observation-kernel.js";
import type { AcceptedCommand } from "./session-store.js";

type ProcessProgress = {
  workId: string;
  value: SessionProgress | null;
  slots: Map<string, SessionToolOutput | null>;
};

const progressByOwner = new WeakMap<
  SqliteOwner,
  Map<string, ProcessProgress>
>();

export function readProcessToolOutputForWork(
  owner: SqliteOwner,
  sessionId: string,
  workId: string,
): readonly SessionToolOutput[] {
  const current = progressByOwner.get(owner)?.get(sessionId);
  return current?.workId === workId
    ? [...current.slots.values()].filter((value) => value !== null)
    : [];
}

const decodeOutputDetails = Schema.decodeUnknownResult(
  Schema.Struct({
    droppedBytes: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
    totalBytes: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  }),
  { onExcessProperty: "error" },
);

/** Run-scoped, replaceable tool snapshots: never stored in Woodstock. */
export function openToolProgress(
  owner: SqliteOwner,
  accepted: AcceptedCommand,
) {
  return Effect.gen(function* () {
    const current = yield* openProcessProgress(owner, accepted);
    const publish = () => {
      publishOwnerObservation(owner, accepted.sessionId, "progress");
    };
    return {
      started: (event: Extract<MuleEvent, { type: "tool_execution_start" }>) =>
        Effect.sync(() => {
          if (
            current.slots.size < 4 &&
            !current.slots.has(event.toolCall.value)
          )
            current.slots.set(event.toolCall.value, null);
        }),
      update: (event: Extract<MuleEvent, { type: "tool_execution_update" }>) =>
        Effect.sync(() => {
          const previous = current.slots.get(event.toolCall.value);
          if (previous === undefined) return;
          const details = decodeOutputDetails(event.result.data);
          // Other tool-specific update contracts do not represent output tails.
          if (Result.isFailure(details)) return;
          const tail = event.result.content
            .filter((part) => part.type === "text")
            .map((part) => part.text)
            .join("");
          if (
            new TextEncoder().encode(tail).byteLength > 8192 ||
            tail.split("\n").length > 200 ||
            details.success.droppedBytes > details.success.totalBytes
          )
            return;
          current.slots.set(event.toolCall.value, {
            // Slots follow Mule execution identity; clients join the provider's
            // call ID carried by assistant calls and durable tool results.
            toolCallId: event.call.id,
            revision: (previous?.revision ?? 0) + 1,
            tail,
            ...details.success,
          });
          publish();
        }),
      terminal: (event: Extract<MuleEvent, { type: "tool_execution_end" }>) =>
        Effect.sync(() => {
          if (current.slots.delete(event.toolCall.value)) publish();
        }),
    };
  });
}

function progressFor(owner: SqliteOwner): Map<string, ProcessProgress> {
  const existing = progressByOwner.get(owner);
  if (existing !== undefined) return existing;
  const created = new Map<string, ProcessProgress>();
  progressByOwner.set(owner, created);
  return created;
}

/** One run-scoped owner for message progress and tool tails. */
function openProcessProgress(owner: SqliteOwner, accepted: AcceptedCommand) {
  return Effect.gen(function* () {
    const sessions = progressFor(owner);
    const existing = sessions.get(accepted.sessionId);
    if (existing?.workId === accepted.commandId) return existing;
    const current: ProcessProgress = {
      workId: accepted.commandId,
      value: null,
      slots: new Map(),
    };
    sessions.set(accepted.sessionId, current);
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        if (sessions.get(accepted.sessionId) !== current) return;
        sessions.delete(accepted.sessionId);
        if (current.value !== null || current.slots.size > 0)
          publishOwnerObservation(owner, accepted.sessionId, "progress");
      }),
    );
    return current;
  });
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
    const current = yield* openProcessProgress(owner, accepted);
    let started: ProgressMessageStart | undefined;

    const clear = Effect.sync(() => {
      const hadProgress = current.value !== null;
      current.value = null;
      if (hadProgress)
        publishOwnerObservation(owner, accepted.sessionId, "progress");
      started = undefined;
    });
    yield* Effect.addFinalizer(() => clear);

    return Object.freeze({
      started: (event) =>
        Effect.sync(() => {
          started = event;
          current.value = null;
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

          const previous = current.value;
          const content = textFor(event);
          if (
            previous?.text === content.text &&
            previous.thinking === content.thinking
          )
            return Effect.void;

          current.value = Object.freeze({
            messageId: event.message.value,
            revision: (previous?.revision ?? 0) + 1,
            text: content.text,
            thinking: content.thinking,
            textTruncated: false,
            thinkingTruncated: false,
          });
          publishOwnerObservation(owner, accepted.sessionId, "progress");
          return Effect.void;
        }),
      terminal: (event) => clear.pipe(Effect.andThen(complete(event))),
    });
  });
}
