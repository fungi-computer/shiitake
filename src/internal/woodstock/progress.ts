import type { MuleEvent } from "@fungi.computer/mule";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Result from "effect/Result";
import type * as Scope from "effect/Scope";

import type {
  SessionProgress,
  SessionProgressDelta,
  SessionToolOutput,
} from "../../client.js";
import { ShiitakeError } from "../../errors.js";
import type { SqliteOwner } from "../contracts.js";
import { publishOwnerObservation } from "../observation-kernel.js";
import type { AcceptedCommand } from "./session-store.js";

type ProcessProgress = {
  workId: string;
  value: LiveMessage | null;
  slots: Map<string, SessionToolOutput | null>;
};

type Tail = {
  chunks: { offset: number; text: string; bytes: number }[];
  length: number;
  bytes: number;
};
type LiveMessage = {
  messageId: string;
  revision: number;
  text: Tail;
  thinking: Tail;
};
const tailBytes = 65_536;
const chunkUnits = 2048;
const encoder = new TextEncoder();
const emptyTail = (): Tail => ({ chunks: [], length: 0, bytes: 0 });

/** Bounded chunks: no concatenation or encoding of the reply-so-far. */
function appendTail(tail: Tail, append: string): void {
  for (let index = 0; index < append.length;) {
    let end = Math.min(index + chunkUnits, append.length);
    // Do not cut a Unicode scalar when bounding a provider's large delta.
    const code = append.charCodeAt(end - 1);
    if (end < append.length && code >= 0xd800 && code <= 0xdbff) end--;
    const text = append.slice(index, end);
    const bytes = encoder.encode(text).byteLength;
    const last = tail.chunks.at(-1);
    if (last !== undefined && last.text.length + text.length <= chunkUnits) {
      last.text += text;
      last.bytes += bytes;
    } else {
      tail.chunks.push({ offset: tail.length, text, bytes });
    }
    tail.length += text.length;
    tail.bytes += bytes;
    index = end;
    while (tail.bytes > tailBytes && tail.chunks.length > 1) {
      const dropped = tail.chunks.shift();
      if (dropped !== undefined) tail.bytes -= dropped.bytes;
    }
  }
}

function tailOffset(tail: Tail): number {
  return tail.chunks[0]?.offset ?? 0;
}

function tailAfter(tail: Tail, offset: number): string {
  return tail.chunks
    .flatMap((chunk) =>
      chunk.offset + chunk.text.length <= offset
        ? []
        : [chunk.text.slice(Math.max(0, offset - chunk.offset))],
    )
    .join("");
}

function snapshot(value: LiveMessage | null): SessionProgress | null {
  return value === null
    ? null
    : {
        messageId: value.messageId,
        revision: value.revision,
        text: tailAfter(value.text, tailOffset(value.text)),
        thinking: tailAfter(value.thinking, tailOffset(value.thinking)),
        textOffset: tailOffset(value.text),
        thinkingOffset: tailOffset(value.thinking),
        textTruncated: tailOffset(value.text) > 0,
        thinkingTruncated: tailOffset(value.thinking) > 0,
      };
}

/** Per-watch position; a gap in the retained tail requires a fresh baseline. */
export type ProgressPosition = Readonly<{
  messageId: string;
  revision: number;
  text: number;
  thinking: number;
}>;

export function progressPosition(
  value: SessionProgress | null,
): ProgressPosition | null {
  return value === null
    ? null
    : {
        messageId: value.messageId,
        revision: value.revision,
        text: value.textOffset + value.text.length,
        thinking: value.thinkingOffset + value.thinking.length,
      };
}

/** Undefined means continuity was lost; null means no new model progress. */
export function readProcessProgressDelta(
  owner: SqliteOwner,
  sessionId: string,
  workId: string,
  after: ProgressPosition | null,
): SessionProgressDelta | null | undefined {
  const current = progressByOwner.get(owner)?.get(sessionId);
  const value = current?.workId === workId ? current.value : null;
  if (value === null) return null;
  const position = after?.messageId === value.messageId ? after : null;
  if (position !== null && position.revision >= value.revision) return null;
  const text = position?.text ?? 0;
  const thinking = position?.thinking ?? 0;
  if (text < tailOffset(value.text) || thinking < tailOffset(value.thinking))
    return undefined;
  return {
    messageId: value.messageId,
    revision: value.revision,
    text: { offset: text, append: tailAfter(value.text, text) },
    thinking: { offset: thinking, append: tailAfter(value.thinking, thinking) },
  };
}

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
  return snapshot(progressFor(owner).get(sessionId)?.value ?? null);
}

/** Read progress only when it belongs to the current durable work head. */
export function readProcessProgressForWork(
  owner: SqliteOwner,
  sessionId: string,
  workId: string,
): SessionProgress | null {
  const current = progressFor(owner).get(sessionId);
  return snapshot(current?.workId === workId ? current.value : null);
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
    let publishedLength = 0;
    let flushTimer: ReturnType<typeof setTimeout> | undefined;
    const cancelFlush = () => {
      if (flushTimer !== undefined) clearTimeout(flushTimer);
      flushTimer = undefined;
    };
    const publish = () => {
      cancelFlush();
      const value = current.value;
      if (value === null) return;
      publishedLength = value.text.length + value.thinking.length;
      publishOwnerObservation(owner, accepted.sessionId, "progress");
    };

    const clear = Effect.sync(() => {
      cancelFlush();
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
          cancelFlush();
          started = event;
          current.value = null;
          publishedLength = 0;
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

          const delta = event.event;
          if (
            (delta.type !== "text_delta" && delta.type !== "thinking_delta") ||
            delta.delta.length === 0
          )
            return Effect.void;
          const first = current.value === null;
          const value = current.value ?? {
            messageId: event.message.value,
            revision: 0,
            text: emptyTail(),
            thinking: emptyTail(),
          };
          appendTail(
            delta.type === "text_delta" ? value.text : value.thinking,
            delta.delta,
          );
          value.revision++;
          current.value = value;
          const length = value.text.length + value.thinking.length;
          // Coalesce tiny tokens, but flush a paused provider within 100 ms.
          // One scope-owned timer holds no reply text and never writes SQLite.
          if (first || length - publishedLength >= 4096) {
            publish();
          } else flushTimer ??= setTimeout(publish, 100);
          return Effect.void;
        }),
      terminal: (event) => clear.pipe(Effect.andThen(complete(event))),
    });
  });
}
