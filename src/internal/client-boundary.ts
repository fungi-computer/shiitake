import * as Result from "effect/Result";
import * as Schema from "effect/Schema";

import { ShiitakeError } from "../errors.js";

const encoder = new TextEncoder();

function boundedText(maximumBytes: number) {
  return Schema.String.check(
    Schema.isMinLength(1),
    Schema.isMaxLength(maximumBytes),
    Schema.makeFilter(
      (value) => encoder.encode(value).byteLength <= maximumBytes,
    ),
  );
}

const boundedId = boundedText(256);
/**
 * The one prompt-text grammar. Human, mail, host and extension prompts are all
 * admitted through the accepted-command door under this bound. Text is
 * stored exactly as sent: no decoder trims it.
 */
const PromptTextSchema = boundedText(256 * 1024);
/** Bounded image payload admitted and persisted by the Session owner. */
export const PromptImageSchema = Schema.Struct({
  type: Schema.Literal("image"),
  data: Schema.String.check(
    Schema.isMinLength(1),
    Schema.isMaxLength(1024 * 1024),
    Schema.isBase64(),
  ),
  mimeType: Schema.String.check(
    Schema.makeFilter((value) =>
      ["image/png", "image/jpeg", "image/gif", "image/webp"].includes(value),
    ),
  ),
});
const PromptImagesSchema = Schema.Array(PromptImageSchema).check(
  Schema.isMinLength(1),
  Schema.isMaxLength(8),
);
/** Delivery policy for input accepted while a Session run is active. */
export type PromptDelivery = "steer" | "followUp";
const PromptDeliverySchema = Schema.Literals(["steer", "followUp"]);
const SessionIdSchema = boundedId;
const decodeSessionPageCursorValue = Schema.decodeUnknownResult(
  boundedText(512),
);
const SessionCreationRequestSchema = Schema.Struct({ requestId: boundedId });
const PromptCommandSchema = Schema.Struct({
  delivery: PromptDeliverySchema,
  requestId: boundedId,
  type: Schema.Literal("prompt"),
  text: Schema.String.check(
    Schema.isMaxLength(256 * 1024),
    Schema.makeFilter(
      (value) => encoder.encode(value).byteLength <= 256 * 1024,
    ),
  ),
  images: Schema.optionalKey(PromptImagesSchema),
}).check(
  Schema.makeFilter(
    (value) =>
      (value.text.length > 0 || (value.images?.length ?? 0) > 0) &&
      encoder.encode(value.text).byteLength +
        (value.images ?? []).reduce(
          (bytes, image) => bytes + image.data.length,
          0,
        ) <=
        1024 * 1024,
  ),
);
const AbortCommandSchema = Schema.Struct({ type: Schema.Literal("abort") });
const CancelCommandSchema = Schema.Struct({
  commandId: boundedId,
  onlyIfPending: Schema.optional(Schema.Boolean),
  type: Schema.Literal("cancel"),
});
/**
 * One human-to-human line recorded into an idle Session outside any run, for
 * example during a live takeover: `operator` is a person answering for the
 * Agent, `party` is the other side's message while the Agent is held. It is
 * context for later runs, is idempotent by request id and starts no run.
 */
const OperatorCommandSchema = Schema.Struct({
  type: Schema.Literal("operator"),
  requestId: boundedId,
  author: Schema.Literals(["operator", "party"]),
  text: PromptTextSchema,
});
const BranchCommandSchema = Schema.Struct({
  type: Schema.Literal("branch"),
  entryId: boundedId,
});
const ForkCommandSchema = Schema.Struct({
  type: Schema.Literal("fork"),
  entryId: boundedId,
});
/**
 * Compact the Session's context now. It is accepted Session work, queued
 * behind any running work and idempotent by `requestId`; it runs no model
 * turn beyond the summary and never enters model context or a title.
 */
/** Bounded focus text a manual compaction passes to its summary. */
export const CompactionInstructionsSchema = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(4_096),
);
const CompactCommandSchema = Schema.Struct({
  type: Schema.Literal("compact"),
  requestId: boundedId,
  /** Focus for the summary, appended as Pi's "Additional focus". */
  instructions: Schema.optionalKey(CompactionInstructionsSchema),
});
/** Work the accepted-command door admits: a prompt or a compaction request. */
const AcceptedCommandSchema = Schema.Union([
  PromptCommandSchema,
  CompactCommandSchema,
]);
/**
 * The longest Session title, in Unicode code points; the derived first-prompt
 * title uses the same bound.
 */
export const sessionTitleLimit = 120;
/** One user-set Session title: trimmed, single-line and bounded. */
export const SessionTitleSchema = Schema.String.check(
  Schema.makeFilter(
    (value) =>
      value.length > 0 &&
      value.trim() === value &&
      !/\p{Cc}/u.test(value) &&
      Array.from(value).length <= sessionTitleLimit,
  ),
);
/**
 * Set or clear the user's Session title. It is display metadata only: it
 * overrides the first-prompt title everywhere the title shows, and `null`
 * restores the derived title. Session identity and history are unchanged.
 */
const TitleCommandSchema = Schema.Struct({
  type: Schema.Literal("title"),
  title: Schema.NullOr(SessionTitleSchema),
});
const SessionCommandSchema = Schema.Union([
  PromptCommandSchema,
  AbortCommandSchema,
  CancelCommandSchema,
  BranchCommandSchema,
  ForkCommandSchema,
  OperatorCommandSchema,
  CompactCommandSchema,
  TitleCommandSchema,
]);
const SessionCommandRequestSchema = Schema.Struct({
  command: SessionCommandSchema,
});
const ReadOptionsSchema = Schema.Struct({
  includeSandboxFailures: Schema.optional(Schema.Boolean),
  limit: Schema.optional(
    Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 200 })),
  ),
  before: Schema.optional(boundedText(512)),
});
const ReadQuerySchema = Schema.Struct({
  includeSandboxFailures: Schema.optional(Schema.Literal("true")),
  limit: Schema.optional(
    Schema.NumberFromString.check(
      Schema.isInt(),
      Schema.isBetween({ minimum: 1, maximum: 200 }),
    ),
  ),
  before: Schema.optional(boundedText(512)),
});
const SubscribeOptionsSchema = Schema.Struct({
  includeSandboxFailures: Schema.optional(Schema.Boolean),
  after: Schema.optional(Schema.String),
});
const EventsQuerySchema = Schema.Struct({
  includeSandboxFailures: Schema.optional(Schema.Literal("true")),
  mode: Schema.optional(Schema.Literal("subscribe")),
  after: Schema.optional(Schema.String),
  limit: Schema.optional(
    Schema.NumberFromString.check(
      Schema.isInt(),
      Schema.isBetween({ minimum: 1, maximum: 200 }),
    ),
  ),
});
const closed = { onExcessProperty: "error" } as const;
const decodeSessionIdValue = Schema.decodeUnknownResult(
  SessionIdSchema,
  closed,
);
export const decodeClientSessionCreationRequest = Schema.decodeUnknownResult(
  SessionCreationRequestSchema,
  closed,
);
/** The accepted-command door's one decoder for queued Session work. */
export const decodeClientAcceptedCommand = Schema.decodeUnknownResult(
  AcceptedCommandSchema,
  closed,
);
/** Decode one owner-issued identity (command, extension or cursor). */
export const decodeClientIdentity = Schema.decodeUnknownResult(boundedId);
export const decodeClientSessionCommand = Schema.decodeUnknownResult(
  SessionCommandSchema,
  closed,
);
export const decodeClientSessionCommandRequest = Schema.decodeUnknownResult(
  SessionCommandRequestSchema,
  closed,
);
const decodeReadOptionsValue = Schema.decodeUnknownResult(
  Schema.Union([ReadOptionsSchema, Schema.Undefined]),
  closed,
);
export const decodeClientReadQuery = Schema.decodeUnknownResult(
  ReadQuerySchema,
  closed,
);
const decodeSubscribeOptionsValue = Schema.decodeUnknownResult(
  SubscribeOptionsSchema,
  closed,
);
export const decodeClientEventsQuery = Schema.decodeUnknownResult(
  EventsQuerySchema,
  closed,
);

/** Normalized C1 read policy after the public option bag is decoded once. */
export type ClientReadOptions = Readonly<{
  includeSandboxFailures?: boolean;
  limit: number;
  before?: string;
}>;

/** Public Session command inferred from its one runtime parser. */
export type SessionCommand = Schema.Schema.Type<typeof SessionCommandSchema>;

/** Public prompt command accepted as durable Session work. */
export type PromptCommand = Schema.Schema.Type<typeof PromptCommandSchema>;

/** Cancel the running turn and all queued prompts, retaining their text. */
export type AbortCommand = Schema.Schema.Type<typeof AbortCommandSchema>;

/** Public command that cancels exactly one accepted request. */
export type CancelCommand = Schema.Schema.Type<typeof CancelCommandSchema>;

/** Public command that selects one complete entry as the active lineage leaf. */
export type BranchCommand = Extract<
  SessionCommand,
  Readonly<{ type: "branch" }>
>;

/** Public command that records one operator or party line without a run. */
export type OperatorCommand = Extract<
  SessionCommand,
  Readonly<{ type: "operator" }>
>;

/** Public command that compacts the Session's context as queued work. */
export type CompactCommand = Extract<
  SessionCommand,
  Readonly<{ type: "compact" }>
>;

/** Accepted Session work the door admits and Watchdog queues. */
export type AcceptedWork = Schema.Schema.Type<typeof AcceptedCommandSchema>;

/** Public command that sets or clears the user's Session title. */
export type TitleCommand = Extract<SessionCommand, Readonly<{ type: "title" }>>;

/** Public command that creates a new Session from one complete entry lineage. */
export type ForkCommand = Extract<SessionCommand, Readonly<{ type: "fork" }>>;

/** Exact C2 replay position after the public option bag is decoded once. */
export type ClientSubscribeOptions = Readonly<{
  after?: string;
  includeSandboxFailures?: boolean;
}>;

/** Decode one ordinary Session-id value without exposing input detail. */
export function decodeClientSessionId(value: string): string {
  const decoded = decodeSessionIdValue(value);
  if (decoded._tag === "Failure")
    throw new ShiitakeError("contract", "invalid", "Session id is invalid");
  return decoded.success;
}

/** Decode one opaque session-page cursor; its owner decodes the position. */
export function decodeClientPageCursor(value: string): string {
  const decoded = decodeSessionPageCursorValue(value);
  if (decoded._tag === "Failure")
    throw new ShiitakeError("contract", "invalid", "Session cursor is invalid");
  return decoded.success;
}

/** Decode the exact public Session creation request. */
export function decodeClientSessionCreationRequestId(
  value: Schema.Schema.Type<typeof SessionCreationRequestSchema>,
): string {
  const decoded = decodeClientSessionCreationRequest(value);
  if (decoded._tag === "Failure")
    throw new ShiitakeError(
      "contract",
      "invalid",
      "Session creation request is invalid",
    );
  return decoded.success.requestId;
}

/** Decode and normalize the exact public C1 read option bag. */
export function decodeClientReadOptions(
  value: Schema.Schema.Type<typeof ReadOptionsSchema> | undefined,
): ClientReadOptions {
  const decoded = decodeReadOptionsValue(value);
  if (decoded._tag === "Failure")
    throw new ShiitakeError("contract", "invalid", "Read options are invalid");
  const read = decoded.success ?? {};
  let options: ClientReadOptions = { limit: read.limit ?? 200 };
  if (read.before !== undefined) options = { ...options, before: read.before };
  if (read.includeSandboxFailures !== undefined)
    options = {
      ...options,
      includeSandboxFailures: read.includeSandboxFailures,
    };
  return Object.freeze(options);
}

/** Decode the exact public C2 subscribe option bag before cursor classification. */
export function decodeClientSubscribeOptions(
  value: ClientSubscribeOptions,
): ClientSubscribeOptions {
  const decoded = decodeSubscribeOptionsValue(value);
  if (decoded._tag === "Failure")
    throw new ShiitakeError(
      "contract",
      "invalid",
      "Subscribe options are invalid",
    );
  let options: ClientSubscribeOptions = {};
  if (decoded.success.after !== undefined)
    options = { ...options, after: decoded.success.after };
  if (decoded.success.includeSandboxFailures !== undefined)
    options = {
      ...options,
      includeSandboxFailures: decoded.success.includeSandboxFailures,
    };
  return Object.freeze(options);
}

/** Take one decoder result, or reject with a sanitized contract error. */
export function requireDecoded<A, E>(
  decoded: Result.Result<A, E>,
  message: string,
): A {
  if (Result.isFailure(decoded))
    throw new ShiitakeError("contract", "invalid", message);
  return decoded.success;
}
