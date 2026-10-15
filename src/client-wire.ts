import { SessionAttentionListSchema } from "./session-attention.js";
import { PromptImageSchema } from "./internal/client-boundary.js";
import { SandboxFailureEventSchema } from "@fungi.computer/mycelium";
import * as Schema from "effect/Schema";

import type {
  Accepted,
  CancelResult,
  ForkedSession,
  SessionCompactionMarker,
  SessionCompactionNotice,
  SessionFrame,
  SessionMessage,
  SessionPendingPrompts,
  SessionProgress,
  SessionSnapshot,
  SessionSummaryPage,
  SessionActivity,
  SessionTerminal,
} from "./client.js";
import { ShiitakeError, ShiitakeErrorWireSchema } from "./errors.js";
import type { ExtensionSlotV0, ExtensionStatusV0, Json } from "./extensions.js";
import { JsonValueSchema } from "./internal/json-value.js";
import {
  StoredPiMessageSchema,
  ExtensionAdmissionOriginSchema,
} from "./internal/woodstock/index.js";

const ExtensionOriginSchema = ExtensionAdmissionOriginSchema.mapFields(
  ({ extensionId, correlation }) => ({ extensionId, correlation }),
);

const SessionMessageSchema: Schema.Codec<SessionMessage> = Schema.Struct({
  meta: Schema.optionalKey(JsonValueSchema),
  origin: Schema.optionalKey(ExtensionOriginSchema),
  id: Schema.String,
  sequence: Schema.Int.check(Schema.isGreaterThan(0)),
  commandId: Schema.NullOr(Schema.String),
  value: StoredPiMessageSchema,
});
const SessionCompactionTriggerSchema = Schema.Literals([
  "manual",
  "threshold",
  "overflow",
]);

const SessionActivitySchema: Schema.Codec<SessionActivity> = Schema.Struct({
  queue: Schema.Union([
    Schema.Struct({ state: Schema.Literal("idle") }),
    Schema.Struct({
      state: Schema.Literal("queued"),
      commandId: Schema.NonEmptyString,
      runningWork: Schema.NullOr(
        Schema.Union([
          Schema.Struct({
            kind: Schema.Literal("session"),
            sessionId: Schema.NonEmptyString,
            commandId: Schema.NonEmptyString,
          }),
          Schema.Struct({
            kind: Schema.Literal("host"),
            lane: Schema.NonEmptyString,
            jobId: Schema.NonEmptyString,
          }),
        ]),
      ),
    }),
    Schema.Struct({
      state: Schema.Literal("running"),
      commandId: Schema.NonEmptyString,
    }),
  ]),
  cancellationRequested: Schema.Boolean,
  retry: Schema.NullOr(
    Schema.Struct({
      attempt: Schema.Int.check(Schema.isGreaterThan(0)),
      maxAttempts: Schema.Int.check(Schema.isGreaterThan(0)),
      delayMs: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
      errorMessage: Schema.String,
      scheduledAt: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
    }),
  ),
  activeTool: Schema.NullOr(Schema.String),
  lastModelChoiceSequence: Schema.NullOr(
    Schema.Int.check(Schema.isGreaterThan(0)),
  ),
  compaction: Schema.NullOr(
    Schema.Struct({ trigger: SessionCompactionTriggerSchema }),
  ),
});
const ExtensionRevisionSchema = Schema.String;
const ExtensionSlotSchema: Schema.Codec<ExtensionSlotV0> = Schema.Union([
  Schema.Struct({
    status: Schema.Literal("running"),
    revision: ExtensionRevisionSchema,
    value: JsonValueSchema,
    catalog: Schema.optionalKey(JsonValueSchema),
  }),
  Schema.Struct({
    status: Schema.Literal("stopped"),
    revision: ExtensionRevisionSchema,
    reason: Schema.String,
  }),
]);
const ExtensionSlotsSchema = Schema.Record(Schema.String, ExtensionSlotSchema);
const ExtensionStatusSchema: Schema.Codec<ExtensionStatusV0> = Schema.Struct({
  id: Schema.String,
  version: Schema.String,
  revision: ExtensionRevisionSchema,
  status: Schema.Literals(["running", "stopped"]),
  reason: Schema.NullOr(Schema.String),
  stoppedAt: Schema.NullOr(Schema.Number),
});

/** Existing terminal wire grammar, shared by exact host settlement facts. */
export const SessionTerminalSchema: Schema.Codec<SessionTerminal> =
  Schema.Struct({
    commandId: Schema.String,
    outcome: Schema.Literals([
      "completed",
      "cancelled",
      "failed",
      "interrupted",
    ]),
    sequence: Schema.Int.check(Schema.isGreaterThan(0)),
    diagnostic: Schema.NullOr(Schema.String),
    meta: Schema.optionalKey(JsonValueSchema),
  });

const SessionCompactionMarkerSchema: Schema.Codec<SessionCompactionMarker> =
  Schema.Struct({
    id: Schema.String,
    sequence: Schema.Int.check(Schema.isGreaterThan(0)),
    at: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
    tokensBefore: Schema.Number,
    tokensAfter: Schema.NullOr(Schema.Number),
    trigger: Schema.NullOr(SessionCompactionTriggerSchema),
  });

const SessionCompactionNoticeSchema: Schema.Codec<SessionCompactionNotice> =
  Schema.Struct({
    id: Schema.String,
    sequence: Schema.Int.check(Schema.isGreaterThan(0)),
    at: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
    trigger: SessionCompactionTriggerSchema,
    result: Schema.Literals(["nothing_to_compact", "failed"]),
    diagnostic: Schema.NullOr(Schema.String),
  });

const SessionProgressSchema: Schema.Codec<SessionProgress> = Schema.Struct({
  messageId: Schema.String,
  revision: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  text: Schema.String,
  thinking: Schema.String,
  textTruncated: Schema.Boolean,
  thinkingTruncated: Schema.Boolean,
});

const SessionPendingPromptsSchema: Schema.Codec<SessionPendingPrompts> =
  Schema.Struct({
    total: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
    prompts: Schema.Array(
      Schema.Struct({
        commandId: Schema.String,
        meta: Schema.optionalKey(JsonValueSchema),
        origin: Schema.optionalKey(ExtensionOriginSchema),
        text: Schema.String,
        images: Schema.optionalKey(Schema.Array(PromptImageSchema)),
        delivery: Schema.Literals(["steer", "followUp"]),
      }),
    ),
    hasMore: Schema.Boolean,
  });

const SessionContextSchema = Schema.Struct({
  tokens: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  window: Schema.NullOr(Schema.Int.check(Schema.isGreaterThan(0))),
});

export const SessionSnapshotSchema = Schema.Struct({
  sessionId: Schema.String,
  cursor: Schema.NullOr(Schema.String),
  hasMore: Schema.Boolean,
  oldestCursor: Schema.optionalKey(Schema.NullOr(Schema.String)),
  messages: Schema.Array(SessionMessageSchema),
  terminals: Schema.Array(SessionTerminalSchema),
  sandboxFailures: Schema.optionalKey(Schema.Array(SandboxFailureEventSchema)),
  compactions: Schema.optionalKey(Schema.Array(SessionCompactionMarkerSchema)),
  compactionNotices: Schema.optionalKey(
    Schema.Array(SessionCompactionNoticeSchema),
  ),
  pending: SessionPendingPromptsSchema,
  activity: SessionActivitySchema,
  progress: Schema.NullOr(SessionProgressSchema),
  extensions: ExtensionSlotsSchema,
  attention: SessionAttentionListSchema,
  context: Schema.optionalKey(SessionContextSchema),
}) satisfies Schema.Codec<SessionSnapshot>;

export const SessionFrameSchema: Schema.Codec<SessionFrame> = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("update"),
    delivery: Schema.Literals(["replay", "live"]),
    cursor: Schema.NullOr(Schema.String),
    messages: Schema.Array(SessionMessageSchema),
    terminals: Schema.Array(SessionTerminalSchema),
    sandboxFailures: Schema.optionalKey(
      Schema.Array(SandboxFailureEventSchema),
    ),
    compactions: Schema.optionalKey(
      Schema.Array(SessionCompactionMarkerSchema),
    ),
    compactionNotices: Schema.optionalKey(
      Schema.Array(SessionCompactionNoticeSchema),
    ),
    pending: SessionPendingPromptsSchema,
    activity: SessionActivitySchema,
    extensions: ExtensionSlotsSchema,
    attention: SessionAttentionListSchema,
    context: Schema.optionalKey(SessionContextSchema),
  }),
  Schema.Struct({
    kind: Schema.Literal("progress"),
    progress: SessionProgressSchema,
    activity: SessionActivitySchema,
  }),
  Schema.Struct({
    kind: Schema.Literal("reset"),
    reason: Schema.Literal("history_unavailable"),
    cursor: Schema.NullOr(Schema.String),
    baseline: SessionSnapshotSchema,
  }),
]);

/** Decode a durable command acceptance value at an RPC boundary. */
export const AcceptedSchema: Schema.Codec<Accepted> = Schema.Struct({
  status: Schema.Literal("accepted"),
  commandId: Schema.String,
});
const CancelResultSchema: Schema.Codec<CancelResult> = Schema.Union([
  AcceptedSchema,
  Schema.Struct({
    status: Schema.Literal("too_late"),
    commandId: Schema.String,
  }),
]);
const ForkedSessionSchema: Schema.Codec<ForkedSession> = Schema.Struct({
  sessionId: Schema.String,
});

const decodeSnapshot = Schema.decodeUnknownSync(
  Schema.fromJsonString(SessionSnapshotSchema),
  { onExcessProperty: "error" },
);
const decodeFrame = Schema.decodeUnknownSync(
  Schema.fromJsonString(SessionFrameSchema),
  { onExcessProperty: "error" },
);
const decodeAccepted = Schema.decodeUnknownSync(
  Schema.fromJsonString(AcceptedSchema),
);
const decodeCancelResult = Schema.decodeUnknownSync(
  Schema.fromJsonString(CancelResultSchema),
);
const decodeForkedSession = Schema.decodeUnknownSync(
  Schema.fromJsonString(ForkedSessionSchema),
);
const decodeError = Schema.decodeUnknownSync(
  Schema.fromJsonString(ShiitakeErrorWireSchema),
);
const decodeExtensionStatus = Schema.decodeUnknownSync(
  Schema.fromJsonString(ExtensionStatusSchema),
);
const decodeExtensionStatuses = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Array(ExtensionStatusSchema)),
);
const decodeJsonValue = Schema.decodeUnknownSync(
  Schema.fromJsonString(JsonValueSchema),
);

/** Parse one serialized extension status. */
export function parseExtensionStatus(value: string): ExtensionStatusV0 {
  return decodeExtensionStatus(value);
}

/** Parse the serialized list of rostered extension statuses. */
export function parseExtensionStatuses(
  value: string,
): readonly ExtensionStatusV0[] {
  return decodeExtensionStatuses(value);
}

/** Parse one encoded extension command output; its contract decodes it. */
export function parseExtensionOutput(value: string): Json {
  return decodeJsonValue(value);
}

/** Parse one serialized command acceptance using Shiitake's public grammar. */
export function parseAccepted(value: string): Accepted {
  return decodeAccepted(value);
}

/** Parse one serialized cancel outcome: accepted, or too late. */
export function parseCancelResult(value: string): CancelResult {
  return decodeCancelResult(value);
}

/** Parse one serialized fork result using Shiitake's public client grammar. */
export function parseForkedSession(value: string): ForkedSession {
  return decodeForkedSession(value);
}

/** Parse one serialized snapshot using Shiitake's public client grammar. */
export function parseSessionSnapshot(value: string): SessionSnapshot {
  return decodeSnapshot(value);
}

/** Parse one serialized frame using Shiitake's public client grammar. */
export function parseSessionFrame(value: string): SessionFrame {
  return decodeFrame(value);
}

/** Parse one serialized canonical Shiitake failure. */
export function parseShiitakeError(value: string): ShiitakeError {
  const failure = decodeError(value);
  return new ShiitakeError(
    failure.boundary,
    failure.cause,
    failure.message,
    failure.requestId,
  );
}

const decodeSessionSummaryPage = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({
      sessions: Schema.Array(
        Schema.Union([
          Schema.Struct({
            sessionId: Schema.NonEmptyString,
            createdAt: Schema.Number,
            lastActivityAt: Schema.Number,
            settledAt: Schema.optionalKey(Schema.Number),
            title: Schema.String,
            state: Schema.Literals(["needs_you", "running", "errored", "done"]),
          }),
          Schema.Struct({
            sessionId: Schema.NonEmptyString,
            state: Schema.Literal("unavailable"),
          }),
        ]),
      ),
      next: Schema.optional(Schema.NonEmptyString),
    }),
  ),
);

/** Decode the existing bounded saved-conversation projection. */
export function parseSessionSummaryPage(value: string): SessionSummaryPage {
  const page = decodeSessionSummaryPage(value);
  return { sessions: page.sessions, next: page.next };
}

const decodeCreatedSession = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Struct({ sessionId: Schema.NonEmptyString })),
);

const decodeDeletedSession = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Struct({ deleted: Schema.Boolean })),
);

/** Decode whether the owner deleted an existing Session. */
export function parseDeletedSession(value: string): boolean {
  return decodeDeletedSession(value).deleted;
}

/** Decode the fresh identity issued by the Agent owner. */
export function parseCreatedSession(value: string): string {
  return decodeCreatedSession(value).sessionId;
}
