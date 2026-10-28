import { SessionAttentionListSchema } from "./session-attention.js";
import type { CommandSettlement } from "./internal/contracts.js";
import { PromptImageSchema } from "./internal/client-boundary.js";
import { SandboxFailureEventSchema } from "@fungi.computer/mycelium";
import * as Schema from "effect/Schema";

import type {
  Accepted,
  DelegationReceipt,
  Held,
  CancelResult,
  WithdrawResult,
  ForkedSession,
  SessionCompactionMarker,
  SessionCompactionNotice,
  SessionFrame,
  SessionMessage,
  SessionPendingPrompts,
  SessionPendingPrompt,
  SessionProgress,
  SessionSnapshot,
  SessionSummaryPage,
  SessionActivity,
  SessionTerminal,
} from "./client.js";
import { ShiitakeError, ShiitakeErrorWireSchema } from "./errors.js";
import type { ExtensionSlotV0, ExtensionStatusV0, Json } from "./extensions.js";
import { JsonValueSchema } from "./internal/json-value.js";
import { HostMetaSchema } from "./internal/host-meta.js";
import {
  StoredPiMessageSchema,
  usage,
  ExtensionAdmissionOriginSchema,
} from "./internal/woodstock/index.js";

const ExtensionOriginSchema = ExtensionAdmissionOriginSchema.mapFields(
  ({ extensionId, correlation }) => ({ extensionId, correlation }),
);

const SessionMessageSchema: Schema.Codec<SessionMessage> = Schema.Struct({
  requestId: Schema.optionalKey(Schema.String),
  meta: Schema.optionalKey(HostMetaSchema),
  data: Schema.optionalKey(JsonValueSchema),
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
const SessionTerminalSchema: Schema.Codec<SessionTerminal> = Schema.Struct({
  commandId: Schema.String,
  outcome: Schema.Literals(["completed", "cancelled", "failed", "interrupted"]),
  sequence: Schema.Int.check(Schema.isGreaterThan(0)),
  diagnostic: Schema.NullOr(Schema.String),
  meta: Schema.optionalKey(HostMetaSchema),
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
    result: Schema.Literals([
      "nothing_to_compact",
      "failed",
      "stale",
      "insufficient",
    ]),
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

const SessionPendingPromptSchema: Schema.Codec<SessionPendingPrompt> =
  Schema.Struct({
    commandId: Schema.String,
    requestId: Schema.String,
    meta: Schema.optionalKey(HostMetaSchema),
    origin: Schema.optionalKey(ExtensionOriginSchema),
    text: Schema.String,
    images: Schema.optionalKey(Schema.Array(PromptImageSchema)),
    delivery: Schema.Literals(["steer", "followUp"]),
  });
const SessionToolOutputSchema = Schema.Struct({
  toolCallId: Schema.String,
  revision: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  tail: Schema.String.check(
    Schema.makeFilter(
      (tail) =>
        new TextEncoder().encode(tail).byteLength <= 8192 &&
        tail.split("\n").length <= 200,
    ),
  ),
  droppedBytes: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  totalBytes: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
});
const ToolOutputSchema = Schema.Array(SessionToolOutputSchema).check(
  Schema.isMaxLength(4),
);

const SessionPendingPromptsSchema: Schema.Codec<SessionPendingPrompts> =
  Schema.Struct({
    total: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
    prompts: Schema.Array(SessionPendingPromptSchema),
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
  toolOutput: ToolOutputSchema,
  extensions: ExtensionSlotsSchema,
  attention: SessionAttentionListSchema,
  context: Schema.optionalKey(SessionContextSchema),
}) satisfies Schema.Codec<SessionSnapshot>;

export const SessionFrameSchema: Schema.Codec<SessionFrame> = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("activity"),
    activity: SessionActivitySchema,
  }),
  Schema.Struct({
    kind: Schema.Literal("tool_output"),
    toolOutput: ToolOutputSchema,
    activity: SessionActivitySchema,
  }),
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
  sessionId: Schema.optionalKey(Schema.String),
});
/** Parse the relay's custody acknowledgment without implying Agent acceptance. */
export const HeldSchema: Schema.Codec<Held> = Schema.Struct({
  status: Schema.Literal("held"),
  reason: Schema.Literal("recipient_offline"),
  message: Schema.Literal("waiting: recipient offline"),
});
/** A delegated send is accepted by its owner or held in durable mail custody. */
export const DelegationReceiptSchema: Schema.Codec<DelegationReceipt> =
  Schema.Union([AcceptedSchema, HeldSchema]);
const decodeDelegationReceipt = Schema.decodeUnknownSync(
  Schema.fromJsonString(DelegationReceiptSchema),
  { onExcessProperty: "error" },
);
/** Decode one delegated send's acceptance or durable-custody result. */
export function parseDelegationReceipt(value: string): DelegationReceipt {
  return decodeDelegationReceipt(value);
}
const UnknownRequestSchema = Schema.Struct({
  status: Schema.Literal("unknown"),
});
const WithdrawResultSchema: Schema.Codec<WithdrawResult> = Schema.Union([
  AcceptedSchema,
  UnknownRequestSchema,
]);
const decodeWithdrawResult = Schema.decodeUnknownSync(
  Schema.fromJsonString(WithdrawResultSchema),
);

/** Decode the owner's atomic request resolution. */
export function parseWithdrawResult(value: string): WithdrawResult {
  return decodeWithdrawResult(value);
}
const CancelResultSchema: Schema.Codec<CancelResult> = Schema.Union([
  AcceptedSchema,
  UnknownRequestSchema,
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

/** Decode the host-authorized newest pending lookup, including an empty result. */
export function parseLatestPendingPrompt(
  value: string,
): SessionPendingPrompt | null {
  return Schema.decodeUnknownSync(
    Schema.fromJsonString(Schema.NullOr(SessionPendingPromptSchema)),
  )(value);
}

/** Parse one serialized fork result using Shiitake's public client grammar. */
export function parseForkedSession(value: string): ForkedSession {
  return decodeForkedSession(value);
}

/** Exact command facts shared by Session clients and host fleet transports. */
export const commandSettlementSchema: Schema.Codec<CommandSettlement> =
  Schema.Struct({
    acceptedSender: Schema.NullOr(Schema.String),
    delegator: Schema.NullOr(Schema.String),
    terminal: SessionTerminalSchema,
    content: Schema.String,
    usage: Schema.Array(
      Schema.Struct({ provider: Schema.String, model: Schema.String, usage }),
    ),
    toolCalls: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
    waitingOnYou: Schema.Array(
      Schema.Struct({
        itemId: Schema.String,
        kind: Schema.String,
        summary: Schema.String,
      }),
    ).check(Schema.isMaxLength(8)),
  });

/** A timeout is null on the JSON wire and undefined at the client boundary. */
export function parseCommandSettlement(
  value: string,
): CommandSettlement | undefined {
  return (
    Schema.decodeUnknownSync(
      Schema.fromJsonString(Schema.NullOr(commandSettlementSchema)),
    )(value) ?? undefined
  );
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
