import { CompactionInstructionsSchema } from "../client-boundary.js";
import { HostMetaSchema } from "../host-meta.js";
import { userContextMessage, VerifiedAgentReplySchema } from "./agent-reply.js";
import { SandboxFailureEventSchema } from "@fungi.computer/mycelium";
import type {
  AssistantMessage,
  ImageContent,
  JsonValue,
  Message,
  SystemMessage,
  MuleIdentity,
  MuleToolResult,
  TextContent,
  ThinkingContent,
  ToolCall,
  ToolResultMessage,
  Usage,
  UserMessage,
} from "@fungi.computer/mule/effect";
import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";

import { ModelPreferenceSchema } from "../model-preference.js";

const closed = { onExcessProperty: "error" } as const;
const encoder = new TextEncoder();

const boundedText = (maximumBytes: number) =>
  Schema.String.pipe(
    Schema.check(Schema.isMinLength(1)),
    Schema.check(
      Schema.makeFilter(
        (value) => encoder.encode(value).byteLength <= maximumBytes,
      ),
    ),
  );

const boundedId = boundedText(256);
const digest = Schema.String.pipe(
  Schema.check(Schema.isPattern(/^[0-9a-f]{64}$/u)),
);
const jsonValue: Schema.Codec<JsonValue> = Schema.suspend(() =>
  Schema.Union([
    Schema.String,
    Schema.Number,
    Schema.Boolean,
    Schema.Null,
    Schema.Array(jsonValue),
    Schema.Record(Schema.String, jsonValue),
  ]),
);
const textContent: Schema.Codec<TextContent> = Schema.Struct({
  type: Schema.Literal("text"),
  text: Schema.String,
  textSignature: Schema.optionalKey(Schema.String),
});
const thinkingContent: Schema.Codec<ThinkingContent> = Schema.Struct({
  type: Schema.Literal("thinking"),
  thinking: Schema.String,
  thinkingSignature: Schema.optionalKey(Schema.String),
  redacted: Schema.optionalKey(Schema.Boolean),
});
const imageContent: Schema.Codec<ImageContent> = Schema.Struct({
  type: Schema.Literal("image"),
  data: Schema.String,
  mimeType: Schema.String,
});
const StoredToolCallSchema: Schema.Codec<ToolCall> = Schema.Struct({
  type: Schema.Literal("toolCall"),
  id: Schema.String,
  name: Schema.String,
  arguments: Schema.Record(Schema.String, jsonValue),
  thoughtSignature: Schema.optionalKey(Schema.String),
  namespace: Schema.optionalKey(Schema.String),
});
/** Provider-native usage grammar shared by stored messages and settlement facts. */
export const usage: Schema.Codec<Usage> = Schema.Struct({
  input: Schema.Number,
  output: Schema.Number,
  cacheRead: Schema.Number,
  cacheWrite: Schema.Number,
  cacheWrite1h: Schema.optionalKey(Schema.Number),
  reasoning: Schema.optionalKey(Schema.Number),
  totalTokens: Schema.Number,
  cost: Schema.Struct({
    input: Schema.Number,
    output: Schema.Number,
    cacheRead: Schema.Number,
    cacheWrite: Schema.Number,
    total: Schema.Number,
  }),
});
const userMessage: Schema.Codec<UserMessage> = Schema.Struct({
  role: Schema.Literal("user"),
  content: Schema.Union([
    Schema.String,
    Schema.mutable(Schema.Array(Schema.Union([textContent, imageContent]))),
  ]),
  timestamp: Schema.Number,
});
const promptMessage = Schema.Struct({
  role: Schema.Literal("user"),
  content: Schema.String,
  timestamp: Schema.Number,
});
type StoredPiTool = NonNullable<SystemMessage["toolsAdded"]>[number];
const storedToolParameters: Schema.Codec<StoredPiTool["parameters"]> =
  Schema.Record(Schema.String, jsonValue);
const storedToolSampling: Schema.Codec<
  NonNullable<StoredPiTool["constrainedSampling"]>
> = Schema.Union([
  Schema.Literal(false),
  Schema.Struct({
    type: Schema.Literal("json_schema"),
    strict: Schema.Literals(["prefer", "require"]),
  }),
  Schema.Struct({
    type: Schema.Literal("grammar"),
    variants: Schema.Struct({
      openai_lark: Schema.optionalKey(Schema.String),
      openai_regex: Schema.optionalKey(Schema.String),
    }),
  }),
]);
const storedPiTool: Schema.Codec<StoredPiTool> = Schema.Struct({
  name: Schema.String,
  description: Schema.String,
  parameters: storedToolParameters,
  constrainedSampling: Schema.optionalKey(storedToolSampling),
});
const systemMessage: Schema.Codec<SystemMessage> = Schema.Struct({
  role: Schema.Literal("system"),
  content: Schema.Union([
    Schema.String,
    Schema.mutable(Schema.Array(textContent)),
  ]),
  sections: Schema.optionalKey(
    Schema.Record(Schema.String, Schema.Union([Schema.String, Schema.Null])),
  ),
  toolsAdded: Schema.optionalKey(Schema.mutable(Schema.Array(storedPiTool))),
  toolsRemoved: Schema.optionalKey(
    Schema.mutable(Schema.Array(Schema.Struct({ name: Schema.String }))),
  ),
  timestamp: Schema.Number,
});
const assistantMessage: Schema.Codec<AssistantMessage> = Schema.Struct({
  role: Schema.Literal("assistant"),
  content: Schema.mutable(
    Schema.Array(
      Schema.Union([textContent, thinkingContent, StoredToolCallSchema]),
    ),
  ),
  api: Schema.String,
  provider: Schema.String,
  model: Schema.String,
  responseModel: Schema.optionalKey(Schema.String),
  responseId: Schema.optionalKey(Schema.String),
  diagnostics: Schema.optionalKey(
    Schema.mutable(
      Schema.Array(
        Schema.Struct({
          type: Schema.String,
          timestamp: Schema.Number,
          error: Schema.optionalKey(
            Schema.Struct({
              name: Schema.optionalKey(Schema.String),
              message: Schema.String,
              stack: Schema.optionalKey(Schema.String),
              code: Schema.optionalKey(
                Schema.Union([Schema.String, Schema.Number]),
              ),
            }),
          ),
          details: Schema.optionalKey(Schema.Record(Schema.String, jsonValue)),
        }),
      ),
    ),
  ),
  usage,
  stopReason: Schema.Literals([
    "pending",
    "stop",
    "length",
    "toolUse",
    "error",
    "aborted",
    "deferred",
  ]),
  deferred: Schema.optionalKey(
    Schema.Struct({
      provider: Schema.String,
      modelId: Schema.String,
      api: Schema.String,
      id: Schema.String,
      expiresAt: Schema.optionalKey(Schema.Number),
      pollAfterMs: Schema.optionalKey(Schema.Number),
      data: Schema.optionalKey(jsonValue),
    }),
  ),
  errorMessage: Schema.optionalKey(Schema.String),
  rawStopReason: Schema.optionalKey(Schema.String),
  providerThinkingLevel: Schema.optionalKey(Schema.String),
  endTurn: Schema.optionalKey(Schema.Boolean),
  timestamp: Schema.Number,
});
const toolResultMessage: Schema.Codec<ToolResultMessage> = Schema.Struct({
  role: Schema.Literal("toolResult"),
  toolCallId: Schema.String,
  toolName: Schema.String,
  content: Schema.mutable(
    Schema.Array(Schema.Union([textContent, imageContent])),
  ),
  details: Schema.optionalKey(jsonValue),
  usage: Schema.optionalKey(usage),
  isError: Schema.Boolean,
  timestamp: Schema.Number,
});

/** Exact Pi message grammar stored by Woodstock without normalization. */
export const StoredPiMessageSchema: Schema.Codec<Message> = Schema.Union([
  systemMessage,
  userMessage,
  assistantMessage,
  toolResultMessage,
]);

const causalKind = Schema.Literals([
  "run",
  "turn",
  "message",
  "tool_call",
  "tool_result",
  "terminal",
]);
const CausalIdentitySchema: Schema.Codec<MuleIdentity> = Schema.suspend(() =>
  Schema.Struct({
    kind: causalKind,
    value: boundedId,
    parent: Schema.optional(CausalIdentitySchema),
  }),
);
const LiveRetryEventSchema = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("auto_retry_start"),
    run: CausalIdentitySchema,
    attempt: Schema.Number,
    maxAttempts: Schema.Number,
    delayMs: Schema.Number,
    errorMessage: Schema.String,
  }),
  Schema.Struct({
    type: Schema.Literal("auto_retry_attempt"),
    run: CausalIdentitySchema,
    attempt: Schema.Number,
  }),
  Schema.Struct({
    type: Schema.Literal("auto_retry_end"),
    run: CausalIdentitySchema,
    attempt: Schema.Number,
    success: Schema.Boolean,
    finalError: Schema.optionalKey(Schema.String),
  }),
]);

/**
 * One retained Pi live-retry observation, as Woodstock stores it under its
 * `live_retry` session entry kind. Mule's retry loop (`retryAssistantCall`
 * wrapped around each assistant call in `packages/mule/src/effect.ts`)
 * produces the generic `MuleRetryObservation`; `run-scope.ts` maps that onto
 * this exact shape before calling `recordLiveRetryEvent`.
 */
export type LiveRetryEvent = Schema.Schema.Type<typeof LiveRetryEventSchema>;
const fingerprint = Schema.String.pipe(
  Schema.check(Schema.isPattern(/^sha256:[0-9a-f]{64}$/u)),
);
/**
 * A roster fingerprint snapshot. Admission records what was offered then;
 * execution facts record what the reopened owner actually ran.
 */
const runContract = Schema.Struct({
  core: digest,
  extensions: Schema.Array(Schema.Tuple([boundedId, fingerprint])).pipe(
    Schema.check(Schema.isMaxLength(128)),
  ),
});
const StoredToolResultSchema: Schema.Codec<MuleToolResult> = Schema.Struct({
  content: Schema.mutable(
    Schema.Array(Schema.Union([textContent, imageContent])),
  ),
  details: Schema.optional(jsonValue),
  usage: Schema.optional(usage),
  isError: Schema.Boolean,
  terminate: Schema.optional(Schema.Boolean),
});
const common = { workId: boundedId };

/** What started one compaction: the user, the context threshold, or an overflow. */
const CompactionTriggerSchema = Schema.Literals([
  "manual",
  "threshold",
  "overflow",
]);
/** What started one compaction. */
export type CompactionTrigger = Schema.Schema.Type<
  typeof CompactionTriggerSchema
>;

/** Closed grammar stored in one retained Session entry JSON column. */
const StoredSessionEntrySchema = Schema.Union([
  Schema.Struct({
    ...common,
    kind: Schema.Literal("sandbox_failure"),
    event: SandboxFailureEventSchema,
  }),
  Schema.Struct({
    kind: Schema.Literal("document"),
    workId: Schema.Null,
    extensionId: boundedId,
    name: boundedId,
    valueJson: Schema.String,
  }),
  Schema.Struct({
    kind: Schema.Literal("model_preference"),
    workId: Schema.Null,
    preference: Schema.NullOr(ModelPreferenceSchema),
  }),
  Schema.Struct({
    ...common,
    kind: Schema.Literal("command"),
    meta: Schema.optionalKey(HostMetaSchema),
    /** Present only when the host admitted this line as a verified Agent reply. */
    reply: Schema.optionalKey(VerifiedAgentReplySchema),
    delivery: Schema.optionalKey(Schema.Literals(["steer", "followUp"])),
    requestId: boundedId,
    targetWorkId: Schema.optionalKey(boundedId),
    receiptId: boundedId,
    message: userMessage,
    contract: runContract,
  }),
  // A manual compaction request: accepted Session work with no message. It
  // never enters model context, a title or the client message projection.
  Schema.Struct({
    ...common,
    kind: Schema.Literal("command"),
    work: Schema.Literal("compact"),
    /** The user's focus for the summary, as Pi's `/compact <instructions>`. */
    instructions: Schema.optionalKey(CompactionInstructionsSchema),
    meta: Schema.optionalKey(HostMetaSchema),
    requestId: boundedId,
    receiptId: boundedId,
    timestamp: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
    contract: runContract,
  }),
  Schema.Struct({
    kind: Schema.Literal("operator"),
    workId: Schema.Null,
    requestId: boundedId,
    author: Schema.Literals(["operator", "party"]),
    meta: Schema.optionalKey(HostMetaSchema),
    message: promptMessage,
  }),
  Schema.Struct({
    ...common,
    kind: Schema.Literal("cancellation"),
    requestId: boundedId,
    receiptId: boundedId,
    targetWorkId: boundedId,
    meta: Schema.optionalKey(HostMetaSchema),
  }),
  Schema.Struct({
    ...common,
    kind: Schema.Literal("model_selection"),
    provider: boundedId,
    model: boundedId,
    catalogRevision: boundedId,
    definitionRevision: boundedId,
    selectionPolicyRevision: boundedId,
  }),
  Schema.Struct({
    ...common,
    kind: Schema.Literal("run_started"),
    run: CausalIdentitySchema,
    contract: runContract,
  }),
  Schema.Struct({
    ...common,
    kind: Schema.Literal("turn_started"),
    run: CausalIdentitySchema,
    turn: CausalIdentitySchema,
  }),
  Schema.Struct({
    ...common,
    kind: Schema.Literal("model_prompt"),
    run: CausalIdentitySchema,
    turn: CausalIdentitySchema,
    attempt: Schema.Number.pipe(
      Schema.check(Schema.isInt()),
      Schema.check(Schema.isGreaterThan(0)),
    ),
    contract: runContract,
    systemPrompt: Schema.String.pipe(
      Schema.check(
        Schema.makeFilter(
          (value) => encoder.encode(value).byteLength <= 1024 * 1024,
        ),
      ),
    ),
  }),
  Schema.Struct({
    ...common,
    kind: Schema.Literal("message_start"),
    run: CausalIdentitySchema,
    turn: CausalIdentitySchema,
    message: CausalIdentitySchema,
    role: Schema.Literals(["assistant", "system", "toolResult", "user"]),
  }),
  Schema.Struct({
    ...common,
    kind: Schema.Literal("message"),
    run: CausalIdentitySchema,
    turn: CausalIdentitySchema,
    message: CausalIdentitySchema,
    value: StoredPiMessageSchema,
  }),
  Schema.Struct({
    ...common,
    kind: Schema.Literal("tool_intent"),
    replay: Schema.Literals(["safe", "unsafe"]).pipe(
      Schema.withDecodingDefaultType(Effect.succeed("unsafe" as const)),
    ),
    contract: runContract,
    run: CausalIdentitySchema,
    turn: CausalIdentitySchema,
    toolCall: CausalIdentitySchema,
    call: StoredToolCallSchema,
  }),
  Schema.Struct({
    ...common,
    kind: Schema.Literal("tool_outcome"),
    run: CausalIdentitySchema,
    turn: CausalIdentitySchema,
    toolCall: CausalIdentitySchema,
    call: StoredToolCallSchema,
    result: StoredToolResultSchema,
  }),
  Schema.Struct({
    ...common,
    kind: Schema.Literal("live_retry"),
    event: LiveRetryEventSchema,
  }),
  Schema.Struct({
    ...common,
    kind: Schema.Literal("compaction"),
    resumesRun: Schema.optionalKey(Schema.Boolean),
    // Optional only because Sessions compacted before these facts existed
    // are still live; every new compaction records both.
    trigger: Schema.optionalKey(CompactionTriggerSchema),
    /** Estimated context tokens right after this compaction. */
    tokensAfter: Schema.optionalKey(Schema.Number),
    firstKeptEntryId: boundedId,
    summary: Schema.String,
    tokensBefore: Schema.Number,
    usage,
    contextRevision: boundedId,
    details: Schema.optionalKey(
      Schema.Struct({
        readFiles: Schema.Array(Schema.String),
        modifiedFiles: Schema.Array(Schema.String),
      }),
    ),
  }),
  // A compaction that committed nothing: a manual request with nothing to
  // summarize, or any compaction that failed. Clients show it as a result.
  Schema.Struct({
    ...common,
    kind: Schema.Literal("compaction_notice"),
    trigger: CompactionTriggerSchema,
    result: Schema.Literals(["nothing_to_compact", "failed"]),
    diagnostic: Schema.optionalKey(
      Schema.String.check(Schema.isMaxLength(200)),
    ),
  }),
  Schema.Struct({
    ...common,
    kind: Schema.Literal("run_terminal"),
    run: CausalIdentitySchema,
    terminal: CausalIdentitySchema,
    outcome: Schema.Literals(["completed", "cancelled", "failed"]),
  }),
  Schema.Struct({
    ...common,
    kind: Schema.Literal("work_terminal"),
    diagnostic: Schema.optionalKey(Schema.String),
    outcome: Schema.Literals([
      "completed",
      "interrupted",
      "cancelled",
      "failed",
    ]),
  }),
]);

export type StoredSessionEntry = Schema.Schema.Type<
  typeof StoredSessionEntrySchema
>;

const StoredSessionEntryTextSchema = Schema.fromJsonString(
  StoredSessionEntrySchema,
);
const JsonTextSchema = Schema.fromJsonString(Schema.Unknown);
const decodeEntryJson = Schema.decodeUnknownResult(
  StoredSessionEntryTextSchema,
  closed,
);
const encodeJson = Schema.encodeSync(JsonTextSchema);
const validateEntryJson = Schema.decodeUnknownSync(
  StoredSessionEntryTextSchema,
  closed,
);

/** Accepted Session work: a prompt, or a manual compaction request. */
export type CommandSessionEntry = Extract<
  StoredSessionEntry,
  { kind: "command" }
>;

/** A manual compaction request; it carries no message. */
export type CompactCommandEntry = Extract<
  CommandSessionEntry,
  { work: "compact" }
>;

/** A prompt command: the only command that enters model context. */
export type PromptCommandEntry = Exclude<
  CommandSessionEntry,
  CompactCommandEntry
>;

/** Whether accepted work is a manual compaction request. */
export function isCompactCommand(
  entry: CommandSessionEntry,
): entry is CompactCommandEntry {
  return "work" in entry;
}

/** The prompt a command carries; a compaction request carries none. */
export function promptCommand(
  entry: StoredSessionEntry,
): PromptCommandEntry | undefined {
  return entry.kind === "command" && !isCompactCommand(entry)
    ? entry
    : undefined;
}

/** When the command was accepted, as its own data records it. */
export function commandTimestamp(entry: CommandSessionEntry): number {
  return isCompactCommand(entry) ? entry.timestamp : entry.message.timestamp;
}

export type OperatorSessionEntry = Extract<
  StoredSessionEntry,
  { kind: "operator" }
>;

/**
 * What a later model run sees of an operator line: always a user-role note,
 * so a human's words are never presented as the model's own output.
 */
export function operatorContextMessage(
  entry: OperatorSessionEntry,
): UserMessage {
  return userContextMessage(
    entry.author === "party"
      ? entry.message
      : {
          role: "user",
          content: `[A human operator replied to the user on your behalf]\n${entry.message.content}`,
          timestamp: entry.message.timestamp,
        },
  );
}

/**
 * What a model run sees of an accepted command: its stored user line, wrapped
 * as a verified Agent reply only when the host admitted it as one.
 */
export function commandContextMessage(entry: PromptCommandEntry): UserMessage {
  return userContextMessage(entry.message, entry.reply);
}

/**
 * What a client sees of an operator line: the party's words as a user
 * message, the operator's as an assistant message from provider `operator`.
 */
export function operatorClientMessage(entry: OperatorSessionEntry): Message {
  if (entry.author === "party") return entry.message;
  const zero = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 };
  return {
    role: "assistant",
    content: [{ type: "text", text: entry.message.content }],
    api: "operator",
    provider: "operator",
    model: "operator",
    usage: {
      input: 0,
      output: 0,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 0,
      cost: zero,
    },
    stopReason: "stop",
    timestamp: entry.message.timestamp,
  };
}

/** Decode the one JSON representation admitted by retained Session storage. */
export function decodeStoredSessionEntry(value: string) {
  return Result.map(decodeEntryJson(value), (entry) =>
    entry.kind === "command" &&
    !isCompactCommand(entry) &&
    entry.delivery === undefined
      ? { ...entry, delivery: "followUp" as const }
      : entry,
  );
}

/** Serialize Pi's JSON representation, then validate the retained entry. */
export function encodeStoredSessionEntry(entry: StoredSessionEntry): string {
  const encoded = encodeJson(entry);
  validateEntryJson(encoded);
  return encoded;
}
