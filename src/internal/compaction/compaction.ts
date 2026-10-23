/**
 * Pi v0.84.4 semantic compaction, adapted only at Woodstock's entry boundary.
 * The retained preparation/result vocabulary is intentionally visible here;
 * Shiitake supplies native Message values and its selected model stream.
 * Range selection follows Pi Durable spec §8.7 (`earendil-works/pi` at
 * 8efd314f) and token estimates are pi-ai's own.
 */
import type {
  Api,
  AssistantMessage,
  Context,
  Model,
  RetryPolicy,
  SimpleStreamOptions,
  Usage,
} from "@earendil-works/pi-ai";
// Leaf helpers, not Pi's root entry, which also evaluates its validators.
import {
  estimateContextTokens,
  estimateMessageTokens,
} from "@earendil-works/pi-ai/utils/estimate";
import { retryAssistantCall } from "@earendil-works/pi-ai/utils/retry";
import { contentText } from "@earendil-works/pi-ai/utils/text";
import { normalizeContext } from "@earendil-works/pi-ai/utils/transcript";
import { uuidv7 } from "@earendil-works/pi-ai/utils/uuid";
import type { MuleStreamFunction } from "@fungi.computer/mule/effect";
import * as Effect from "effect/Effect";

import type { BorrowedCompactionViewV0 } from "../../extensions.js";
import { ShiitakeError } from "../../errors.js";
import {
  computeFileLists,
  createFileOps,
  extractFileOpsFromMessage,
  formatFileOperations,
  serializeConversation,
  SUMMARIZATION_PROMPT,
  SUMMARIZATION_SYSTEM_PROMPT,
  TURN_PREFIX_SUMMARIZATION_PROMPT,
  UPDATE_SUMMARIZATION_PROMPT,
  compactionSummaryText,
  type CompactionSettings,
  summaryFailure,
} from "./utils.js";

export type CompactionSourceEntry =
  | Readonly<{
      id: string;
      kind: "message";
      message: import("@earendil-works/pi-ai").Message;
    }>
  | Readonly<{
      id: string;
      kind: "compaction";
      /** Pi's entry head: the first entry the summary keeps. */
      head: string;
      /** Pi's entry model message: the summary the model reads. */
      model: import("@earendil-works/pi-ai").UserMessage;
      usage: Usage;
      details?: Readonly<{
        readFiles: readonly string[];
        modifiedFiles: readonly string[];
      }>;
    }>;

export type CompactionPreparation = Readonly<{
  firstKeptEntryId: string;
  messagesToSummarize: readonly import("@earendil-works/pi-ai").Message[];
  turnPrefixMessages: readonly import("@earendil-works/pi-ai").Message[];
  isSplitTurn: boolean;
  tokensBefore: number;
  previousSummary?: string;
  fileOps: ReturnType<typeof createFileOps>;
  settings: CompactionSettings;
}>;

export type CompactionResult = Readonly<{
  summary: string;
  firstKeptEntryId: string;
  tokensBefore: number;
  usage: Usage;
  details: Readonly<{
    readFiles: readonly string[];
    modifiedFiles: readonly string[];
  }>;
}>;

/** A summary is a model call: its bounded provider message is shown. */
function failure(message: string): ShiitakeError {
  return new ShiitakeError("model", "rejected", message);
}

function combineUsage(first: Usage, second: Usage): Usage {
  const combined: Usage = {
    input: first.input + second.input,
    output: first.output + second.output,
    cacheRead: first.cacheRead + second.cacheRead,
    cacheWrite: first.cacheWrite + second.cacheWrite,
    totalTokens: first.totalTokens + second.totalTokens,
    cost: {
      input: first.cost.input + second.cost.input,
      output: first.cost.output + second.cost.output,
      cacheRead: first.cost.cacheRead + second.cost.cacheRead,
      cacheWrite: first.cost.cacheWrite + second.cost.cacheWrite,
      total: first.cost.total + second.cost.total,
    },
  };
  if (first.cacheWrite1h !== undefined || second.cacheWrite1h !== undefined)
    combined.cacheWrite1h =
      (first.cacheWrite1h ?? 0) + (second.cacheWrite1h ?? 0);
  if (first.reasoning !== undefined || second.reasoning !== undefined)
    combined.reasoning = (first.reasoning ?? 0) + (second.reasoning ?? 0);
  return combined;
}

function contextMessages(entries: readonly CompactionSourceEntry[]) {
  let latest: (CompactionSourceEntry & { kind: "compaction" }) | undefined;
  for (const entry of entries) {
    if (entry.kind === "compaction") latest = entry;
  }
  const latestCompaction = latest;
  const keptIndex =
    latestCompaction === undefined
      ? 0
      : Math.max(
          0,
          entries.findIndex(({ id }) => id === latestCompaction.head),
        );
  const messages = entries.flatMap((entry, index) =>
    entry.kind === "message" && index >= keptIndex ? [entry.message] : [],
  );
  if (latestCompaction === undefined) return messages;
  return [latestCompaction.model, ...messages];
}

function extractFileOperations(
  messages: readonly import("@earendil-works/pi-ai").Message[],
  entries: readonly CompactionSourceEntry[],
  previousIndex: number,
) {
  const fileOps = createFileOps();
  const previous = entries[previousIndex];
  if (previous?.kind === "compaction" && previous.details !== undefined) {
    for (const file of previous.details.readFiles) fileOps.read.add(file);
    for (const file of previous.details.modifiedFiles) fileOps.edited.add(file);
  }
  for (const message of messages) extractFileOpsFromMessage(message, fileOps);
  return fileOps;
}

type CutClassification = Readonly<{
  isSplitTurn: boolean;
  turnStartIndex: number;
}>;

/**
 * Whether a result for one of `calls` follows `from` before the next assistant
 * message, so a user message there sits between a tool call and its result.
 */
function resultFollows(
  entries: readonly CompactionSourceEntry[],
  from: number,
  endIndex: number,
  calls: ReadonlySet<string>,
): boolean {
  for (let index = from; index < endIndex; index += 1) {
    const entry = entries[index];
    if (entry?.kind !== "message") continue;
    if (entry.message.role === "assistant") return false;
    if (
      entry.message.role === "toolResult" &&
      calls.has(entry.message.toolCallId)
    )
      return true;
  }
  return false;
}

/**
 * Pi Durable §8.7 rule 1: cut candidates are user and assistant messages.
 * Tool results and system messages never are, so a kept assistant message
 * keeps its tool results; a user message is not one while a result for a call
 * of the assistant before it still follows it.
 */
function cutPoints(
  entries: readonly CompactionSourceEntry[],
  startIndex: number,
  endIndex: number,
): readonly number[] {
  const points: number[] = [];
  let openCalls: ReadonlySet<string> = new Set();
  for (let index = startIndex; index < endIndex; index += 1) {
    const entry = entries[index];
    if (entry?.kind !== "message") continue;
    const message = entry.message;
    if (message.role === "assistant") {
      points.push(index);
      openCalls = new Set(
        message.content.flatMap((block) =>
          block.type === "toolCall" ? [block.id] : [],
        ),
      );
    } else if (
      message.role === "user" &&
      !resultFollows(entries, index + 1, endIndex, openCalls)
    )
      points.push(index);
  }
  return points;
}

/**
 * Pi Durable §8.7 rules 2-3: walk newest to oldest adding each message's
 * estimate; where the sum reaches `keepRecentTokens`, cut at the first
 * candidate at or after that entry, or the newest candidate when none
 * follows. Nothing is compacted while the sum never reaches it.
 */
function selectCutIndex(
  entries: readonly CompactionSourceEntry[],
  startIndex: number,
  endIndex: number,
  keepRecentTokens: number,
): number | undefined {
  const points = cutPoints(entries, startIndex, endIndex);
  let accumulated = 0;
  for (let index = endIndex - 1; index >= startIndex; index -= 1) {
    const entry = entries[index];
    if (entry?.kind !== "message") continue;
    accumulated += estimateMessageTokens(entry.message);
    if (accumulated >= keepRecentTokens)
      return points.find((candidate) => candidate >= index) ?? points.at(-1);
  }
  return undefined;
}

function classifyCut(
  entries: readonly CompactionSourceEntry[],
  startIndex: number,
  cut: number,
): CutClassification {
  const cutEntry = entries[cut];
  const startsTurn =
    cutEntry?.kind === "message" && cutEntry.message.role === "user";
  let turnStartIndex = -1;
  if (!startsTurn) {
    for (let index = cut; index >= startIndex; index -= 1) {
      const entry = entries[index];
      if (entry?.kind === "message" && entry.message.role === "user") {
        turnStartIndex = index;
        break;
      }
    }
  }
  return {
    isSplitTurn: !startsTurn && turnStartIndex >= 0,
    turnStartIndex,
  };
}

function findCutPoint(
  entries: readonly CompactionSourceEntry[],
  startIndex: number,
  endIndex: number,
  keepRecentTokens: number,
) {
  const cut = selectCutIndex(entries, startIndex, endIndex, keepRecentTokens);
  if (cut === undefined)
    return {
      firstKeptEntryIndex: startIndex,
      isSplitTurn: false,
      turnStartIndex: -1,
    };
  return { firstKeptEntryIndex: cut, ...classifyCut(entries, startIndex, cut) };
}

/** Mechanically retained Pi preparation, using only context-visible Woodstock entries. */
export function prepareCompaction(
  entries: readonly CompactionSourceEntry[],
  settings: CompactionSettings,
): CompactionPreparation | undefined {
  const last = entries.at(-1);
  if (last?.kind === "compaction") return undefined;
  let previousSummary: string | undefined;
  let previousIndex = -1;
  let boundaryStart = 0;
  for (let index = entries.length - 1; index >= 0; index -= 1) {
    const entry = entries[index];
    if (entry?.kind === "compaction") {
      previousSummary = compactionSummaryText(entry.model);
      previousIndex = index;
      const kept = entries.findIndex(
        (candidate) => candidate.id === entry.head,
      );
      boundaryStart = kept >= 0 ? kept : index + 1;
      break;
    }
  }
  const visible = entries.flatMap((entry, index) =>
    entry.kind === "message" && index >= boundaryStart ? [entry] : [],
  );
  const tokensBefore = estimateContextTokens(contextMessages(entries)).tokens;
  const cut = findCutPoint(
    visible,
    0,
    visible.length,
    settings.keepRecentTokens,
  );
  const firstKept = visible[cut.firstKeptEntryIndex];
  if (firstKept?.kind !== "message") return undefined;
  const historyEnd = cut.isSplitTurn
    ? cut.turnStartIndex
    : cut.firstKeptEntryIndex;
  const messagesToSummarize = Object.freeze(
    visible.slice(0, historyEnd).map(({ message }) => message),
  );
  const turnPrefixMessages = Object.freeze(
    cut.isSplitTurn
      ? visible
          .slice(cut.turnStartIndex, cut.firstKeptEntryIndex)
          .map(({ message }) => message)
      : [],
  );
  if (messagesToSummarize.length === 0 && turnPrefixMessages.length === 0)
    return undefined;
  const fileOps = extractFileOperations(
    messagesToSummarize,
    entries,
    previousIndex,
  );
  for (const message of turnPrefixMessages)
    extractFileOpsFromMessage(message, fileOps);
  const base = {
    firstKeptEntryId: firstKept.id,
    messagesToSummarize,
    turnPrefixMessages,
    isSplitTurn: cut.isSplitTurn,
    tokensBefore,
    fileOps,
    settings,
  };
  return previousSummary === undefined ? base : { ...base, previousSummary };
}

// Pi v0.84.4's compaction retry defaults; retryAssistantCall owns the retry policy.
const RETRY_POLICY: RetryPolicy = {
  enabled: true,
  maxRetries: 3,
  baseDelayMs: 2000,
};

function completeSummarization(
  model: Model<Api>,
  context: Context,
  options: SimpleStreamOptions,
  streamFn?: MuleStreamFunction,
) {
  const requestOptions = {
    ...options,
    cacheRetention: "none" as const,
    sessionId: options.sessionId ?? uuidv7(),
  };
  return retryAssistantCall(
    () =>
      streamFn
        ? Promise.resolve(
            streamFn(model, normalizeContext(context), requestOptions),
          ).then((stream) => stream.result())
        : import("@earendil-works/pi-ai/compat").then(({ completeSimple }) =>
            completeSimple(model, context, requestOptions),
          ),
    RETRY_POLICY,
    requestOptions.signal,
  );
}

function buildContext(promptText: string): Context {
  return {
    systemPrompt: SUMMARIZATION_SYSTEM_PROMPT,
    messages: [
      {
        role: "user",
        content: [{ type: "text", text: promptText }],
        timestamp: Date.now(),
      },
    ],
  };
}

function historyPrompt(
  preparation: CompactionPreparation,
  hasPreviousSummary: boolean,
  instructions: string | undefined,
): string {
  const previousSummary = preparation.previousSummary ?? "";
  const basePrompt = hasPreviousSummary
    ? UPDATE_SUMMARIZATION_PROMPT
    : SUMMARIZATION_PROMPT;
  const focused =
    instructions === undefined
      ? basePrompt
      : `${basePrompt}\n\nAdditional focus: ${instructions}`;
  return `<conversation>\n${serializeConversation(preparation.messagesToSummarize)}\n</conversation>\n\n${hasPreviousSummary ? `<previous-summary>\n${previousSummary}\n</previous-summary>\n\n` : ""}${focused}`;
}

function prefixPrompt(preparation: CompactionPreparation): string {
  return `# Conversation\n${serializeConversation(preparation.turnPrefixMessages)}\n\n# Instructions\n${TURN_PREFIX_SUMMARIZATION_PROMPT}`;
}

async function summarize(
  model: Model<Api>,
  maxTokens: number,
  signal: AbortSignal | undefined,
  prompt: string,
  label: string,
  streamFn?: MuleStreamFunction,
): Promise<AssistantMessage> {
  const options: SimpleStreamOptions =
    signal === undefined ? { maxTokens } : { maxTokens, signal };
  const response = await completeSummarization(
    model,
    buildContext(prompt),
    options,
    streamFn,
  );
  const failureMessage = summaryFailure(response, label);
  if (failureMessage !== undefined) throw new Error(failureMessage);
  return response;
}

function historySummary(
  preparation: CompactionPreparation,
  model: Model<Api>,
  maxTokens: number,
  signal: AbortSignal | undefined,
  instructions: string | undefined,
  streamFn?: MuleStreamFunction,
): Promise<AssistantMessage | undefined> {
  if (preparation.messagesToSummarize.length === 0)
    return Promise.resolve(undefined);
  const hasPreviousSummary =
    preparation.previousSummary !== undefined &&
    preparation.previousSummary.length > 0;
  return summarize(
    model,
    maxTokens,
    signal,
    historyPrompt(preparation, hasPreviousSummary, instructions),
    "Summarization",
    streamFn,
  );
}

function prefixSummary(
  preparation: CompactionPreparation,
  model: Model<Api>,
  signal: AbortSignal | undefined,
  streamFn?: MuleStreamFunction,
): Promise<AssistantMessage | undefined> {
  if (!preparation.isSplitTurn || preparation.turnPrefixMessages.length === 0)
    return Promise.resolve(undefined);
  return summarize(
    model,
    Math.min(
      Math.floor(0.5 * preparation.settings.reserveTokens),
      model.maxTokens > 0 ? model.maxTokens : Number.POSITIVE_INFINITY,
    ),
    signal,
    prefixPrompt(preparation),
    "Turn prefix summarization",
    streamFn,
  );
}

function composeCompactionResult(
  preparation: CompactionPreparation,
  historyResult: AssistantMessage | undefined,
  turnPrefixResult: AssistantMessage | undefined,
): CompactionResult {
  let summary: string;
  let usage: Usage;
  if (preparation.isSplitTurn) {
    if (turnPrefixResult === undefined)
      throw new Error("Split-turn compaction did not produce a prefix summary");
    // With no new history to summarize, the previous summary still stands.
    const historyText =
      historyResult === undefined
        ? (preparation.previousSummary ?? "No prior history.")
        : contentText(historyResult.content);
    summary = `${historyText}\n\n---\n\n**Turn Context (split turn):**\n\n${contentText(turnPrefixResult.content)}`;
    usage =
      historyResult === undefined
        ? turnPrefixResult.usage
        : combineUsage(historyResult.usage, turnPrefixResult.usage);
  } else {
    if (historyResult === undefined)
      throw new Error("Compaction did not produce a history summary");
    summary = contentText(historyResult.content);
    usage = historyResult.usage;
  }
  const files = computeFileLists(preparation.fileOps);
  return {
    summary:
      summary + formatFileOperations(files.readFiles, files.modifiedFiles),
    firstKeptEntryId: preparation.firstKeptEntryId,
    tokensBefore: preparation.tokensBefore,
    usage,
    details: files,
  };
}

async function compactPromise(
  preparation: CompactionPreparation,
  model: Model<Api>,
  signal: AbortSignal | undefined,
  instructions: string | undefined,
  streamFn?: MuleStreamFunction,
): Promise<CompactionResult> {
  const maxTokens = Math.min(
    Math.floor(0.8 * preparation.settings.reserveTokens),
    model.maxTokens > 0 ? model.maxTokens : Number.POSITIVE_INFINITY,
  );
  // Pi runs the two summaries one after the other: in order, and never two
  // concurrent calls against a single-concurrency provider (Pi #5536).
  const historyResult = await historySummary(
    preparation,
    model,
    maxTokens,
    signal,
    instructions,
    streamFn,
  );
  const turnPrefixResult =
    preparation.isSplitTurn && preparation.turnPrefixMessages.length > 0
      ? await prefixSummary(preparation, model, signal, streamFn)
      : undefined;
  return composeCompactionResult(preparation, historyResult, turnPrefixResult);
}

/** Generate Pi's history and optional split-turn summaries, in order. */
export function compact(
  preparation: CompactionPreparation,
  model: Model<Api>,
  streamFn?: MuleStreamFunction,
  /** A manual compaction's focus, appended to the history summary prompt. */
  instructions?: string,
): Effect.Effect<CompactionResult, ShiitakeError> {
  return Effect.tryPromise({
    try: (effectSignal) =>
      compactPromise(preparation, model, effectSignal, instructions, streamFn),
    catch: (error) =>
      failure(error instanceof Error ? error.message : String(error)),
  });
}

export function borrowedView(
  preparation: CompactionPreparation,
  result: CompactionResult,
  sessionId: string,
  contextRevision: string,
): BorrowedCompactionViewV0 {
  return {
    sessionId,
    messagesToSummarize: preparation.messagesToSummarize,
    turnPrefixMessages: preparation.turnPrefixMessages,
    result,
    contextRevision,
  };
}
