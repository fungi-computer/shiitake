/**
 * Adapted from the maintained Pi v0.84.4 coding-agent compaction utilities.
 * The only destination delta is carrying Pi's native Message values directly.
 */
import type { AssistantMessage, Message, Usage } from "@earendil-works/pi-ai";
import * as Schema from "effect/Schema";

const isString = Schema.is(Schema.String);

const COMPACTION_SUMMARY_PREFIX = `The conversation history before this point was compacted into the following summary:

<summary>
`;

const COMPACTION_SUMMARY_SUFFIX = `
</summary>`;

/** Pi's native compaction boundary projection, retained without a custom role. */
export function compactionSummaryMessage(
  summary: string,
  timestamp: number,
): Message {
  return {
    role: "user",
    content: [
      {
        type: "text",
        text: COMPACTION_SUMMARY_PREFIX + summary + COMPACTION_SUMMARY_SUFFIX,
      },
    ],
    timestamp,
  };
}

export type FileOperations = {
  edited: Set<string>;
  read: Set<string>;
  written: Set<string>;
};

export function createFileOps(): FileOperations {
  return { edited: new Set(), read: new Set(), written: new Set() };
}

export function extractFileOpsFromMessage(
  message: Message,
  fileOps: FileOperations,
): void {
  if (message.role !== "assistant") return;
  for (const block of message.content) {
    if (block.type !== "toolCall") continue;
    const path: unknown = block.arguments.path;
    if (!isString(path)) continue;
    if (block.name === "edit") fileOps.edited.add(path);
    if (block.name === "read") fileOps.read.add(path);
    if (block.name === "write") fileOps.written.add(path);
  }
}

export function computeFileLists(fileOps: FileOperations) {
  const modified = new Set([...fileOps.edited, ...fileOps.written]);
  return {
    modifiedFiles: [...modified].sort(),
    readFiles: [...fileOps.read].filter((file) => !modified.has(file)).sort(),
  };
}

export function formatFileOperations(
  readFiles: readonly string[],
  modifiedFiles: readonly string[],
): string {
  const sections: string[] = [];
  if (readFiles.length > 0)
    sections.push(`<read-files>\n${readFiles.join("\n")}\n</read-files>`);
  if (modifiedFiles.length > 0)
    sections.push(
      `<modified-files>\n${modifiedFiles.join("\n")}\n</modified-files>`,
    );
  return sections.length === 0 ? "" : `\n\n${sections.join("\n\n")}`;
}

const TOOL_RESULT_MAX_CHARS = 2000;

function textContent(
  content: readonly { type: string; text?: string }[],
): string {
  return content
    .filter((part) => part.type === "text")
    .map((part) => part.text ?? "")
    .join("");
}

function truncateForSummary(text: string): string {
  if (text.length <= TOOL_RESULT_MAX_CHARS) return text;
  const remainingCharacters = String(text.length - TOOL_RESULT_MAX_CHARS);
  return `${text.slice(0, TOOL_RESULT_MAX_CHARS)}\n\n[... ${remainingCharacters} more characters truncated]`;
}

/** Pi v0.84.4's summary serialization, retained as a separate walk. */
function serializeUserMessage(message: Extract<Message, { role: "user" }>) {
  const content = isString(message.content)
    ? message.content
    : textContent(message.content);
  return content ? `[User]: ${content}` : undefined;
}

function serializeAssistantMessage(
  message: Extract<Message, { role: "assistant" }>,
): string[] {
  const text: string[] = [];
  const thinking: string[] = [];
  const calls: string[] = [];
  for (const block of message.content) {
    if (block.type === "text") text.push(block.text);
    else if (block.type === "thinking") thinking.push(block.thinking);
    else {
      const args = Object.entries(block.arguments)
        .map(([key, value]) => `${key}=${JSON.stringify(value)}`)
        .join(", ");
      calls.push(`${block.name}(${args})`);
    }
  }
  const parts: string[] = [];
  if (thinking.length > 0)
    parts.push(`[Assistant thinking]: ${thinking.join("\n")}`);
  if (text.length > 0) parts.push(`[Assistant]: ${text.join("\n")}`);
  if (calls.length > 0)
    parts.push(`[Assistant tool calls]: ${calls.join("; ")}`);
  return parts;
}

function serializeToolResultMessage(
  message: Extract<Message, { role: "toolResult" }>,
) {
  const content = textContent(message.content);
  return content ? `[Tool result]: ${truncateForSummary(content)}` : undefined;
}

export function serializeConversation(messages: readonly Message[]): string {
  const parts: string[] = [];
  for (const message of messages) {
    if (message.role === "system") {
      const content = isString(message.content)
        ? message.content
        : textContent(message.content);
      if (content) parts.push(`[System]: ${content}`);
    } else if (message.role === "user") {
      const serialized = serializeUserMessage(message);
      if (serialized !== undefined) parts.push(serialized);
    } else if (message.role === "assistant") {
      parts.push(...serializeAssistantMessage(message));
    } else {
      const serialized = serializeToolResultMessage(message);
      if (serialized !== undefined) parts.push(serialized);
    }
  }
  return parts.join("\n\n");
}

const ESTIMATED_IMAGE_CHARS = 4800;

function estimateTextAndImageContentChars(
  content: string | readonly { type: string; text?: string }[],
): number {
  if (isString(content)) return content.length;
  return content.reduce(
    (chars, block) =>
      chars +
      (block.type === "text"
        ? (block.text?.length ?? 0)
        : block.type === "image"
          ? ESTIMATED_IMAGE_CHARS
          : 0),
    0,
  );
}

/** Pi's chars/4 estimator, including its image and tool-call constants. */
export function estimateTokens(message: Message): number {
  let chars = 0;
  switch (message.role) {
    case "system":
      return Math.ceil(estimateTextAndImageContentChars(message.content) / 4);
    case "user":
      return Math.ceil(estimateTextAndImageContentChars(message.content) / 4);
    case "assistant":
      for (const block of message.content) {
        if (block.type === "text") chars += block.text.length;
        else if (block.type === "thinking") chars += block.thinking.length;
        else
          chars += block.name.length + JSON.stringify(block.arguments).length;
      }
      return Math.ceil(chars / 4);
    case "toolResult":
      return Math.ceil(estimateTextAndImageContentChars(message.content) / 4);
  }
}

function calculateContextTokens(usage: Usage): number {
  return (
    usage.totalTokens ||
    usage.input + usage.output + usage.cacheRead + usage.cacheWrite
  );
}

function validAssistantUsage(message: Message): Usage | undefined {
  if (message.role !== "assistant") return undefined;
  if (message.stopReason === "aborted" || message.stopReason === "error")
    return undefined;
  return calculateContextTokens(message.usage) > 0 ? message.usage : undefined;
}

export function estimateContextTokens(messages: readonly Message[]) {
  let index: number | null = null;
  let usageTokens = 0;
  for (let cursor = messages.length - 1; cursor >= 0; cursor -= 1) {
    const message = messages[cursor];
    if (message === undefined) continue;
    const usage = validAssistantUsage(message);
    if (usage !== undefined) {
      index = cursor;
      usageTokens = calculateContextTokens(usage);
      break;
    }
  }
  if (index === null) {
    const tokens = messages.reduce(
      (sum, message) => sum + estimateTokens(message),
      0,
    );
    return {
      tokens,
      usageTokens: 0,
      trailingTokens: tokens,
      lastUsageIndex: null,
    };
  }
  const trailingTokens = messages
    .slice(index + 1)
    .reduce((sum, message) => sum + estimateTokens(message), 0);
  return {
    tokens: usageTokens + trailingTokens,
    usageTokens,
    trailingTokens,
    lastUsageIndex: index,
  };
}

export function shouldCompact(
  contextTokens: number,
  contextWindow: number,
  settings: CompactionSettings,
): boolean {
  if (!settings.enabled) return false;
  return contextTokens > contextWindow - settings.reserveTokens;
}

export type CompactionSettings = {
  enabled: boolean;
  reserveTokens: number;
  keepRecentTokens: number;
};

export const DEFAULT_COMPACTION_SETTINGS: CompactionSettings = {
  enabled: true,
  reserveTokens: 16384,
  keepRecentTokens: 20000,
};

export const SUMMARIZATION_SYSTEM_PROMPT = `You are a context summarization assistant. Your task is to read a conversation between a user and an AI coding assistant, then produce a structured summary following the exact format specified.

Do NOT continue the conversation. Do NOT respond to any questions in the conversation. ONLY output the structured summary.`;

export const SUMMARIZATION_PROMPT = `The messages above are a conversation to summarize. Create a structured context checkpoint summary that another LLM will use to continue the work.

Use this EXACT format:

## Goal
[What is the user trying to accomplish? Can be multiple items if the session covers different tasks.]

## Constraints & Preferences
- [Any constraints, preferences, or requirements mentioned by user]
- [Or "(none)" if none were mentioned]

## Progress
### Done
- [x] [Completed tasks/changes]

### In Progress
- [ ] [Current work]

### Blocked
- [Issues preventing progress, if any]

## Key Decisions
- **[Decision]**: [Brief rationale]

## Next Steps
1. [Ordered list of what should happen next]

## Critical Context
- [Any data, examples, or references needed to continue]
- [Or "(none)" if not applicable]

Keep each section concise. Preserve exact file paths, function names, and error messages.`;

const UPDATE_SUMMARIZATION_INSTRUCTIONS = `Update the existing structured summary with new information. RULES:
- PRESERVE all existing information from the previous summary
- ADD new progress, decisions, and context from the new messages
- UPDATE the Progress section: move items from "In Progress" to "Done" when completed
- UPDATE "Next Steps" based on what was accomplished
- PRESERVE exact file paths, function names, and error messages
- If something is no longer relevant, you may remove it

Use this EXACT format:

## Goal
[Preserve existing goals, add new ones if the task expanded]

## Constraints & Preferences
- [Preserve existing, add new ones discovered]

## Progress
### Done
- [x] [Include previously done items AND newly completed items]

### In Progress
- [ ] [Current work - update based on progress]

### Blocked
- [Current blockers - remove if resolved]

## Key Decisions
- **[Decision]**: [Brief rationale] (preserve all previous, add new)

## Next Steps
1. [Update based on current state]

## Critical Context
- [Preserve important context, add new if needed]

Keep each section concise. Preserve exact file paths, function names, and error messages.`;

export const UPDATE_SUMMARIZATION_PROMPT = `The messages above are NEW conversation messages to incorporate into the existing summary provided in <previous-summary> tags.

${UPDATE_SUMMARIZATION_INSTRUCTIONS}`;

// Pi 0.99.2's split-turn prompt (#9908): the earlier wording read as an
// instruction to reconstruct hidden messages, which some models refused.
export const TURN_PREFIX_SUMMARIZATION_PROMPT = `The messages above are earlier context from an ongoing conversation. Later messages are stored separately and do not need to be reconstructed.

Create a concise checkpoint of the user's request and the progress shown above. This checkpoint will be placed before the later messages so the conversation can continue with the necessary context.

## Original Request
[What did the user ask for?]

## Progress So Far
- [Key decisions and work completed in these messages]

## Context Needed to Continue
- [Information from these messages needed to understand the later work]

Only summarize information explicitly present above. Do not infer or recreate later messages.`;

export function summaryFailure(
  response: AssistantMessage,
  label: string,
): string | undefined {
  if (response.stopReason === "error") {
    const errorMessage = response.errorMessage;
    return `${label} failed: ${errorMessage !== undefined && errorMessage.length > 0 ? errorMessage : "Unknown error"}`;
  }
  if (response.stopReason === "length")
    return `${label} failed: generation hit the token cap and the summary is incomplete`;
  return response.content.some((part) => part.type === "toolCall")
    ? `${label} attempted to call a tool`
    : undefined;
}
