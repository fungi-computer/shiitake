/**
 * Adapted from the maintained Pi v0.84.4 coding-agent compaction utilities.
 * Token estimates are pi-ai's own (`@earendil-works/pi-ai/utils/estimate`).
 * The only destination delta is carrying Pi's native Message values directly.
 */
import type {
  AssistantMessage,
  Message,
  UserMessage,
} from "@earendil-works/pi-ai";
import * as Schema from "effect/Schema";

const isString = Schema.is(Schema.String);

const COMPACTION_SUMMARY_PREFIX = `The conversation history before this point was compacted into the following summary:

<summary>
`;

const COMPACTION_SUMMARY_SUFFIX = `
</summary>`;

/**
 * Pi's compaction entry model message (Pi Durable `compaction.ts`): the
 * summary as the user message the model reads, built once when the entry is
 * appended and stored with it.
 */
export function compactionSummaryMessage(
  summary: string,
  timestamp: number,
): UserMessage {
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

/** The summary text inside a stored compaction model message. */
export function compactionSummaryText(message: UserMessage): string {
  const block = Array.isArray(message.content) ? message.content[0] : undefined;
  const text = block?.type === "text" ? block.text : undefined;
  if (
    text === undefined ||
    !text.startsWith(COMPACTION_SUMMARY_PREFIX) ||
    !text.endsWith(COMPACTION_SUMMARY_SUFFIX)
  )
    throw new Error("Compaction model message is not a summary");
  return text.slice(
    COMPACTION_SUMMARY_PREFIX.length,
    text.length - COMPACTION_SUMMARY_SUFFIX.length,
  );
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
  const calls: string[] = [];
  for (const block of message.content) {
    if (block.type === "text") text.push(block.text);
    else if (block.type === "toolCall") {
      const args = Object.entries(block.arguments)
        .map(([key, value]) => `${key}=${JSON.stringify(value)}`)
        .join(", ");
      calls.push(`${block.name}(${args})`);
    }
  }
  const parts: string[] = [];
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

export function shouldCompact(
  contextTokens: number,
  contextWindow: number,
  settings: CompactionSettings,
): boolean {
  if (!settings.enabled) return false;
  return contextTokens > contextWindow - settings.reserveTokens;
}

/**
 * A background compaction starts above the blocking threshold minus
 * `backgroundTokens` and at or below the blocking threshold; past that the
 * blocking check owns it. Token-based only: byte limits are Zookeeper's.
 * Unclamped like Pi Durable generation.ts:322-339 (Levi, 2026-10-17: match Pi); keepRecentTokens gates small windows.
 */
export function shouldCompactInBackground(
  contextTokens: number,
  contextWindow: number,
  settings: CompactionSettings,
): boolean {
  if (!settings.enabled || settings.backgroundTokens === 0) return false;
  const blocking = contextWindow - settings.reserveTokens;
  return (
    contextTokens > blocking - settings.backgroundTokens &&
    !shouldCompact(contextTokens, contextWindow, settings)
  );
}

export type CompactionSettings = {
  enabled: boolean;
  reserveTokens: number;
  keepRecentTokens: number;
  /** Tokens before the blocking threshold at which a background compaction starts; 0 turns it off. */
  backgroundTokens: number;
};

export const DEFAULT_COMPACTION_SETTINGS: CompactionSettings = {
  enabled: true,
  reserveTokens: 16384,
  keepRecentTokens: 20000,
  backgroundTokens: 32768,
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
