import {
  authorMeta,
  messageAuthor,
  FungiMessageMetaSchema,
} from "@fungi.computer/agar/agents";
import type { SessionPendingPrompt, SessionSnapshot } from "./client.js";

/** The host decides which Agent work may be delivered to this participant. */
export type ParticipantPolicy = Readonly<{
  /** Whether this Session command is current for the participant. */
  turnIsCurrent: (sessionId: string, commandId: string) => boolean;
  /** Whether this participant may receive the addressed Agent reply. */
  deliverAgentReply: (
    sessionId: string,
    commandId: string,
    reply: Readonly<{ kind: "message"; messageId: string }>,
  ) => boolean;
}>;

/** Filter participant delivery only; the holder's stored Session is unchanged. */
export function participantSnapshot(
  snapshot: SessionSnapshot,
  owner: ParticipantPolicy,
): SessionSnapshot {
  const active =
    snapshot.activity.queue.state !== "idle" &&
    owner.turnIsCurrent(snapshot.sessionId, snapshot.activity.queue.commandId);
  return {
    ...snapshot,
    messages: snapshot.messages.filter(
      ({ id, commandId, value }) =>
        value.role !== "assistant" ||
        value.provider === "operator" ||
        (commandId !== null &&
          owner.deliverAgentReply(snapshot.sessionId, commandId, {
            kind: "message",
            messageId: id,
          })),
    ),
    progress: active ? snapshot.progress : null,
    activity: active
      ? snapshot.activity
      : {
          ...snapshot.activity,
          queue: { state: "idle" },
          cancellationRequested: false,
          activeTool: null,
          retry: null,
          lastModelChoiceSequence: null,
          compaction: null,
        },
  };
}

/** What a visitor may see of their own conversation Session. */
export type VisitorView = Readonly<{
  status: "idle" | "working";
  lines: readonly Readonly<{
    id: string;
    from: "visitor" | "agent" | "operator";
    text: string;
  }>[];
  progress: string | null;
}>;

/** Public pending facts exclude the host's private metadata and admission origin. */
export type VisitorPendingPrompt = Pick<
  SessionPendingPrompt,
  "commandId" | "requestId" | "text" | "delivery" | "images"
>;

function sourceRequestId(
  meta: SessionSnapshot["messages"][number]["meta"],
): string {
  const parsed = FungiMessageMetaSchema.parse(meta);
  if (parsed.sourceRequestId === undefined)
    throw new Error("Visitor entry is missing its original request identity");
  return parsed.sourceRequestId;
}

/** Project one owner-selected prompt without exposing host identity or correlation metadata. */
export function visitorPendingPrompt(
  prompt: SessionPendingPrompt,
  visitorId: string,
): VisitorPendingPrompt {
  if (messageAuthor(prompt.meta)?.id !== visitorId)
    throw new Error("Visitor pending lookup returned another author's prompt");
  const projected: VisitorPendingPrompt = {
    commandId: prompt.commandId,
    requestId: sourceRequestId(prompt.meta),
    text: prompt.text,
    delivery: prompt.delivery,
  };
  if (prompt.images !== undefined)
    return { ...projected, images: prompt.images };
  return projected;
}

function userText(
  value: Extract<
    SessionSnapshot["messages"][number]["value"],
    { role: "user" }
  >,
): string {
  return Array.isArray(value.content)
    ? value.content
        .flatMap((part) => (part.type === "text" ? [part.text] : []))
        .join("")
    : value.content;
}

function visitorCommands(
  snapshot: SessionSnapshot,
  visitorId: string,
): ReadonlySet<string> {
  const commands = new Set<string>();
  for (const message of snapshot.messages)
    if (
      message.value.role === "user" &&
      messageAuthor(message.meta)?.id === visitorId &&
      message.commandId !== null
    )
      commands.add(message.commandId);
  for (const prompt of snapshot.pending.prompts)
    if (messageAuthor(prompt.meta)?.id === visitorId)
      commands.add(prompt.commandId);
  return commands;
}

function visibleMessages(
  snapshot: SessionSnapshot,
  visitorId: string,
): SessionSnapshot["messages"] {
  const commands = visitorCommands(snapshot, visitorId);
  return snapshot.messages.filter(({ commandId, value, meta }) =>
    value.role === "user"
      ? messageAuthor(meta)?.id === visitorId
      : value.role === "assistant" &&
        (value.provider === "operator" ||
          (commandId !== null && commands.has(commandId))),
  );
}

/** Visitor-scoped Session facts use the visitor's public address, never the internal Session ID. */
export function visitorSnapshot(
  snapshot: SessionSnapshot,
  visitorId: string,
): SessionSnapshot {
  const commands = visitorCommands(snapshot, visitorId);
  const messages: SessionSnapshot["messages"][number][] = [];
  for (const { id, sequence, commandId, value, meta } of visibleMessages(
    snapshot,
    visitorId,
  )) {
    if (value.role === "user") {
      messages.push({
        id,
        sequence,
        commandId,
        requestId: sourceRequestId(meta),
        value: {
          role: "user",
          content: userText(value),
          timestamp: value.timestamp,
        },
      });
    } else if (value.role === "assistant") {
      const message: SessionSnapshot["messages"][number] = {
        id,
        sequence,
        commandId,
        value: {
          role: "assistant",
          content: value.content.filter((part) => part.type === "text"),
          api: "visitor",
          provider: "visitor",
          model: "visitor",
          usage: {
            input: 0,
            output: 0,
            cacheRead: 0,
            cacheWrite: 0,
            totalTokens: 0,
            cost: {
              input: 0,
              output: 0,
              cacheRead: 0,
              cacheWrite: 0,
              total: 0,
            },
          },
          stopReason: value.stopReason,
          timestamp: value.timestamp,
        },
      };
      messages.push(
        value.provider === "operator"
          ? {
              ...message,
              // A visitor never sees which member answered.
              meta: authorMeta({
                id: "channel-operator",
                name: "A person from the team",
              }),
            }
          : message,
      );
    }
  }
  const ownWork =
    snapshot.activity.queue.state !== "idle" &&
    commands.has(snapshot.activity.queue.commandId);
  const pendingPrompts = snapshot.pending.prompts
    .filter(({ meta }) => messageAuthor(meta)?.id === visitorId)
    .map((prompt) => visitorPendingPrompt(prompt, visitorId));
  const projected: SessionSnapshot = {
    sessionId: visitorId,
    cursor: snapshot.cursor,
    hasMore: snapshot.hasMore,
    messages,
    terminals: snapshot.terminals
      .filter(({ commandId }) => commands.has(commandId))
      .map(({ commandId, outcome, sequence }) => ({
        commandId,
        outcome,
        sequence,
        diagnostic: null,
      })),
    pending: {
      total: pendingPrompts.length,
      hasMore: snapshot.pending.hasMore,
      prompts: pendingPrompts,
    },
    activity: {
      queue: !ownWork
        ? { state: "idle" }
        : snapshot.activity.queue.state === "queued"
          ? { ...snapshot.activity.queue, runningWork: null }
          : snapshot.activity.queue,
      cancellationRequested: ownWork && snapshot.activity.cancellationRequested,
      activeTool: null,
      lastModelChoiceSequence: null,
      compaction: null,
      retry:
        !ownWork || snapshot.activity.retry === null
          ? null
          : { ...snapshot.activity.retry, errorMessage: "Chat is retrying" },
    },
    toolOutput: [],
    progress:
      !ownWork || snapshot.progress === null
        ? null
        : { ...snapshot.progress, thinking: "", thinkingTruncated: false },
    extensions: {},
    attention: [],
  };
  return snapshot.oldestCursor === undefined
    ? projected
    : { ...projected, oldestCursor: snapshot.oldestCursor };
}

/** Project only this visitor's host-attributed prompts, their replies, and explicit operator replies. */
export function visitorView(
  snapshot: SessionSnapshot | null,
  visitorId: string,
): VisitorView {
  if (snapshot === null) return { status: "idle", lines: [], progress: null };
  const lines: VisitorView["lines"][number][] = [];
  for (const { id, value } of visibleMessages(snapshot, visitorId)) {
    if (value.role === "user") {
      lines.push({
        id,
        from: "visitor",
        text: userText(value),
      });
    } else if (value.role === "assistant") {
      const text = value.content
        .flatMap((part) => (part.type === "text" ? [part.text] : []))
        .join("")
        .trim();
      if (text.length > 0)
        lines.push({
          id,
          from: value.provider === "operator" ? "operator" : "agent",
          text,
        });
    }
  }
  for (const prompt of snapshot.pending.prompts)
    if (messageAuthor(prompt.meta)?.id === visitorId)
      lines.push({
        id: `pending:${prompt.commandId}`,
        from: "visitor",
        text: prompt.text,
      });
  const commands = visitorCommands(snapshot, visitorId);
  const working =
    (snapshot.activity.queue.state !== "idle" &&
      commands.has(snapshot.activity.queue.commandId)) ||
    snapshot.pending.prompts.some(
      ({ meta }) => messageAuthor(meta)?.id === visitorId,
    );
  const progress = working ? (snapshot.progress?.text.trim() ?? "") : "";
  return {
    status: working ? "working" : "idle",
    lines,
    progress: progress.length > 0 ? progress : null,
  };
}
