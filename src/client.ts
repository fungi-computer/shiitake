import type { SessionAttention } from "./session-attention.js";
import type { CommandSettlement } from "./internal/contracts.js";
export type { CommandSettlement } from "./internal/contracts.js";
export { type SessionAttention } from "./session-attention.js";
import type { SandboxFailureEvent } from "@fungi.computer/mycelium";
import type { ModelPreference } from "./internal/model-preference.js";
import type { Message } from "@fungi.computer/mule/effect";
import type {
  ExtensionCommandCallerV0,
  ExtensionSlotV0,
  ExtensionStatusV0,
  Json,
} from "./extensions.js";
import type {
  AbortCommand,
  BranchCommand,
  CompactCommand,
  OperatorCommand,
  TitleCommand,
  CancelCommand,
  ForkCommand,
  PromptCommand,
  PromptDelivery,
  SessionCommand,
} from "./internal/client-boundary.js";

export {
  createShiitakeClient,
  type ShiitakeClientOptions,
} from "./remote-client.js";

export {
  bindAgentExtension,
  bindSessionExtension,
  type BoundAgentExtensionV0,
  type BoundCallV0,
  type BoundSessionExtensionV0,
  type ExtensionViewV0,
  type ScopedCommandsV0,
} from "./extension-client.js";

export {
  AcceptedSchema,
  DelegationReceiptSchema,
  HeldSchema,
  parseAccepted,
  parseDelegationReceipt,
  parseCancelResult,
  parseLatestPendingPrompt,
  parseWithdrawResult,
  parseForkedSession,
  parseSessionFrame,
  parseSessionSnapshot,
  parseSessionSummaryPage,
  parseShiitakeError,
} from "./client-wire.js";

export type {
  AbortCommand,
  BranchCommand,
  CompactCommand,
  CancelCommand,
  ForkCommand,
  OperatorCommand,
  PromptCommand,
  PromptDelivery,
  SessionCommand,
  TitleCommand,
} from "./internal/client-boundary.js";

export {
  participantSnapshot,
  visitorPendingPrompt,
  visitorSnapshot,
  visitorView,
  type ParticipantPolicy,
  type VisitorLine,
  type VisitorPendingPrompt,
  type VisitorView,
} from "./client-visitor.js";
/** Semantic acknowledgement returned after one durable acceptance commit. */
export type Accepted = Readonly<{
  /** Indicates that the owner transaction committed. */
  status: "accepted";
  /** Owner-issued durable identity for the accepted work. */
  commandId: string;
  /** The recipient's Session, when this is delegated mail acceptance. */
  sessionId?: string;
}>;

/** Durable relay custody, not acceptance of a command by the recipient. */
export type Held = Readonly<{
  status: "held";
  reason: "recipient_offline";
  message: "waiting: recipient offline";
}>;

/** A delegation send can be accepted by the Agent or held by its relay. */
export type DelegationReceipt = Accepted | Held;

/**
 * Cancelling found nothing left to cancel: the command is unknown to this
 * Session, has already started (for an `onlyIfPending` cancel) or has already
 * settled. It is an ordinary outcome, never an error.
 */
export type TooLate = Readonly<{
  status: "too_late";
  /** The command the caller tried to cancel. */
  commandId: string;
}>;

/** No command has been admitted under this request key in the addressed Session. */
export type UnknownRequest = Readonly<{ status: "unknown" }>;

/** Outcome of one cancel command or request-based recall. */
export type CancelResult = Accepted | TooLate | UnknownRequest;

/** Atomic admission resolution: accepted work is kept; an unknown key is fenced forever. */
export type WithdrawResult = Accepted | UnknownRequest;

/** Result returned after a fork commits a new cold-readable Session. */
export type ForkedSession = Readonly<{
  /** Identity of the newly created Session. */
  sessionId: string;
}>;

/** Execute one retained type-discriminated Session command. */
export type SessionCommandHandler = {
  /** Accept a prompt or a compaction request as queued Session work. */
  (input: PromptCommand | CompactCommand): Promise<Accepted>;
  /** Cancel one accepted command, or report that it is too late. */
  (input: CancelCommand): Promise<CancelResult>;
  /** Abort the active run, select a lineage leaf, record an operator line, or set the title. */
  (
    input: AbortCommand | BranchCommand | OperatorCommand | TitleCommand,
  ): Promise<undefined>;
  /** Fork one complete entry lineage into a new Session. */
  (input: ForkCommand): Promise<ForkedSession>;
  /** Execute an already-decoded command union. */
  (
    input: SessionCommand,
  ): Promise<Accepted | CancelResult | ForkedSession | undefined>;
};

/** Optional controls for the Promise-friendly text sender. */
export type SendOptions = Readonly<{
  /** Images retained alongside text in the accepted user message. */
  images?: PromptCommand["images"];
  /** Deliver at the next safe active-run boundary or after the current run. */
  delivery?: PromptDelivery;
  /** Private retry key, generated once when omitted; not the admitted command ID. */
  requestId?: string;
}>;

/**
 * Model-facing Pi content. Application tool display payloads belong to the
 * surrounding entry's `data`, never to a message sent back to a provider.
 */
export type ModelMessage =
  | Exclude<Message, { role: "toolResult" }>
  | Omit<Extract<Message, { role: "toolResult" }>, "details">;

/** One completed model message and its separate application payload. */
export type SessionMessage = Readonly<{
  /** Caller identity of a prompt or operator line; absent on generated messages. */
  requestId?: string;
  /**
   * Opaque metadata the in-process host attached; Shiitake never reads it.
   * Decode it with the host's own schema.
   */
  meta?: Json;
  /** Opaque application display payload, independent of host metadata. */
  data?: Json;
  /** Durable extension producer of this user message; absent for human prompts. */
  origin?: Readonly<{ extensionId: string; correlation: string | null }>;
  /** Stable conversation identity; live progress completes under this value. */
  id: string;
  /** Woodstock order of the durable entry carrying this message. */
  sequence: number;
  /** Owner-issued accepted work correlation for this message, otherwise null. */
  commandId: string | null;
  /** Model content; provider replay metadata remains unchanged. */
  value: ModelMessage;
}>;

/** Minimal constructor seam for runtimes without a global EventSource. */
export type ShiitakeEventSourceConstructor = new (
  url: string,
) => Pick<EventTarget, "addEventListener"> &
  Pick<EventSource, "close"> & { readonly readyState: number };

/** Durable outcome for one accepted work item on the selected Session lineage. */
export type SessionTerminal = Readonly<{
  /** Accepted work correlated through the command's Woodstock work_id. */
  commandId: string;
  /** Durable terminal outcome; this is the settlement authority. */
  outcome: "completed" | "cancelled" | "failed" | "interrupted";
  /** Woodstock order of the durable terminal entry. */
  sequence: number;
  /**
   * The newest failure message the work recorded (an assistant error or a
   * final retry error), exactly as stored; null when it recorded none.
   */
  diagnostic: string | null;
  /**
   * Opaque host metadata of the command that ended this work: the
   * cancellation's, when a host-attributed cancel ended it. Shiitake never
   * reads it.
   */
  meta?: Json;
}>;

/** Blocking executor identity and Session display metadata, never messages or provider data. */
export type AgentRunningWork =
  | Readonly<{
      kind: "session";
      sessionId: string;
      commandId: string;
      /** Owner-issued display title; empty when no title is known. */
      title: string;
    }>
  | Readonly<{ kind: "host"; lane: string; jobId: string }>;

/**
 * Raw facts about what a Session is doing now. Shiitake publishes facts only;
 * a client decides what they mean (see Parakeet's `sessionViewState`).
 */
export type SessionActivity = Readonly<{
  /**
   * The Session's place in the Agent's queue. The command id is the accepted
   * work's identity, the one a person may cancel with `onlyIfPending`.
   */
  queue:
    | Readonly<{ state: "idle" }>
    | Readonly<{
        state: "queued";
        commandId: string;
        /** Live work ahead of this command; null means no executor work is active. */
        runningWork: AgentRunningWork | null;
      }>
    | Readonly<{ state: "running"; commandId: string }>;
  /** A cancellation of the head work was requested but has not settled. */
  cancellationRequested: boolean;
  /** The running work's latest retry is waiting to be attempted, else null. */
  retry: Readonly<{
    attempt: number;
    maxAttempts: number;
    delayMs: number;
    errorMessage: string;
    /** When the retry was scheduled, in milliseconds since the epoch. */
    scheduledAt: number;
  }> | null;
  /** Tool with an intent but no outcome yet for the running work, else null. */
  activeTool: string | null;
  /** Woodstock order of the newest model choice on the Session, else null. */
  lastModelChoiceSequence: number | null;
  /** A compaction is running now, started by this trigger; else null. */
  compaction: Readonly<{ trigger: SessionCompactionTrigger }> | null;
}>;

/** Encoded extension slots keyed by extension identity; absent means unavailable. */
export type SessionExtensionSlots = Readonly<Record<string, ExtensionSlotV0>>;

/** Generic Session-scope extension commands. */
export type SessionExtensionControls = Readonly<{
  /** Call one Session-scope command with its encoded input and optional host caller. */
  call: (
    command: string,
    input: Json,
    caller?: ExtensionCommandCallerV0,
  ) => Promise<Json>;
}>;

/** Generic Agent-scope extension controls. */
export type AgentExtensionControls = Readonly<{
  /** Every rostered extension and whether it is running. */
  list: () => Promise<readonly ExtensionStatusV0[]>;
  /** Resume a stopped extension; it replays its settled backlog once. */
  resume: (extensionId: string) => Promise<ExtensionStatusV0>;
  /** Call one Agent-scope command with its encoded input; resolves the encoded output. */
  call: (extensionId: string, command: string, input: Json) => Promise<Json>;
}>;

/** Process-local running tool tail; never a durable Session fact. */
export type SessionToolOutput = Readonly<{
  toolCallId: string;
  revision: number;
  tail: string;
  droppedBytes: number;
  totalBytes: number;
}>;

/** Process-local assistant progress; never a durable Session fact. */
export type SessionProgress = Readonly<{
  /** Stable message identity shared with the eventual completed Pi message. */
  messageId: string;
  revision: number;
  text: string;
  thinking: string;
  /** UTF-16 offsets of the retained tails, not byte offsets. */
  textOffset: number;
  thinkingOffset: number;
  textTruncated: boolean;
  thinkingTruncated: boolean;
}>;

/** Append-only progress on the wire. Offsets count JavaScript string units. */
export type SessionProgressDelta = Readonly<{
  messageId: string;
  revision: number;
  text: Readonly<{ offset: number; append: string }>;
  thinking: Readonly<{ offset: number; append: string }>;
}>;

/** Fold a decoded append once; a missing prefix waits for an owner reset. */
export function appendSessionProgress(
  current: SessionProgress | null,
  delta: SessionProgressDelta,
): SessionProgress | null {
  const previous = current?.messageId === delta.messageId ? current : null;
  if (previous !== null && previous.revision >= delta.revision) return current;
  const textEnd =
    previous === null ? 0 : previous.textOffset + previous.text.length;
  const thinkingEnd =
    previous === null ? 0 : previous.thinkingOffset + previous.thinking.length;
  if (delta.text.offset > textEnd || delta.thinking.offset > thinkingEnd)
    return current;
  return {
    messageId: delta.messageId,
    revision: delta.revision,
    text:
      (previous?.text ?? "") +
      delta.text.append.slice(Math.max(0, textEnd - delta.text.offset)),
    thinking:
      (previous?.thinking ?? "") +
      delta.thinking.append.slice(
        Math.max(0, thinkingEnd - delta.thinking.offset),
      ),
    textOffset: previous?.textOffset ?? 0,
    thinkingOffset: previous?.thinkingOffset ?? 0,
    textTruncated: previous?.textTruncated ?? false,
    thinkingTruncated: previous?.thinkingTruncated ?? false,
  };
}

/** Accepted prompt waiting to enter the conversation, not a scheduling command. */
export type SessionPendingPrompt = Readonly<{
  /** Caller identity, available even before the command response reaches its client. */
  requestId: string;
  /** Host-admitted metadata, retained opaquely before execution. */
  meta?: SessionMessage["meta"];
  /** Durable extension producer, absent for human prompts. */
  origin?: SessionMessage["origin"];
  /** Owner-issued identity of the waiting accepted work. */
  commandId: string;
  text: string;
  images?: PromptCommand["images"];
  delivery: PromptDelivery;
}>;

/** Bounded current admission view, in acceptance order rather than execution order. */
export type SessionPendingPrompts = Readonly<{
  /** Exact number of waiting admissions, including those outside this page. */
  total: number;
  prompts: readonly SessionPendingPrompt[];
  /** Additional waiting admissions did not fit this view. */
  hasMore: boolean;
}>;

/**
 * Minimal full-history compaction boundary marker. `sequence` is the
 * Woodstock order of the row this marker precedes (ties sort the marker
 * first); `at` is when the compaction ran. The agent's model context is
 * unaffected by this marker; it exists only for the client timeline.
 */
export type SessionCompactionMarker = Readonly<{
  /** Stable identity of the durable compaction entry. */
  id: string;
  /** Woodstock order of the row this marker immediately precedes. */
  sequence: number;
  /** When the compaction ran, in milliseconds since the epoch. */
  at: number;
  /** Estimated context tokens the compaction started from. */
  tokensBefore: number;
  /** Estimated context tokens right after it; null for older compactions. */
  tokensAfter: number | null;
  /** What started it; null for compactions recorded before triggers were. */
  trigger: SessionCompactionTrigger | null;
}>;

/** What started a compaction: the user, the context threshold, or an overflow. */
export type SessionCompactionTrigger = "manual" | "threshold" | "overflow";

/**
 * A compaction that changed nothing: a manual request with nothing to
 * summarize yet, a compaction that failed, or a finished background summary
 * dropped unplaced: stale because the context had moved past its head, or
 * insufficient because placing it would have left the context over the
 * limit. Automatic failures are never silent; each one is a notice at its own
 * place in the timeline.
 */
export type SessionCompactionNotice = Readonly<{
  /** Stable identity of the durable notice entry. */
  id: string;
  /** Woodstock order of the notice entry. */
  sequence: number;
  /** When it was recorded, in milliseconds since the epoch. */
  at: number;
  trigger: SessionCompactionTrigger;
  result: "nothing_to_compact" | "failed" | "stale" | "insufficient";
  /** A failure's bounded reason; null for the other results. */
  diagnostic: string | null;
}>;

/**
 * The Session's context use as compaction measures it, against the window of
 * the model a run would select now; the window is null when that model
 * reports none.
 */
export type SessionContext = Readonly<{
  tokens: number;
  window: number | null;
}>;

/** Bounded Session baseline at one opaque owner cursor. */
export type SessionSnapshot = Readonly<{
  /** Session represented by the snapshot. */
  sessionId: string;
  /** Opaque owner cursor at the snapshot boundary, or null when empty. */
  cursor: string | null;
  /** Whether an older durable entry exists before this bounded page. */
  hasMore: boolean;
  /**
   * Opaque cursor naming the oldest row in this page; pass as `before` on
   * `read()` to page further back. Null once no older history remains.
   */
  oldestCursor?: string | null;
  /** Bounded exact Pi messages visible at the cursor. */
  messages: readonly SessionMessage[];
  /** Durable run_terminal facts visible at the cursor. */
  terminals: readonly SessionTerminal[];
  /** Every compaction boundary marker carried by this bounded page. */
  compactions?: readonly SessionCompactionMarker[];
  /** Every compaction notice carried by this bounded page. */
  compactionNotices?: readonly SessionCompactionNotice[];
  /** Operator diagnostics, excluded from the model conversation. */
  sandboxFailures?: readonly SandboxFailureEvent[];
  /** Current accepted prompts not yet entered or settled. */
  pending: SessionPendingPrompts;
  /** Raw facts about the Session's current activity. */
  activity: SessionActivity;
  /** Latest replaceable progress snapshot, if an assistant message is live. */
  progress: SessionProgress | null;
  /** Current bounded tool tails. Empty outside this process's running work. */
  toolOutput: readonly SessionToolOutput[];
  /** Encoded projection slot of each extension granted one. */
  extensions: SessionExtensionSlots;
  /** Open requests for human attention from extensions. */
  attention: readonly SessionAttention[];
  /** Context use at the tip; before reads carry this tip figure too, and it
   * is absent before the Session has context. */
  context?: SessionContext;
}>;

/** One bounded replay or live frame. */
export type SessionFrame =
  | Readonly<{
      /** Process-local activity change for the Session's current queue head. */
      kind: "activity";
      activity: SessionActivity;
    }>
  | Readonly<{
      kind: "tool_output";
      toolOutput: readonly SessionToolOutput[];
      activity: SessionActivity;
    }>
  | Readonly<{
      /** Bounded Session-projection update discriminator. */
      kind: "update";
      /** Whether replay or a post-commit hint discovered the update. */
      delivery: "replay" | "live";
      /** Opaque cursor naming the last consumed private entry. */
      cursor: string | null;
      /** Current accepted prompts, replaced even when the history cursor is unchanged. */
      pending: SessionPendingPrompts;
      /** Newly visible exact Pi messages, possibly empty for status changes. */
      messages: readonly SessionMessage[];
      /** Durable run_terminal facts carried by this bounded update. */
      terminals: readonly SessionTerminal[];
      /** Compaction markers whose durable entry landed in this bounded update. */
      compactions?: readonly SessionCompactionMarker[];
      /** Compaction notices whose durable entry landed in this bounded update. */
      compactionNotices?: readonly SessionCompactionNotice[];
      /** Operator diagnostics, excluded from the model conversation. */
      sandboxFailures?: readonly SandboxFailureEvent[];
      /** Raw facts about the Session's activity at this cursor. */
      activity: SessionActivity;
      /** Encoded projection slot of each extension granted one. */
      extensions: SessionExtensionSlots;
      /** Open requests for human attention from extensions. */
      attention: readonly SessionAttention[];
      /**
       * Context use at the Session tip when the update moved history and is
       * the final page; live pages with more to come and non-final replay pages
       * carry no figure. Absent before the Session has context; absent figures
       * leave the last one in place.
       */
      context?: SessionContext;
    }>
  | Readonly<{
      /** Only new text/thinking; snapshots, not frames, carry retained tails. */
      kind: "progress";
      progress: SessionProgressDelta;
      /** Raw facts about the Session's activity read with this progress. */
      activity: SessionActivity;
    }>
  | Readonly<{
      /** History-reset frame discriminator. */
      kind: "reset";
      /** Stable neutral reason for unavailable continuity. */
      reason: "history_unavailable";
      /** Opaque cursor at the replacement baseline boundary. */
      cursor: string | null;
      /** Fresh owner-atomic replacement baseline. */
      baseline: SessionSnapshot;
    }>;

/** Strict-after replay request. */
export type SubscribeOptions = Readonly<{
  /** Explicit operator inspection; never enable for a model capability. */
  includeSandboxFailures?: boolean;
  /** Resume strictly after this opaque cursor when present. */
  after?: string;
}>;

/** Bounded baseline request. */
export type ReadOptions = Readonly<{
  /** Explicit operator inspection; ordinary reads omit private diagnostics. */
  includeSandboxFailures?: boolean;
  /** Maximum number of messages to include in the bounded baseline. */
  limit?: number;
  /**
   * Page strictly before this opaque cursor (from a snapshot's
   * `oldestCursor`) instead of reading the latest page.
   */
  before?: string;
}>;

/** Race-safe observation request. */
export type WatchOptions = ReadOptions;

/** Race-safe baseline plus live changes resource. */
export type SessionWatch = Readonly<{
  /** Race-safe baseline observed before live changes. */
  snapshot: SessionSnapshot;
  /** Live frames after the baseline boundary. */
  changes: AsyncIterable<SessionFrame>;
  /** Release the live observation resources. */
  close: () => Promise<void>;
}>;

/** Addressable Session interface. Addressing itself performs no I/O. */
export type ShiitakeSession = Readonly<{
  /**
   * Address used to share observation across current client mounts.
   * Remote addresses include the transport endpoint, not durable owner identity.
   */
  address: string;
  /** Execute one retained type-discriminated Session command. */
  command: SessionCommandHandler;
  /** Resolve a stalled request atomically; withdrawn keys can never be admitted later. */
  withdraw: (requestId: string) => Promise<WithdrawResult>;
  /** Cancel the running turn and all queued prompts, retaining their text. */
  abort: () => Promise<void>;
  /** Send text with an optional private caller idempotency identity. */
  send: (text: string, options?: SendOptions) => Promise<Accepted>;
  /** Cancel exactly the accepted work named by its owner-issued identity. */
  cancel: (
    input: Readonly<{ commandId: string; onlyIfPending?: boolean }>,
  ) => Promise<CancelResult>;
  /** Newest pending prompt in this host-authorized identity, never an oldest-page guess. */
  latestPending: () => Promise<SessionPendingPrompt | null>;
  /** Address one rostered extension's Session-scope commands. */
  extension: (extensionId: string) => SessionExtensionControls;
  /** Lineage-local request for later runs; null inherits the Agent default. */
  modelPreference: Readonly<{
    /** Read the saved override; null selects the Agent default. */
    read: () => Promise<ModelPreference | null>;
    /** Save an override for later acquisition without starting a provider call. */
    set: (preference: ModelPreference | null) => Promise<void>;
  }>;
  /** Read a bounded durable baseline. */
  read: (input?: ReadOptions) => Promise<SessionSnapshot>;
  /**
   * Read one exact command's settlement, independent of the selected lineage.
   * Wait at most timeoutMs (0–30000, default 0) for a settlement hint.
   * Timeout returns undefined; abort releases only the read, never the work.
   */
  readSettlement: (
    commandId: string,
    options?: Readonly<{ timeoutMs?: number; signal?: AbortSignal }>,
  ) => Promise<CommandSettlement | undefined>;
  /** Subscribe to strict-after replay and live frames. */
  subscribe: (input: SubscribeOptions) => AsyncIterable<SessionFrame>;
  /** Open a race-safe baseline plus live changes resource. */
  watch: (input?: WatchOptions) => Promise<SessionWatch>;
}>;

/**
 * Owner-decided inbox state, from Watchdog's queue head and latest outcome.
 * `running` covers queued, thinking, streaming, retrying and cancelling;
 * `errored` covers failed, interrupted and `outcome_unknown`; `done` covers
 * completed, cancelled and never run. Any open extension attention produces
 * `needs_you`, taking priority over live and settled work.
 */
export type SessionInboxState = "needs_you" | "running" | "errored" | "done";

/** Read-only saved conversation projection; opening does not update recency. */
export type SessionSummary = Readonly<{
  createdAt: number;
  /** Latest durable conversation change or run transition. */
  lastActivityAt: number;
  /** When the latest run settled; absent until one settles. */
  settledAt?: number;
  title: string;
  state: SessionInboxState;
}>;

/** One listed conversation, or a saved Session its owner could not read. */
export type SessionSummaryRow =
  | (SessionSummary & Readonly<{ sessionId: string }>)
  | Readonly<{ sessionId: string; state: "unavailable" }>;

/**
 * One owner-bounded page: `needs_you`, then `running`, then settled rows, each
 * newest activity first. `next` is an opaque owner cursor.
 */
export type SessionSummaryPage = Readonly<{
  sessions: readonly SessionSummaryRow[];
  next: string | undefined;
}>;

/** Constructed durable harness. */
export type ShiitakeAgent = Readonly<{
  /** Return the same address when creation is retried with this request ID. */
  createSession: (requestId: string) => Promise<string>;
  /**
   * Permanently delete one idle Session and its history; resolves false when
   * it does not exist. A Session with queued or running work is a conflict.
   */
  deleteSession: (sessionId: string) => Promise<boolean>;
  /** Enumerate existing conversations without creating or touching them. */
  listSessions: (after?: string) => Promise<SessionSummaryPage>;
  /** List, resume and call rostered extensions. */
  extensions: AgentExtensionControls;
  /** Address a session without performing I/O. */
  session: (sessionId: string) => ShiitakeSession;
  /** Close the owned harness and release resources. */
  close: () => Promise<void>;
}>;

export type { ModelPreference } from "./internal/model-preference.js";
