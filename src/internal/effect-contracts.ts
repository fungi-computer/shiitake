import type { HostMetaOptions } from "./host-meta.js";
import type { ExtensionRosterV0 } from "../extensions.js";
import type {
  Api,
  AssistantMessage,
  Model,
  MuleStreamFunction,
  RetryPolicy,
} from "@fungi.computer/mule/effect";
import type {
  CapabilitySource,
  ExecutionOptions,
  ModuleRegistration,
} from "@fungi.computer/mycelium";
import type {
  GrantsDelivery,
  GrantsDeliveryResult,
  ToolAccess,
} from "./contracts.js";
import type { Sandbox } from "@fungi.computer/mycelium/effect";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import type * as Scope from "effect/Scope";
import type { WatchdogError } from "@fungi.computer/watchdog";
import type {
  WatchdogExecutionDecision,
  WatchdogRunningJob,
  ConcurrentWatchdogRuntimeEffect,
} from "@fungi.computer/watchdog/effect";

import type {
  AbortCommand,
  Accepted,
  BranchCommand,
  OperatorCommand,
  TitleCommand,
  CancelCommand,
  CancelResult,
  ForkCommand,
  ForkedSession,
  CompactCommand,
  PromptCommand,
  PromptDelivery,
  ReadOptions,
  SessionCommand,
  SessionFrame,
  SessionSnapshot,
  SessionPendingPrompt,
  SessionSummaryPage,
  SubscribeOptions,
  WatchOptions,
} from "../client.js";
import type { ShiitakeError } from "../errors.js";
import type { ExtensionStatusV0, Json } from "../extensions.js";
import type {
  InstructionSnapshot,
  ModelDescription,
  RuntimePendingWork,
  SqliteOwner,
  StoragePressureCheck,
  CompactionOptions,
} from "./contracts.js";

import type { ModelPreference } from "./model-preference.js";

/** Effect-native acquired Pi model and finalizer. */
export type ModelAcquisitionEffect = Readonly<{
  /** Exact Pi model selected for this run. */
  model: Model<Api>;
  /** Optional explicit Pi stream function for custom providers and tests. */
  stream?: MuleStreamFunction;
  /** Provider’s bounded Retry-After for a clean HTTP refusal, if reported. */
  retryAfterMs?: (message: AssistantMessage) => number | undefined;
  /** Typed retry decision for this door's exact message; unknown means false. */
  retryable: (message: AssistantMessage) => boolean;
  /** Owner-authored reason for this door's failure message, never SDK prose. */
  failureReason?: (message: AssistantMessage) => string | undefined;
  /** Effect finalizer for acquired model resources. */
  release: Effect.Effect<void, ShiitakeError>;
}>;

/** Effect-native model acquisition owner. */
export type ModelSourceEffect = Readonly<{
  /** Acquire one selection and its request-scoped egress. */
  acquire: (
    input: Readonly<{ sessionId: string; preference: ModelPreference | null }>,
  ) => Effect.Effect<ModelAcquisitionEffect, ShiitakeError>;
  /** Read the would-be selection's context window; absent leaves it unknown. */
  describe?: (
    input: Readonly<{ sessionId: string; preference: ModelPreference | null }>,
  ) => Effect.Effect<ModelDescription, ShiitakeError>;
}>;

/** Effect-native instruction snapshot owner. */
export type InstructionSourceEffect = Readonly<{
  /** Load the session's bounded instruction snapshot. */
  load: (
    input: Readonly<{ sessionId: string; commandId: string }>,
  ) => Effect.Effect<InstructionSnapshot, ShiitakeError, Scope.Scope>;
}>;

/** Effect-native Mycelium construction inputs. */
export type ToolSourceEffect = Readonly<{
  /** Host-owned limit for one scoped tool invocation. */
  execution?: ExecutionOptions;
  /** Mycelium module descriptors visible in the run scope. */
  modules: readonly ModuleRegistration[];
  /** Dynamic host capability projections acquired by Mycelium. */
  sources: readonly CapabilitySource[];
  /**
   * Effect-native sandbox boundary for scoped capabilities. Adapt
   * Promise-shaped work with Mycelium's `fromTypedSandbox`.
   */
  sandbox: Sandbox;
  /**
   * The Agent-wide access defaults for every tool of the run, as data. A
   * Session's grants the host delivered with the owner's `grants.deliver` override
   * the default key by key. Mycelium reads them at every call; Shiitake never
   * interprets them. Absent is full access.
   */
  access?: ToolAccess;
}>;

/** Effect-native owner for scheduling the opaque durable reconciler. */
export type RuntimeHostEffect = Readonly<{
  /** Bind the one repeatable zero-input reconciliation program. */
  bind: (
    input: Readonly<{
      reconcile: Effect.Effect<void, ShiitakeError>;
      pendingWork: Effect.Effect<RuntimePendingWork, ShiitakeError>;
    }>,
  ) => Effect.Effect<
    Readonly<{
      /** Commit owner mutations together with the host wake guarantee. No external I/O. */
      commit: <A>(
        mutation: Effect.Effect<A, ShiitakeError>,
      ) => Effect.Effect<A, ShiitakeError>;
      /** Request one later reconciliation without running it inline. */
      requestWake: Effect.Effect<void, ShiitakeError>;
      /** Stop callbacks and release the bound host registration. */
      close: Effect.Effect<void, ShiitakeError>;
    }>,
    ShiitakeError
  >;
}>;

/** Validated construction bag for owned Effect composition. */
export type ShiitakeEffectOptions = Readonly<{
  /**
   * The schema of the opaque metadata this host attaches to prompts and
   * operator lines. Shiitake checks, stores and returns it; it never reads it.
   */
  meta?: HostMetaOptions;
  /** Native owner storage and synchronous transaction capability. */
  store: SqliteOwner;
  /** Host storage-pressure check; absent never refuses for storage pressure. */
  storagePressure?: StoragePressureCheck;
  /** Compaction budgets; absent uses Pi's defaults for every model. */
  compaction?: CompactionOptions;
  /** One host lane in Shiitake's existing Watchdog, opened before recovery. */
  hostJobs?: Readonly<{
    lane: string;
    /** Open the host executor before persisted work is recovered. */
    open: (runtime: ConcurrentWatchdogRuntimeEffect) => Readonly<{
      /** Execute a job claimed for this host lane. */
      execute: (
        job: WatchdogRunningJob,
      ) => Effect.Effect<WatchdogExecutionDecision, WatchdogError>;
    }>;
  }>;
  /** Effect-native Pi model owner. */
  model: ModelSourceEffect;
  /** Effect-native instruction snapshot owner. */
  instructions: InstructionSourceEffect;
  /** Effect-native scoped capability inputs, omitted when no tools exist. */
  tools?: ToolSourceEffect;
  /** Runtime owner that schedules the opaque durable reconciler. */
  runtime: RuntimeHostEffect;
  /**
   * The host's explicit extension roster and grants. Required: a host with no
   * extensions passes `{ roster: [], grants: [] }`.
   */
  extensions: ExtensionRosterV0;
  /**
   * Pi's bounded retry policy around each live assistant call. Omitted
   * means `piCodingAgentRetryDefaults` (Pi coding-agent's own numbers).
   */
  retry?: RetryPolicy;
  /**
   * Maximum silence between live provider stream events, in milliseconds.
   * Omitted means `piHttpIdleTimeoutMs` (Pi coding-agent's own default).
   */
  modelStreamIdleTimeoutMs?: number;
}>;

/** Effect-native race-safe watch result. */
export type ShiitakeEffectWatch = Readonly<{
  /** Race-safe durable baseline. */
  snapshot: SessionSnapshot;
  /** Live frames after the baseline boundary. */
  changes: Stream.Stream<SessionFrame, ShiitakeError>;
  /** Effect finalizer for the live observation. */
  close: Effect.Effect<void, ShiitakeError>;
}>;

/** Effect-native addressable Session handle. */
export type ShiitakeEffectCommandHandler = {
  /** Accept one durable prompt command. */
  (
    input: PromptCommand | CompactCommand,
  ): Effect.Effect<Accepted, ShiitakeError>;
  /** Cancel one accepted command, or report that it is too late. */
  (input: CancelCommand): Effect.Effect<CancelResult, ShiitakeError>;
  /** Abort the active run, select a lineage leaf, or record an operator line. */
  (
    input: AbortCommand | BranchCommand | OperatorCommand | TitleCommand,
  ): Effect.Effect<undefined, ShiitakeError>;
  /** Fork one complete entry lineage into a new Session. */
  (input: ForkCommand): Effect.Effect<ForkedSession, ShiitakeError>;
  /** Execute an already-decoded command union. */
  (
    input: SessionCommand,
  ): Effect.Effect<
    Accepted | CancelResult | ForkedSession | undefined,
    ShiitakeError
  >;
};

/** Delivery intent and an optional private retry key for Effect-native sending. */
export type ShiitakeEffectSendOptions = Readonly<{
  /** Images retained alongside text in the accepted user message. */
  images?: PromptCommand["images"];
  delivery?: PromptDelivery;
  requestId?: string;
}>;

/** Effect-native addressable Session handle. */
export type ShiitakeEffectSession = Readonly<{
  /** Execute one retained type-discriminated Session command. */
  command: ShiitakeEffectCommandHandler;
  /** Resolve admission or durably fence a request that has not been admitted. */
  withdraw: (
    requestId: string,
  ) => Effect.Effect<import("../client.js").WithdrawResult, ShiitakeError>;
  /** Cancel the running turn and all queued prompts, retaining their text. */
  abort: Effect.Effect<void, ShiitakeError>;
  /** Send text with one generated or explicitly supplied request identity. */
  send: (
    text: string,
    options?: ShiitakeEffectSendOptions,
  ) => Effect.Effect<Accepted, ShiitakeError>;
  /** Cancel exactly the accepted work named by its owner-issued identity. */
  cancel: (input: {
    commandId: string;
    onlyIfPending?: boolean;
  }) => Effect.Effect<CancelResult, ShiitakeError>;
  /** Newest pending prompt scoped by the host's bound identity. */
  latestPending: Effect.Effect<SessionPendingPrompt | null, ShiitakeError>;
  /** Address one rostered extension's Session-scope commands. */
  extension: (extensionId: string) => ShiitakeEffectSessionExtension;
  /** Lineage-local selection used when the next run acquires its model. */
  modelPreference: Readonly<{
    /** Read the saved override; null selects the Agent default. */
    read: Effect.Effect<ModelPreference | null, ShiitakeError>;
    /** Save an override without starting a provider call. */
    set: (
      preference: ModelPreference | null,
    ) => Effect.Effect<void, ShiitakeError>;
  }>;
  /** Read a bounded durable baseline. */
  read: (input?: ReadOptions) => Effect.Effect<SessionSnapshot, ShiitakeError>;
  /** Subscribe to strict-after replay and live frames. */
  subscribe: (
    input: SubscribeOptions,
  ) => Stream.Stream<SessionFrame, ShiitakeError>;
  /** Open a race-safe baseline plus live changes resource. */
  watch: (
    input?: WatchOptions,
  ) => Effect.Effect<ShiitakeEffectWatch, ShiitakeError>;
}>;

/** Effect-native Session-scope extension commands. */
export type ShiitakeEffectSessionExtension = Readonly<{
  /** Run one command with its encoded input; succeeds with the encoded output. */
  call: (command: string, input: Json) => Effect.Effect<Json, ShiitakeError>;
}>;

/** Effect-native Agent-scope extension controls. */
export type ShiitakeEffectAgentExtensions = Readonly<{
  /** Every rostered extension and whether it is running. */
  list: Effect.Effect<readonly ExtensionStatusV0[], ShiitakeError>;
  /** Resume a stopped extension; it replays its settled backlog once. */
  resume: (
    extensionId: string,
  ) => Effect.Effect<ExtensionStatusV0, ShiitakeError>;
  /** Run one Agent-scope command with its encoded input. */
  call: (
    extensionId: string,
    command: string,
    input: Json,
  ) => Effect.Effect<Json, ShiitakeError>;
}>;

/** Effect-native durable harness. */
export type ShiitakeEffectAgent = Readonly<{
  /** Return the Session identity reserved for this request ID. */
  createSession: (requestId: string) => Effect.Effect<string, ShiitakeError>;
  /** Permanently delete one idle Session; false when it does not exist. */
  deleteSession: (sessionId: string) => Effect.Effect<boolean, ShiitakeError>;
  /** Read one bounded page of existing conversations through the store owner. */
  listSessions: (
    after?: string,
  ) => Effect.Effect<SessionSummaryPage, ShiitakeError>;
  /** List, resume and call rostered extensions. */
  extensions: ShiitakeEffectAgentExtensions;
  /** Address a session without performing I/O. */
  session: (sessionId: string) => ShiitakeEffectSession;
  /** Effect finalizer for the owned harness. */
  close: Effect.Effect<void, ShiitakeError>;
}>;

/**
 * The in-process owner of a durable harness. Only the host holds it, so only
 * the host can write a Session's grants or attach metadata; no HTTP or
 * remote client can.
 */
export type ShiitakeEffectOwner = ShiitakeEffectAgent &
  Readonly<{
    /** Stage a configuration without disturbing an in-flight turn. */
    prepareConfiguration: (
      configuration: Pick<ShiitakeEffectOptions, "extensions" | "tools">,
    ) => Effect.Effect<
      Readonly<{
        commit: Effect.Effect<void, ShiitakeError>;
        discard: Effect.Effect<void>;
      }>,
      ShiitakeError
    >;
    /** Each Session's delivered tool grants, read at every call. */
    grants: Readonly<{
      /** Store one versioned delivery; versions only move forward. */
      deliver: (
        sessionId: string,
        delivery: GrantsDelivery,
      ) => Effect.Effect<GrantsDeliveryResult, ShiitakeError>;
      /** One Session's newest delivery; null when the host delivered none. */
      read: (
        sessionId: string,
      ) => Effect.Effect<GrantsDelivery | null, ShiitakeError>;
    }>;
    /**
     * The same Agent whose prompts and operator lines carry `meta`, checked
     * against the host's metadata schema and stored beside each message.
     */
    withMeta: (meta: Json) => ShiitakeEffectAgent;
  }>;
