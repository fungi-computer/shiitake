import type { HostMetaOptions } from "./host-meta.js";
import type { SessionTerminal, ShiitakeAgent } from "../client.js";
import type {
  GrantsDelivery,
  GrantsDeliveryResult,
} from "./woodstock/index.js";
import type { Json } from "../extensions.js";
import type { ExtensionRosterV0 } from "../extensions.js";
import type {
  Api,
  AssistantMessage,
  Model,
  MuleStreamFunction,
} from "@fungi.computer/mule/effect";
import type {
  AccessOptions,
  CapabilitySource,
  ExecutionOptions,
  ModuleRegistration,
  Sandbox,
} from "@fungi.computer/mycelium";

import type { ModelPreference } from "./model-preference.js";

import type {
  WatchdogSqliteValue,
  WatchdogSqliteRow,
  WatchdogSqliteOwner,
  WatchdogSqliteCursor,
} from "@fungi.computer/watchdog";
export { watchdogSqliteRowSchema as sqliteRowSchema } from "@fungi.computer/watchdog";

/** Native SQLite scalar and row grammar shared with the work queue owner. */
export type SqliteValue = WatchdogSqliteValue;
/** Raw row grammar shared by Woodstock and Watchdog. */
export type SqliteRow = WatchdogSqliteRow;

/** Native rows stay raw until the query's maintained schema decodes them. */
export type SqliteCursor = WatchdogSqliteCursor &
  Readonly<{
    /** Return one raw row or throw when none exists. */
    one: () => SqliteRow;
  }>;

/** Synchronous SQL and transactions held by the surrounding runtime. */
export type SqliteOwner = Omit<WatchdogSqliteOwner, "sql"> &
  Readonly<{
    /** Native synchronous SQL, returning raw rows for the query owner to decode. */
    sql: Readonly<{
      /** Execute a statement with SQLite scalar bindings. */
      exec: (statement: string, ...bindings: SqliteValue[]) => SqliteCursor;
    }>;
    /**
     * How the host names this store when Woodstock refuses part of it, so its
     * operator knows which store to act on: its Durable Object or its file.
     * Absent is "Woodstock store".
     */
    storeName?: string;
  }>;

/** Acquired Pi model and optional explicit Pi stream function. */
export type ModelAcquisition = Readonly<{
  /** Exact Pi model selected for this run. */
  model: Model<Api>;
  /** Optional explicit Pi stream function for custom providers and tests. */
  stream?: MuleStreamFunction;
  /** Typed retry decision for this door's exact message; unknown means false. */
  retryable: (message: AssistantMessage) => boolean;
  /** Provider's bounded Retry-After for a clean HTTP refusal, if reported. */
  retryAfterMs?: (message: AssistantMessage) => number | undefined;
  /**
   * Describe only a failure message produced by this acquired model door.
   * Return its owner-authored safe reason, never arbitrary SDK error prose.
   * Unknown messages must return undefined.
   */
  failureReason?: (message: AssistantMessage) => string | undefined;
  /** Provider prose for this door's exact failed turn; never a diagnostic. */
  providerError?: (message: AssistantMessage) => string | undefined;
  /** Release the acquired model resources. */
  release: () => Promise<void>;
}>;

/** Deterministic host seam for model selection and ready egress. */
export type ModelSource = Readonly<{
  /** Acquire one selection and its request-scoped egress. */
  acquire: (
    input: Readonly<{
      sessionId: string;
      preference: ModelPreference | null;
      signal: AbortSignal;
    }>,
  ) => Promise<ModelAcquisition>;
  /**
   * Read the context window of the model a run would select now, without
   * contacting the provider. Null when the model has none; a source without
   * this reader leaves the window unknown.
   */
  describe?: (
    input: Readonly<{
      sessionId: string;
      preference: ModelPreference | null;
      signal: AbortSignal;
    }>,
  ) => Promise<ModelDescription>;
}>;

/** What a Session's footer may know about its selected model. */
export type ModelDescription = Readonly<{ contextWindow: number | null }>;

/** Immutable instruction snapshot captured for one run. */
export type InstructionSnapshot = Readonly<{
  /** Revision of the captured instruction inputs. */
  revision: string;
  /** Bounded instruction text for one run. */
  text: string;
  /** Capability modules captured with this instruction snapshot. */
  modules?: readonly ModuleRegistration[];
  /** Release run-scoped host resources after the accepted run settles. */
  release?: () => void;
}>;

/** Host seam for one bounded instruction snapshot. */
export type InstructionSource = Readonly<{
  /** Load the session's bounded instruction snapshot. */
  load: (
    input: Readonly<{
      sessionId: string;
      commandId: string;
      signal: AbortSignal;
    }>,
  ) => Promise<InstructionSnapshot>;
}>;

/** Mycelium construction inputs captured by the Shiitake-owned run scope. */
export type ToolSource = Readonly<{
  /** Host-owned limit for one scoped tool invocation. */
  execution?: ExecutionOptions;
  /** Mycelium module descriptors visible in the run scope. */
  modules?: readonly ModuleRegistration[];
  /**
   * Dynamic host capability projections acquired by Mycelium. Each run acquires
   * them with its Session in `context.session`, so a host projects only what
   * that Session is permitted to use.
   */
  sources?: readonly CapabilitySource[];
  /** Sandbox used by the scoped capability surface. */
  sandbox: Sandbox;
  /**
   * The Agent-wide access defaults for every tool of the run, as data. A
   * Session's grants the host delivered with the owner's `grants.deliver` override
   * the default key by key. Mycelium reads them at every call; Shiitake never
   * interprets them. Absent is full access.
   */
  access?: ToolAccess;
}>;

/** Promise host for scheduling the opaque durable reconciler. */
export type RuntimePendingWork = Readonly<{
  /** Immediate recovery work remains subject to the host's retry cadence. */
  pending: boolean;
  /** Earliest owner-supplied wall-clock candidate for the shared host wake. */
  nextWakeAt: number | null;
}>;

/** Promise host for scheduling the opaque durable reconciler. */
export type RuntimeHost = Readonly<{
  /** Bind the one repeatable zero-input reconciliation callback. */
  bind: (
    input: Readonly<{
      reconcile: () => Promise<void>;
      pendingWork: () => Promise<RuntimePendingWork>;
    }>,
  ) => Promise<
    Readonly<{
      /** Commit owner mutations together with the host wake guarantee. No external I/O. */
      commit: <A>(mutation: () => Promise<A>) => Promise<A>;
      /** Request one later reconciliation without running it inline. */
      requestWake: () => Promise<void>;
      /** Stop callbacks and release the bound host registration. */
      close: () => Promise<void>;
    }>
  >;
}>;

/**
 * A host's storage-pressure check. Shiitake owns the refusal door and the
 * typed `storage_pressure` error; the host owns the numbers behind it, such
 * as Zookeeper's budget and thresholds. While the check reports pressure,
 * new Sessions and new prompts are refused and admitted work finishes.
 */
export type StoragePressureCheck = Readonly<{
  /**
   * Whether the owner is under storage pressure now. Called only for a new
   * Session or a new prompt, never for a replay or for writes that finish
   * admitted work. A throw, such as an unreadable measurement, refuses the
   * new work with that error.
   */
  underPressure: () => boolean;
}>;

/** One model's compaction token budgets; omitted fields fall back. */
export type CompactionBudget = Readonly<{
  /** Tokens reserved for the model's response; compaction runs past window minus this. */
  reserveTokens?: number;
  /** Recent tokens kept verbatim instead of summarized. */
  keepRecentTokens?: number;
  /** Tokens before the blocking threshold at which compaction starts in the background; 0 turns it off (default 32768). */
  backgroundTokens?: number;
}>;

/**
 * Pi's compaction settings. Each budget resolves through the exact
 * `provider/modelId` override, then the ordinary value, then Pi's default
 * (16384 reserve, 20000 kept, 32768 background). `enabled` is global and governs automatic
 * compaction only; a manual compaction always runs.
 */
export type CompactionOptions = CompactionBudget &
  Readonly<{
    enabled?: boolean;
    /** Budgets keyed by exact, case-sensitive `provider/modelId`. */
    modelOverrides?: Readonly<Record<string, CompactionBudget>>;
  }>;

/** Validated immutable construction bag for the durable harness. */
export type ShiitakeOptions = Readonly<{
  /**
   * The schema of the opaque metadata this host attaches to prompts and
   * operator lines. Shiitake checks, stores and returns it; the host owns its
   * grammar and may select a principal identity path for recall authorization.
   */
  meta?: HostMetaOptions;
  /** Native owner storage and synchronous transaction capability. */
  store: SqliteOwner;
  /** Host storage-pressure check; absent never refuses for storage pressure. */
  storagePressure?: StoragePressureCheck;
  /** Compaction budgets; absent uses Pi's defaults for every model. */
  compaction?: CompactionOptions;
  /** Runtime host that schedules the opaque durable reconciler. */
  runtime: RuntimeHost;
  /** Host model source returning Pi's own model representation. */
  model: ModelSource;
  /** Host instruction snapshot seam. */
  instructions: InstructionSource;
  /** Scoped Mycelium construction inputs, omitted when no capabilities exist. */
  tools?: ToolSource;
  /**
   * The host's explicit extension roster and grants. Required: a host with no
   * extensions passes `{ roster: [], grants: [] }`.
   */
  extensions: ExtensionRosterV0;
}>;

/**
 * A host's access data for its Agent's tools. Grants are keyed by module id;
 * a module an extension built is granted by that extension's id. A missing id
 * keeps full access, so a first-time host writes no permission code.
 */
export type ToolAccess = Pick<
  AccessOptions,
  /** The Agent-wide default; a Session's delivered grants override it. */
  | "grants"
  /** Keys refused unless a delivery or the default names them: fail closed. */
  | "required"
  /** Ask a person to approve one `confirm` call; absent refuses those calls. */
  | "confirm"
  /** Operator-only observer of each refusal; never changes the decision. */
  | "onRefused"
>;

/**
 * The in-process owner of a durable harness. Only the host holds it, so only
 * the host can write a Session's grants or attach metadata; no HTTP or
 * remote client can.
 */
export type ShiitakeOwnerAgent = ShiitakeAgent &
  Readonly<{
    /** Stage a configuration without disturbing an in-flight turn. */
    prepareConfiguration: (
      configuration: Pick<ShiitakeOptions, "extensions" | "tools">,
    ) => Promise<
      Readonly<{
        /** Publish synchronously; the next turn applies the prepared roster. */
        commit: () => void;
        /** Drop the staged configuration without applying it. */
        discard: () => Promise<void>;
      }>
    >;
    /** Each Session's delivered tool grants, read at every call. */
    grants: Readonly<{
      /**
       * Store one versioned delivery of the host's decision, synchronously,
       * so the very next tool call reads it. Versions never move back: an
       * older one throws a conflict and writes nothing. The same version with
       * other grants replaces them; an identical replay is `unchanged`.
       */
      deliver: (
        sessionId: string,
        delivery: GrantsDelivery,
      ) => GrantsDeliveryResult;
      /** One Session's newest delivery; null when the host delivered none. */
      read: (sessionId: string) => GrantsDelivery | null;
    }>;
    /**
     * The same Agent whose prompts and operator lines carry `meta`, checked
     * against the host's metadata schema and stored beside each message.
     */
    withMeta: (meta: Json) => ShiitakeAgent;
  }>;

export type {
  GrantsDelivery,
  GrantsDeliveryResult,
} from "./woodstock/index.js";
export type {
  HostMetaOptions,
  StandardHostMetaResult,
  StandardHostMetaSchema,
} from "./host-meta.js";

/** Exact accepted-work facts, independent of the current Session branch or page. */
export type CommandSettlement = Readonly<{
  /** Original Agent sender recorded at acceptance, never the terminal's canceller. */
  acceptedSender: string | null;
  /** Durable terminal and the command that ended the work's opaque metadata. */
  terminal: SessionTerminal;
  /** Last completed assistant text; cancelled work returns no answer. */
  content: string;
  /** Provider-native usage facts, retaining model identity without transcript content. */
  usage: readonly Pick<AssistantMessage, "provider" | "model" | "usage">[];
  /** Distinct tool-call intentions belonging to this exact work. */
  toolCalls: number;
}>;
