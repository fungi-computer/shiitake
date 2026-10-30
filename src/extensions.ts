import type { FileTool } from "@fungi.computer/knapsack";
import type { AttentionItem, AttentionRecipient } from "./session-attention.js";
export {
  AttentionItemSchema,
  AttentionRecipientSchema,
  type AttentionItem,
  type AttentionRecipient,
  type SessionAttention,
} from "./session-attention.js";
import type { Message, Usage } from "@fungi.computer/mule/effect";
import type { ModuleRegistration } from "@fungi.computer/mycelium";
import * as Brand from "effect/Brand";
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import type { StateUpdate } from "./internal/extension-hook.js";
import type { DocumentHandlesV0, DocumentsV0 } from "./session-documents.js";

import {
  ExtensionId,
  extensionFingerprintSchema,
  extensionRevisionSchema,
  type CommandsV0,
  type ExtensionCapabilityV0,
  type ExtensionContractV0,
  type ExtensionManifestV0,
} from "./extension-contract.js";
export {
  ExtensionId,
  extensionIdentifier,
  extensionVersion,
} from "./extension-contract.js";
import { extensionVersion } from "./extension-contract.js";
export type {
  AnyCommandSpecV0,
  ClientActivityStateV0,
  ClientActivityV0,
  CommandsV0,
  CommandSpecV0,
  ExtensionCapabilityV0,
  ExtensionContractV0,
  ExtensionManifestV0,
  ProjectionSpecV0,
  ShiitakeExtensionProtocolV0,
} from "./extension-contract.js";

import {
  registerExtensionCommands,
  type RegisteredExtensionCommandV0,
} from "./internal/extension-command-registry.js";
export type { RegisteredExtensionCommandV0 } from "./internal/extension-command-registry.js";
import {
  commandTools,
  type CommandToolsV0,
} from "./internal/extension-command-tools.js";
export type {
  CommandToolOptionsV0,
  CommandToolsV0,
} from "./internal/extension-command-tools.js";
import {
  bindExtensionState,
  bindExtensionDocuments,
  type ExtensionRuntimePortV0,
  type CollectionBindingV0,
} from "./internal/extension-context.js";
export type {
  ExtensionRuntimePortV0,
  CollectionBindingV0,
} from "./internal/extension-context.js";
import { definitionReader } from "./internal/extension-definitions.js";
import {
  ExtensionHookError,
  invokeExtensionHook,
  invokeSyncHandler,
  invokeTransactionHook,
} from "./internal/extension-hook.js";
export { ExtensionHookError } from "./internal/extension-hook.js";
export type { StateUpdate, SyncResult } from "./internal/extension-hook.js";
import { JsonValueSchema } from "./internal/json-value.js";
import type { ShiitakeError } from "./errors.js";

// ---------------------------------------------------------------------------
// Protocol shiitake.extension/v0: generic primitives, zero feature names.
//
// Core owns doors, durability, isolation and identity; an extension owns its
// vocabulary, policy and prose. Every primitive below has a second consumer
// besides any one product feature.
// ---------------------------------------------------------------------------

/** `sha256:<64 hex>` derived by core from a definition; never authored. */
export type ExtensionRevision = typeof extensionRevisionSchema.Type;
/** `sha256:<64 hex>` over one revision and its sorted granted capabilities. */
export type ExtensionFingerprint = typeof extensionFingerprintSchema.Type;

/** JSON value accepted as hashed extension configuration. */
export type Json =
  | null
  | boolean
  | number
  | string
  | readonly Json[]
  | Readonly<{ [key: string]: Json }>;

const parseJsonSource = Schema.decodeUnknownSync(
  Schema.fromJsonString(JsonValueSchema),
);

/** Parse a serialized extension value into the finite JSON contract. */
export function parseExtensionJson(source: string): Json {
  return parseJsonSource(source);
}

/** Host decision: the exact subset of requested capabilities it grants. */
export type ExtensionGrantV0 = Readonly<{
  id: ExtensionId;
  /** Missing grant row means an empty grant. */
  capabilities: readonly ExtensionCapabilityV0[];
  /**
   * The key the host's tool grants name this extension's run_tools modules
   * by; defaults to the extension id. Several extensions may share one key.
   */
  grantKey?: string;
}>;

/** Present only inside a core-run read (projection, run tools, hooks). */
export class ExtensionRead extends Context.Service<
  ExtensionRead,
  Readonly<{ extensionId: ExtensionId }>
>()("@fungi.computer/shiitake/ExtensionRead") {}

/**
 * Present only inside a core-run write transaction (hooks, commands,
 * `transact`). Core runs these Effects synchronously inside the owner
 * transaction; an asynchronous boundary is a defect that stops the extension.
 */
export class ExtensionTx extends Context.Service<
  ExtensionTx,
  Readonly<{ extensionId: ExtensionId }>
>()("@fungi.computer/shiitake/ExtensionTx") {}

/** Durable record failure: stored or supplied values broke their schema or bounds. */
export class ExtensionStateError extends Data.TaggedError(
  "ExtensionStateError",
)<{
  reason: "decode" | "encode" | "bound" | "unavailable" | "session_absent";
  message: string;
}> {}

/** The door refused an extension admission. */
export class AdmissionError extends Data.TaggedError("AdmissionError")<{
  reason: "invalid" | "conflict" | "closed" | "archived";
  message: string;
}> {}

/** Typed rejection of one extension command, returned to its caller. */
export class ExtensionCommandError extends Data.TaggedError(
  "ExtensionCommandError",
)<{
  reason: "invalid" | "conflict" | "not_found" | "rejected";
  message: string;
}> {}

/** Fail a hook with this (or die) to stop this extension with a visible reason. */
export class ExtensionStop extends Data.TaggedError("ExtensionStop")<{
  reason: string;
}> {}

/** Typed activation failure; the extension is stopped with this message. */
export class ExtensionActivationError extends Data.TaggedError(
  "ExtensionActivationError",
)<{ message: string }> {}

/** Failures a background hook may return; each one stops only its extension. */
export type HookError =
  ExtensionStop | ExtensionStateError | AdmissionError | ExtensionHookError;

/** One namespaced collection: its row schema is the only stored-row decoder. */
export type CollectionSpecV0<A, I> = Readonly<{
  schema: Schema.Codec<A, I>;
  /** Capture this schema and its typed operations before core erases the declaration. */
  bind: (
    port: ExtensionRuntimePortV0,
    name: string,
  ) => Readonly<{ read: CollectionV0<A>; update: UpdateCollectionV0<A> }>;
  /** Decode migration input before invoking the declared index. */
  indexFromEncoded: (
    value: Json,
  ) => Result.Result<string | null, Schema.SchemaError>;
  /** Primary key, 1..256 characters. */
  key: (value: A) => string;
  /** Optional secondary index value, 1..256 characters or null. */
  index?: (value: A) => string | null;
}>;
/** Existential collection spec stored by core. */
export type AnyCollectionSpecV0 = Readonly<{
  schema: Schema.Codec<unknown>;
  /** Captured handle factory; callers retain its concrete return type. */
  bind: (port: ExtensionRuntimePortV0, name: string) => CollectionBindingV0;
  /** Schema-owned migration indexing. */
  indexFromEncoded: (
    value: Json,
  ) => Result.Result<string | null, Schema.SchemaError>;
  /** Primary key of one decoded row. */
  key: (value: never) => string;
  /** Optional secondary index of one decoded row. */
  index?: (value: never) => string | null;
}>;
/** Collections declared by one extension. */
export type CollectionsV0 = Readonly<Record<string, AnyCollectionSpecV0>>;
/** One stored record as seen by a pure migration. */
export type StoredRecordV0 = Readonly<{
  collection: string;
  key: string;
  value: Json;
}>;
/** Versioned state declaration; core runs pure row migrations at activation. */
export type StateSpecV0<C extends CollectionsV0> = Readonly<{
  /** Positive integer; bump on any collection schema change. */
  version: number;
  collections: C;
  /**
   * Keyed by from-version. Total and pure; core applies them in order in one
   * transaction at activation, then re-decodes every row. A stored version
   * above the declared version fails admission (no downgrade).
   */
  migrations: Readonly<
    Record<number, (row: StoredRecordV0) => readonly StoredRecordV0[]>
  >;
}>;
/** The versioned state declaration as authored; version defaults to 1. */
export type StateDeclarationV0<C extends CollectionsV0> = Readonly<{
  version?: number | undefined;
  collections: C;
  migrations?: StateSpecV0<C>["migrations"] | undefined;
}>;

/** Marks a `defineCollection` result, across copies of this package. */
const collectionBrand = Symbol.for("@fungi.computer/shiitake/collection");

const stateDeclarationKeys: ReadonlySet<string> = new Set([
  "version",
  "collections",
  "migrations",
]);

/** The full form names `collections` and nothing outside the declaration keys. */
function isStateDeclaration<C extends CollectionsV0>(
  value: StateDeclarationV0<C> | C,
): value is StateDeclarationV0<C> {
  const keys = Object.keys(value);
  return (
    keys.includes("collections") &&
    keys.every((key) => stateDeclarationKeys.has(key))
  );
}

/** Typed access to one collection inside a core transaction. */
export type CollectionV0<A> = Readonly<{
  /** Synchronous read, available only while core builds a Session projection. */
  peek: (key: string) => Option.Option<A>;
  /** Bounded synchronous page while core builds a Session projection. */
  peekPage: (
    query: Readonly<{
      index?: string | undefined;
      after?: string | undefined;
      limit: number;
    }>,
  ) => readonly A[];
  /** Read one record by key. */
  get: (
    key: string,
  ) => Effect.Effect<Option.Option<A>, ExtensionStateError, ExtensionRead>;
  /** Ordered by key, optionally within one index value; `limit` is 1..64. */
  page: (
    query: Readonly<{
      index?: string | undefined;
      after?: string | undefined;
      limit: number;
    }>,
  ) => Effect.Effect<readonly A[], ExtensionStateError, ExtensionRead>;
  /** Upsert one record under its declared key. */
  put: (value: A) => Effect.Effect<void, ExtensionStateError, ExtensionTx>;
  /** Delete one record; an absent key is a no-op. */
  remove: (
    key: string,
  ) => Effect.Effect<void, ExtensionStateError, ExtensionTx>;
}>;
/** The transaction-local interface used by plain synchronous state.update. */
export type UpdateCollectionV0<A> = Readonly<{
  /** Read one row by its collection key. */
  get: (key: string) => Option.Option<A>;
  /** Read at most 64 rows in key order. */
  page: (
    query: Readonly<{
      index?: string | undefined;
      after?: string | undefined;
      limit: number;
    }>,
  ) => readonly A[];
  /** Schema-validate and upsert one record. */
  put: (value: A) => void;
  /** Remove one record if present. */
  remove: (key: string) => void;
}>;
/** Transaction-scoped collections plus post-commit Session notification. */
export type UpdateStateV0<C extends CollectionsV0> = {
  readonly [K in keyof C]: ReturnType<C[K]["bind"]>["update"];
} & Readonly<{
  /** Publish a changed Session after the update commits. */
  observe: (sessionId: string) => void;
}>;
/** Synchronous, read-only collection access inside `state.read`. */
export type ReadStateV0<C extends CollectionsV0> = {
  readonly [K in keyof C]: Pick<
    ReturnType<C[K]["bind"]>["update"],
    "get" | "page"
  >;
};
/** Typed handles for every declared collection. */
export type StateHandleV0<C extends CollectionsV0> = {
  readonly [K in keyof C]: ReturnType<C[K]["bind"]>["read"];
} & Readonly<{
  /** One short, synchronous SQLite transaction; async callbacks are refused. */
  update: StateUpdate<UpdateStateV0<C>>;
  /**
   * Read rows synchronously outside a transaction (prompt sections, tools).
   * Unavailable inside a command, hook or projection; those already hold one.
   */
  read: StateUpdate<ReadStateV0<C>>;
}>;

/** Watchdog settlement or exact Woodstock terminal of one run. */
export type RunOutcomeV0 =
  "completed" | "failed" | "cancelled" | "interrupted" | "outcome_unknown";
/** Extension that admitted a run, with its stored correlation. */
export type RunOriginV0 = Readonly<{
  extensionId: ExtensionId;
  correlation: string | null;
}>;
/** One settled run joined by core with its terminal and admission origin. */
export type SettledRunV0 = Readonly<{
  /** Watchdog settled-record sequence; delivery is ascending. */
  sequence: number;
  sessionId: string;
  workId: string;
  /** Watchdog settlement. */
  outcome: RunOutcomeV0;
  /** Exact Woodstock terminal for this work, joined by core. */
  terminal: RunOutcomeV0 | "missing";
  /** Distinct tool calls started during this work, including failed calls. */
  toolCalls: number;
  /** Null for human, mail or host admissions. */
  origin: RunOriginV0 | null;
}>;
/** 1..64 records, ascending, each delivered to each running extension once. */
export type SettledPageV0 = readonly SettledRunV0[];

/** One prompt sent through the accepted-command door. */
export type SendInputV0 = Readonly<{
  sessionId: string;
  /** Refuse absent targets instead of creating a Session for this admission. */
  requireExistingSession?: boolean;
  /** Encoded prompt, decoded by the same schema as human prompts. */
  prompt: Readonly<{
    text: string;
    requestId: string;
    delivery: "followUp" | "steer";
  }>;
  /** `idle` admits only while the Session queue head is idle. */
  when: "now" | "idle";
  /** Stored with the admission and returned on `SettledRunV0.origin`. */
  correlation?: string;
  /** This work continues earlier work of the Session and keeps its sender. */
  continuesWorkId?: string;
}>;
/** Door outcome for one extension admission. */
export type SendResultV0 =
  | Readonly<{
      _tag: "Accepted";
      commandId: string;
      delivery: "new" | "replay";
    }>
  | Readonly<{ _tag: "NotReady" }>;

/** Durable per-extension wake timers (the scheduling substrate). */
export type TimersV0 = Readonly<{
  /** Upsert one timer; `dueAt` is epoch milliseconds. */
  set: (
    key: string,
    dueAt: number,
  ) => Effect.Effect<void, ExtensionStateError, ExtensionTx>;
  /** Delete one timer; an absent key is a no-op. */
  cancel: (
    key: string,
  ) => Effect.Effect<void, ExtensionStateError, ExtensionTx>;
  /** Read one timer's due instant. */
  read: (
    key: string,
  ) => Effect.Effect<Option.Option<number>, ExtensionStateError, ExtensionRead>;
}>;
/** A fired timer; its row is consumed unless the handler sets it again. */
export type FiredTimerV0 = Readonly<{
  key: string;
  dueAt: number;
  firedAt: number;
}>;

/** A command's scope fixes whether its invocation names a Session. */
export type CommandSessionIdsV0 = {
  agent: null;
  session: string;
};

/** Host-authenticated caller context; never decoded from command JSON or HTTP. */
export type ExtensionCommandCallerV0 = Readonly<{
  agentId: string;
  sessionId: string;
}>;

/** Handlers for every declared command, run in one core transaction. */
export type CommandHandlersV0<C extends CommandsV0> = {
  readonly [K in keyof C]: (
    call: Readonly<{
      sessionId: CommandSessionIdsV0[C[K]["scope"]];
      input: C[K]["input"]["Type"];
      /** Present only when the owning in-process host authenticates a caller. */
      caller?: ExtensionCommandCallerV0;
    }>,
  ) =>
    | C[K]["output"]["Type"]
    | Effect.Effect<
        C[K]["output"]["Type"],
        ExtensionCommandError | ExtensionStateError | AdmissionError,
        ExtensionTx | ExtensionRead
      >;
};
/** Transaction-scoped, extension-owned attention on an existing Session. */
export type AttentionV0 = Readonly<{
  /** Upsert this extension’s item and publish the committed Session change. */
  set: (
    sessionId: string,
    item: AttentionItem,
  ) => Effect.Effect<void, ExtensionStateError, ExtensionTx>;
  /** Whether any extension holds an inbox item asking a person to act (a question or approval). */
  awaitsPerson: (
    sessionId: string,
  ) => Effect.Effect<boolean, ExtensionStateError, ExtensionRead>;
  /** Clear this extension’s item; other owners cannot be changed. */
  clear: (
    sessionId: string,
    id: string,
  ) => Effect.Effect<void, ExtensionStateError, ExtensionTx>;
}>;

/** Everything core hands one activated extension. */
export type ExtensionContextV0<
  S extends CollectionsV0,
  Config extends Json,
  D extends DocumentsV0 = DocumentsV0,
> = Readonly<{
  manifest: ExtensionManifestV0;
  revision: ExtensionRevision;
  grants: readonly ExtensionCapabilityV0[];
  /** The hashed value; prose and constants come only from here. */
  config: Config;
  state: StateHandleV0<S>;
  /** Transaction-scoped handles for this extension's Session documents. */
  documents: DocumentHandlesV0<D>;
  /** Send one prompt through the accepted-command door in this transaction. */
  send: (
    input: SendInputV0,
  ) => Effect.Effect<SendResultV0, AdmissionError, ExtensionTx>;
  timers: TimersV0;
  attention: AttentionV0;
  /** Committed selected Session position, or zero before its first entry. Requires state. */
  sessionPosition: (
    sessionId: string,
  ) => Effect.Effect<number, ExtensionStateError, ExtensionRead>;
  /** Observe accepted steering without consuming Mule's next control delivery. Requires run_tools. */
  hasPendingSteer: (
    run: Pick<RunIdentityV0, "sessionId" | "workId">,
  ) => Effect.Effect<boolean, ExtensionStateError, ExtensionRead>;
  /**
   * The Agent Session that sent this run, or null when a person (or the host)
   * did. Attention an extension raises for the run waits on it first.
   * Requires run_tools.
   */
  workSender: (
    run: Pick<RunIdentityV0, "sessionId" | "workId">,
  ) => Effect.Effect<
    AttentionRecipient | null,
    ExtensionStateError,
    ExtensionRead
  >;
  /** Whether this run durably recorded a successful tool outcome. Requires run_tools. */
  hasSuccessfulToolResult: (
    run: Pick<RunIdentityV0, "sessionId" | "workId">,
    toolCallId: string,
  ) => Effect.Effect<boolean, ExtensionStateError, ExtensionRead>;
  /** Publish a Session observation after this transaction commits. */
  observe: (sessionId: string) => Effect.Effect<void, never, ExtensionTx>;
  /** Process-local live-view demand, excluding exact-command settlement waiters. */
  isObserved: (sessionId: string) => boolean;
  /**
   * Run one write transaction from outside a hook (for example a Mycelium
   * tool execute). Core commits, publishes, and requests a wake when it
   * admitted work or changed a timer. Fails with a `store`/`unavailable`
   * `ShiitakeError` when the stop this body caused could not be recorded
   * durably.
   */
  transact: <A, E>(
    body: Effect.Effect<A, E, ExtensionTx | ExtensionRead>,
  ) => Effect.Effect<A, E | ExtensionStop | ShiitakeError>;
}>;

/** One run the extension may attach tools to. */
export type RunIdentityV0 = Readonly<{
  sessionId: string;
  workId: string;
  origin: RunOriginV0 | null;
}>;

/** A named, bounded system-prompt section; core orders by extension and name. */
export type PromptSectionV0 = Readonly<{ name: string; text: string }>;

/** Hooks returned by one activation; each runs in its own isolation boundary. */
export type AgentHooksV0<C extends CommandsV0, P> = Readonly<{
  /** Receive each settled run once; the cursor advances in the same transaction. */
  onSettled?: (
    page: SettledPageV0,
  ) => Effect.Effect<void, HookError, ExtensionTx | ExtensionRead>;
  /** Handle one fired timer in its own transaction. */
  onTimer?: (
    timer: FiredTimerV0,
  ) => Effect.Effect<void, HookError, ExtensionTx | ExtensionRead>;
  /** Attach one Mycelium module to a run this extension participates in. */
  moduleForRun?: (
    run: RunIdentityV0,
  ) => Effect.Effect<
    Option.Option<ModuleRegistration>,
    HookError,
    ExtensionRead
  >;
  /** The exclusive Files holder's branded tools, captured once per run. */
  filesForRun?: (
    run: RunIdentityV0,
  ) => Effect.Effect<
    Option.Option<readonly FileTool[]>,
    HookError,
    ExtensionRead
  >;
  /** Re-evaluated for each model request, including a retry. */
  prompt?: (
    run: RunIdentityV0,
  ) =>
    | readonly PromptSectionV0[]
    | Promise<readonly PromptSectionV0[]>
    | Effect.Effect<readonly PromptSectionV0[], HookError, ExtensionRead>;
  /** Project this extension's slot for one Session observation. */
  project?: (sessionId: string) => Option.Option<P>;
  /** The exclusive compaction handoff, when granted `compaction_handoff`. */
  compactionHandoff?: CompactionHandoffV0;
  /** Command handlers keyed by command name. */
  commands: CommandHandlersV0<C>;
}>;

/** Core's activated representation; authors use ExtensionDefinitionV0 below. */
/** Opaque rostered value; core reads the definition existentially. */
export type RosteredExtensionV0 = Readonly<{
  manifest: ExtensionManifestV0;
  /** Private captured definition; absent on an unregistered roster entry. */
  [definitionReader]?: () => AnyExtensionDefinitionV0;
}> &
  Brand.Brand<"RosteredExtensionV0">;

/** A defined extension: its roster entry and the client contract it declares. */
export type DefinedExtensionV0<
  C extends CommandsV0,
  P,
  PI,
> = RosteredExtensionV0 & Readonly<{ contract: ExtensionContractV0<C, P, PI> }>;

/** Contributions registered against the builder's captured metadata. */
export type ExtensionDefinitionV0<
  S extends CollectionsV0,
  C extends CommandsV0,
  P,
  PI,
  Config extends Json,
  D extends DocumentsV0 = DocumentsV0,
> = Readonly<{
  catalog?: Json;
  commands?: Readonly<{
    specs: C;
    /** Register schema-checked transaction handlers. */
    handlers: (
      context: ExtensionContextV0<S, Config, D>,
    ) => CommandHandlersV0<C>;
  }>;
  projection?: Readonly<{
    schema: Schema.Codec<P, PI>;
    /** Project the advertised command catalog from the same Session value. */
    catalog?: (value: P) => Json;
    /**
     * Read the Session slot synchronously inside the snapshot transaction.
     * Return the value (or an `Option`); `undefined` publishes no slot value.
     */
    read: (
      context: ExtensionContextV0<S, Config, D>,
      sessionId: string,
    ) => P | Option.Option<P> | undefined;
  }>;
  /** Rebuild named system-prompt sections before each model request. */
  prompt?: (
    context: ExtensionContextV0<S, Config, D>,
    run: RunIdentityV0,
  ) =>
    | readonly PromptSectionV0[]
    | Promise<readonly PromptSectionV0[]>
    | Effect.Effect<readonly PromptSectionV0[], HookError, ExtensionRead>;
  /** Contribute a gated Mycelium module outside the write transaction. */
  tools?: (
    context: ExtensionContextV0<S, Config, D>,
    run: RunIdentityV0,
    /** Each declared command as a model tool, bound to this run's Session. */
    commands: CommandToolsV0<C>,
  ) =>
    | ModuleRegistration
    | null
    | Promise<ModuleRegistration | null>
    | Effect.Effect<
        ModuleRegistration | null | Option.Option<ModuleRegistration>,
        HookError,
        ExtensionRead
      >;
  /** Supply only Knapsack-minted direct tools; core captures and gates them. */
  files?: (
    context: ExtensionContextV0<S, Config, D>,
    run: RunIdentityV0,
  ) =>
    | readonly FileTool[]
    | null
    | Promise<readonly FileTool[] | null>
    | Effect.Effect<
        readonly FileTool[] | null | Option.Option<readonly FileTool[]>,
        HookError,
        ExtensionRead
      >;
  hooks?: Readonly<{
    /** Acquire activation-scoped resources outside the write transaction. */
    start?: (
      context: ExtensionContextV0<S, Config, D>,
    ) =>
      | void
      | Promise<void>
      | Effect.Effect<void, ExtensionActivationError, Scope.Scope>;
    /** Deliver settled runs and commit the cursor in the same transaction. */
    onSettled?: (
      context: ExtensionContextV0<S, Config, D>,
      page: SettledPageV0,
    ) => void | Effect.Effect<void, HookError, ExtensionTx | ExtensionRead>;
    /** Consume a timer and its state changes in the same transaction. */
    onTimer?: (
      context: ExtensionContextV0<S, Config, D>,
      timer: FiredTimerV0,
    ) => void | Effect.Effect<void, HookError, ExtensionTx | ExtensionRead>;
  }>;
  timers?: true;
  /** Request the generic human-attention door. */
  attention?: true;
  /** Consume the borrowed compaction view before it is released. */
  compaction?: (context: ExtensionContextV0<S, Config, D>) => Readonly<{
    /** Finish the borrowed handoff, including asynchronous delivery. */
    consume: (
      view: BorrowedCompactionViewV0,
    ) =>
      | CompactionHandoffResultV0
      | Promise<CompactionHandoffResultV0>
      | Effect.Effect<
          CompactionHandoffResultV0,
          CompactionHandoffErrorV0 | ExtensionHookError
        >;
  }>;
}>;

/** Reject Promise-returning transactional commands even when output is `{}`. */
export type CheckedCommandHandlers<H> = {
  readonly [K in keyof H]: H[K] extends (...args: never[]) => infer Result
    ? Result extends Promise<unknown>
      ? never
      : H[K]
    : never;
};

/** Caller-owned extension metadata and handlers accepted by the v0 builder. */
export type ExtensionAuthorDefinitionV0<
  S extends CollectionsV0,
  C extends CommandsV0,
  P,
  PI,
  Config extends Json,
  D extends DocumentsV0,
  H extends CommandHandlersV0<C>,
> = Omit<ExtensionDefinitionV0<S, C, P, PI, Config, D>, "commands"> &
  Readonly<{
    commands?: Readonly<{
      specs: C;
      /** Register the author's schema-checked transaction handlers. */
      handlers: (
        context: ExtensionContextV0<S, Config, D>,
      ) => CommandHandlersV0<NoInfer<C>> & H & CheckedCommandHandlers<H>;
    }>;
  }>;

/** Concrete metadata with no declared fields. */
export type EmptyExtensionFieldsV0 = Readonly<Record<never, never>>;

/** Metadata captured before author callbacks are registered. */
type ExtensionMetadataV0<
  S extends CollectionsV0,
  Config extends Json,
  D extends DocumentsV0,
> = Readonly<{
  id: ExtensionId;
  version: string;
  config: Config;
  state: StateSpecV0<S>;
  documents: D;
  declaresState: boolean;
  declaresDocuments: boolean;
}>;

/** Configure typed metadata, then register callbacks against its actual values. */
export class ExtensionBuilderV0<
  S extends CollectionsV0,
  Config extends Json,
  D extends DocumentsV0,
> {
  private constructor(
    private readonly metadata: ExtensionMetadataV0<S, Config, D>,
  ) {}
  /** Begin with concrete empty metadata and no requested capability. */
  static fromIdentity(
    identity: Readonly<{ id: ExtensionId; version: string }>,
  ): ExtensionBuilderV0<
    EmptyExtensionFieldsV0,
    EmptyExtensionFieldsV0,
    EmptyExtensionFieldsV0
  > {
    return new ExtensionBuilderV0({
      ...identity,
      config: {},
      state: { version: 1, collections: {}, migrations: {} },
      documents: {},
      declaresState: false,
      declaresDocuments: false,
    });
  }
  /** Hash this explicit JSON configuration with the extension revision. */
  config<Next extends Json>(value: Next): ExtensionBuilderV0<S, Next, D> {
    return new ExtensionBuilderV0({ ...this.metadata, config: value });
  }
  /**
   * Request state and bind handles to each collection's declared schema.
   * Pass the collections alone (state version 1, no migrations) or the full
   * versioned declaration.
   */
  state<Next extends CollectionsV0>(
    value: StateDeclarationV0<Next> | Next,
  ): ExtensionBuilderV0<Next, Config, D> {
    const declared = isStateDeclaration(value) ? value : { collections: value };
    if (
      isStateDeclaration(value)
        ? !Object.values(value.collections).every(
            (spec) => collectionBrand in spec,
          )
        : Object.keys(value).some((name) => stateDeclarationKeys.has(name))
    )
      throw new Error(
        "A collection named collections, version or migrations needs the full state declaration",
      );
    return new ExtensionBuilderV0({
      ...this.metadata,
      state: {
        version: declared.version ?? 1,
        collections: declared.collections,
        migrations: declared.migrations ?? {},
      },
      declaresState: true,
    });
  }
  /** Request bounded Session documents under the state grant. */
  documents<Next extends DocumentsV0>(
    value: Next,
  ): ExtensionBuilderV0<S, Config, Next> {
    return new ExtensionBuilderV0({
      ...this.metadata,
      documents: value,
      declaresDocuments: true,
    });
  }
  /** Register commands without a client entry. */
  define<
    C extends CommandsV0,
    P,
    PI,
    H extends CommandHandlersV0<C> = CommandHandlersV0<C>,
  >(
    definition: ExtensionAuthorDefinitionV0<S, C, P, PI, Config, D, H> & {
      readonly commands: Readonly<{
        specs: C;
        handlers: (
          context: ExtensionContextV0<S, Config, D>,
        ) => CommandHandlersV0<NoInfer<C>> & H & CheckedCommandHandlers<H>;
      }>;
    },
  ): DefinedExtensionV0<C, P, PI>;
  /** Register contributions without commands or a client entry. */
  define<P, PI>(
    definition: ExtensionAuthorDefinitionV0<
      S,
      EmptyExtensionFieldsV0,
      P,
      PI,
      Config,
      D,
      EmptyExtensionFieldsV0
    > & {
      readonly commands?: never;
    },
  ): DefinedExtensionV0<EmptyExtensionFieldsV0, P, PI>;
  define<
    C extends CommandsV0,
    P,
    PI,
    H extends CommandHandlersV0<C> = CommandHandlersV0<C>,
  >(
    definition: ExtensionAuthorDefinitionV0<S, C, P, PI, Config, D, H>,
  ): DefinedExtensionV0<C | EmptyExtensionFieldsV0, P, PI> {
    return compileExtension<S, C, P, PI, Config, D, H>(
      this.metadata,
      definition,
    );
  }
}

/** An extension id, or an id with an explicit SemVer version. */
export type ExtensionIdentityV0 =
  string | Readonly<{ id: string; version?: string | undefined }>;

const decodeExtensionId = Schema.decodeUnknownSync(ExtensionId);
const decodeExtensionVersion = Schema.decodeUnknownSync(extensionVersion);
const isBareExtensionId = Schema.is(Schema.String);

/**
 * Begin with honest empty defaults; optional contributions are declared
 * explicitly. The version defaults to `0.0.0` and any SemVer, including a
 * pre-release, is valid. It is part of the extension's revision, which already
 * covers schemas, config and requested capabilities; a different version marks
 * a behaviour change those do not show, such as a fixed hook.
 */
export function defineExtension(
  identity: ExtensionIdentityV0,
): ExtensionBuilderV0<
  EmptyExtensionFieldsV0,
  EmptyExtensionFieldsV0,
  EmptyExtensionFieldsV0
> {
  const declared = isBareExtensionId(identity) ? { id: identity } : identity;
  return ExtensionBuilderV0.fromIdentity({
    id: decodeExtensionId(declared.id),
    version: decodeExtensionVersion(declared.version ?? "0.0.0"),
  });
}

function compileExtension<
  S extends CollectionsV0,
  C extends CommandsV0,
  P,
  PI,
  Config extends Json,
  D extends DocumentsV0,
  H extends CommandHandlersV0<C>,
>(
  metadata: ExtensionMetadataV0<S, Config, D>,
  definition: ExtensionAuthorDefinitionV0<S, C, P, PI, Config, D, H>,
): DefinedExtensionV0<C | EmptyExtensionFieldsV0, P, PI> {
  if (
    definition.projection?.catalog !== undefined &&
    definition.catalog === undefined
  )
    throw new Error("A projected catalog requires an advertised catalog");
  const requests: ExtensionCapabilityV0[] = [];
  if (definition.attention === true) requests.push("attention");
  if (metadata.declaresState || metadata.declaresDocuments)
    requests.push("state");
  if (definition.hooks?.onSettled !== undefined)
    requests.push("settled", "send");
  if (definition.commands !== undefined) requests.push("commands", "send");
  if (definition.hooks?.onTimer !== undefined || definition.timers === true)
    requests.push("timers", "send");
  if (definition.projection !== undefined) requests.push("projection");
  if (definition.tools !== undefined) requests.push("run_tools");
  if (definition.files !== undefined) requests.push("files");
  if (definition.prompt !== undefined) requests.push("prompt");
  if (definition.compaction !== undefined) requests.push("compaction_handoff");
  const manifest: ExtensionManifestV0 = {
    protocol: "shiitake.extension/v0",
    id: metadata.id,
    version: metadata.version,
    requests: [...new Set(requests)],
  };
  const contract: ExtensionContractV0<C | EmptyExtensionFieldsV0, P, PI> = {
    id: metadata.id,
    commands: definition.commands?.specs ?? {},
    projection:
      definition.projection === undefined
        ? null
        : { schema: definition.projection.schema },
  };
  const { hooks, commands, projection, prompt, tools, files, compaction } =
    definition;
  const onSettled = hooks?.onSettled;
  const onTimer = hooks?.onTimer;
  const engine: AnyExtensionDefinitionV0 = {
    manifest,
    contract,
    config: metadata.config,
    state: metadata.state,
    activate: (port) => {
      const context: ExtensionContextV0<S, Config, D> = Object.freeze({
        ...port.context,
        config: metadata.config,
        state: bindExtensionState(metadata.state.collections, port),
        documents: bindExtensionDocuments(metadata.documents, port),
      });
      return Effect.gen(function* () {
        if (hooks?.start !== undefined)
          yield* invokeExtensionHook(() => hooks.start?.(context));
        const registered =
          commands === undefined
            ? new Map<string, RegisteredExtensionCommandV0>()
            : registerExtensionCommands(
                commands.specs,
                commands.handlers(context),
              );
        const result = {
          commands: registered,
        } satisfies Omit<AgentHooksV0<C, P>, "commands"> & {
          commands: ReadonlyMap<string, RegisteredExtensionCommandV0>;
        };
        if (onSettled !== undefined)
          Object.assign(result, {
            onSettled: (page: SettledPageV0) =>
              invokeTransactionHook(() => onSettled(context, page)),
          });
        if (onTimer !== undefined)
          Object.assign(result, {
            onTimer: (timer: FiredTimerV0) =>
              invokeTransactionHook(() => onTimer(context, timer)),
          });
        if (tools !== undefined)
          Object.assign(result, {
            moduleForRun: (run: RunIdentityV0) =>
              invokeExtensionHook(() =>
                tools(
                  context,
                  run,
                  commandTools(
                    commands?.specs,
                    () => registered,
                    context.transact,
                    run,
                    context.grants.includes("commands"),
                  ),
                ),
              ).pipe(
                Effect.map((value) =>
                  Option.isOption(value) ? value : Option.fromNullishOr(value),
                ),
              ),
          });
        if (files !== undefined)
          Object.assign(result, {
            filesForRun: (run: RunIdentityV0) =>
              invokeExtensionHook(() => files(context, run)).pipe(
                Effect.map((value) =>
                  Option.isOption(value) ? value : Option.fromNullishOr(value),
                ),
              ),
          });
        if (prompt !== undefined)
          Object.assign(result, {
            prompt: (run: RunIdentityV0) => prompt(context, run),
          });
        if (projection !== undefined)
          Object.assign(result, {
            project: (sessionId: string) => {
              const value = invokeSyncHandler(
                () => projection.read(context, sessionId),
                undefined,
              );
              return Option.isOption(value)
                ? value
                : Option.fromUndefinedOr(value);
            },
          });
        const projectCatalog = projection?.catalog;
        if (projection !== undefined && projectCatalog !== undefined)
          Object.assign(result, {
            projectCatalog: (value: Json) =>
              projectCatalog(
                Schema.decodeUnknownSync(projection.schema)(value),
              ),
          });
        if (compaction !== undefined) {
          const contribution = compaction(context);
          Object.assign(result, {
            compactionHandoff: {
              consume: (view: BorrowedCompactionViewV0) =>
                invokeExtensionHook(() => contribution.consume(view)),
            },
          });
        }
        return result;
      });
    },
  };
  if (metadata.declaresDocuments)
    Object.assign(engine, { documents: metadata.documents });
  if (definition.catalog !== undefined)
    Object.assign(engine, { catalog: definition.catalog });
  const brand = Brand.nominal<RosteredExtensionV0>();
  const rostered = Object.assign(
    brand({ manifest, [definitionReader]: () => engine }),
    { contract },
  );
  Object.defineProperty(rostered, definitionReader, { enumerable: false });
  return Object.freeze(rostered);
}

/** Existential hooks as stored by core; each handler is re-typed by its schema. */
export type AnyAgentHooksV0 = Readonly<{
  /** Receive each settled run once. */
  onSettled?: NonNullable<AgentHooksV0<CommandsV0, never>["onSettled"]>;
  /** Handle one fired timer. */
  onTimer?: NonNullable<AgentHooksV0<CommandsV0, never>["onTimer"]>;
  /** Attach one Mycelium module to a participating run. */
  moduleForRun?: NonNullable<AgentHooksV0<CommandsV0, never>["moduleForRun"]>;
  filesForRun?: NonNullable<AgentHooksV0<CommandsV0, never>["filesForRun"]>;
  prompt?: NonNullable<AgentHooksV0<CommandsV0, never>["prompt"]>;
  /** Project this extension's slot for one Session observation. */
  project?: (sessionId: string) => Option.Option<unknown>;
  /** Decode the projected Session JSON before producing its advertised catalog. */
  projectCatalog?: (value: Json) => Json;
  compactionHandoff?: CompactionHandoffV0;
  /** Schema-bound handlers captured at activation. */
  commands: ReadonlyMap<string, RegisteredExtensionCommandV0>;
}>;

/** Existential definition stored by core. */
export type AnyExtensionDefinitionV0 = Readonly<{
  manifest: ExtensionManifestV0;
  contract: Readonly<{
    id: ExtensionId;
    commands: CommandsV0;
    projection: Readonly<{ schema: Schema.Codec<unknown> }> | null;
  }>;
  config: Json;
  state: StateSpecV0<CollectionsV0>;
  documents?: DocumentsV0;
  /** Advertised human command catalog, opaque to core. */
  catalog?: Json;
  /** Core passes the context built for this definition's own types. */
  activate: (
    port: ExtensionRuntimePortV0,
  ) => Effect.Effect<
    AnyAgentHooksV0,
    ExtensionActivationError | ExtensionHookError,
    Scope.Scope
  >;
}>;

/** Declare one collection with its key inferred from the row schema. */
export function defineCollection<A, I>(
  schema: Schema.Codec<A, I>,
  keys: Readonly<{
    key: (value: A) => string;
    index?: (value: A) => string | null;
  }>,
): CollectionSpecV0<A, I> {
  const spec: CollectionSpecV0<A, I> = {
    schema,
    ...keys,
    bind: (port, name) => ({
      read: port.collection(name, spec),
      update: port.updateCollection(name, spec),
    }),
    indexFromEncoded: (value) =>
      Result.map(
        Schema.decodeUnknownResult(schema)(value),
        (decoded) => spec.index?.(decoded) ?? null,
      ),
  };
  Object.defineProperty(spec, collectionBrand, { value: true });
  return Object.freeze(spec);
}

/** A host's explicit roster and grants. */
export type ExtensionRosterV0 = Readonly<{
  /** At most 64 entries; unordered; activated in ASCII id order. */
  roster: readonly RosteredExtensionV0[];
  grants: readonly ExtensionGrantV0[];
}>;

/** Visible lifecycle of one rostered extension (wire representation). */
export type ExtensionStatusV0 = Readonly<{
  id: string;
  version: string;
  /** The extension's core-derived revision. */
  revision: string;
  status: "running" | "stopped";
  reason: string | null;
  stoppedAt: number | null;
}>;

/** One extension's encoded slot in a Session snapshot or frame. */
export type ExtensionSlotV0 =
  | Readonly<{
      status: "running";
      /** The extension's core-derived revision. */
      revision: string;
      /** Encoded projection value, or null when the extension has none. */
      value: Json;
      /** The extension's advertised command catalog, when it has one. */
      catalog?: Json;
    }>
  | Readonly<{
      status: "stopped";
      /** The extension's core-derived revision. */
      revision: string;
      reason: string;
    }>;

/** Borrowed view of the exact objects already built for compaction. */
export type BorrowedCompactionViewV0 = Readonly<{
  /** Session being compacted; a host binds the Agent itself. */
  sessionId: string;
  /** Exact Pi preparation array summarized by the history call. */
  messagesToSummarize: readonly Message[];
  /** Exact Pi preparation array summarized for a split turn, when present. */
  turnPrefixMessages: readonly Message[];
  /** Actual Pi compaction result values, before their scope is released. */
  result: Readonly<{
    summary: string;
    firstKeptEntryId: string;
    tokensBefore: number;
    usage: Usage;
    details?: Readonly<{
      readFiles: readonly string[];
      modifiedFiles: readonly string[];
    }>;
  }>;
  /** Owner-issued revision of the selected context being compacted. */
  contextRevision: string;
}>;

/** Result returned before a borrowed compaction view may be released. */
export type CompactionHandoffResultV0 =
  | Readonly<{
      type: "accepted";
      /** Opaque receipt issued by the archive owner. */
      receipt: string;
    }>
  | Readonly<{ type: "discarded_by_policy" }>;

/** Typed failure returned by the compaction contribution. */
export type CompactionHandoffErrorV0 = Readonly<{
  /** Stable error tag for handoff failure. */
  _tag: "CompactionHandoffError";
  /** Bounded, caller-safe explanation. */
  message: string;
}>;

/** The exclusive compaction handoff callable of one extension. */
export type CompactionHandoffV0 = Readonly<{
  /** Consume one borrowed view before its objects are released. */
  consume: (
    view: BorrowedCompactionViewV0,
  ) => Effect.Effect<
    CompactionHandoffResultV0,
    CompactionHandoffErrorV0 | ExtensionHookError
  >;
}>;
