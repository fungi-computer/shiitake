import type { CommandSettlement } from "./internal/contracts.js";
import type { BirdDogMail } from "@fungi.computer/bird-dog";
import type { VerifiedAgentReply } from "./internal/woodstock/index.js";
import type { BirdDogEffect } from "@fungi.computer/bird-dog/effect";
import type { Accepted } from "./client.js";
import type {
  ColdSessionPage,
  ColdSessionPageRequest,
} from "./cold-sessions.js";
import type { HostMetaOptions } from "./internal/host-meta.js";
import { DurableObject } from "cloudflare:workers";
import type {
  CapabilitySource,
  ModuleRegistration,
  Sandbox,
} from "@fungi.computer/mycelium";
import { fromTypedSandbox } from "@fungi.computer/mycelium/effect";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Redacted from "effect/Redacted";
import {
  WatchdogError,
  type WatchdogExecutionDecision,
  type WatchdogReadResult,
  type WatchdogRunningJob,
  type WatchdogTransactionalProjection,
  type WatchdogTickResult,
} from "@fungi.computer/watchdog";

import type { ExtensionRosterV0, Json } from "./extensions.js";
import { ShiitakeError } from "./errors.js";
import {
  createShiitakeHttpRoutes,
  type ShiitakeHttpEnv,
} from "./http-routes.js";
import { Hono } from "hono";
import type {
  InstructionSource,
  ModelSource,
  GrantsDelivery,
  GrantsDeliveryResult,
  ShiitakeOwnerAgent,
  SqliteOwner,
  StoragePressureCheck,
  CompactionOptions,
  ToolAccess,
} from "./internal/contracts.js";
import {
  runShiitakePromise,
  toPromiseOwner,
} from "./internal/effect-client.js";
import type {
  RuntimeHostEffect,
  ShiitakeEffectOptions,
  ToolSourceEffect,
} from "./internal/effect-contracts.js";
import { adaptModelSource } from "./internal/model-acquisition.js";
import { decodeClientSessionId } from "./internal/client-boundary.js";
import {
  deliverSessionGrants,
  prepareSessionGrants,
  pruneSessionInboxTombstones,
  readColdSessionPage,
  readSessionInboxChanges,
  readSessionSummary,
  readSessionInboxSummary,
  type SessionInboxChanges,
} from "./internal/woodstock/index.js";

export type { SessionInboxChanges } from "./internal/woodstock/index.js";
export type {
  ColdSessionPage,
  ColdSessionPageRequest,
  ColdSessionRow,
} from "./cold-sessions.js";
export type {
  GrantsDelivery,
  GrantsDeliveryResult,
  ShiitakeOwnerAgent,
  ToolAccess,
} from "./internal/contracts.js";

/**
 * Deliver one Session's grants into a Cloudflare owner's own storage,
 * synchronously, whether or not its runtime is open; the very next tool call
 * reads it. A plain function, not a Durable Object method, so no other
 * Durable Object can call it over RPC: only code running inside the owner,
 * holding its storage, delivers grants.
 */
export function deliverSessionGrantsToStorage(
  storage: SqliteOwner,
  sessionId: string,
  delivery: GrantsDelivery,
): GrantsDeliveryResult {
  return storage.transactionSync(() => {
    prepareSessionGrants(storage);
    return deliverSessionGrants(
      storage,
      decodeClientSessionId(sessionId),
      delivery,
    );
  });
}

/** Native Durable Object capabilities used by the Cloudflare adapter. */
export type CloudflareShiitakeState = Readonly<{
  /** Native owner-local SQLite used by Woodstock and Watchdog. */
  storage: SqliteOwner &
    Pick<DurableObjectStorage, "getAlarm" | "setAlarm" | "deleteAlarm"> & {
      /** Commit an asynchronous owner mutation through native storage. */
      transaction<A>(body: () => Promise<A>): Promise<A>;
    };
  /** Extend the Durable Object event lifetime through runtime reconciliation. */
  waitUntil: (task: Promise<unknown>) => void;
  /** The Durable Object's identity, named when its store refuses a Session. */
  id?: Readonly<{
    /** The identity as text, used to name the store in refusals. */
    toString: () => string;
  }>;
}>;

/** Non-empty Mycelium capabilities supplied by a Cloudflare host. */
export type CloudflareShiitakeTools = Readonly<{
  modules?: readonly ModuleRegistration[];
  sources?: readonly CapabilitySource[];
  sandbox: Sandbox;
  /**
   * The Agent-wide access defaults for every tool of the run, as data. A
   * Session's grants the host delivered with the owner's `grants.deliver` override
   * the default key by key. Mycelium reads them at every call; Shiitake never
   * interprets them. Absent is full access.
   */
  access?: ToolAccess;
}>;

/**
 * Durable host work that shares Shiitake's one native alarm, such as a
 * host-owned delivery outbox. Shiitake never interprets it.
 */
export type CloudflareHostWork = Readonly<{
  /** Read synchronously inside the alarm transaction. */
  pending: () => Readonly<{ due: boolean; nextWakeAt: number | null }>;
  /** One bounded pass, requested independently on every durable wake. */
  run: () => Promise<void>;
}>;

/** Host jobs use a separate slot in Shiitake's one Watchdog and native alarm. */
export type CloudflareHostJobs = Readonly<{
  lane: string;
  /** Open the host executor on the shared transactional job store. */
  open: (
    runtime: Readonly<{
      transactional: WatchdogTransactionalProjection;
      readJob: (jobId: string) => Promise<WatchdogReadResult>;
      /** Execute one job in the construction-time host partition. */
      tickConcurrent: () => Promise<WatchdogTickResult>;
    }>,
  ) => Readonly<{
    /** Execute one job claimed for this host lane. */
    execute: (job: WatchdogRunningJob) => Promise<WatchdogExecutionDecision>;
  }>;
}>;

/** ACP's maintained transport, supplied by the host without a package cycle. */
export type CloudflareShiitakeAcpServer = Readonly<{
  /**
   * Forward an authenticated ACP HTTP request to the maintained SDK server,
   * with the metadata the host's middleware attached to it.
   */
  handleRequest: (
    request: Request,
    meta: Json | undefined,
  ) => Promise<Response>;
}>;

/** Cloudflare host inputs; the model owner resolves one frozen run selection. */
export type CloudflareShiitakeOptions = Readonly<{
  /**
   * The schema of the opaque metadata this host attaches to prompts and
   * operator lines. Shiitake checks, stores and returns it; it never reads it.
   */
  meta?: HostMetaOptions;
  model: ModelSource;
  instructions: InstructionSource;
  tools?: readonly [] | CloudflareShiitakeTools;
  /**
   * The host's storage-pressure check for this Durable Object; absent never
   * refuses for storage pressure. The budget and thresholds are the host's.
   */
  storagePressure?: StoragePressureCheck;
  /** Compaction budgets; absent uses Pi's defaults for every model. */
  compaction?: CompactionOptions;
  /** The host's explicit extension roster and grants; there is no default. */
  extensions: ExtensionRosterV0;
  /**
   * Owner startup work, such as extension commands or storing Session grants,
   * finishes before any durable run or host work is recovered.
   */
  initialize?: (agent: ShiitakeOwnerAgent) => Promise<void>;
  /** Independent host work kept alive by the same alarm; absent means none. */
  hostWork?: readonly CloudflareHostWork[];
  /** Optional host lane in the same Watchdog; use hostWork for due retries. */
  hostJobs?: CloudflareHostJobs;
  /** Host-owned addressed mail and settlement replies; absent disables mail. */
  mailRoom?: BirdDogEffect;
  /**
   * Lazily open ACP over the owned in-process handle. The host authorizes
   * ingress and supplies its context policy. ACP borrows this handle: closing
   * a protocol connection must release its watches, never close the runtime.
   * Absent means `/acp` is unavailable.
   */
  acp?: (agent: ShiitakeOwnerAgent) => Promise<CloudflareShiitakeAcpServer>;
}>;

function runtimeFailure(
  cause: "closed" | "unavailable",
  message: string,
): ShiitakeError {
  return new ShiitakeError("runtime", cause, message);
}

function earliest(a: number | null, b: number | null): number | null {
  if (a === null) return b;
  return b === null ? a : Math.min(a, b);
}

/** Bounded backoff for a native alarm that failed outside any reconciliation. */
const ALARM_RETRY_BASE_MS = 1_000;
const ALARM_RETRY_CAP_MS = 30_000;

/** Exponential growth from one failed alarm attempt, never past the cap. */
function alarmRetryDelayMs(consecutiveFailures: number): number {
  return Math.min(
    ALARM_RETRY_BASE_MS * 2 ** (consecutiveFailures - 1),
    ALARM_RETRY_CAP_MS,
  );
}

/** One native alarm protects queued and executing owner-local work. */
function runtimeHost(
  state: CloudflareShiitakeState,
  hostWork: readonly CloudflareHostWork[] = [],
  startup: Effect.Effect<boolean>,
) {
  let alarm: (() => Promise<void>) | undefined;
  const runtime: RuntimeHostEffect = {
    bind: ({ reconcile, pendingWork }) =>
      Effect.sync(() => {
        let closed = false;
        const isClosed = () => closed;
        let mutationVersion = Symbol();

        const arm = async () => {
          const existing = await state.storage.getAlarm();
          const recoveryAt = Date.now() + 5_000;
          await state.storage.setAlarm(
            existing === null ? recoveryAt : Math.min(existing, recoveryAt),
          );
        };
        /** Arm at the earliest due time (host work included), or disarm. */
        const armAt = async (due: number | null) => {
          if (due === null) await state.storage.deleteAlarm();
          else await state.storage.setAlarm(due);
        };
        const unavailable = () =>
          runtimeFailure("unavailable", "Shiitake durable wake is unavailable");
        const persistWake = Effect.tryPromise({ try: arm, catch: unavailable });

        const hostPass = async () => {
          const failures: unknown[] = [];
          for (const [moduleIndex, work] of hostWork.entries()) {
            try {
              await work.run();
            } catch (error) {
              await runShiitakePromise(
                Effect.logError(
                  "shiitake_reconciliation_failed",
                  Redacted.make(error),
                ).pipe(
                  Effect.annotateLogs({
                    operation: "host-work.run",
                    moduleIndex,
                    failureKind:
                      error instanceof Error ? "Error" : "NonErrorThrown",
                  }),
                ),
              );
              failures.push(error);
            }
          }
          if (failures.length > 0)
            throw new AggregateError(failures, "Host work failed");
        };
        function pass(run: () => Promise<void>) {
          let active: Promise<void> | undefined;
          let rerun = false;
          let retry = false;
          const slot = {
            get active() {
              return active;
            },
            get rerun() {
              return rerun;
            },
            get retry() {
              return retry;
            },
            stop() {
              rerun = false;
            },
            schedule,
          };
          function schedule(): void {
            if (closed) return;
            if (active !== undefined) {
              rerun = true;
              return;
            }
            const task = Promise.resolve().then(async () => {
              const reconciledVersion = mutationVersion;
              try {
                // Recover only after owner startup commands have committed.
                if (!(await runShiitakePromise(startup)) || isClosed()) return;
                await run();
                if (closed) return;
                retry = false;
                // Native transactions serialize this read/disarm with acceptance.
                await state.storage.transaction(async () => {
                  if (closed) return;
                  const owned = await runShiitakePromise(pendingWork);
                  const host = hostWork.reduce<
                    ReturnType<CloudflareHostWork["pending"]>
                  >(
                    (pending, work) => {
                      const next = work.pending();
                      return {
                        due: pending.due || next.due,
                        nextWakeAt: earliest(
                          pending.nextWakeAt,
                          next.nextWakeAt,
                        ),
                      };
                    },
                    { due: false, nextWakeAt: null },
                  );
                  const pending = {
                    pending: owned.pending || host.due,
                    nextWakeAt: earliest(owned.nextWakeAt, host.nextWakeAt),
                  };
                  // A mutation can need reconciliation before it creates a job
                  // (for example an extension command that sets a timer). An older
                  // run cannot disarm it.
                  if (
                    reconciledVersion !== mutationVersion ||
                    passes.some(
                      (other) =>
                        other.retry ||
                        other.rerun ||
                        (other !== slot && other.active !== undefined),
                    ) ||
                    pending.pending
                  )
                    await arm();
                  else await armAt(pending.nextWakeAt);
                });
              } catch (error) {
                retry = true;
                // Not an Effect here: this is a plain async callback driven by
                // waitUntil. runShiitakePromise is the same bridge the rest of
                // this file uses to reach the host's configured Logger.
                // Retain the original failure without printing provider or Session
                // data: Effect's default Logger does not redact raw Error objects.
                await runShiitakePromise(
                  Effect.logError(
                    "shiitake_reconciliation_failed",
                    Redacted.make(error),
                  ).pipe(
                    Effect.annotateLogs({
                      operation: "durable-wake.reconcile",
                      failureKind:
                        error instanceof Error ? "Error" : "NonErrorThrown",
                    }),
                  ),
                );
                // Keep recovery independent of another request, including failure
                // after job settlement but before Session reconciliation finishes.
                if (!closed) await arm();
              } finally {
                if (active === task) active = undefined;
                if (rerun) {
                  rerun = false;
                  schedule();
                }
              }
            });
            active = task;
            state.waitUntil(task);
          }
          return slot;
        }
        const passes = [
          pass(() => runShiitakePromise(reconcile)),
          pass(hostPass),
        ];
        function schedule(): void {
          for (const slot of passes) slot.schedule();
        }

        const requestWake = Effect.suspend(() =>
          closed
            ? Effect.fail(
                runtimeFailure("closed", "Shiitake runtime is closed"),
              )
            : persistWake.pipe(Effect.tap(() => Effect.sync(schedule))),
        );
        alarm = () => runShiitakePromise(requestWake);
        return Object.freeze({
          commit: <A>(mutation: Effect.Effect<A, ShiitakeError>) =>
            Effect.tryPromise({
              try: () =>
                state.storage.transaction(async () => {
                  const result = await runShiitakePromise(mutation);
                  // Invalidate before the transaction returns, including a lost
                  // commit acknowledgement. Rollback may cause one extra wake.
                  mutationVersion = Symbol();
                  await arm();
                  return result;
                }),
              catch: (error) =>
                error instanceof ShiitakeError ? error : unavailable(),
            }),
          requestWake,
          close: Effect.suspend(() => {
            closed = true;
            for (const slot of passes) slot.stop();
            return Effect.tryPromise({
              try: async () => {
                const active = passes.flatMap((slot) =>
                  slot.active === undefined ? [] : [slot.active],
                );
                const outcomes = await Promise.allSettled(active);
                const failures: unknown[] = [];
                for (const outcome of outcomes) {
                  if (outcome.status === "rejected")
                    failures.push(outcome.reason);
                }
                if (failures.length > 0)
                  throw new AggregateError(failures, "Runtime close failed");
              },
              catch: unavailable,
            });
          }),
        });
      }),
  };
  return {
    runtime,
    alarm: () =>
      alarm === undefined
        ? Promise.reject(
            runtimeFailure("unavailable", "Shiitake runtime is not bound"),
          )
        : alarm(),
  };
}

function configuredTools(
  tools: CloudflareShiitakeOptions["tools"],
): ToolSourceEffect | undefined {
  if (tools === undefined || !("sandbox" in tools)) return undefined;
  return {
    modules: tools.modules ?? [],
    sources: tools.sources ?? [],
    sandbox: fromTypedSandbox(tools.sandbox),
    access: tools.access ?? {},
  };
}

function effectOptions(
  state: CloudflareShiitakeState,
  options: CloudflareShiitakeOptions,
  runtime: RuntimeHostEffect,
): ShiitakeEffectOptions {
  const tools = configuredTools(options.tools);
  const base: Omit<ShiitakeEffectOptions, "tools"> = {
    store: state.storage,
    runtime,
    extensions: options.extensions,
    instructions: {
      load: ({ sessionId, commandId }) =>
        Effect.tryPromise({
          try: (signal) =>
            options.instructions.load({ sessionId, commandId, signal }),
          catch: () =>
            new ShiitakeError(
              "instructions",
              "rejected",
              "Instruction acquisition failed",
            ),
        }),
    },
    model: adaptModelSource(options.model),
  };
  if (options.hostJobs !== undefined) {
    const hostJobs = options.hostJobs;
    Object.assign(base, {
      hostJobs: {
        lane: hostJobs.lane,
        open: (
          watchdog: Parameters<
            NonNullable<ShiitakeEffectOptions["hostJobs"]>["open"]
          >[0],
        ) => {
          const host = hostJobs.open({
            transactional: watchdog.transactional,
            readJob: (jobId) => runShiitakePromise(watchdog.read(jobId)),
            tickConcurrent: () => runShiitakePromise(watchdog.tickConcurrent()),
          });
          return {
            execute: (job: WatchdogRunningJob) =>
              Effect.tryPromise({
                try: () => host.execute(job),
                catch: (error) => new WatchdogError("execution_failed", error),
              }),
          };
        },
      },
    });
  }
  if (options.meta !== undefined) Object.assign(base, { meta: options.meta });
  if (options.storagePressure !== undefined)
    Object.assign(base, { storagePressure: options.storagePressure });
  if (options.compaction !== undefined)
    Object.assign(base, { compaction: options.compaction });
  return tools === undefined ? base : { ...base, tools };
}

/** Cloudflare host lifecycle; alarm forwarding stays outside the neutral client. */
export type CloudflareShiitake = ShiitakeOwnerAgent &
  Readonly<{
    /** Exact settled command facts, outside the neutral Session transport. */
    readSettlement: (
      sessionId: string,
      commandId: string,
    ) => Promise<CommandSettlement | undefined>;
    /**
     * Admit host-authorized mail, preserving its parent address and opaque
     * metadata. `reply` is the host's authentication of a delegated reply.
     */
    acceptMail: (
      mail: BirdDogMail,
      meta?: Json,
      reply?: VerifiedAgentReply,
      stageAcceptedNotice?: (mail: BirdDogMail) => void,
      /** Grants for a new Session, written in the acceptance transaction. */
      sessionGrantDelivery?: (sessionId: string) => GrantsDelivery,
    ) => Promise<Accepted>;
    /** Deliver the native owner alarm to the bound runtime. */
    alarm: () => Promise<void>;
    /** Snapshot actual live executors, independent of instruction leases. */
    activeSessions: () => Promise<readonly string[]>;
    /** Abort each currently executing Session through its ordinary cancellation door. */
    abortActive: () => Promise<void>;
  }>;

/** Construct Shiitake over one Durable Object's SQLite and lifetime. */
/**
 * The state with its storage named for Woodstock's refusals after the
 * Durable Object's id, when it has one. Its owner and every reader of it
 * share this one storage object, so the name travels with the owner.
 */
function namedState(state: CloudflareShiitakeState): CloudflareShiitakeState {
  const { id, storage } = state;
  if (id === undefined || storage.storeName !== undefined) return state;
  return {
    id,
    waitUntil: (task) => {
      state.waitUntil(task);
    },
    storage: {
      sql: storage.sql,
      transactionSync: (operation) => storage.transactionSync(operation),
      transaction: (body) => storage.transaction(body),
      getAlarm: (alarmOptions) => storage.getAlarm(alarmOptions),
      setAlarm: (time, alarmOptions) => storage.setAlarm(time, alarmOptions),
      deleteAlarm: (alarmOptions) => storage.deleteAlarm(alarmOptions),
      storeName: `Durable Object ${id.toString()}`,
    },
  };
}

/**
 * Open Shiitake over one Durable Object's storage. It resolves once the runtime
 * is started and `initialize` has run; if `initialize` throws, the runtime is
 * closed and the error is rethrown.
 */
export function makeCloudflareShiitake(
  unnamed: CloudflareShiitakeState,
  options: CloudflareShiitakeOptions,
): Promise<CloudflareShiitake> {
  const state = namedState(unnamed);
  const startup = Deferred.makeUnsafe<boolean>();
  const host = runtimeHost(state, options.hostWork, Deferred.await(startup));
  const coreOptions = effectOptions(state, options, host.runtime);
  return import("./internal/runtime-core.js")
    .then(({ openRuntimeCore }) =>
      runShiitakePromise(openRuntimeCore(coreOptions, options.mailRoom)),
    )
    .then(
      async ({ agent, activeSessions, acceptMail, readSettlement }) => {
        const handle = toPromiseOwner(agent);
        try {
          await options.initialize?.(handle);
        } catch (cause) {
          await runShiitakePromise(Deferred.succeed(startup, false));
          await runShiitakePromise(agent.close);
          throw cause;
        }
        await runShiitakePromise(Deferred.succeed(startup, true));
        return Object.freeze({
          ...handle,
          readSettlement: (sessionId: string, commandId: string) =>
            runShiitakePromise(readSettlement(sessionId, commandId)),
          acceptMail: (
            mail: BirdDogMail,
            meta?: Json,
            reply?: VerifiedAgentReply,
            stageAcceptedNotice?: (mail: BirdDogMail) => void,
            sessionGrantDelivery?: (sessionId: string) => GrantsDelivery,
          ) => {
            if (options.mailRoom === undefined)
              return Promise.reject(
                runtimeFailure("unavailable", "Addressed mail is unavailable"),
              );
            return runShiitakePromise(
              acceptMail(
                mail,
                meta,
                reply,
                stageAcceptedNotice,
                sessionGrantDelivery,
              ),
            );
          },
          alarm: host.alarm,
          activeSessions: () => runShiitakePromise(activeSessions),
          abortActive: async () => {
            for (const sessionId of await runShiitakePromise(activeSessions))
              await runShiitakePromise(agent.session(sessionId).abort);
          },
        });
      },
      async (cause: unknown) => {
        await runShiitakePromise(Deferred.succeed(startup, false));
        throw cause;
      },
    );
}

/** Owns runtime opening, HTTP/ACP transport, inbox observation and alarm recovery. */
export abstract class ShiitakeOwner<
  Env,
  State extends CloudflareShiitakeState = CloudflareShiitakeState,
> {
  constructor(
    protected readonly ctx: State,
    protected readonly env: Env,
  ) {}
  private agentPromise: Promise<CloudflareShiitake> | undefined;
  private openAcp: CloudflareShiitakeOptions["acp"];
  #named: CloudflareShiitakeState | undefined;
  /** This owner's state, its storage named once for Woodstock's refusals. */
  private get named(): CloudflareShiitakeState {
    return (this.#named ??= namedState(this.ctx));
  }
  private acpServer: Promise<CloudflareShiitakeAcpServer> | undefined;
  /** Consecutive native alarm failures; reset on the next successful alarm. */
  private alarmFailures = 0;
  private readonly routes = createShiitakeHttpRoutes(
    async (sessionId, meta) => {
      const agent = await this.shiitake();
      return (meta === undefined ? agent : agent.withMeta(meta)).session(
        sessionId,
      );
    },
    async (after) => (await this.shiitake()).listSessions(after),
    async (requestId) => (await this.shiitake()).createSession(requestId),
    async (sessionId) => (await this.shiitake()).deleteSession(sessionId),
    async () => (await this.shiitake()).extensions,
  );

  /** Supply this agent's model, instructions, and optional capabilities. */
  protected abstract shiitakeOptions():
    CloudflareShiitakeOptions | Promise<CloudflareShiitakeOptions>;

  /** Borrow this owner's runtime directly; only the host owns its lifetime. */
  public async shiitake(): Promise<CloudflareShiitake> {
    // Publish the flight before loading options: concurrent cold requests
    // share both first-use imports and the runtime they configure.
    this.agentPromise ??= Promise.resolve().then(async () => {
      const options = await this.shiitakeOptions();
      this.openAcp = options.acp;
      return makeCloudflareShiitake(this.named, options);
    });
    try {
      return await this.agentPromise;
    } catch (error) {
      this.agentPromise = undefined;
      throw error;
    }
  }

  /** Stop live execution without constructing a cold owner's runtime. */
  public async abortActiveSessions(): Promise<void> {
    await (await this.agentPromise)?.abortActive();
  }

  /** Close owned resources before storage erasure; a cold owner has none. */
  public async closeShiitake(): Promise<void> {
    await (await this.agentPromise)?.close();
  }

  /** Indexed owner-local existence check; corrupt stored Sessions remain unavailable. */
  public async hasStoredSession(sessionId: string): Promise<boolean> {
    await this.shiitake();
    const inbox = readSessionInboxSummary(this.named.storage, sessionId);
    if (inbox === undefined || inbox.state === "unavailable") return false;
    return (
      (await runShiitakePromise(
        readSessionSummary(this.named.storage, sessionId),
      )) !== undefined
    );
  }

  /** Bounded strict-after read of this Agent's changed inbox rows. */
  public async readInboxChanges(
    after: number,
    limit: number,
  ): Promise<SessionInboxChanges> {
    await this.shiitake();
    return readSessionInboxChanges(this.named.storage, after, limit);
  }

  /**
   * One bounded, read-only page of Sessions with stored bytes, last activity
   * and an idle flag, coldest first. The store keeper's only view of cold
   * Sessions; it carries no SQL, transaction or write.
   */
  public async readColdSessions(
    request: ColdSessionPageRequest,
  ): Promise<ColdSessionPage> {
    await this.shiitake();
    return await runShiitakePromise(
      readColdSessionPage(this.named.storage, request),
    );
  }

  /** Latest inbox revision; read only after the owner has opened. */
  public readInboxRevision(): number {
    return readSessionInboxChanges(
      this.named.storage,
      Number.MAX_SAFE_INTEGER,
      0,
    ).latest;
  }

  /** Prune deletion deltas after every current member inbox has acknowledged them. */
  public pruneInboxTombstones(through: number): void {
    pruneSessionInboxTombstones(this.named.storage, through);
  }

  /** Persist a wake and run one reconciliation pass, including host work. */
  public async wakeRuntime(): Promise<void> {
    await (await this.shiitake()).alarm();
  }

  /**
   * Native alarms reconstruct the owner without an incoming user request.
   * A construction or wake failure must not strand the claimed work on
   * platform alarm retries: it is caught, logged, and explicitly re-armed
   * with bounded backoff instead.
   */
  public async alarm(): Promise<void> {
    try {
      await (await this.shiitake()).alarm();
      this.alarmFailures = 0;
    } catch (error) {
      this.alarmFailures += 1;
      const attempt = this.alarmFailures;
      const retryInMs = alarmRetryDelayMs(attempt);
      await runShiitakePromise(
        Effect.logError("shiitake_alarm_failed").pipe(
          Effect.annotateLogs({
            cause: error instanceof Error ? error.message : String(error),
            attempt,
            retryInMs,
          }),
        ),
      );
      try {
        await this.ctx.storage.setAlarm(Date.now() + retryInMs);
      } catch (rearmError) {
        // The platform's own alarm retry is the last resort if even this
        // explicit re-arm fails; surface it rather than fail silently.
        await runShiitakePromise(
          Effect.logError("shiitake_alarm_rearm_failed").pipe(
            Effect.annotateLogs({
              cause:
                rearmError instanceof Error
                  ? rearmError.message
                  : String(rearmError),
              attempt,
            }),
          ),
        );
        throw error;
      }
    }
  }

  /** Serve Shiitake's complete transport without subclass routing. */
  private app: Hono<ShiitakeHttpEnv> | undefined;

  /**
   * Shiitake's whole HTTP surface (Session routes and ACP) as one
   * Hono app. A host mounts it behind its own middleware, which alone may
   * set the `shiitakeMeta` variable.
   */
  protected shiitakeApp(): Hono<ShiitakeHttpEnv> {
    return (this.app ??= new Hono<ShiitakeHttpEnv>()
      .all("/acp", async (context) => {
        const agent = await this.shiitake();
        if (this.openAcp === undefined)
          return new Response(null, { status: 404 });
        const opening = (this.acpServer ??= this.openAcp(agent));
        let server: CloudflareShiitakeAcpServer;
        try {
          server = await opening;
        } catch (error) {
          if (this.acpServer === opening) this.acpServer = undefined;
          throw error;
        }
        return server.handleRequest(
          context.req.raw,
          context.get("shiitakeMeta"),
        );
      })
      .route("/", this.routes)
      // ACP failures reach the Durable Object caller unchanged,
      // as they did before the routes shared one app.
      .onError((error) => {
        throw error;
      }));
  }

  /** Serve one request through Shiitake's whole HTTP surface. */
  public async fetch(request: Request): Promise<Response> {
    return await this.shiitakeApp().fetch(request);
  }
}

/** Native Cloudflare entry points; all runtime lifetime belongs to the owner. */
export abstract class ShiitakeDO<Env> extends DurableObject<Env> {
  #owner: ShiitakeOwner<Env> | undefined;

  protected abstract shiitakeOptions():
    CloudflareShiitakeOptions | Promise<CloudflareShiitakeOptions>;

  protected runtimeOwner(): ShiitakeOwner<Env> {
    const options = () => this.shiitakeOptions();
    return (this.#owner ??= new (class extends ShiitakeOwner<Env> {
      protected override shiitakeOptions() {
        return options();
      }
    })(this.ctx, this.env));
  }

  protected shiitake() {
    return this.runtimeOwner().shiitake();
  }
  protected abortActiveSessions() {
    return this.runtimeOwner().abortActiveSessions();
  }
  protected closeShiitake() {
    return this.runtimeOwner().closeShiitake();
  }
  protected hasStoredSession(sessionId: string) {
    return this.runtimeOwner().hasStoredSession(sessionId);
  }
  protected readInboxChanges(after: number, limit: number) {
    return this.runtimeOwner().readInboxChanges(after, limit);
  }
  protected readInboxRevision() {
    return this.runtimeOwner().readInboxRevision();
  }
  protected readColdSessions(request: ColdSessionPageRequest) {
    return this.runtimeOwner().readColdSessions(request);
  }
  protected pruneInboxTombstones(through: number) {
    this.runtimeOwner().pruneInboxTombstones(through);
  }
  protected wakeRuntime() {
    return this.runtimeOwner().wakeRuntime();
  }
  /** Reconcile due runtime and host work through the owner. */
  public override alarm() {
    return this.runtimeOwner().alarm();
  }
  /**
   * The host's own routes, in front of Shiitake's: return a response to answer
   * the request, or undefined to let Shiitake serve it. Absent means none.
   */
  protected hostFetch?(request: Request): Promise<Response> | undefined;
  /** Route an authenticated client request through the host's routes, then the owner. */
  public override fetch(request: Request) {
    return this.hostFetch?.(request) ?? this.runtimeOwner().fetch(request);
  }
}
