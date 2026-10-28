import {
  agentJobFromInput,
  type BirdDogEffect,
} from "@fungi.computer/bird-dog/effect";
import { WatchdogError } from "@fungi.computer/watchdog";
import {
  makeRuntime as makeWatchdogRuntime,
  type WatchdogQueueHead,
  type EffectWatchdogOptions,
} from "@fungi.computer/watchdog/effect";
import * as Effect from "effect/Effect";
import * as Latch from "effect/Latch";

import type { AgentRunningWork } from "../client.js";
import type { GrantsDelivery } from "./contracts.js";
import { publishAgentExecutionChange } from "./observation-kernel.js";
import { ShiitakeError } from "../errors.js";
import {
  agentTurnRecovery,
  executeAcceptedRun,
} from "./accepted-run-executor.js";
import { openAdmissionDoor, type AdmissionDoor } from "./admission-door.js";
import { openCompactionSettings } from "./compaction/settings.js";
import type { ShiitakeEffectOptions } from "./effect-contracts.js";
import {
  admitExtensionRosterV0,
  type AdmittedExtensionV0,
} from "./extension-roster.js";
import {
  coreConsumer,
  reportConsumer,
  openExtensionRuntime,
  type ExtensionRuntime,
} from "./extension-runtime.js";
import { reconcileRuntime } from "./runtime-reconciliation.js";
import {
  advanceCursor,
  createSessionWithinTransaction,
  deliverSessionGrants,
  prepareExtensionStore,
  projectSessionRun,
  openStorageAdmission,
  readCursor,
  type RecordAcceptedCancellation,
  type RunContract,
  type StorageAdmission,
} from "./woodstock/index.js";

/** Bound lifecycle and owner-transaction doors used by the public coordinator. */
export type RuntimeCoordinator = Readonly<{
  prepareConfiguration: import("./effect-contracts.js").ShiitakeEffectOwner["prepareConfiguration"];
  activeSessions: Effect.Effect<readonly string[]>;
  runningWork: (
    jobId: string,
  ) => Effect.Effect<AgentRunningWork | null, ShiitakeError>;
  assertOpen: Effect.Effect<void, ShiitakeError>;
  /** Rostered v1 extensions bound to core's doors and tables. */
  extensions: ExtensionRuntime;
  beginClose: Effect.Effect<void>;
  /** The one accepted-command door every prompt producer admits through. */
  door: AdmissionDoor;
  /** Reserve a Session within the admission door's enclosing transaction. */
  createSessionWithinTransaction: (requestId: string) => string;
  /** Write new-Session grants through the enclosing admission transaction. */
  stageSessionGrantsWithinTransaction: (
    sessionId: string,
    delivery: GrantsDelivery,
  ) => void;
  /** Storage-pressure check for new Sessions and new prompts. */
  storage: StorageAdmission;
  recordAcceptedCancellation: RecordAcceptedCancellation;
  assertIdleSession: (queue: string) => void;
  signalCancellation: (
    jobId: string,
    cancellationId: string,
  ) => Effect.Effect<void, ShiitakeError>;
  commit: <A>(
    mutation: Effect.Effect<A, ShiitakeError>,
  ) => Effect.Effect<A, ShiitakeError>;
  requestWake: Effect.Effect<void, ShiitakeError>;
  readQueueHead: (
    queue: string,
  ) => Effect.Effect<WatchdogQueueHead, ShiitakeError>;
  /** Transactional queue-head projection used by owner-atomic cancellation. */
  readQueueHeadTransactional: (queue: string) => WatchdogQueueHead;
  close: Effect.Effect<void, ShiitakeError>;
}>;

type CoordinatorLifecycleState = "open" | "closing" | "closed";

type HostExecutorCell = {
  current:
    | ReturnType<NonNullable<ShiitakeEffectOptions["hostJobs"]>["open"]>
    | undefined;
};

type BoundRuntimeSelfReference = {
  requestWake?: Effect.Effect<void, ShiitakeError>;
  commit?: <A>(
    mutation: Effect.Effect<A, ShiitakeError>,
  ) => Effect.Effect<A, ShiitakeError>;
};

type ExtensionRuntimeCell = { current: ExtensionRuntime | undefined };

function runtimeFailure(message: string): ShiitakeError {
  return new ShiitakeError("runtime", "rejected", message);
}

/** Construct Watchdog once and bind its deep tick to the host wake door. */
export function openRuntimeCoordinator(
  options: ShiitakeEffectOptions,
  coreDigest: string,
  admitted: readonly AdmittedExtensionV0[],
  mailRoom?: BirdDogEffect,
): Effect.Effect<RuntimeCoordinator, ShiitakeError> {
  return Effect.gen(function* () {
    if (options.hostJobs?.lane === "default")
      return yield* Effect.fail(
        runtimeFailure("Host lane conflicts with Agent work"),
      );
    let lifecycleState: CoordinatorLifecycleState = "open";
    let retainedFailure: ShiitakeError | undefined;
    let retainedBeforeClose: ShiitakeError | undefined;
    const closing = Latch.makeUnsafe(false);
    const retainFailure = (failure: ShiitakeError) =>
      Effect.sync(() => {
        if (lifecycleState === "open") retainedFailure ??= failure;
      });
    const beginClose = Effect.suspend(() => {
      if (lifecycleState !== "open") return Effect.void;
      lifecycleState = "closing";
      retainedBeforeClose = retainedFailure;
      return closing.open;
    }).pipe(Effect.uninterruptible);
    const boundRuntime: BoundRuntimeSelfReference = {};
    const extensionsRef: ExtensionRuntimeCell = { current: undefined };
    let turnOptions = options;
    let pendingConfiguration:
      | Readonly<{
          configuration: Pick<ShiitakeEffectOptions, "extensions" | "tools">;
          apply: Effect.Effect<void, ShiitakeError>;
        }>
      | undefined;
    let preparing = false;
    const requestBoundWake = Effect.suspend(
      () =>
        boundRuntime.requestWake ??
        Effect.fail(runtimeFailure("Runtime host is unavailable")),
    );
    const hostExecutor: HostExecutorCell = { current: undefined };
    const activeSessions = new Set<string>();
    const runningSlots = new Map<"serial" | "host", AgentRunningWork>();
    const watchdogOptions: EffectWatchdogOptions = {
      owner: options.store,
      // Each Watchdog slot serializes ticks and inspects only its partition. A claim
      // found on a new owner has no surviving execution (including DO eviction).
      // Hosts must keep one runtime per SQLite owner, never two live coordinators.
      isAlive: () => Effect.succeed(false),
      executor: {
        execute: (job) =>
          Effect.acquireUseRelease(
            Effect.sync(() => {
              const runningWork: AgentRunningWork =
                job.lane === options.hostJobs?.lane
                  ? { kind: "host", lane: job.lane, jobId: job.jobId }
                  : {
                      kind: "session",
                      sessionId: job.queue,
                      commandId: job.jobId,
                    };
              runningSlots.set(
                runningWork.kind === "host" ? "host" : "serial",
                runningWork,
              );
              if (runningWork.kind === "session") activeSessions.add(job.queue);
              // A different Session's wait changes when this executor changes.
              publishAgentExecutionChange(options.store);
            }),
            () =>
              job.lane === options.hostJobs?.lane
                ? (hostExecutor.current?.execute(job) ??
                  Effect.fail(new WatchdogError("execution_failed")))
                : Effect.gen(function* () {
                    const pending = pendingConfiguration;
                    if (pending !== undefined) {
                      yield* pending.apply;
                      const { tools: oldTools, ...base } = options;
                      void oldTools;
                      turnOptions = { ...base, ...pending.configuration };
                      pendingConfiguration = undefined;
                    }
                    return yield* executeAcceptedRun(
                      turnOptions,
                      coreDigest,
                      job,
                      extensionsRef.current,
                    );
                  }).pipe(
                    Effect.tapError((error) =>
                      Effect.logError("configuration_apply_failed", error),
                    ),
                    Effect.mapError(
                      () => new WatchdogError("execution_failed"),
                    ),
                  ),
            () =>
              Effect.sync(() => {
                if (job.lane !== options.hostJobs?.lane)
                  activeSessions.delete(job.queue);
                runningSlots.delete(
                  job.lane === options.hostJobs?.lane ? "host" : "serial",
                );
                publishAgentExecutionChange(options.store);
              }),
          ),
      },
      wake: {
        recompute: () =>
          requestBoundWake.pipe(
            Effect.mapError(() => new WatchdogError("wake_unavailable")),
          ),
      },
      // Inbox rows change in the same transaction as the queue they project.
      project: (transition) => {
        projectSessionRun(options.store, transition, Date.now());
      },
    };
    const openWatchdog =
      options.hostJobs === undefined
        ? makeWatchdogRuntime(watchdogOptions)
        : makeWatchdogRuntime({
            ...watchdogOptions,
            concurrentLane: options.hostJobs.lane,
          }).pipe(
            Effect.tap((runtime) =>
              Effect.sync(() => {
                hostExecutor.current = options.hostJobs?.open(runtime);
              }),
            ),
          );
    const watchdog = yield* openWatchdog.pipe(
      Effect.mapError(() => runtimeFailure("Watchdog is unavailable")),
    );
    const contract = (): RunContract => ({
      core: coreDigest,
      extensions: [...(extensionsRef.current?.runParticipants() ?? [])],
    });
    // Refuse invalid compaction budgets when the owner opens, not mid-run.
    yield* Effect.try({
      try: () => openCompactionSettings(options.compaction),
      catch: (error) =>
        error instanceof ShiitakeError
          ? error
          : runtimeFailure("Compaction settings are unavailable"),
    });
    const storage = yield* Effect.try({
      try: () => openStorageAdmission(options.storagePressure),
      catch: (error) =>
        error instanceof ShiitakeError
          ? error
          : runtimeFailure("Storage budget is unavailable"),
    });
    const door = openAdmissionDoor({
      owner: options.store,
      admitNew: storage.admitNew,
      hostMeta: options.meta,
      contract,
      enqueue: (work, mail) => {
        const input = { work: { ...work, recovery: agentTurnRecovery } };
        watchdog.transactional.enqueueAcceptedJob(
          agentJobFromInput(mail === undefined ? input : { ...input, mail }),
        );
      },
      readQueueHead: (queue) => watchdog.transactional.readQueueHead(queue),
      commit: (mutation) =>
        Effect.suspend(() =>
          boundRuntime.commit === undefined
            ? Effect.fail(runtimeFailure("Runtime host is unavailable"))
            : boundRuntime.commit(mutation),
        ),
      requestWake: requestBoundWake,
    });
    yield* Effect.try({
      try: () => {
        prepareExtensionStore(options.store);
        if (readCursor(options.store, coreConsumer) === undefined)
          options.store.transactionSync(() => {
            advanceCursor(options.store, coreConsumer, 0);
          });
        // A store that predates reports starts them at the core cursor
        // instead of reporting its history.
        if (readCursor(options.store, reportConsumer) === undefined)
          options.store.transactionSync(() => {
            advanceCursor(
              options.store,
              reportConsumer,
              readCursor(options.store, coreConsumer) ?? 0,
            );
          });
      },
      catch: () =>
        new ShiitakeError(
          "store",
          "unavailable",
          "Extension store is unavailable",
        ),
    });
    const extensions = yield* openExtensionRuntime({
      owner: options.store,
      readQueueHead: (sessionId) =>
        watchdog.transactional.readQueueHead(sessionId),
      door,
      admitted,
      requestWake: requestBoundWake,
    });
    extensionsRef.current = extensions;
    const reconcile = reconcileRuntime({
      watchdog,
      store: options.store,
      extensions,
      requestWake: requestBoundWake,
      mailRoom,
    }).pipe(Effect.tapError(retainFailure));
    const pendingWork = Effect.gen(function* () {
      const watchdogPending = yield* watchdog.hasPendingWork;
      const now = Date.now();
      const cursors = yield* Effect.try({
        try: () => [
          readCursor(options.store, coreConsumer) ?? 0,
          readCursor(options.store, reportConsumer) ?? 0,
          ...extensions.settledConsumers().map(({ cursor }) => cursor),
        ],
        catch: () => runtimeFailure("Settled cursor is unavailable"),
      });
      const settledLag =
        (yield* watchdog.readSettled(1, Math.min(...cursors))).length > 0;
      const timers = yield* Effect.try({
        try: () => extensions.timerWake(now),
        catch: () => runtimeFailure("Extension timers are unavailable"),
      });
      const mailWake =
        mailRoom === undefined
          ? undefined
          : yield* mailRoom
              .nextWakeAt()
              .pipe(
                Effect.mapError(() =>
                  runtimeFailure("Reply retry wake is unavailable"),
                ),
              );
      // Carrier owns reply retry state. Keep it in the same durable answer
      // as runtime work, so a host cannot erase its wake with an idle result.
      const nextWakeAt =
        timers.nextWakeAt === null
          ? (mailWake ?? null)
          : mailWake === undefined
            ? timers.nextWakeAt
            : Math.min(timers.nextWakeAt, mailWake);
      return {
        pending:
          watchdogPending ||
          settledLag ||
          timers.due ||
          (mailWake !== undefined && mailWake <= now),
        nextWakeAt,
      };
    });
    const bound = yield* options.runtime
      .bind({
        reconcile,
        pendingWork: pendingWork.pipe(
          Effect.mapError(() => runtimeFailure("Pending work is unavailable")),
        ),
      })
      .pipe(Effect.onError(() => extensions.close.pipe(Effect.orDie)));
    const requestWake = Effect.suspend(() =>
      lifecycleState === "open"
        ? Effect.raceFirst(
            bound.requestWake.pipe(Effect.catch(retainFailure)),
            closing.await,
          )
        : Effect.void,
    );
    boundRuntime.requestWake = requestWake;
    boundRuntime.commit = bound.commit;
    // Every host must get one post-bind tick to recover durable work after a cold reopen.
    yield* bound.requestWake.pipe(
      Effect.onError(() =>
        Effect.all([bound.close, extensions.close]).pipe(Effect.orDie),
      ),
    );

    return Object.freeze({
      prepareConfiguration: (configuration) =>
        Effect.gen(function* () {
          if (
            lifecycleState !== "open" ||
            preparing ||
            pendingConfiguration !== undefined
          )
            return yield* Effect.fail(
              runtimeFailure(
                "A configuration change is already pending, or the Agent is closing",
              ),
            );
          preparing = true;
          const staged = yield* admitExtensionRosterV0(
            configuration.extensions,
          ).pipe(
            Effect.flatMap((next) => extensions.stage(next)),
            Effect.onError(() =>
              Effect.sync(() => {
                preparing = false;
              }),
            ),
          );
          let settled = false;
          return {
            commit: Effect.try({
              try: () => {
                if (settled || lifecycleState !== "open")
                  throw runtimeFailure(
                    "Configuration preparation is no longer available",
                  );
                pendingConfiguration = { configuration, apply: staged.apply };
                settled = true;
                preparing = false;
              },
              catch: (cause) =>
                cause instanceof ShiitakeError
                  ? cause
                  : runtimeFailure("Configuration commit failed"),
            }),
            discard: Effect.suspend(() => {
              if (settled) return Effect.void;
              settled = true;
              preparing = false;
              return staged.discard;
            }),
          };
        }),
      activeSessions: Effect.sync(() => [...activeSessions]),
      runningWork: (jobId) =>
        watchdog.read(jobId).pipe(
          Effect.map((result) =>
            result.status === "found"
              ? (runningSlots.get(
                  result.job.lane === options.hostJobs?.lane
                    ? "host"
                    : "serial",
                ) ?? null)
              : null,
          ),
          Effect.mapError(
            (cause) =>
              new ShiitakeError(
                "runtime",
                "rejected",
                "Running work is unavailable",
                undefined,
                cause,
              ),
          ),
        ),
      extensions,
      commit: bound.commit,
      assertOpen: Effect.suspend(() =>
        lifecycleState === "open"
          ? Effect.void
          : Effect.fail(
              new ShiitakeError("runtime", "closed", "Shiitake is closed"),
            ),
      ),
      beginClose,
      door,
      createSessionWithinTransaction: (requestId) =>
        createSessionWithinTransaction(
          options.store,
          requestId,
          storage.admitNew,
        ),
      stageSessionGrantsWithinTransaction: (sessionId, delivery) => {
        deliverSessionGrants(options.store, sessionId, delivery);
      },
      storage,
      recordAcceptedCancellation: ({ jobId, cancellationId, onlyIfPending }) =>
        watchdog.transactional.recordCancellationIntent({
          jobId,
          cancellationId,
          onlyIfQueued: onlyIfPending,
        }),
      assertIdleSession: (queue) => {
        try {
          const head = watchdog.transactional.readQueueHead(queue);
          if (head.state !== "idle")
            throw new ShiitakeError(
              "runtime",
              "conflict",
              "Session branch or fork requires an idle Session",
            );
        } catch (error) {
          if (error instanceof ShiitakeError) throw error;
          throw runtimeFailure("Session status is unavailable");
        }
      },
      signalCancellation: (jobId, cancellationId) =>
        watchdog.cancel({ jobId, cancellationId }).pipe(
          Effect.flatMap((result) =>
            result.status === "recorded" || result.status === "replayed"
              ? Effect.void
              : Effect.fail(runtimeFailure("Cancellation is unavailable")),
          ),
          Effect.mapError(() => runtimeFailure("Cancellation is unavailable")),
        ),
      readQueueHead: (queue) =>
        watchdog
          .readQueueHead(queue)
          .pipe(
            Effect.mapError(() =>
              runtimeFailure("Session status is unavailable"),
            ),
          ),
      readQueueHeadTransactional: (queue) =>
        watchdog.transactional.readQueueHead(queue),
      requestWake,
      close: beginClose.pipe(
        Effect.andThen(
          Effect.matchCauseEffect(bound.close, {
            onFailure: (cause) =>
              extensions.close.pipe(
                Effect.andThen(
                  retainedBeforeClose === undefined
                    ? Effect.failCause(cause)
                    : Effect.fail(retainedBeforeClose),
                ),
              ),
            onSuccess: () =>
              extensions.close.pipe(
                Effect.andThen(
                  retainedBeforeClose === undefined
                    ? Effect.void
                    : Effect.fail(retainedBeforeClose),
                ),
              ),
          }),
        ),
        Effect.ensuring(
          Effect.sync(() => {
            lifecycleState = "closed";
          }),
        ),
      ),
    });
  });
}
