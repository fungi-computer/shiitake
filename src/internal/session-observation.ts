import * as Effect from "effect/Effect";
import type * as Context from "effect/Context";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as Semaphore from "effect/Semaphore";
import * as Scope from "effect/Scope";
import * as Exit from "effect/Exit";
import type * as Fiber from "effect/Fiber";
import type {
  SessionContext,
  SessionFrame,
  SessionSnapshot,
  SubscribeOptions,
  WatchOptions,
} from "../client.js";
import { ShiitakeError } from "../errors.js";
import {
  decodeClientReadOptions,
  decodeClientSessionId,
  decodeClientSubscribeOptions,
  type ClientReadOptions,
  type ClientSubscribeOptions,
} from "./client-boundary.js";
import type {
  ShiitakeEffectOptions,
  ShiitakeEffectWatch,
} from "./effect-contracts.js";
import {
  mergeObservationSignals,
  openSessionFrameFeed,
  type ObservationKernel,
  type ObservationRegistration,
  type ObservationSignal,
  type SessionFrameQueue,
} from "./observation-kernel.js";
import type { RuntimeCoordinator } from "./runtime-coordinator.js";
import {
  readSessionActivity,
  type ObservedActivity,
} from "./session-status.js";
import { prepareSessionContext } from "./session-context.js";
import {
  compactionEntrySequence,
  decodeWoodstockCursor,
  progressPosition,
  readProcessProgressDelta,
  type ProgressPosition,
  readSession,
  readSessionReplayPage,
} from "./woodstock/index.js";
import { sessionHeartbeatIntervalMs } from "../session-liveness.js";

function currentSessionWork(runtime: RuntimeCoordinator, sessionId: string) {
  return runtime
    .readQueueHead(sessionId)
    .pipe(
      Effect.flatMap((head) =>
        (head.state === "queued"
          ? runtime.runningWork(head.jobId)
          : Effect.succeed(null)
        ).pipe(Effect.map((runningWork) => ({ head, runningWork }))),
      ),
    );
}

function currentSessionActivity(
  options: ShiitakeEffectOptions,
  runtime: RuntimeCoordinator,
  sessionId: string,
) {
  return currentSessionWork(runtime, sessionId).pipe(
    Effect.map(({ head, runningWork }) =>
      readSessionActivity(options.store, sessionId, head, runningWork),
    ),
  );
}

/** Carry the Session's context use on a baseline, when it has one. */
function withContext(
  snapshot: SessionSnapshot,
  context: SessionContext | undefined,
): SessionSnapshot {
  return context === undefined
    ? snapshot
    : Object.freeze({ ...snapshot, context });
}

type SnapshotCheckpoint = Readonly<{
  beforeRead?: () => void;
  afterRead?: () => void;
}>;

/** Read the baseline and its context use for one Session. */
function readSessionWithContext(
  options: ShiitakeEffectOptions,
  runtime: RuntimeCoordinator,
  sessionId: string,
  input: Parameters<typeof readSession>[3],
  checkpoint: SnapshotCheckpoint = {},
): Effect.Effect<SessionSnapshot, ShiitakeError> {
  return currentSessionWork(runtime, sessionId).pipe(
    Effect.flatMap(({ head, runningWork }) => {
      checkpoint.beforeRead?.();
      return readSession(
        options.store,
        sessionId,
        () => readSessionActivity(options.store, sessionId, head, runningWork),
        input,
        () => runtime.extensions.project(sessionId),
        (snapshot) => ({
          snapshot,
          context: prepareSessionContext(
            options,
            sessionId,
            snapshot.cursor,
            input?.before === undefined,
          ),
        }),
      );
    }),
    Effect.flatMap(({ snapshot, context }) => {
      checkpoint.afterRead?.();
      return context.pipe(Effect.map((value) => withContext(snapshot, value)));
    }),
  );
}

function decodeSubscriptionInput(
  sessionId: string,
  input: SubscribeOptions,
): Effect.Effect<
  Readonly<{ sessionId: string; options: ClientSubscribeOptions }>,
  ShiitakeError
> {
  return Effect.try({
    try: () =>
      Object.freeze({
        sessionId: decodeClientSessionId(sessionId),
        options: decodeClientSubscribeOptions(input),
      }),
    catch: (error) =>
      error instanceof ShiitakeError
        ? error
        : new ShiitakeError(
            "contract",
            "invalid",
            "Subscribe options are invalid",
          ),
  });
}

function resetFrame(
  options: ShiitakeEffectOptions,
  runtime: RuntimeCoordinator,
  sessionId: string,
  includeSandboxFailures = false,
  checkpoint: SnapshotCheckpoint = {},
): Effect.Effect<Extract<SessionFrame, { kind: "reset" }>, ShiitakeError> {
  return runtime.assertOpen.pipe(
    Effect.andThen(
      readSessionWithContext(
        options,
        runtime,
        sessionId,
        { includeSandboxFailures },
        checkpoint,
      ),
    ),
    Effect.map((baseline): Extract<SessionFrame, { kind: "reset" }> =>
      Object.freeze({
        kind: "reset",
        reason: "history_unavailable",
        cursor: baseline.cursor,
        baseline,
      }),
    ),
  );
}

type SubscriptionState = Readonly<{
  after: string | undefined;
  delivery: "replay" | "live";
  awaitSignal: boolean;
}>;
type SubscriptionStep = readonly [
  readonly SessionFrame[],
  Option.Option<SubscriptionState>,
];

function continueFromReset(
  frame: Extract<SessionFrame, { kind: "reset" }>,
): SubscriptionStep {
  return [
    [frame],
    Option.some({
      after: frame.cursor ?? undefined,
      delivery: frame.baseline.hasMore ? "replay" : "live",
      awaitSignal: !frame.baseline.hasMore,
    }),
  ];
}

type SubscriptionSource =
  | Readonly<{
      kind: "live";
      registration: ObservationRegistration;
      progress: SessionSnapshot["progress"];
      stamp: () => void;
    }>
  | Readonly<{ kind: "replay"; checkpoint: () => void }>;

function subscriptionFrames(
  options: ShiitakeEffectOptions,
  runtime: RuntimeCoordinator,
  sessionId: string,
  after: string | undefined,
  source: SubscriptionSource,
  includeSandboxFailures = false,
): Stream.Stream<SessionFrame, ShiitakeError> {
  const registration = source.kind === "live" ? source.registration : undefined;
  let position: ProgressPosition | null =
    source.kind === "live" ? progressPosition(source.progress) : null;
  let toolOutput: ObservedActivity["toolOutput"] = [];
  const reset = () =>
    resetFrame(
      options,
      runtime,
      sessionId,
      includeSandboxFailures,
      source.kind === "replay"
        ? { beforeRead: source.checkpoint }
        : { afterRead: source.stamp },
    ).pipe(
      Effect.map((frame) => {
        position = progressPosition(frame.baseline.progress);
        toolOutput = frame.baseline.toolOutput;
        return source.kind === "replay"
          ? ([[frame], Option.none<SubscriptionState>()] as const)
          : continueFromReset(frame);
      }),
    );
  const liveFrames = (
    activity: ObservedActivity,
  ): readonly SessionFrame[] | undefined => {
    const queue = activity.facts.queue;
    const delta =
      queue.state !== "running"
        ? null
        : readProcessProgressDelta(
            options.store,
            sessionId,
            queue.commandId,
            position,
          );
    if (delta === undefined) return undefined;
    const frames: SessionFrame[] = [];
    if (delta !== null) {
      frames.push({
        kind: "progress",
        progress: delta,
        activity: activity.facts,
      });
      position = {
        messageId: delta.messageId,
        revision: delta.revision,
        text: delta.text.offset + delta.text.append.length,
        thinking: delta.thinking.offset + delta.thinking.append.length,
      };
    }
    if (
      toolOutput.length !== activity.toolOutput.length ||
      toolOutput.some((slot, index) => slot !== activity.toolOutput[index])
    ) {
      toolOutput = activity.toolOutput;
      frames.push({
        kind: "tool_output",
        toolOutput,
        activity: activity.facts,
      });
    }
    return frames;
  };
  const initial: SubscriptionState = {
    after,
    delivery: source.kind,
    // Live registration precedes its baseline; pending hints cover any join
    // writes. Replaying it unconditionally adds an unprompted idle read.
    awaitSignal: source.kind === "live",
  };
  return Stream.paginate<SubscriptionState, SessionFrame, ShiitakeError>(
    initial,
    (state): Effect.Effect<SubscriptionStep, ShiitakeError> =>
      (registration === undefined
        ? Effect.succeed(undefined)
        : state.awaitSignal
          ? registration.awaitSignal.pipe(
              Effect.flatMap((signal) =>
                registration.pollSignal.pipe(
                  Effect.map((pending) =>
                    pending === undefined
                      ? signal
                      : mergeObservationSignals(signal, pending),
                  ),
                ),
              ),
            )
          : registration.pollSignal
      ).pipe(
        Effect.flatMap(
          (
            signal: ObservationSignal | undefined,
          ): Effect.Effect<SubscriptionStep, ShiitakeError> => {
            if (signal === "closed")
              return Effect.succeed([[], Option.none<SubscriptionState>()]);
            if (signal === "reset") return reset();

            return runtime.assertOpen.pipe(
              Effect.andThen(
                currentSessionActivity(options, runtime, sessionId),
              ),
              Effect.flatMap((activity) => {
                if (source.kind === "replay") source.checkpoint();
                registration?.setQueued(
                  activity.facts.queue.state === "queued",
                );
                if (
                  (signal === "progress" || signal === "activity") &&
                  state.delivery === "live"
                ) {
                  const frames = liveFrames(activity);
                  if (frames === undefined) return reset();
                  // This path reads no durable page; stamp the live projection.
                  if (source.kind === "live") source.stamp();
                  return Effect.succeed<SubscriptionStep>([
                    [
                      ...(signal === "activity"
                        ? [
                            {
                              kind: "activity" as const,
                              activity: activity.facts,
                            },
                          ]
                        : []),
                      ...frames,
                    ],
                    Option.some({ ...state, awaitSignal: true }),
                  ]);
                }
                return readSessionReplayPage(
                  options.store,
                  sessionId,
                  state.after,
                  state.delivery,
                  activity,
                  () => runtime.extensions.project(sessionId),
                  includeSandboxFailures,
                  (page) => {
                    const latestCursor =
                      page.kind === "page"
                        ? page.frames
                            .flatMap((frame) =>
                              frame.kind === "progress" ||
                              frame.kind === "activity" ||
                              frame.kind === "tool_output" ||
                              frame.kind === "heartbeat"
                                ? []
                                : [frame.cursor],
                            )
                            .at(-1)
                        : undefined;
                    // Capture the final moving page's stored context in the
                    // same read transaction as its cursor. Model description
                    // remains deferred until after that transaction.
                    const context =
                      page.kind === "page" &&
                      !page.hasMore &&
                      page.frames.some(
                        (frame) =>
                          frame.kind === "update" &&
                          frame.cursor !== state.after,
                      )
                        ? prepareSessionContext(
                            options,
                            sessionId,
                            latestCursor ?? null,
                          )
                        : Effect.succeed(undefined);
                    return { page, latestCursor, context };
                  },
                ).pipe(
                  Effect.flatMap(
                    ({
                      page,
                      latestCursor,
                      context,
                    }): Effect.Effect<SubscriptionStep, ShiitakeError> => {
                      if (page.kind === "reset") return reset();
                      if (source.kind === "live") source.stamp();

                      if (!page.hasMore) toolOutput = activity.toolOutput;
                      const progressFrames = page.hasMore
                        ? []
                        : liveFrames(activity);
                      if (progressFrames === undefined) return reset();

                      const next: SubscriptionState = {
                        after: latestCursor ?? state.after,
                        delivery: page.hasMore ? state.delivery : "live",
                        awaitSignal: !page.hasMore,
                      };
                      return context.pipe(
                        Effect.map((context): SubscriptionStep => [
                          [
                            ...(context === undefined
                              ? page.frames
                              : page.frames.map((frame) =>
                                  frame.kind === "update" &&
                                  frame.cursor === latestCursor
                                    ? Object.freeze({ ...frame, context })
                                    : frame,
                                )),
                            ...progressFrames,
                          ],
                          source.kind === "replay" && !page.hasMore
                            ? Option.none<SubscriptionState>()
                            : Option.some(next),
                        ]),
                      );
                    },
                  ),
                );
              }),
            );
          },
        ),
      ),
  );
}

type LiveSessionFeed = Readonly<{
  registration: ObservationRegistration;
  frames: ReturnType<typeof openSessionFrameFeed>;
  stop: () => void;
}>;

type SessionFeedLifecycle = {
  stopped: boolean;
  producer?: Fiber.Fiber<void>;
  heartbeat?: Fiber.Fiber<void>;
};

/** One projection producer and heartbeat clock per watched Session. */
function openLiveSessionFeeds(
  options: ShiitakeEffectOptions,
  runtime: RuntimeCoordinator,
  observation: ObservationKernel,
  ownerContext: Context.Context<never>,
) {
  type Lane = {
    lock: Semaphore.Semaphore;
    feed: LiveSessionFeed | undefined;
  };
  const lanes = new Map<string, Lane>();
  const scope = Scope.makeUnsafe();
  let watchers = 0;
  const acquire = (
    sessionId: string,
    includeSandboxFailures: boolean,
  ): Effect.Effect<
    Readonly<{ feed: LiveSessionFeed; queue: SessionFrameQueue }>,
    ShiitakeError
  > =>
    Effect.suspend(() => {
      let lane = lanes.get(sessionId);
      if (lane === undefined) {
        lane = { lock: Semaphore.makeUnsafe(1), feed: undefined };
        lanes.set(sessionId, lane);
      }
      const current = lane;
      return current.lock.withPermit(
        Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            if (lanes.get(sessionId) !== current)
              return yield* acquire(sessionId, includeSandboxFailures);
            let feed = current.feed;
            if (feed === undefined) {
              const registration = yield* observation.register(sessionId).pipe(
                Effect.onError(() =>
                  Effect.sync(() => {
                    if (lanes.get(sessionId) === current)
                      lanes.delete(sessionId);
                  }),
                ),
              );
              const lifecycle: SessionFeedLifecycle = { stopped: false };
              const frames = openSessionFrameFeed(() => {
                stop();
              });
              const stop = () => {
                if (lifecycle.stopped) return;
                lifecycle.stopped = true;
                registration.close();
                lifecycle.producer?.interruptUnsafe();
                lifecycle.heartbeat?.interruptUnsafe();
                frames.end();
                current.feed = undefined;
                if (lanes.get(sessionId) === current) lanes.delete(sessionId);
              };
              feed = { registration, frames, stop };
              current.feed = feed;
              const baseline = yield* restore(
                readSessionWithContext(options, runtime, sessionId, {
                  includeSandboxFailures: true,
                }),
              ).pipe(Effect.onError(() => Effect.sync(stop)));
              registration.setQueued(
                baseline.activity.queue.state === "queued",
              );
              let revision = registration.revision;
              // Replace the joining request's context while forking, so the
              // long-lived children inherit only the Agent's services/span.
              lifecycle.producer = yield* Effect.forkIn(
                subscriptionFrames(
                  options,
                  runtime,
                  sessionId,
                  baseline.cursor ?? undefined,
                  {
                    kind: "live",
                    registration,
                    progress: baseline.progress,
                    stamp: () => {
                      revision = registration.revision;
                    },
                  },
                  true,
                ).pipe(
                  Stream.runForEach((frame) =>
                    Effect.sync(() => {
                      frames.publish(frame, revision);
                    }),
                  ),
                  Effect.catch((error) =>
                    Effect.sync(() => {
                      frames.end(error);
                    }),
                  ),
                  Effect.ensuring(Effect.sync(stop)),
                ),
                scope,
                { uninterruptible: false },
              ).pipe(Effect.setContext(ownerContext));
              if (!lifecycle.stopped)
                lifecycle.heartbeat = yield* Effect.forkIn(
                  Stream.fromEffectRepeat(
                    Effect.sleep(sessionHeartbeatIntervalMs).pipe(
                      Effect.as({ kind: "heartbeat" } as const),
                    ),
                  ).pipe(
                    Stream.runForEach((frame) =>
                      Effect.sync(() => {
                        frames.publish(frame);
                      }),
                    ),
                  ),
                  scope,
                  { uninterruptible: false },
                ).pipe(Effect.setContext(ownerContext));
            }
            const queue = feed.frames.register(includeSandboxFailures);
            return { feed, queue };
          }),
        ),
      );
    });
  return {
    acquire: (sessionId: string, includeSandboxFailures: boolean) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.suspend(() => {
          if (watchers >= 64)
            return Effect.fail(
              new ShiitakeError(
                "runtime",
                "rejected",
                "Observation capacity is exhausted",
              ),
            );
          watchers += 1;
          let released = false;
          const release = () => {
            if (released) return;
            released = true;
            watchers -= 1;
          };
          return restore(acquire(sessionId, includeSandboxFailures)).pipe(
            Effect.map(({ feed, queue }) => ({
              feed,
              queue: Object.freeze({
                ...queue,
                close: () => {
                  release();
                  queue.close();
                },
              }),
            })),
            Effect.onError(() => Effect.sync(release)),
          );
        }),
      ),
    close: Effect.sync(() => {
      for (const lane of lanes.values()) lane.feed?.stop();
      lanes.clear();
    }).pipe(Effect.andThen(Scope.close(scope, Exit.void))),
  };
}

type LiveSessionFeeds = ReturnType<typeof openLiveSessionFeeds>;

function queuedSessionFrames(
  queue: SessionFrameQueue,
): Stream.Stream<SessionFrame | "reset", ShiitakeError> {
  return Stream.paginate(null, () =>
    queue.take.pipe(
      Effect.map((frame) =>
        frame === "closed"
          ? ([[], Option.none<null>()] as const)
          : ([[frame], Option.some(null)] as const),
      ),
    ),
  );
}

/** Durable rows dedup by sequence; ephemeral appends dedup only by revision. */
function reconcileSessionFrames(
  source: Stream.Stream<SessionFrame | "reset", ShiitakeError>,
  initialCursor: string | null,
  initialProgress: SessionSnapshot["progress"],
  reset: () => Effect.Effect<
    Extract<SessionFrame, { kind: "reset" }>,
    ShiitakeError
  >,
): Stream.Stream<SessionFrame, ShiitakeError> {
  let cursor = initialCursor;
  let position = progressPosition(initialProgress);
  return source.pipe(
    Stream.mapEffect(
      (incoming): Effect.Effect<readonly SessionFrame[], ShiitakeError> =>
        Effect.gen(function* () {
          let frame = incoming === "reset" ? yield* reset() : incoming;
          if (frame.kind === "heartbeat") return [frame];
          if (frame.kind === "progress") {
            const previous =
              position?.messageId === frame.progress.messageId
                ? position
                : null;
            if (
              previous !== null &&
              previous.revision >= frame.progress.revision
            )
              return [];
            const text = previous?.text ?? 0;
            const thinking = previous?.thinking ?? 0;
            if (
              frame.progress.text.offset > text ||
              frame.progress.thinking.offset > thinking
            )
              frame = yield* reset();
            else {
              position = {
                messageId: frame.progress.messageId,
                revision: frame.progress.revision,
                text:
                  frame.progress.text.offset +
                  frame.progress.text.append.length,
                thinking:
                  frame.progress.thinking.offset +
                  frame.progress.thinking.append.length,
              };
            }
          }
          if (frame.kind === "reset") {
            cursor = frame.cursor;
            position = progressPosition(frame.baseline.progress);
            return [frame];
          }
          if (frame.kind !== "update") return [frame];
          const previous =
            cursor === null ? undefined : decodeWoodstockCursor(cursor);
          const next =
            frame.cursor === null
              ? undefined
              : decodeWoodstockCursor(frame.cursor);
          if (
            previous !== undefined &&
            next !== undefined &&
            previous.generation !== next.generation
          ) {
            const baseline = yield* reset();
            cursor = baseline.cursor;
            position = progressPosition(baseline.baseline.progress);
            return [baseline];
          }
          const sequence = previous?.sequence ?? 0;
          const candidateCursor =
            next !== undefined && next.sequence > sequence
              ? frame.cursor
              : cursor;
          const duplicate = (
            rows: readonly { readonly sequence: number }[] | undefined,
          ) => rows?.some((row) => row.sequence <= sequence) === true;
          if (
            candidateCursor === frame.cursor &&
            !duplicate(frame.messages) &&
            !duplicate(frame.terminals) &&
            !frame.compactions?.some(
              (marker) => compactionEntrySequence(marker) <= sequence,
            ) &&
            !duplicate(frame.compactionNotices)
          ) {
            cursor = frame.cursor;
            return [frame];
          }
          const rows = {
            ...frame,
            cursor: candidateCursor,
            messages: frame.messages.filter((row) => row.sequence > sequence),
            terminals: frame.terminals.filter((row) => row.sequence > sequence),
          };
          if (frame.compactions !== undefined)
            rows.compactions = frame.compactions.filter(
              (marker) => compactionEntrySequence(marker) > sequence,
            );
          if (frame.compactionNotices !== undefined)
            rows.compactionNotices = frame.compactionNotices.filter(
              (row) => row.sequence > sequence,
            );
          cursor = rows.cursor;
          return [rows];
        }),
    ),
    Stream.flattenIterable,
  );
}

function subscribeSession(
  options: ShiitakeEffectOptions,
  runtime: RuntimeCoordinator,
  feeds: LiveSessionFeeds,
  sessionId: string,
  input: SubscribeOptions,
): Stream.Stream<SessionFrame, ShiitakeError> {
  return Stream.unwrap(
    runtime.assertOpen.pipe(
      Effect.andThen(decodeSubscriptionInput(sessionId, input)),
      Effect.flatMap((decoded) =>
        Effect.acquireRelease(
          feeds.acquire(
            decoded.sessionId,
            decoded.options.includeSandboxFailures === true,
          ),
          ({ queue }) => Effect.sync(queue.close),
        ).pipe(
          Effect.map(({ feed, queue }) => {
            const checkpoint = () => {
              queue.checkpoint(feed.registration.revision);
            };
            return reconcileSessionFrames(
              subscriptionFrames(
                options,
                runtime,
                decoded.sessionId,
                decoded.options.after,
                { kind: "replay", checkpoint },
                decoded.options.includeSandboxFailures === true,
              ).pipe(Stream.concat(queuedSessionFrames(queue))),
              decoded.options.after ?? null,
              null,
              () =>
                resetFrame(
                  options,
                  runtime,
                  decoded.sessionId,
                  decoded.options.includeSandboxFailures === true,
                  { beforeRead: checkpoint },
                ),
            );
          }),
        ),
      ),
    ),
  );
}

function decodeWatchInput(
  sessionId: string,
  input: WatchOptions | undefined,
): Effect.Effect<
  Readonly<{ sessionId: string; options: ClientReadOptions }>,
  ShiitakeError
> {
  return Effect.try({
    try: () =>
      Object.freeze({
        sessionId: decodeClientSessionId(sessionId),
        options: decodeClientReadOptions(input),
      }),
    catch: (error) =>
      error instanceof ShiitakeError
        ? error
        : new ShiitakeError("contract", "invalid", "Watch options are invalid"),
  });
}

function watchSession(
  options: ShiitakeEffectOptions,
  runtime: RuntimeCoordinator,
  feeds: LiveSessionFeeds,
  sessionId: string,
  input: WatchOptions | undefined,
): Effect.Effect<ShiitakeEffectWatch, ShiitakeError> {
  return runtime.assertOpen.pipe(
    Effect.andThen(decodeWatchInput(sessionId, input)),
    Effect.flatMap((decoded) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const { feed, queue } = yield* feeds.acquire(
            decoded.sessionId,
            decoded.options.includeSandboxFailures === true,
          );
          const checkpoint = () => {
            queue.checkpoint(feed.registration.revision);
          };
          return yield* restore(
            runtime.assertOpen.pipe(
              Effect.andThen(
                readSessionWithContext(
                  options,
                  runtime,
                  decoded.sessionId,
                  decoded.options,
                  { beforeRead: checkpoint },
                ),
              ),
              Effect.map((snapshot): ShiitakeEffectWatch => {
                return Object.freeze({
                  snapshot,
                  changes: reconcileSessionFrames(
                    queuedSessionFrames(queue),
                    snapshot.cursor,
                    snapshot.progress,
                    () =>
                      resetFrame(
                        options,
                        runtime,
                        decoded.sessionId,
                        decoded.options.includeSandboxFailures === true,
                        { beforeRead: checkpoint },
                      ),
                  ).pipe(Stream.ensuring(Effect.sync(queue.close))),
                  close: Effect.sync(queue.close),
                });
              }),
            ),
          ).pipe(Effect.onError(() => Effect.sync(queue.close)));
        }),
      ),
    ),
  );
}

/** Shared live observation; Woodstock remains the bounded baseline and replay owner. */
export function openSessionObservation(
  options: ShiitakeEffectOptions,
  runtime: RuntimeCoordinator,
  observation: ObservationKernel,
  ownerContext: Context.Context<never>,
) {
  const feeds = openLiveSessionFeeds(
    options,
    runtime,
    observation,
    ownerContext,
  );
  return Object.freeze({
    read: (sessionId: string, input: Parameters<typeof readSession>[3]) =>
      readSessionWithContext(options, runtime, sessionId, input),
    subscribe: (sessionId: string, input: SubscribeOptions) =>
      subscribeSession(options, runtime, feeds, sessionId, input),
    watch: (sessionId: string, input: WatchOptions | undefined) =>
      watchSession(options, runtime, feeds, sessionId, input),
    close: feeds.close,
  });
}
