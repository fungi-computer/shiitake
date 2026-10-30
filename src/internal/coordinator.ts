import { nanoid } from "@fungi.computer/cairn";
import { parseModelPreference } from "./model-preference.js";
import type { BirdDogMail } from "@fungi.computer/bird-dog";
import { acceptedNotice } from "@fungi.computer/bird-dog/effect";
import { FungiMessageMetaSchema } from "@fungi.computer/agar/agents";
import type { VerifiedAgentReply } from "./woodstock/index.js";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import type {
  AbortCommand,
  Accepted,
  CompactCommand,
  BranchCommand,
  OperatorCommand,
  TitleCommand,
  CancelCommand,
  CancelResult,
  ForkCommand,
  ForkedSession,
  PromptCommand,
  SessionCommand,
  SessionContext,
  SessionFrame,
  SessionSnapshot,
  SubscribeOptions,
  WatchOptions,
} from "../client.js";
import { ShiitakeError } from "../errors.js";
import type { ExtensionCommandCallerV0, Json } from "../extensions.js";
import type { ModelPreference } from "./model-preference.js";
import {
  decodeClientReadOptions,
  decodeClientSettlementRequest,
  requireDecoded,
  decodeClientSessionCommand,
  decodeClientSessionId,
  decodeClientSubscribeOptions,
  type ClientReadOptions,
  type ClientSubscribeOptions,
} from "./client-boundary.js";
import type {
  ShiitakeEffectAgent,
  ShiitakeEffectOwner,
  ShiitakeEffectOptions,
  ShiitakeEffectSession,
  ShiitakeEffectWatch,
} from "./effect-contracts.js";
import {
  mergeObservationSignals,
  openObservationKernel,
  publishAgentExecutionChange,
  publishOwnerObservation,
  type ObservationKernel,
  type ObservationRegistration,
  type ObservationSignal,
} from "./observation-kernel.js";
import type { RuntimeCoordinator } from "./runtime-coordinator.js";
import type { AdmitRequest } from "./admission-door.js";
import { checkHostMeta } from "./host-meta.js";
import {
  readSessionActivity,
  type ObservedActivity,
} from "./session-status.js";
import { prepareSessionContext } from "./session-context.js";
import {
  progressPosition,
  readProcessProgressDelta,
  type ProgressPosition,
  abortSession,
  cancelSession,
  branchSession,
  recordOperatorLine,
  forkSession,
  readSession,
  readCommandSettlement,
  readSessionModelPreference,
  readSessionReplayPage,
  readSessionInbox,
  readLatestPendingPrompt,
  createSession,
  deleteSession,
  deliverSessionGrants,
  readSessionGrants,
  setSessionModelPreference,
  setSessionTitle,
  type GrantsDelivery,
  readAdmissionIdentity,
  assertAdmissionIdentity,
} from "./woodstock/index.js";

/** Generic Agent-scope extension controls over the runtime's one call door. */
function agentExtensions(runtime: RuntimeCoordinator) {
  return Object.freeze({
    list: runtime.assertOpen.pipe(Effect.map(() => runtime.extensions.list())),
    resume: (extensionId: string) =>
      runtime.assertOpen.pipe(
        Effect.andThen(runtime.extensions.resume(extensionId)),
      ),
    call: (extensionId: string, command: string, input: Json) =>
      runtime.assertOpen.pipe(
        Effect.andThen(
          runtime.extensions.call(
            { scope: "agent" },
            extensionId,
            command,
            input,
          ),
        ),
      ),
  });
}

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

/** Read the baseline and its context use for one Session. */
function readSessionWithContext(
  options: ShiitakeEffectOptions,
  runtime: RuntimeCoordinator,
  sessionId: string,
  input: Parameters<typeof readSession>[3],
): Effect.Effect<SessionSnapshot, ShiitakeError> {
  return currentSessionWork(runtime, sessionId).pipe(
    Effect.flatMap(({ head, runningWork }) =>
      readSession(
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
      ),
    ),
    Effect.flatMap(({ snapshot, context }) =>
      context.pipe(Effect.map((value) => withContext(snapshot, value))),
    ),
  );
}

function sessionOwnerOperation<A>(
  runtime: RuntimeCoordinator,
  sessionId: string,
  operation: (decodedSessionId: string) => A,
): Effect.Effect<A, ShiitakeError> {
  return runtime.assertOpen.pipe(
    Effect.andThen(
      Effect.try({
        try: () => operation(decodeClientSessionId(sessionId)),
        catch: (error) =>
          error instanceof ShiitakeError
            ? error
            : new ShiitakeError(
                "store",
                "rejected",
                "Session owner operation failed",
              ),
      }),
    ),
  );
}

function decodeSessionCommand(
  sessionId: string,
  input: SessionCommand,
): Effect.Effect<
  Readonly<{ sessionId: string; command: SessionCommand }>,
  ShiitakeError
> {
  return Effect.try({
    try: () => {
      const decodedCommand = decodeClientSessionCommand(input);
      if (decodedCommand._tag === "Failure")
        throw new ShiitakeError(
          "contract",
          "invalid",
          "Session command is invalid",
        );
      return Object.freeze({
        sessionId: decodeClientSessionId(sessionId),
        command: decodedCommand.success,
      });
    },
    catch: (error) =>
      error instanceof ShiitakeError
        ? error
        : new ShiitakeError(
            "contract",
            "invalid",
            "Session command is invalid",
          ),
  });
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
): Effect.Effect<Extract<SessionFrame, { kind: "reset" }>, ShiitakeError> {
  return runtime.assertOpen.pipe(
    Effect.andThen(
      readSessionWithContext(options, runtime, sessionId, {
        includeSandboxFailures,
      }),
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

function subscriptionFrames(
  options: ShiitakeEffectOptions,
  runtime: RuntimeCoordinator,
  registration: ObservationRegistration,
  sessionId: string,
  after: string | undefined,
  delivery: "replay" | "live",
  includeSandboxFailures = false,
  baselineProgress: SessionSnapshot["progress"] = null,
): Stream.Stream<SessionFrame, ShiitakeError> {
  let position: ProgressPosition | null = progressPosition(baselineProgress);
  let toolOutput: ObservedActivity["toolOutput"] = [];
  const reset = () =>
    resetFrame(options, runtime, sessionId, includeSandboxFailures).pipe(
      Effect.map((frame) => {
        position = progressPosition(frame.baseline.progress);
        toolOutput = frame.baseline.toolOutput;
        return continueFromReset(frame);
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
    delivery,
    awaitSignal: false,
  };
  return Stream.paginate<SubscriptionState, SessionFrame, ShiitakeError>(
    initial,
    (state): Effect.Effect<SubscriptionStep, ShiitakeError> =>
      (state.awaitSignal
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
                registration.setQueued(activity.facts.queue.state === "queued");
                if (
                  (signal === "progress" || signal === "activity") &&
                  state.delivery === "live"
                ) {
                  const frames = liveFrames(activity);
                  if (frames === undefined) return reset();
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
                              frame.kind === "tool_output"
                                ? []
                                : [frame.cursor],
                            )
                            .at(-1)
                        : undefined;
                    // Prepare a figure only for the final moving update,
                    // while its cursor and stored context share the read.
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
                          Option.some(next),
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

function subscribeSession(
  options: ShiitakeEffectOptions,
  runtime: RuntimeCoordinator,
  observation: ObservationKernel,
  sessionId: string,
  input: SubscribeOptions,
): Stream.Stream<SessionFrame, ShiitakeError> {
  return Stream.unwrap(
    runtime.assertOpen.pipe(
      Effect.andThen(decodeSubscriptionInput(sessionId, input)),
      Effect.flatMap((decoded) =>
        Effect.acquireRelease(
          observation.register(decoded.sessionId),
          (registration) => Effect.sync(registration.close),
        ).pipe(
          Effect.map((registration) =>
            subscriptionFrames(
              options,
              runtime,
              registration,
              decoded.sessionId,
              decoded.options.after,
              "replay",
              decoded.options.includeSandboxFailures === true,
            ),
          ),
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
  observation: ObservationKernel,
  sessionId: string,
  input: WatchOptions | undefined,
): Effect.Effect<ShiitakeEffectWatch, ShiitakeError> {
  return runtime.assertOpen.pipe(
    Effect.andThen(decodeWatchInput(sessionId, input)),
    Effect.flatMap((decoded) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const registration = yield* observation.register(decoded.sessionId);
          return yield* restore(
            runtime.assertOpen.pipe(
              Effect.andThen(
                readSessionWithContext(
                  options,
                  runtime,
                  decoded.sessionId,
                  decoded.options,
                ),
              ),
              Effect.map((snapshot): ShiitakeEffectWatch => {
                registration.setQueued(
                  snapshot.activity.queue.state === "queued",
                );
                return Object.freeze({
                  snapshot,
                  changes: subscriptionFrames(
                    options,
                    runtime,
                    registration,
                    decoded.sessionId,
                    snapshot.cursor ?? undefined,
                    "live",
                    decoded.options.includeSandboxFailures === true,
                    snapshot.progress,
                  ).pipe(Stream.ensuring(Effect.sync(registration.close))),
                  close: Effect.sync(registration.close),
                });
              }),
            ),
          ).pipe(Effect.onError(() => Effect.sync(registration.close)));
        }),
      ),
    ),
  );
}

/**
 * Admit one user prompt, mail prompt or compaction request through the
 * accepted-command door.
 */
function admitUserWork(
  runtime: RuntimeCoordinator,
  sessionId: string,
  input: PromptCommand | CompactCommand,
  meta: Json | undefined,
  mail?: BirdDogMail,
  reply?: VerifiedAgentReply,
): Effect.Effect<Accepted, ShiitakeError> {
  let request: AdmitRequest = {
    sessionId,
    command: input,
    when: "now",
    origin: { kind: "user" },
  };
  if (meta !== undefined) request = { ...request, meta };
  if (mail !== undefined) request = { ...request, mail };
  if (reply !== undefined) request = { ...request, reply };
  return runtime.assertOpen.pipe(
    Effect.andThen(runtime.door.admit(request)),
    Effect.flatMap((admission) =>
      admission.status === "accepted"
        ? Effect.succeed<Accepted>({
            status: "accepted",
            commandId: admission.commandId,
          })
        : Effect.fail(
            new ShiitakeError(
              "runtime",
              "unavailable",
              "Session command was not admitted",
            ),
          ),
    ),
  );
}

function cancelRequestedSession(
  options: ShiitakeEffectOptions,
  runtime: RuntimeCoordinator,
  sessionId: string,
  input: CancelCommand,
  meta: Json | undefined,
): Effect.Effect<CancelResult, ShiitakeError> {
  return runtime.assertOpen.pipe(
    Effect.andThen(
      sessionOwnerOperation(runtime, sessionId, (decoded) => {
        if (options.meta?.identityPath !== undefined && meta !== undefined)
          assertAdmissionIdentity(
            options.store,
            decoded,
            input.commandId,
            readAdmissionIdentity(
              options.store,
              options.meta.identityPath,
              meta,
            ),
          );
      }),
    ),
    Effect.andThen(
      cancelSession(
        options.store,
        sessionId,
        input.commandId,
        input.onlyIfPending,
        () => runtime.readQueueHeadTransactional(sessionId),
        runtime.recordAcceptedCancellation,
        meta,
      ),
    ),
    Effect.flatMap((cancellation) =>
      cancellation === undefined
        ? Effect.succeed<CancelResult>({
            status: "too_late",
            commandId: input.commandId,
          })
        : (cancellation.active
            ? runtime.signalCancellation(
                cancellation.accepted.commandId,
                cancellation.accepted.entryId,
              )
            : runtime.requestWake
          ).pipe(
            Effect.as<Accepted>({
              status: "accepted",
              commandId: cancellation.accepted.commandId,
            }),
          ),
    ),
  );
}

function abortCurrentSession(
  options: ShiitakeEffectOptions,
  runtime: RuntimeCoordinator,
  sessionId: string,
  meta: Json | undefined,
) {
  return runtime.assertOpen.pipe(
    Effect.andThen(
      runtime.commit(
        abortSession(
          options.store,
          sessionId,
          () => runtime.readQueueHeadTransactional(sessionId),
          runtime.recordAcceptedCancellation,
          meta,
        ),
      ),
    ),
    Effect.flatMap((cancellation) =>
      cancellation === undefined
        ? runtime.requestWake
        : runtime.signalCancellation(
            cancellation.commandId,
            cancellation.entryId,
          ),
    ),
  );
}

function selectBranch(
  options: ShiitakeEffectOptions,
  runtime: RuntimeCoordinator,
  sessionId: string,
  input: BranchCommand,
): Effect.Effect<undefined, ShiitakeError> {
  return runtime.assertOpen.pipe(
    Effect.andThen(
      branchSession(options.store, {
        sessionId,
        type: "branch",
        entryId: input.entryId,
        assertIdleSession: runtime.assertIdleSession,
      }),
    ),
    Effect.as(undefined),
  );
}

function forkBranch(
  options: ShiitakeEffectOptions,
  runtime: RuntimeCoordinator,
  sessionId: string,
  input: ForkCommand,
): Effect.Effect<ForkedSession, ShiitakeError> {
  return runtime.assertOpen.pipe(
    Effect.andThen(
      forkSession(options.store, {
        sessionId,
        type: "fork",
        entryId: input.entryId,
        assertIdleSession: runtime.assertIdleSession,
      }),
    ),
  );
}

function executeSessionCommand(
  options: ShiitakeEffectOptions,
  runtime: RuntimeCoordinator,
  sessionId: string,
  meta: Json | undefined,
  input: PromptCommand | CompactCommand,
): Effect.Effect<Accepted, ShiitakeError>;
function executeSessionCommand(
  options: ShiitakeEffectOptions,
  runtime: RuntimeCoordinator,
  sessionId: string,
  meta: Json | undefined,
  input: CancelCommand,
): Effect.Effect<CancelResult, ShiitakeError>;
function executeSessionCommand(
  options: ShiitakeEffectOptions,
  runtime: RuntimeCoordinator,
  sessionId: string,
  meta: Json | undefined,
  input: AbortCommand | BranchCommand | OperatorCommand | TitleCommand,
): Effect.Effect<undefined, ShiitakeError>;
function executeSessionCommand(
  options: ShiitakeEffectOptions,
  runtime: RuntimeCoordinator,
  sessionId: string,
  meta: Json | undefined,
  input: ForkCommand,
): Effect.Effect<ForkedSession, ShiitakeError>;
function executeSessionCommand(
  options: ShiitakeEffectOptions,
  runtime: RuntimeCoordinator,
  sessionId: string,
  meta: Json | undefined,
  input: SessionCommand,
): Effect.Effect<
  Accepted | CancelResult | ForkedSession | undefined,
  ShiitakeError
>;
function executeSessionCommand(
  options: ShiitakeEffectOptions,
  runtime: RuntimeCoordinator,
  sessionId: string,
  meta: Json | undefined,
  input: SessionCommand,
): Effect.Effect<
  Accepted | CancelResult | ForkedSession | undefined,
  ShiitakeError
> {
  return runtime.assertOpen.pipe(
    Effect.andThen(decodeSessionCommand(sessionId, input)),
    // The one path every command's host metadata takes: checked once against
    // the host's schema, then stored on whatever entry the command writes.
    Effect.flatMap((decoded) =>
      checkCommandMeta(options, meta).pipe(
        Effect.map((checked) => ({ ...decoded, checked })),
      ),
    ),
    Effect.flatMap(
      ({
        sessionId: decodedSessionId,
        command,
        checked,
      }): Effect.Effect<
        Accepted | CancelResult | ForkedSession | undefined,
        ShiitakeError
      > => {
        switch (command.type) {
          case "prompt":
          case "compact":
            return admitUserWork(runtime, decodedSessionId, command, checked);
          case "abort":
            return abortCurrentSession(
              options,
              runtime,
              decodedSessionId,
              checked,
            ).pipe(Effect.as(undefined));
          case "cancel":
            return cancelRequestedSession(
              options,
              runtime,
              decodedSessionId,
              command,
              checked,
            );
          case "branch":
            return selectBranch(options, runtime, decodedSessionId, command);
          case "fork":
            return forkBranch(options, runtime, decodedSessionId, command);
          case "operator":
            return runtime.assertOpen.pipe(
              Effect.andThen(
                recordOperatorLine(options.store, {
                  sessionId: decodedSessionId,
                  requestId: command.requestId,
                  author: command.author,
                  meta: checked,
                  text: command.text,
                  assertIdleSession: runtime.assertIdleSession,
                }),
              ),
              Effect.as(undefined),
            );
          case "title":
            return runtime
              .commit(
                sessionOwnerOperation(runtime, decodedSessionId, (decoded) => {
                  setSessionTitle(options.store, decoded, command.title);
                }),
              )
              .pipe(
                Effect.tap(() =>
                  Effect.sync(() => {
                    publishAgentExecutionChange(options.store);
                  }),
                ),
                Effect.as(undefined),
              );
        }
      },
    ),
  );
}

/** Check one command's host metadata against the host's schema, once. */
function checkCommandMeta(
  options: ShiitakeEffectOptions,
  meta: Json | undefined,
): Effect.Effect<Json | undefined, ShiitakeError> {
  return Effect.try({
    try: () =>
      meta === undefined ? undefined : checkHostMeta(options.meta, meta),
    catch: (cause) =>
      cause instanceof ShiitakeError
        ? cause
        : new ShiitakeError("contract", "invalid", "Host metadata is invalid"),
  });
}

/** Construct the Effect-native coordinator over the private owner modules. */
export function openCoordinator(
  options: ShiitakeEffectOptions,
  runtime: RuntimeCoordinator,
): ShiitakeEffectOwner {
  const observation = openObservationKernel(options.store);
  /** One Session handle; `meta` is the host metadata its prompts carry. */
  const sessionFor = (
    sessionId: string,
    meta: Json | undefined,
  ): ShiitakeEffectSession => {
    function command(
      input: PromptCommand | CompactCommand,
    ): Effect.Effect<Accepted, ShiitakeError>;
    function command(
      input: CancelCommand,
    ): Effect.Effect<CancelResult, ShiitakeError>;
    function command(
      input: AbortCommand | BranchCommand | OperatorCommand | TitleCommand,
    ): Effect.Effect<undefined, ShiitakeError>;
    function command(
      input: ForkCommand,
    ): Effect.Effect<ForkedSession, ShiitakeError>;
    function command(
      input: SessionCommand,
    ): Effect.Effect<
      Accepted | CancelResult | ForkedSession | undefined,
      ShiitakeError
    >;
    function command(input: SessionCommand) {
      return executeSessionCommand(options, runtime, sessionId, meta, input);
    }
    return Object.freeze({
      command,
      withdraw: (requestId: string) =>
        runtime.assertOpen.pipe(
          Effect.andThen(runtime.door.withdraw(sessionId, requestId)),
        ),
      abort: command({ type: "abort" }),
      send: (
        text: string,
        input?: {
          delivery?: "steer" | "followUp";
          requestId?: string;
          images?: PromptCommand["images"];
        },
      ) =>
        Effect.sync(() => input?.requestId ?? nanoid()).pipe(
          Effect.flatMap((requestId) => {
            const prompt: PromptCommand = {
              type: "prompt",
              delivery: input?.delivery ?? "steer",
              requestId,
              text,
            };
            return command(
              input?.images === undefined
                ? prompt
                : { ...prompt, images: input.images },
            ).pipe(
              Effect.mapError((error) =>
                error.requestId === requestId
                  ? error
                  : new ShiitakeError(
                      error.boundary,
                      error.cause,
                      error.message,
                      requestId,
                    ),
              ),
            );
          }),
        ),
      cancel: (input: { commandId: string; onlyIfPending?: boolean }) =>
        command({ type: "cancel", ...input }),
      latestPending: sessionOwnerOperation(runtime, sessionId, (decoded) =>
        readLatestPendingPrompt(
          options.store,
          decoded,
          options.meta?.identityPath === undefined || meta === undefined
            ? undefined
            : readAdmissionIdentity(
                options.store,
                options.meta.identityPath,
                meta,
              ),
        ),
      ),
      modelPreference: Object.freeze({
        read: sessionOwnerOperation(runtime, sessionId, (decoded) =>
          readSessionModelPreference(options.store, decoded),
        ),
        set: (preference: ModelPreference | null) =>
          Effect.try({
            try: () => parseModelPreference(preference),
            catch: () =>
              new ShiitakeError(
                "contract",
                "invalid",
                "Model preference is invalid",
              ),
          }).pipe(
            Effect.flatMap((parsed) =>
              runtime
                .commit(
                  sessionOwnerOperation(runtime, sessionId, (decoded) => {
                    setSessionModelPreference(options.store, decoded, parsed);
                    return decoded;
                  }),
                )
                .pipe(
                  // Once committed, observers re-read the Session: the
                  // selection's window may have changed.
                  Effect.tap((decoded) =>
                    Effect.sync(() => {
                      publishOwnerObservation(options.store, decoded);
                    }),
                  ),
                  Effect.asVoid,
                ),
            ),
          ),
      }),
      extension: (extensionId: string) =>
        Object.freeze({
          call: (
            command: string,
            input: Json,
            caller?: ExtensionCommandCallerV0,
          ) =>
            runtime.assertOpen.pipe(
              Effect.andThen(
                runtime.extensions.call(
                  { scope: "session", sessionId },
                  extensionId,
                  command,
                  input,
                  caller,
                ),
              ),
            ),
        }),
      read: (input) =>
        runtime.assertOpen.pipe(
          Effect.andThen(
            readSessionWithContext(options, runtime, sessionId, input),
          ),
        ),
      readSettlement: (commandId, timeoutMs = 0) =>
        Effect.scoped(
          Effect.gen(function* () {
            yield* runtime.assertOpen;
            const decoded = yield* Effect.try({
              try: () => ({
                sessionId: decodeClientSessionId(sessionId),
                ...requireDecoded(
                  decodeClientSettlementRequest({ commandId, timeoutMs }),
                  "Settlement request is invalid",
                ),
              }),
              catch: (cause) =>
                cause instanceof ShiitakeError
                  ? cause
                  : new ShiitakeError(
                      "contract",
                      "invalid",
                      "Settlement request is invalid",
                    ),
            });
            // Subscribe before reading: a commit racing the first read cannot
            // be missed. Only settlement hints reach this registration.
            const registration = yield* Effect.acquireRelease(
              observation.register(decoded.sessionId, "settlement"),
              (value) => Effect.sync(value.close),
            );
            const read = readCommandSettlement(
              options.store,
              decoded.sessionId,
              decoded.commandId,
            );
            const initial = yield* read;
            if (initial !== undefined || decoded.timeoutMs === 0)
              return initial;
            return yield* Effect.gen(function* () {
              for (;;) {
                const hint = yield* registration.awaitSignal;
                if (hint === "closed")
                  return yield* Effect.fail(
                    new ShiitakeError(
                      "runtime",
                      "closed",
                      "Shiitake is closed",
                    ),
                  );
                const settled = yield* read;
                if (settled !== undefined) return settled;
              }
            }).pipe(
              Effect.timeoutOption(decoded.timeoutMs),
              Effect.map(Option.getOrUndefined),
            );
          }),
        ),
      subscribe: (input) =>
        subscribeSession(options, runtime, observation, sessionId, input),
      watch: (input) =>
        watchSession(options, runtime, observation, sessionId, input),
    });
  };
  const agent: ShiitakeEffectAgent = Object.freeze({
    createSession: (requestId) =>
      runtime.assertOpen.pipe(
        Effect.andThen(
          createSession(options.store, requestId, runtime.storage.admitNew),
        ),
      ),
    deleteSession: (sessionId) =>
      runtime.assertOpen.pipe(
        Effect.andThen(
          deleteSession(options.store, {
            sessionId,
            assertIdleSession: runtime.assertIdleSession,
          }),
        ),
      ),
    listSessions: (after?: string) =>
      runtime.assertOpen.pipe(
        Effect.andThen(readSessionInbox(options.store, after)),
      ),
    extensions: agentExtensions(runtime),
    session: (sessionId): ShiitakeEffectSession =>
      sessionFor(sessionId, undefined),
    close: runtime.beginClose.pipe(Effect.andThen(observation.close)),
  });
  return Object.freeze({
    ...agent,
    prepareConfiguration: runtime.prepareConfiguration,
    grants: Object.freeze({
      // A synchronous owner write: a delivery lands before any other work
      // can interleave, so the very next tool call reads it.
      deliver: (sessionId: string, delivery: GrantsDelivery) =>
        sessionOwnerOperation(runtime, sessionId, (decoded) =>
          options.store.transactionSync(() =>
            deliverSessionGrants(options.store, decoded, delivery),
          ),
        ),
      read: (sessionId: string) =>
        sessionOwnerOperation(runtime, sessionId, (decoded) =>
          readSessionGrants(options.store, decoded),
        ),
    }),
    withMeta: (meta: Json): ShiitakeEffectAgent =>
      Object.freeze({
        ...agent,
        session: (sessionId: string) => sessionFor(sessionId, meta),
      }),
  });
}

/** Whether the sender asked this mail to steer the recipient's running turn. */
function steersRunningTurn(mail: BirdDogMail): boolean {
  const meta = FungiMessageMetaSchema.safeParse(mail.meta);
  return meta.success && meta.data.delegation?.steer === true;
}

/**
 * Accept one addressed mail envelope; the door decodes it as a prompt. `reply`
 * is the host's authentication of a delegated Agent's reply; only then does
 * the model see the line inside the verified-reply tag.
 */
export function acceptAddressedMail(
  runtime: RuntimeCoordinator,
  mail: BirdDogMail,
  meta?: Json,
  reply?: VerifiedAgentReply,
  stageMail?: (mail: BirdDogMail) => void,
  sessionGrantDelivery?: (sessionId: string) => GrantsDelivery,
) {
  if (mail.to.sessionId === undefined) {
    if (reply !== undefined || mail.replyTo.length === 0)
      throw new Error("Only delegated work can create a recipient Session");
    return runtime.door.transact((admit) => {
      const sessionId = runtime.createSessionWithinTransaction(mail.key);
      const grants = sessionGrantDelivery?.(sessionId);
      if (grants !== undefined)
        runtime.stageSessionGrantsWithinTransaction(sessionId, grants);
      const addressed = {
        ...mail,
        to: { ...mail.to, sessionId },
      };
      const request: Omit<AdmitRequest, "meta"> = {
        sessionId,
        requireExistingSession: true,
        command: {
          requestId: mail.key,
          delivery: "followUp",
          type: "prompt",
          text: mail.content,
        },
        when: "now",
        origin: { kind: "user" },
        mail: addressed,
        onAccepted: (_acceptedSessionId, commandId) => {
          if (stageMail === undefined)
            throw new Error("Recipient Agent mail outbox is unavailable");
          stageMail(acceptedNotice(mail, sessionId, commandId, Date.now()));
        },
      };
      const result = admit(meta === undefined ? request : { ...request, meta });
      if (result.status !== "accepted")
        throw new Error("Sessionless addressed mail was not admitted");
      runtime.recordCreatedSessionDelegatorWithinTransaction(
        sessionId,
        addressed,
      );
      return {
        status: "accepted" as const,
        commandId: result.commandId,
        sessionId,
      };
    });
  }
  return admitUserWork(
    runtime,
    mail.to.sessionId,
    {
      requestId: mail.key,
      delivery:
        reply !== undefined || steersRunningTurn(mail) ? "steer" : "followUp",
      type: "prompt",
      text: mail.content,
    },
    meta,
    mail,
    reply,
  );
}
