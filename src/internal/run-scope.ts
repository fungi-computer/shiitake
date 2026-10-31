import type { FileTool } from "@fungi.computer/knapsack";
import {
  muleEffect,
  piCodingAgentRetryDefaults,
  type MuleEvent,
  type MuleError,
  type MuleListener,
  type MuleRetryObservation,
  type MuleRunInput,
  type MuleTool,
} from "@fungi.computer/mule/effect";
import type {
  ModuleRegistration,
  CapabilitySession,
} from "@fungi.computer/mycelium";
import { make as makeMycelium } from "@fungi.computer/mycelium/effect";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Cause from "effect/Cause";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";

import { ShiitakeError } from "../errors.js";
import type { InstructionSnapshot } from "./contracts.js";
import type { ShiitakeEffectOptions } from "./effect-contracts.js";
import type {
  CompactionHandoffV0,
  PromptSectionV0,
  Json,
} from "../extensions.js";
import {
  compactSession,
  runWithCompaction,
  type ManualCompactionOutcome,
} from "./compaction/session-compaction.js";
import { openCompactionSettings } from "./compaction/settings.js";
import { acquireOwnedModel } from "./model-acquisition.js";
import {
  returnedFailureReason,
  unavailableRunReason,
} from "./run-failure-reason.js";
import {
  isCompactCommand,
  openMuleRecorder,
  readSessionGrants,
  recordLiveRetryEvent,
  recordSandboxFailure,
  takeSessionControls,
  type LiveRetryEvent,
  type PreparedSessionRun,
  type RunContract,
} from "./woodstock/index.js";

// A run writes its terminal before scope release. Allow one second per foreign
// release before detaching it: the held-model regression exercises the full
// grace and proves Watchdog can settle and claim the next priority-1 steer.
// Keep the test's eventual queue-idle budget tied to this product bound.
export const runFinalizerGraceMs = 1_000;

function boundedRunFinalizer<E>(
  name: "instructions" | "model" | "tools",
  workId: string,
  release: Effect.Effect<void, E>,
): Effect.Effect<void, E> {
  return Effect.gen(function* () {
    // A detached release may finish later; interrupting a finalizer itself
    // cannot bound an uninterruptible scope.
    const fiber = yield* Effect.forkDetach(release);
    const finished = yield* Effect.raceFirst(
      Fiber.join(fiber).pipe(Effect.as(true)),
      Effect.sleep(runFinalizerGraceMs).pipe(Effect.as(false)),
    );
    if (!finished)
      yield* Effect.logError("shiitake_run_finalizer_hung").pipe(
        Effect.annotateLogs({ workId, finalizer: name }),
      );
  });
}

const toolSetupMessages = {
  missing: "Instruction capabilities have no tool runtime",
  runtime: "Tool runtime failed",
  lease: "Tool lease failed",
} as const;

/** Closed setup phases owned here; foreign error messages never define them. */
export class ToolSetupError extends ShiitakeError {
  constructor(private readonly phase: keyof typeof toolSetupMessages) {
    super("runtime", "rejected", toolSetupMessages[phase]);
  }

  get diagnostic(): string {
    return toolSetupMessages[this.phase];
  }
}

/** Closed reasons a rejected run names, never a foreign error's own text. */
const runRejectedDiagnostics = {
  model: "model-failed",
  tool: "tool-failed",
  control: "control-failed",
  listener: "listener-failed",
} as const;

/** A run Mule ended with a non-model error; it names a closed reason. */
export class RunRejectedError extends ShiitakeError {
  constructor(
    private readonly reason: keyof typeof runRejectedDiagnostics,
    underlying: MuleError,
  ) {
    super(
      "runtime",
      "rejected",
      "Mule rejected the durable command run",
      undefined,
      underlying,
    );
  }

  get diagnostic(): string {
    return runRejectedDiagnostics[this.reason];
  }
}

/** Name a non-model Mule error by its closed reason. */
function rejectedRun(error: MuleError): RunRejectedError {
  return new RunRejectedError(error.phase, error);
}

/**
 * Map Mule's retry transitions onto Woodstock's retained `live_retry` facts.
 * A started attempt ends the backoff countdown before streaming resumes.
 */
function toLiveRetryEvent(
  event: MuleRetryObservation,
  model: Readonly<{ provider: string; id: string }>,
): LiveRetryEvent {
  if (event.type === "retry_attempt_started")
    return {
      type: "auto_retry_attempt",
      run: event.run,
      attempt: event.attempt,
    };
  // Mule's retry text is Pi's private error body. Woodstock's retry journal
  // feeds status and recovery, not the owner's failed-turn conversation.
  const safeReason = (
    "Model request failed. Retry the request. " +
    unavailableRunReason({
      runId: event.run.value,
      provider: model.provider,
      model: model.id,
    })
  ).slice(0, 200);
  return event.type === "retry_scheduled"
    ? {
        type: "auto_retry_start",
        run: event.run,
        attempt: event.attempt,
        maxAttempts: event.maxAttempts,
        delayMs: event.delayMs,
        errorMessage: safeReason,
      }
    : event.finalError === undefined
      ? {
          type: "auto_retry_end",
          run: event.run,
          attempt: event.attempt,
          success: event.success,
        }
      : {
          type: "auto_retry_end",
          run: event.run,
          attempt: event.attempt,
          success: event.success,
          finalError: safeReason,
        };
}

/** Acquire the run's tools for exactly one Session, so hosts can project per Session. */
function acquiredTools(
  options: ShiitakeEffectOptions,
  sessionId: string,
  workId: string,
  meta: Json | undefined,
  instructions: Pick<InstructionSnapshot, "modules">,
  extensionModules: readonly ModuleRegistration[],
  extensionFileTools: readonly FileTool[],
): Effect.Effect<readonly MuleTool[], ShiitakeError, Scope.Scope> {
  const toolSource = options.tools;
  const instructionModules = instructions.modules ?? [];
  const modules = [...instructionModules, ...extensionModules];
  if (toolSource === undefined) {
    if (instructionModules.length > 0 || extensionFileTools.length > 0)
      return Effect.fail(new ToolSetupError("missing"));
    return Effect.succeed([]);
  }
  return Effect.gen(function* () {
    let session: CapabilitySession = { sessionId, commandId: workId };
    if (meta !== undefined) session = { ...session, meta };
    const runtimeOptions = {
      sources: toolSource.sources,
      session,
      sandbox: toolSource.sandbox,
      onSandboxFailure: (
        event: import("@fungi.computer/mycelium").SandboxFailureEvent,
      ) =>
        recordSandboxFailure(
          options.store,
          { sessionId, commandId: workId },
          event,
        ).pipe(Effect.orDie),
      modules: [...toolSource.modules, ...modules],
      tools: extensionFileTools,
      access: {
        ...toolSource.access,
        // Mycelium reads the stored grants at every call, so a grant the
        // host changes mid-run applies to the very next call.
        sessionGrants: () =>
          readSessionGrants(options.store, sessionId)?.grants ?? {},
      },
    };
    const toolScope = yield* Scope.make();
    yield* Effect.addFinalizer(() =>
      boundedRunFinalizer(
        "tools",
        workId,
        Scope.close(toolScope, Exit.succeed(undefined)),
      ),
    );
    const runtime = yield* makeMycelium(
      toolSource.execution === undefined
        ? runtimeOptions
        : { ...runtimeOptions, execution: toolSource.execution },
    ).pipe(
      Effect.provideService(Scope.Scope, toolScope),
      Effect.mapError(() => new ToolSetupError("runtime")),
    );
    const lease = yield* runtime.acquire().pipe(
      Effect.provideService(Scope.Scope, toolScope),
      Effect.mapError(() => new ToolSetupError("lease")),
    );
    return [lease.executeTool, ...lease.tools];
  });
}

/** Run one accepted command with Pi compaction explicitly composed around Mule. */
export function runAcceptedCommand(
  options: ShiitakeEffectOptions,
  prepared: PreparedSessionRun,
  executionContract: RunContract,
  extensionModules: readonly ModuleRegistration[],
  extensionFileTools: readonly FileTool[],
  compactionHandoff: CompactionHandoffV0 | undefined,
  promptSections?: () => Effect.Effect<
    readonly PromptSectionV0[],
    ShiitakeError
  >,
) {
  return Effect.scoped(
    Effect.gen(function* () {
      const instructions = yield* options.instructions.load({
        sessionId: prepared.command.sessionId,
        commandId: prepared.command.entry.workId,
      });
      const releaseInstructions = instructions.release;
      if (releaseInstructions !== undefined)
        yield* Effect.addFinalizer(() =>
          boundedRunFinalizer(
            "instructions",
            prepared.command.entry.workId,
            Effect.sync(releaseInstructions),
          ),
        );
      const acquired = yield* acquireOwnedModel(
        options.model
          .acquire({
            sessionId: prepared.command.sessionId,
            preference: prepared.modelPreference,
          })
          .pipe(
            Effect.map((model) => ({
              ...model,
              release: boundedRunFinalizer(
                "model",
                prepared.command.entry.workId,
                model.release,
              ),
            })),
          ),
      );
      const tools = yield* acquiredTools(
        options,
        prepared.command.sessionId,
        prepared.command.entry.workId,
        prepared.command.entry.meta,
        instructions,
        extensionModules,
        extensionFileTools,
      );
      const accepted = Object.freeze({
        sessionId: prepared.command.sessionId,
        requestId: prepared.command.entry.requestId,
        commandId: prepared.command.entry.workId,
      });
      const recorder = yield* openMuleRecorder(
        options.store,
        accepted,
        executionContract,
      );
      const controlMessages = (
        delivery: "steer" | "followUp",
      ): Effect.Effect<
        readonly import("@fungi.computer/mule/effect").Message[],
        import("@fungi.computer/mule/effect").MuleError
      > =>
        takeSessionControls(options.store, {
          sessionId: accepted.sessionId,
          workId: accepted.commandId,
          delivery,
        }).pipe(
          Effect.map((controls) => controls.map(({ message }) => message)),
          Effect.mapError((error): MuleError => ({
            _tag: "MuleError",
            // Control failures are terminal-worthy; listener failures
            // suppress the terminal notification because the listener
            // cannot store it.
            phase: "control",
            message:
              error instanceof ShiitakeError
                ? error.message
                : "Session control delivery failed",
          })),
        );
      let resume = prepared.resume;
      const settings = openCompactionSettings(options.compaction)(
        acquired.model,
      );
      let failureReason: string | undefined;
      let failedModelAttempts = 0;
      const run = yield* runWithCompaction({
        store: options.store,
        sessionId: prepared.command.sessionId,
        workId: prepared.command.entry.workId,
        model: acquired.model,
        stream: acquired.stream,
        handoff: compactionHandoff,
        settings,
        path: prepared.path,
        messages: prepared.messages,
        resuming: resume !== undefined,
        run: (workingMessages, betweenTurns) => {
          failedModelAttempts = 0;
          const continuation = resume;
          resume = undefined;
          // A failed lifecycle notification precedes Mule's returned outcome
          // (or typed error). Join them before publishing the single terminal;
          // no observer can win a race against a later diagnostic settlement.
          let failedEnd: Extract<MuleEvent, { type: "run_end" }> | undefined;
          const listener: MuleListener = (event: MuleEvent) => {
            if (event.type === "run_end" && event.outcome === "failed") {
              failedEnd = event;
              return Effect.void;
            }
            if (
              event.type === "message_end" &&
              event.value.role === "assistant" &&
              event.value.stopReason === "error"
            ) {
              const providerError = acquired.providerError?.(event.value);
              if (providerError !== undefined)
                return recorder({
                  ...event,
                  value: { ...event.value, providerError },
                });
            }
            return recorder(event);
          };
          const input: MuleRunInput = {
            model: acquired.model,
            modelStreamIdleTimeoutMs: options.modelStreamIdleTimeoutMs,
            retry: options.retry ?? piCodingAgentRetryDefaults,
            retryAfterMs: acquired.retryAfterMs,
            retryable: acquired.retryable,
            systemPrompt: instructions.text,
            prepareSystemPrompt: () =>
              (promptSections === undefined
                ? Effect.succeed<readonly PromptSectionV0[]>([])
                : promptSections()
              ).pipe(
                Effect.map(
                  (sections) =>
                    instructions.text +
                    sections
                      .map(
                        ({ name, text }) => `\n\n[extension:${name}]\n${text}`,
                      )
                      .join(""),
                ),
                Effect.mapError((cause) => ({
                  _tag: "MuleError" as const,
                  phase: "model" as const,
                  message: "Extension prompt is unavailable",
                  cause,
                })),
              ),
            messages: workingMessages,
            resume: continuation,
            tools,
            getSteeringMessages: () => controlMessages("steer"),
            prepareNextTurn: (turn) =>
              betweenTurns(turn.context, turn.newMessages.length).pipe(
                Effect.mapError((error): MuleError => ({
                  _tag: "MuleError",
                  phase: "control",
                  message: error.message,
                })),
              ),
            onRetry: (event) => {
              if (event.type === "retry_finished" && !event.success)
                failedModelAttempts = event.attempt + 1;
              return recordLiveRetryEvent(
                options.store,
                accepted,
                toLiveRetryEvent(event, acquired.model),
                event.turn,
              );
            },
          };
          return muleEffect
            .run(
              acquired.stream === undefined
                ? input
                : { ...input, stream: acquired.stream },
              listener,
            )
            .pipe(
              Effect.onExit((exit) => {
                if (failedEnd === undefined) return Effect.void;
                if (Exit.isSuccess(exit)) {
                  const cause =
                    exit.value.modelLimitReason ??
                    acquired.failureReason?.(exit.value.message) ??
                    returnedFailureReason(exit.value);
                  failureReason = (
                    failedModelAttempts > 1 &&
                    exit.value.modelLimitReason === undefined &&
                    acquired.rejection?.(exit.value.message)?.code !==
                      "usage-limit-reached"
                      ? `The model failed ${String(failedModelAttempts)} times on this conversation: ${cause}`
                      : cause
                  ).slice(0, 200);
                  return recorder(failedEnd, failureReason);
                }
                const error = Cause.findErrorOption(exit.cause);
                return recorder(
                  failedEnd,
                  Option.isNone(error)
                    ? unavailableRunReason({
                        runId: failedEnd.run.value,
                        provider: acquired.model.provider,
                        model: acquired.model.id,
                      })
                    : error.value.phase === "model"
                      ? `model:rejected: ${error.value.message}`.slice(0, 200)
                      : `runtime:rejected: ${rejectedRun(error.value).diagnostic}`,
                );
              }),
              Effect.mapError((error) =>
                error.phase === "model"
                  ? new ShiitakeError(
                      "model",
                      "rejected",
                      error.message,
                      undefined,
                      error,
                    )
                  : rejectedRun(error),
              ),
            );
        },
      });
      return { run, failureReason };
    }),
  );
}

/**
 * Run one accepted compaction request: the run's model acquisition and the
 * Session's one compaction commit, with no model turn and no tools.
 */
export function runAcceptedCompaction(
  options: ShiitakeEffectOptions,
  prepared: PreparedSessionRun,
  compactionHandoff: CompactionHandoffV0 | undefined,
): Effect.Effect<ManualCompactionOutcome, ShiitakeError> {
  return Effect.scoped(
    Effect.gen(function* () {
      const acquired = yield* acquireOwnedModel(
        options.model.acquire({
          sessionId: prepared.command.sessionId,
          preference: prepared.modelPreference,
        }),
      );
      const entry = prepared.command.entry;
      return yield* compactSession({
        store: options.store,
        sessionId: prepared.command.sessionId,
        workId: entry.workId,
        model: acquired.model,
        stream: acquired.stream,
        handoff: compactionHandoff,
        settings: openCompactionSettings(options.compaction)(acquired.model),
        path: prepared.path,
        instructions: isCompactCommand(entry) ? entry.instructions : undefined,
      });
    }),
  );
}
