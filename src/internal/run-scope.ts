import type { FileTool } from "@fungi.computer/knapsack";
import {
  muleEffect,
  piCodingAgentRetryDefaults,
  piHttpIdleTimeoutMs,
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
import * as Cause from "effect/Cause";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import type * as Scope from "effect/Scope";

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

function runtimeFailure(message: string): ShiitakeError {
  return new ShiitakeError("runtime", "rejected", message);
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

/**
 * Map Mule's retry transitions onto Woodstock's retained `live_retry` facts.
 * A started attempt ends the backoff countdown before streaming resumes.
 */
function toLiveRetryEvent(event: MuleRetryObservation): LiveRetryEvent {
  if (event.type === "retry_attempt_started")
    return {
      type: "auto_retry_attempt",
      run: event.run,
      attempt: event.attempt,
    };
  return event.type === "retry_scheduled"
    ? {
        type: "auto_retry_start",
        run: event.run,
        attempt: event.attempt,
        maxAttempts: event.maxAttempts,
        delayMs: event.delayMs,
        errorMessage: event.errorMessage,
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
          finalError: event.finalError,
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
    const runtime = yield* makeMycelium(
      toolSource.execution === undefined
        ? runtimeOptions
        : { ...runtimeOptions, execution: toolSource.execution },
    ).pipe(Effect.mapError(() => new ToolSetupError("runtime")));
    const lease = yield* runtime
      .acquire()
      .pipe(Effect.mapError(() => new ToolSetupError("lease")));
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
        yield* Effect.addFinalizer(() => Effect.sync(releaseInstructions));
      const acquired = yield* acquireOwnedModel(
        options.model.acquire({
          sessionId: prepared.command.sessionId,
          preference: prepared.modelPreference,
        }),
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
            return recorder(event);
          };
          const input: MuleRunInput = {
            model: acquired.model,
            modelStreamIdleTimeoutMs:
              options.modelStreamIdleTimeoutMs ?? piHttpIdleTimeoutMs,
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
            onRetry: (event) =>
              recordLiveRetryEvent(
                options.store,
                accepted,
                toLiveRetryEvent(event),
                event.turn,
              ),
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
                  failureReason = (
                    acquired.failureReason?.(exit.value.message) ??
                    returnedFailureReason(exit.value)
                  ).slice(0, 200);
                  return recorder(failedEnd, failureReason);
                }
                const error = Cause.findErrorOption(exit.cause);
                return recorder(
                  failedEnd,
                  Option.isSome(error) && error.value.phase === "model"
                    ? `model:rejected: ${error.value.message}`.slice(0, 200)
                    : unavailableRunReason({
                        runId: failedEnd.run.value,
                        provider: acquired.model.provider,
                        model: acquired.model.id,
                      }),
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
                  : runtimeFailure("Mule rejected the durable command run"),
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
