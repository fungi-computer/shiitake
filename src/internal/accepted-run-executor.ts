import { decodeAgentJob } from "@fungi.computer/bird-dog/effect";
import {
  type WatchdogExecutionDecision,
  type WatchdogRunningJob,
} from "@fungi.computer/watchdog";
import { Mycelium } from "@fungi.computer/mycelium";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";

import { ShiitakeError } from "../errors.js";
import { ExtensionId } from "../extension-contract.js";
import type { RunIdentityV0 } from "../extensions.js";
import type { ShiitakeEffectOptions } from "./effect-contracts.js";
import { publishOwnerObservation } from "./observation-kernel.js";
import { returnedFailureReason } from "./run-failure-reason.js";
import { coreProtocolDigest } from "./extension-roster.js";
import {
  runAcceptedCommand,
  runAcceptedCompaction,
  RunRejectedError,
  ToolSetupError,
} from "./run-scope.js";
import { CompactionCommitError } from "./compaction/session-compaction.js";
import type { ExtensionRuntime } from "./extension-runtime.js";
import {
  BrokenSessionError,
  isCompactCommand,
  loadAcceptedRun,
  readExtensionAdmissionOrigin,
  recordWorkSettlement,
  type RunContract,
} from "./woodstock/index.js";

/** Bounded process-loss recovery for every accepted Agent turn. */
export const agentTurnRecovery = Object.freeze({ maxRecoveries: 2 });

function runtimeFailure(message: string): ShiitakeError {
  return new ShiitakeError("runtime", "rejected", message);
}

/**
 * Settle a run that could not finish. The durable job records the sanitized
 * boundary and category, plus a safe model-host or closed tool-setup diagnostic.
 * Original provider causes stay in the server log only.
 */
function failedDecision(error: ShiitakeError): WatchdogExecutionDecision {
  const category = `${error.boundary}:${error.cause}`;
  const diagnostic =
    error instanceof ToolSetupError ||
    error instanceof RunRejectedError ||
    error instanceof CompactionCommitError ||
    error instanceof BrokenSessionError
      ? error.diagnostic
      : error.boundary === "model"
        ? error.message
        : undefined;
  return {
    kind: "failed",
    reason:
      diagnostic === undefined
        ? category
        : `${category}: ${diagnostic}`.slice(0, 200),
  };
}

/** Log a run failure with its sanitized reason and underlying cause. */
function logRunFailed(
  workId: string,
  error: ShiitakeError,
): Effect.Effect<void> {
  return Effect.logError("shiitake_run_failed", Redacted.make(error)).pipe(
    Effect.annotateLogs({
      workId,
      reason: `${error.boundary}:${error.cause}`,
    }),
  );
}

/** Execute one claimed opaque job through Shiitake's accepted run scope. */
export function executeAcceptedRun(
  options: ShiitakeEffectOptions,
  coreDigest: string,
  job: WatchdogRunningJob,
  extensions: ExtensionRuntime | undefined,
) {
  return Effect.gen(function* () {
    publishOwnerObservation(options.store, job.queue);
    const payload = yield* Effect.try({
      try: () => decodeAgentJob(job),
      catch: () => runtimeFailure("Agent work payload is invalid"),
    });
    const prepared = yield* loadAcceptedRun(options.store, payload.work);
    if (prepared.abnormalTerminal !== undefined)
      return { kind: prepared.abnormalTerminal };
    const incompatible = extensions?.incompatibleMigrations()[0];
    if (incompatible !== undefined) {
      yield* Effect.logError("shiitake_run_contract_changed").pipe(
        Effect.annotateLogs({
          workId: job.jobId,
          terminal: `contract_changed: ${incompatible}`,
        }),
      );
      return yield* Effect.fail(
        runtimeFailure(`contract_changed: ${incompatible}`),
      );
    }
    // A cold reopen selects its current admitted code at the next execution
    // boundary. The command's admission contract remains an audit fact.
    const participants = new Set(
      extensions?.runParticipants().map(([id]) => id) ?? [],
    );
    // A compaction request runs the Session's one compaction commit with the
    // run's model and handoff, and no model turn. A failure settles through
    // the ordinary failed decision with its owner-closed diagnostic.
    // "Nothing to compact" completes the work; the Session records it as a
    // compaction notice, which clients show as the request's result.
    if (isCompactCommand(prepared.command.entry)) {
      yield* runAcceptedCompaction(
        options,
        prepared,
        extensions?.compactionHandoff(participants),
      );
      yield* recordWorkSettlement(options.store, {
        sessionId: prepared.command.sessionId,
        commandFactId: payload.work.commandFactId,
        workId: prepared.command.entry.workId,
        outcome: "completed",
      });
      return { kind: "completed" as const };
    }
    const origin = yield* Effect.try({
      try: () =>
        readExtensionAdmissionOrigin(
          options.store,
          payload.work.sessionId,
          payload.work.commandFactId,
          prepared.command.entry.workId,
        ),
      catch: () => runtimeFailure("Accepted run origin is unavailable"),
    });
    const runIdentity: RunIdentityV0 = {
      sessionId: prepared.command.sessionId,
      workId: prepared.command.entry.workId,
      origin:
        origin === null
          ? null
          : {
              extensionId: Schema.decodeUnknownSync(ExtensionId)(
                origin.extensionId,
              ),
              correlation: origin.correlation,
            },
    };
    const fileTools =
      extensions === undefined
        ? []
        : yield* extensions.filesForRun(runIdentity, participants);
    // Every participant offers its tools under the grant key its host grant
    // names (its id by default); Mycelium decides each call.
    const extensionModules =
      extensions === undefined
        ? []
        : yield* Effect.forEach([...participants], (extensionId) =>
            extensions.modulesForRun(runIdentity, new Set([extensionId])).pipe(
              Effect.flatMap((modules) =>
                Effect.try({
                  try: () => {
                    const grantKey =
                      options.extensions.grants.find(
                        (grant) => grant.id === extensionId,
                      )?.grantKey ?? extensionId;
                    return modules.map((module) =>
                      Mycelium.grantAs(module, grantKey),
                    );
                  },
                  catch: () => runtimeFailure("Extension tools are invalid"),
                }),
              ),
            ),
          ).pipe(Effect.map((groups) => groups.flat()));
    const extensionHandoff = extensions?.compactionHandoff(participants);
    const executionContract: RunContract = {
      core:
        fileTools.length === 0
          ? coreDigest
          : yield* coreProtocolDigest(
              fileTools.map((tool) => tool.descriptor.name),
            ),
      extensions: extensions?.runParticipants() ?? [],
    };

    const { run, failureReason } = yield* runAcceptedCommand(
      options,
      prepared,
      executionContract,
      extensionModules,
      fileTools,
      extensionHandoff,
      extensions === undefined
        ? undefined
        : () => extensions.promptSections(runIdentity, participants),
    );
    switch (run.status) {
      case "completed":
        return { kind: "completed" as const };
      case "cancelled":
        return { kind: "cancelled" as const };
      case "failed":
        return {
          kind: "failed" as const,
          reason: failureReason ?? returnedFailureReason(run),
        };
    }
  }).pipe(
    Effect.catch((error) =>
      logRunFailed(job.jobId, error).pipe(Effect.as(failedDecision(error))),
    ),
  );
}
