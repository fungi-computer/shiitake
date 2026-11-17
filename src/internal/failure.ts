import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";

/** Retain the original failure without printing Session or provider data. */
export function reportCatchFailure(
  operation: string,
  cause: unknown,
  metadata?: Readonly<{ errorName: string; errorCode?: string }>,
): void {
  Effect.runFork(
    Effect.logError("shiitake_operation_failed", Redacted.make(cause)).pipe(
      Effect.annotateLogs({
        operation,
        errorKind: cause instanceof Error ? "error" : "non-error",
        ...metadata,
      }),
    ),
  );
}
