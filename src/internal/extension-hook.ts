import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

/** An outside handler rejected or threw before Shiitake could use its result. */
export class ExtensionHookError extends Data.TaggedError("ExtensionHookError")<{
  cause: unknown;
}> {}

/** Lift an outside handler once, outside any synchronous write transaction. */
export function invokeExtensionHook<A, E = never, R = never>(
  handler: () => A | Promise<A> | Effect.Effect<A, E, R>,
): Effect.Effect<A, E | ExtensionHookError, R> {
  return Effect.suspend((): Effect.Effect<A, E | ExtensionHookError, R> => {
    const result = handler();
    if (Effect.isEffect(result)) return result;
    return Effect.tryPromise({
      try: () => Promise.resolve(result),
      catch: (cause) => new ExtensionHookError({ cause }),
    });
  });
}

/** A write callback must resolve before the owner commits its SQLite transaction. */
export type SyncResult<A> =
  A extends Option.Option<unknown>
    ? A
    : A extends Promise<unknown> | Effect.Effect<unknown, unknown, unknown>
      ? never
      : A;

/** Public state-update shape: infer the return *before* rejecting async results. */
export type StateUpdate<Tx> = <A>(handler: (tx: Tx) => SyncResult<A>) => A;

/** Raised before SQLite commits when an untyped handler returns async work. */
export class AsyncExtensionTransactionError extends Error {
  constructor() {
    super("Extension transaction handler must return synchronously");
    this.name = "AsyncExtensionTransactionError";
  }
}

/** Reject a runtime-invalid handler before the surrounding transaction commits. */
export function invokeSyncHandler<Tx, A>(handler: (tx: Tx) => A, tx: Tx): A {
  const value = handler(tx);
  if (
    value instanceof Promise ||
    (Effect.isEffect(value) && !Option.isOption(value))
  )
    throw new AsyncExtensionTransactionError();
  return value;
}

/** Adapt a synchronous callback or a synchronously executed Effect to the owner transaction. */
export function invokeTransactionHook<A, E, R>(
  handler: () => A | Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> {
  return Effect.suspend((): Effect.Effect<A, E, R> => {
    const result = handler();
    if (result instanceof Promise) throw new AsyncExtensionTransactionError();
    if (Option.isOption(result)) return Effect.succeed(result);
    if (Effect.isEffect(result)) return result;
    return Effect.succeed(result);
  });
}
