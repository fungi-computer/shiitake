import type { Json } from "../extensions.js";
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import type { ModuleRegistration } from "@fungi.computer/mycelium";
import type { BirdDogMail } from "@fungi.computer/bird-dog";
import type { VerifiedAgentReply } from "./woodstock/index.js";
import type { BirdDogEffect } from "@fungi.computer/bird-dog/effect";
import type { Accepted } from "../client.js";
import type { ShiitakeEffectHost } from "../effect.js";
import { ShiitakeError } from "../errors.js";
import type {
  CommandSettlement,
  RuntimeHost,
  ShiitakeOptions,
  ShiitakeOwnerAgent,
} from "./contracts.js";
import {
  liftTools,
  runShiitakePromise,
  toPromiseOwner,
} from "./effect-client.js";
import type {
  RuntimeHostEffect,
  ShiitakeEffectOwner,
  ShiitakeEffectOptions,
} from "./effect-contracts.js";
import { adaptModelSource } from "./model-acquisition.js";

function boundary(
  boundaryName: ConstructorParameters<typeof ShiitakeError>[0],
  message: string,
): ShiitakeError {
  return new ShiitakeError(boundaryName, "rejected", message);
}

const identifier = Schema.String.pipe(
  Schema.check(Schema.isMinLength(1)),
  Schema.check(Schema.isMaxLength(256)),
);
const instructionText = Schema.String.pipe(
  Schema.check(Schema.isMinLength(1)),
  Schema.check(
    Schema.makeFilter(
      (value) => new TextEncoder().encode(value).byteLength <= 32_000,
    ),
  ),
);
const instructionSchema = Schema.Struct({
  revision: identifier,
  text: instructionText,
  // The facade preserves opaque registration handles; Mycelium's compiler
  // validates their WeakMap identity when the run scope installs them.
  modules: Schema.optionalKey(
    Schema.declare<readonly ModuleRegistration[]>(
      (input): input is readonly ModuleRegistration[] =>
        Array.isArray(input) && input.every(Predicate.isObject),
    ),
  ),
});
type RuntimeHandle = Awaited<ReturnType<RuntimeHost["bind"]>>;
const runtimeBindSchema = Schema.declare<RuntimeHost["bind"]>(
  (input): input is RuntimeHost["bind"] => Predicate.isFunction(input),
);
const runtimeMethodSchema = Schema.declare<RuntimeHandle["close"]>(
  (input): input is RuntimeHandle["close"] => Predicate.isFunction(input),
);
const runtimeHostSchema = Schema.Struct({ bind: runtimeBindSchema });
const runtimeCommitSchema = Schema.declare<RuntimeHandle["commit"]>(
  (input): input is RuntimeHandle["commit"] => Predicate.isFunction(input),
);
const runtimeHandleSchema = Schema.Struct({
  commit: runtimeCommitSchema,
  requestWake: runtimeMethodSchema,
  close: runtimeMethodSchema,
});
const runtimeCloseSchema = Schema.Struct({ close: runtimeMethodSchema });

const decodeInstructions = Schema.decodeUnknownResult(instructionSchema);
const decodeRuntimeHost = Schema.decodeUnknownResult(runtimeHostSchema);
const decodeRuntimeHandle = Schema.decodeUnknownResult(runtimeHandleSchema);
const decodeRuntimeClose = Schema.decodeUnknownResult(runtimeCloseSchema);

function runtimeInvalid(cause?: unknown): ShiitakeError {
  return new ShiitakeError(
    "runtime",
    "invalid",
    "Runtime host is invalid",
    undefined,
    cause,
  );
}

function runtimeUnavailable(): ShiitakeError {
  return new ShiitakeError(
    "runtime",
    "unavailable",
    "Runtime host is unavailable",
  );
}

function liftRuntimeMethod(
  receiver: RuntimeHandle,
  method: RuntimeHandle["close"],
): Effect.Effect<void, ShiitakeError> {
  return Effect.try({
    try: () => method.call(receiver),
    catch: runtimeUnavailable,
  }).pipe(
    Effect.flatMap((result) =>
      Effect.tryPromise({
        try: () => Promise.resolve(result),
        catch: runtimeUnavailable,
      }),
    ),
    Effect.flatMap((result) =>
      result === undefined ? Effect.void : Effect.fail(runtimeInvalid()),
    ),
  );
}

function failInvalidAfterCleanup(
  receiver: RuntimeHandle,
  close: RuntimeHandle["close"] | undefined,
  cause?: unknown,
): Effect.Effect<never, ShiitakeError> {
  const decoded = decodeRuntimeClose({ close });
  if (decoded._tag === "Failure") return Effect.fail(runtimeInvalid(cause));
  return Effect.uninterruptible(
    liftRuntimeMethod(receiver, decoded.success.close),
  ).pipe(
    Effect.matchCauseEffect({
      onFailure: (cleanup) => Effect.fail(runtimeInvalid({ cause, cleanup })),
      onSuccess: () => Effect.fail(runtimeInvalid(cause)),
    }),
  );
}

function liftRuntimeHandle(
  value: RuntimeHandle,
): Effect.Effect<
  Effect.Success<ReturnType<RuntimeHostEffect["bind"]>>,
  ShiitakeError
> {
  return Effect.suspend(() => {
    if (!Predicate.isObject(value)) return Effect.fail(runtimeInvalid());
    const receiver = value;
    let close: RuntimeHandle["close"] | undefined;
    try {
      close = receiver.close;
    } catch (cause) {
      return Effect.fail(runtimeInvalid(cause));
    }
    let requestWake: RuntimeHandle["requestWake"] | undefined;
    try {
      requestWake = receiver.requestWake;
    } catch (cause) {
      return failInvalidAfterCleanup(receiver, close, cause);
    }
    const decoded = decodeRuntimeHandle({
      commit: receiver.commit,
      requestWake,
      close,
    });
    if (decoded._tag === "Failure")
      return failInvalidAfterCleanup(receiver, close);
    return Effect.succeed(
      Object.freeze({
        commit: <A>(mutation: Effect.Effect<A, ShiitakeError>) =>
          Effect.tryPromise({
            try: () => receiver.commit(() => runShiitakePromise(mutation)),
            catch: (error) =>
              error instanceof ShiitakeError ? error : runtimeUnavailable(),
          }),
        requestWake: liftRuntimeMethod(receiver, decoded.success.requestWake),
        close: liftRuntimeMethod(receiver, decoded.success.close),
      }),
    );
  });
}

function liftRuntime(
  options: ShiitakeOptions,
): Effect.Effect<RuntimeHostEffect, ShiitakeError> {
  return Effect.try({
    try: () => {
      const runtime = options.runtime;
      if (!Predicate.isObject(runtime)) throw runtimeInvalid();
      const decoded = decodeRuntimeHost(runtime);
      if (decoded._tag === "Failure") throw runtimeInvalid();
      return { receiver: runtime, bind: decoded.success.bind };
    },
    catch: runtimeInvalid,
  }).pipe(
    Effect.map(({ receiver, bind }) =>
      Object.freeze({
        bind: ({ reconcile, pendingWork }) =>
          Effect.try({
            try: () =>
              bind.call(receiver, {
                reconcile: () => runShiitakePromise(reconcile),
                pendingWork: () => runShiitakePromise(pendingWork),
              }),
            catch: runtimeUnavailable,
          }).pipe(
            Effect.flatMap((result) =>
              Effect.tryPromise({
                try: () => Promise.resolve(result),
                catch: runtimeUnavailable,
              }),
            ),
            Effect.flatMap(liftRuntimeHandle),
          ),
      }),
    ),
  );
}

function liftOptions(
  options: ShiitakeOptions,
  runtime: RuntimeHostEffect,
): ShiitakeEffectOptions {
  const base: Omit<ShiitakeEffectOptions, "tools"> = {
    // Native SQL and transactionSync are synchronous owner capabilities; do
    // not unwrap them into a second Promise/CAS storage abstraction.
    store: options.store,
    runtime,
    extensions: options.extensions,
    instructions: {
      load: ({ sessionId, commandId }) =>
        Effect.tryPromise({
          try: (signal) =>
            options.instructions.load({ sessionId, commandId, signal }),
          catch: () =>
            boundary("instructions", "Instruction acquisition failed"),
        }).pipe(
          Effect.flatMap((value) => {
            const decoded = decodeInstructions(value);
            return decoded._tag === "Success"
              ? Effect.succeed(decoded.success)
              : Effect.fail(
                  new ShiitakeError(
                    "contract",
                    "invalid",
                    "Instruction revision is invalid",
                  ),
                );
          }),
        ),
    },
    model: adaptModelSource(options.model),
  };
  if (options.meta !== undefined) Object.assign(base, { meta: options.meta });
  if (options.storagePressure !== undefined)
    Object.assign(base, { storagePressure: options.storagePressure });
  if (options.compaction !== undefined)
    Object.assign(base, { compaction: options.compaction });
  const toolSource = options.tools;
  if (toolSource === undefined) return base;
  return { ...base, tools: liftTools(toolSource) };
}

/** Open the single Promise adaptation over the canonical Effect agent. */
export function openPromiseAgent(
  options: ShiitakeOptions,
  make: (
    options: ShiitakeEffectOptions,
  ) => Effect.Effect<ShiitakeEffectOwner, ShiitakeError>,
): Promise<ShiitakeOwnerAgent> {
  return runShiitakePromise(
    liftRuntime(options).pipe(
      Effect.flatMap((runtime) => make(liftOptions(options, runtime))),
    ),
  ).then(toPromiseOwner);
}

/** Open the Promise-facing host over the canonical Effect host composition. */
export function openPromiseHost(
  options: ShiitakeOptions,
  mailRoom: BirdDogEffect,
  openHost: (
    options: ShiitakeEffectOptions,
    mailRoom: BirdDogEffect,
  ) => Effect.Effect<ShiitakeEffectHost, ShiitakeError>,
): Promise<
  Readonly<{
    agent: ShiitakeOwnerAgent;
    readSettlement: (
      sessionId: string,
      commandId: string,
    ) => Promise<CommandSettlement | undefined>;
    acceptMail: (
      mail: BirdDogMail,
      meta?: Json,
      reply?: VerifiedAgentReply,
      stageAcceptedNotice?: (mail: BirdDogMail) => void,
    ) => Promise<Accepted>;
  }>
> {
  return runShiitakePromise(
    liftRuntime(options).pipe(
      Effect.flatMap((runtime) =>
        openHost(liftOptions(options, runtime), mailRoom),
      ),
    ),
  ).then(({ agent, acceptMail, readSettlement }) =>
    Object.freeze({
      agent: toPromiseOwner(agent),
      readSettlement: (sessionId: string, commandId: string) =>
        runShiitakePromise(readSettlement(sessionId, commandId)),
      acceptMail: (
        mail: BirdDogMail,
        meta?: Json,
        reply?: VerifiedAgentReply,
        stageAcceptedNotice?: (mail: BirdDogMail) => void,
      ) =>
        runShiitakePromise(acceptMail(mail, meta, reply, stageAcceptedNotice)),
    }),
  );
}
