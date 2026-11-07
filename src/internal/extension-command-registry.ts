import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import type {
  AdmissionError,
  CommandHandlersV0,
  CommandSessionIdsV0,
  CommandsV0,
  ExtensionCommandError,
  ExtensionCommandCallerV0,
  ExtensionRead,
  ExtensionPreparationV0,
  ExtensionStateError,
  ExtensionTx,
  Json,
} from "../extensions.js";
import { invokeTransactionHook } from "./extension-hook.js";
import { JsonValueSchema } from "./json-value.js";

/** Failures retained across read-only preparation and atomic dispatch. @inline */
type CommandFailure =
  ExtensionCommandError | ExtensionStateError | AdmissionError;
/** Parsed input retains its typed preparation and atomic body. */
export type RegisteredExtensionCommandV0 = (
  frame: Readonly<{
    sessionId: string | null;
    input: Json;
    caller?: ExtensionCommandCallerV0;
  }>,
) => Result.Result<
  ExtensionPreparationV0<
    Json,
    ExtensionCommandError | ExtensionStateError | AdmissionError
  >,
  Schema.SchemaError
>;

type CommandCall<Scope extends keyof CommandSessionIdsV0, Input> = Readonly<{
  sessionId: CommandSessionIdsV0[Scope];
  input: Input;
  caller?: ExtensionCommandCallerV0;
}>;

type SessionSchemas = {
  [K in keyof CommandSessionIdsV0]: Schema.Codec<CommandSessionIdsV0[K]>;
};
const sessionSchemas: SessionSchemas = {
  agent: Schema.Null,
  session: Schema.String,
};

function bindCommand<
  Scope extends keyof CommandSessionIdsV0,
  Input extends Schema.Codec<unknown>,
  Output extends Schema.Codec<unknown>,
>(
  spec: Readonly<{ scope: Scope; input: Input; output: Output }>,
  handler:
    | ((
        call: CommandCall<Scope, Input["Type"]>,
      ) =>
        | Output["Type"]
        | Effect.Effect<
            Output["Type"],
            CommandFailure,
            ExtensionTx | ExtensionRead
          >)
    | Readonly<{
        prepare: (
          call: CommandCall<Scope, Input["Type"]>,
        ) => ExtensionPreparationV0<Output["Type"], CommandFailure>;
      }>,
): RegisteredExtensionCommandV0 {
  const session: Schema.Codec<CommandSessionIdsV0[Scope]> =
    sessionSchemas[spec.scope];
  const decodeSession = Schema.decodeUnknownResult(session);
  const decodeInput = Schema.decodeUnknownResult(spec.input, {
    onExcessProperty: "error",
  });
  const encodeBody = (
    body: Effect.Effect<
      Output["Type"],
      CommandFailure,
      ExtensionTx | ExtensionRead
    >,
  ) =>
    body.pipe(
      Effect.flatMap((output) =>
        Schema.encodeEffect(spec.output)(output).pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(JsonValueSchema)),
          Effect.orDie,
        ),
      ),
    );
  return (frame) =>
    Result.flatMap(decodeSession(frame.sessionId), (sessionId) =>
      Result.map(decodeInput(frame.input), (input) => {
        const call =
          frame.caller === undefined
            ? { sessionId, input }
            : { sessionId, input, caller: frame.caller };
        return "prepare" in handler
          ? Effect.suspend(() => handler.prepare(call)).pipe(
              Effect.map(encodeBody),
            )
          : Effect.succeed(
              encodeBody(invokeTransactionHook(() => handler(call))),
            );
      }),
    );
}

/** Capture each declared command's schemas and matching handler at activation. */
export function registerExtensionCommands<C extends CommandsV0>(
  specs: C,
  handlers: CommandHandlersV0<C>,
): ReadonlyMap<string, RegisteredExtensionCommandV0> {
  const registry = new Map<string, RegisteredExtensionCommandV0>();
  for (const name of Object.keys(handlers)) {
    const spec = specs[name];
    const handler = handlers[name];
    if (spec === undefined || handler === undefined) continue;
    registry.set(
      name,
      bindCommand<C[string]["scope"], C[string]["input"], C[string]["output"]>(
        spec,
        handler,
      ),
    );
  }
  return registry;
}
