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
  ExtensionStateError,
  ExtensionTx,
  Json,
} from "../extensions.js";
import { invokeTransactionHook } from "./extension-hook.js";
import { JsonValueSchema } from "./json-value.js";

type CommandFailure =
  ExtensionCommandError | ExtensionStateError | AdmissionError;
/** A parsed invocation retains its typed handler until the transaction runs. */
export type RegisteredExtensionCommandV0 = (
  frame: Readonly<{
    sessionId: string | null;
    input: Json;
    caller?: ExtensionCommandCallerV0;
  }>,
) => Result.Result<
  () => Effect.Effect<
    Json,
    ExtensionCommandError | ExtensionStateError | AdmissionError,
    ExtensionTx | ExtensionRead
  >,
  Schema.SchemaError
>;

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
  handler: (
    call: Readonly<{
      sessionId: CommandSessionIdsV0[Scope];
      input: Input["Type"];
      caller?: ExtensionCommandCallerV0;
    }>,
  ) =>
    | Output["Type"]
    | Effect.Effect<
        Output["Type"],
        CommandFailure,
        ExtensionTx | ExtensionRead
      >,
): RegisteredExtensionCommandV0 {
  const session: Schema.Codec<CommandSessionIdsV0[Scope]> =
    sessionSchemas[spec.scope];
  const decodeSession = Schema.decodeUnknownResult(session);
  const decodeInput = Schema.decodeUnknownResult(spec.input, {
    onExcessProperty: "error",
  });
  return (frame) =>
    Result.flatMap(decodeSession(frame.sessionId), (sessionId) =>
      Result.map(
        decodeInput(frame.input),
        (input) => () =>
          invokeTransactionHook(() =>
            handler(
              frame.caller === undefined
                ? { sessionId, input }
                : { sessionId, input, caller: frame.caller },
            ),
          ).pipe(
            Effect.flatMap((output) =>
              Schema.encodeEffect(spec.output)(output).pipe(
                Effect.flatMap(Schema.decodeUnknownEffect(JsonValueSchema)),
                Effect.orDie,
              ),
            ),
          ),
      ),
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
