import type { OperationRegistration } from "@fungi.computer/mycelium";
// The runtime-free entry keeps extensions bundleable into hosted isolates.
import { Mycelium } from "@fungi.computer/mycelium/define";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";

import type {
  CommandsV0,
  ExtensionContextV0,
  Json,
  RunIdentityV0,
} from "../extensions.js";
import type { RegisteredExtensionCommandV0 } from "./extension-command-registry.js";
import { ShiitakeError } from "../errors.js";
import { JsonValueSchema } from "./json-value.js";

/** How one command appears to the model when exposed as a tool. */
export type CommandToolOptionsV0 = Readonly<{
  /** Discovery text for the model. */
  description: string;
}>;

/** Expose declared commands as model tools, bound to the current run's Session. */
export type CommandToolsV0<C extends CommandsV0> = Readonly<{
  /** The named command as one Mycelium operation running the same handler. */
  tool: (
    name: keyof C & string,
    options: CommandToolOptionsV0,
  ) => OperationRegistration;
}>;

const decodeJson = Schema.decodeUnknownEffect(JsonValueSchema);

/** Build the model-facing tool for each command, bound to one run. */
export function commandTools<C extends CommandsV0>(
  specs: C | undefined,
  registered: () => ReadonlyMap<string, RegisteredExtensionCommandV0>,
  transact: ExtensionContextV0<
    Readonly<Record<never, never>>,
    Json
  >["transact"],
  run: RunIdentityV0,
  /** Whether the host granted `commands`; the model door matches the member door. */
  granted: boolean,
): CommandToolsV0<C> {
  return Object.freeze({
    tool: (name: keyof C & string, options: CommandToolOptionsV0) => {
      const spec = specs?.[name];
      if (spec === undefined)
        throw new Error(`Extension declares no command named ${name}`);
      // Mycelium owns the model-facing JSON (the codec's JSON form); the
      // command door owns its wire form. Cross between them through the
      // command's own codecs, then run the same registered handler.
      const encodeInput = Schema.encodeUnknownEffect(spec.input);
      const decodeOutput = Schema.decodeUnknownEffect(spec.output);
      // A command is a write and declares no idempotency, so its tool is
      // never replayed: replay is always "unsafe".
      const commandOperation = Mycelium.operation({
        description: options.description,
        replay: "unsafe",
        input: spec.input,
        output: spec.output,
        execute: (input) =>
          Effect.runPromise(
            Effect.gen(function* () {
              if (!granted)
                return yield* Effect.fail(
                  new ShiitakeError(
                    "extensions",
                    "unavailable",
                    "Extension is unavailable",
                  ),
                );
              const command = registered().get(name);
              if (command === undefined)
                return yield* Effect.die(
                  new Error(`Command ${name} has no handler`),
                );
              const wire = yield* encodeInput(input).pipe(
                Effect.flatMap(decodeJson),
              );
              const invocation = command({
                sessionId: spec.scope === "session" ? run.sessionId : null,
                input: wire,
              });
              if (Result.isFailure(invocation))
                return yield* Effect.fail(invocation.failure);
              const output = yield* transact(invocation.success());
              return yield* decodeOutput(output);
            }),
          ),
      });
      return commandOperation;
    },
  });
}
