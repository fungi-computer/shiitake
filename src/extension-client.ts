import * as Result from "effect/Result";
import * as Schema from "effect/Schema";

import type {
  SessionExtensionSlots,
  ShiitakeAgent,
  ShiitakeSession,
} from "./client.js";
import { ShiitakeError } from "./errors.js";
import type {
  CommandsV0,
  ExtensionContractV0,
  ExtensionSlotV0,
  Json,
} from "./extensions.js";
import { JsonValueSchema } from "./internal/json-value.js";

const decodeJson = Schema.decodeUnknownResult(JsonValueSchema);

/** What one client sees of an extension in a Session observation. */
export type ExtensionViewV0<P> =
  | Readonly<{ status: "unavailable" }>
  | Readonly<{ status: "stopped"; reason: string }>
  | Readonly<{ status: "running"; value: P | null }>;

/** Command names of one scope, typed by the shared contract. */
export type ScopedCommandsV0<
  C extends CommandsV0,
  S extends "session" | "agent",
> = {
  [K in keyof C & string]: C[K]["scope"] extends S ? K : never;
}[keyof C & string];

/** Typed call surface of one contract's commands in one scope. */
export type BoundCallV0<C extends CommandsV0, S extends "session" | "agent"> = <
  K extends ScopedCommandsV0<C, S>,
>(
  name: K,
  input: C[K]["input"]["Type"],
) => Promise<C[K]["output"]["Type"]>;

/** One Session bound to one extension contract. */
export type BoundSessionExtensionV0<C extends CommandsV0, P> = Readonly<{
  /** Decode this extension's slot from a snapshot or update frame. */
  view: (
    observation: Readonly<{ extensions: SessionExtensionSlots }>,
  ) => ExtensionViewV0<P>;
  /** Encode, call, and decode one Session-scope command. */
  call: BoundCallV0<C, "session">;
}>;

/** One Agent bound to one extension contract. */
export type BoundAgentExtensionV0<C extends CommandsV0> = Readonly<{
  /** Encode, call, and decode one Agent-scope command. */
  call: BoundCallV0<C, "agent">;
}>;

function contractFailure(message: string): ShiitakeError {
  return new ShiitakeError("contract", "invalid", message);
}

function typedCall<C extends CommandsV0, S extends "session" | "agent">(
  contract: Readonly<{ commands: C }>,
  send: (command: string, input: Json) => Promise<Json>,
): BoundCallV0<C, S> {
  return async <K extends ScopedCommandsV0<C, S>>(
    name: K,
    input: C[K]["input"]["Type"],
  ): Promise<C[K]["output"]["Type"]> => {
    const spec = Object.hasOwn(contract.commands, name)
      ? contract.commands[name]
      : undefined;
    if (spec === undefined)
      throw contractFailure("Extension command is unknown");
    const encoded = Schema.encodeUnknownResult(spec.input)(input).pipe(
      Result.flatMap(decodeJson),
    );
    if (encoded._tag === "Failure")
      throw contractFailure("Extension command input is invalid");
    const output = Schema.decodeUnknownResult<C[K]["output"]>(spec.output)(
      await send(name, encoded.success),
    );
    if (output._tag === "Failure")
      throw contractFailure("Extension output does not match its contract");
    return output.success;
  };
}

/** Decode one observed slot using its owning contract's projection. */
export function viewExtension<C extends CommandsV0, P, PI>(
  contract: ExtensionContractV0<C, P, PI>,
  slot: ExtensionSlotV0 | undefined,
): ExtensionViewV0<P> {
  if (slot === undefined) return { status: "unavailable" };
  if (slot.status === "stopped")
    return { status: "stopped", reason: slot.reason };
  if (slot.value === null || contract.projection === null)
    return { status: "running", value: null };
  const decoded = Schema.decodeUnknownResult(contract.projection.schema)(
    slot.value,
  );
  return decoded._tag === "Success"
    ? { status: "running", value: decoded.success }
    : {
        status: "stopped",
        reason: "Extension projection does not match this client",
      };
}

/** Bind one Session to one extension contract; the contract's schemas decode. */
export function bindSessionExtension<C extends CommandsV0, P, PI>(
  session: Pick<ShiitakeSession, "extension">,
  contract: ExtensionContractV0<C, P, PI>,
): BoundSessionExtensionV0<C, P> {
  const address = session.extension(contract.id);
  return Object.freeze({
    view: (observation) =>
      viewExtension(
        contract,
        Object.hasOwn(observation.extensions, contract.id)
          ? observation.extensions[contract.id]
          : undefined,
      ),
    call: typedCall(contract, (command, input) => address.call(command, input)),
  });
}

/** Bind one Agent to one extension contract's Agent-scope commands. */
export function bindAgentExtension<C extends CommandsV0, P, PI>(
  agent: Pick<ShiitakeAgent, "extensions">,
  contract: ExtensionContractV0<C, P, PI>,
): BoundAgentExtensionV0<C> {
  return Object.freeze({
    call: typedCall(contract, (command, input) =>
      agent.extensions.call(contract.id, command, input),
    ),
  });
}
