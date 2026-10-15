import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import {
  ExtensionId,
  type ClientActivityV0,
  type CommandsV0,
  type ExtensionContractV0,
} from "./extension-contract.js";
import { viewExtension, type ExtensionViewV0 } from "./extension-client.js";
import type { ExtensionSlotV0 } from "./extensions.js";
import { invokeExtensionHook } from "./internal/extension-hook.js";
import {
  rowIdentitySchemaV1,
  rowLabelSchemaV1,
  rowSchemaV1,
} from "@fungi.computer/gills";

export type { ClientActivityV0 } from "./extension-contract.js";
export { viewExtension } from "./extension-client.js";

const unsigned = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const sequence = Schema.Int.check(Schema.isGreaterThan(0));
const objectArguments = Schema.Record(Schema.String, Schema.Json);
// Extension commands belong to Shiitake; Gill rows only carry navigation.
const commandAction = Schema.Struct({
  id: rowIdentitySchemaV1,
  label: rowLabelSchemaV1,
  input: Schema.optional(objectArguments),
});
const commandRow = Schema.Struct({
  ...rowSchemaV1.fields,
  actions: Schema.optional(
    Schema.Array(commandAction).check(Schema.isMaxLength(8)),
  ),
});
const chipRow = rowSchemaV1.check(
  Schema.makeFilter((row) => row.open === undefined, {
    message: "Client chips are display only",
  }),
);

const panelRow = Schema.Struct({
  ...commandRow.fields,
  inspection: Schema.optionalKey(
    Schema.Struct({
      commandId: rowIdentitySchemaV1,
      title: rowLabelSchemaV1,
      rows: Schema.Array(chipRow).check(Schema.isMaxLength(200)),
    }),
  ),
});

/** One display-only status chip; order is local to the entry. */
export const clientChipSchemaV1 = Schema.Struct({
  id: rowIdentitySchemaV1,
  order: Schema.Number.check(Schema.isFinite()),
  row: chipRow,
  compact: Schema.optional(rowLabelSchemaV1),
});
/** An admitted, display-only status chip. */
export type ClientChipV1 = typeof clientChipSchemaV1.Type;
/** One committed presentation event anchored to its real command, if any. */
export const clientTimelineCellSchemaV1 = Schema.Struct({
  id: rowIdentitySchemaV1,
  sequence,
  at: unsigned,
  commandId: Schema.NullOr(Schema.String),
  rows: Schema.Array(commandRow).check(Schema.isMaxLength(200)),
}).check(
  Schema.makeFilter((cell) => cell.id === String(cell.sequence), {
    message: "Client event identity must equal its sequence",
  }),
);
/** An admitted committed event with its producer identity and anchor. */
export type ClientTimelineCellV1 = typeof clientTimelineCellSchemaV1.Type;

const unique = (ids: readonly string[]) => new Set(ids).size === ids.length;
/** Closed, bounded presentation data shared by every renderer. */
export const clientContributionSchemaV1 = Schema.Struct({
  chips: Schema.Array(clientChipSchemaV1).check(Schema.isMaxLength(4)),
  panel: Schema.Array(panelRow).check(Schema.isMaxLength(16)),
  timeline: Schema.Struct({
    cells: Schema.Array(clientTimelineCellSchemaV1).check(
      Schema.isMaxLength(32),
    ),
    omitted: unsigned,
  }),
  waiting: Schema.NullOr(
    Schema.Struct({ summary: rowLabelSchemaV1, cancel: commandAction }),
  ),
}).check(
  Schema.makeFilter(
    (value) =>
      value.chips.length +
        value.panel.reduce(
          (count, row) => count + 1 + (row.inspection?.rows.length ?? 0),
          0,
        ) +
        value.timeline.cells.reduce(
          (count, cell) => count + cell.rows.length,
          0,
        ) <=
      200,
    { message: "Client contribution exceeds 200 rows" },
  ),
  Schema.makeFilter(
    (value) =>
      unique(value.chips.map((chip) => chip.id)) &&
      unique(value.panel.map((row) => row.id)) &&
      value.panel.every((row) =>
        unique((row.inspection?.rows ?? []).map((child) => child.id)),
      ) &&
      unique(value.timeline.cells.map((cell) => cell.id)) &&
      value.timeline.cells.every((cell) =>
        unique(cell.rows.map((row) => row.id)),
      ),
    { message: "Client contribution contains duplicate identities" },
  ),
);
/** The closed V1 presentation grammar returned by a client entry. */
export type ClientContributionV1 = typeof clientContributionSchemaV1.Type;
/** Borrowed latest-value activity; each iterator subscribes separately. */
export type ClientExtensionContextV1 = Readonly<{
  /** Independent latest-value activity subscriptions, ended on detach. */
  activity: AsyncIterable<ClientActivityV0>;
  /** The selected Session's client attachment lifetime. */
  signal: AbortSignal;
  /** Report the original attachment error to the host's notice and log owner. */
  onError: (error: Error) => void;
}>;
/** Installed client protocol; projection decoding remains inside the author closure. */
export type ClientEntryV1 = Readonly<{
  protocol: "shiitake.client/v1";
  id: ExtensionId;
  /** Render this owner's correlated prompts as notes instead of human bubbles. */
  correlatedPrompts?: "note";
  /** Normalized lifetime attachment; authors use defineClientEntry for async code. */
  attach?: (
    context: ClientExtensionContextV1,
  ) => Effect.Effect<void, Error, Scope.Scope>;
  /** Synchronously produce V1 data from the observed slot; the host admits it once. */
  contribute?: (slot: ExtensionSlotV0 | undefined) => ClientContributionV1;
}>;
const entrySchema: Schema.Codec<ClientEntryV1, unknown> = Schema.Struct({
  protocol: Schema.Literal("shiitake.client/v1"),
  id: ExtensionId,
  correlatedPrompts: Schema.optionalKey(Schema.Literal("note")),
  attach: Schema.optionalKey(
    Schema.declare<NonNullable<ClientEntryV1["attach"]>>(
      (value): value is NonNullable<ClientEntryV1["attach"]> =>
        Predicate.isFunction(value),
    ),
  ),
  contribute: Schema.optionalKey(
    Schema.declare<NonNullable<ClientEntryV1["contribute"]>>(
      (value): value is NonNullable<ClientEntryV1["contribute"]> =>
        Predicate.isFunction(value),
    ),
  ),
}).check(
  Schema.makeFilter(
    (entry) => entry.attach !== undefined || entry.contribute !== undefined,
    { message: "Client entry requires attach or contribute" },
  ),
);
const decodeEntry = Schema.decodeUnknownSync(entrySchema, {
  onExcessProperty: "error",
});
/** Admit exactly V1; installed executable code is not sandboxed. */
export const parseClientEntryV1 = decodeEntry;
/** One typed author seam, usable with ordinary async attachment code. */
export function defineClientEntry<C extends CommandsV0, P, PI>(
  contract: ExtensionContractV0<C, P, PI>,
  client: Readonly<{
    correlatedPrompts?: ClientEntryV1["correlatedPrompts"];
    attach?: (
      context: ClientExtensionContextV1,
    ) => void | Promise<void> | Effect.Effect<void, Error, Scope.Scope>;
    contribute?: (view: ExtensionViewV0<P>) => ClientContributionV1;
  }>,
): ClientEntryV1 {
  if (client.contribute !== undefined && contract.projection === null)
    throw new Error("Client contribution requires a projection");
  const entry = {
    protocol: "shiitake.client/v1",
    id: contract.id,
  } satisfies Pick<ClientEntryV1, "protocol" | "id">;
  if (client.correlatedPrompts !== undefined)
    Object.assign(entry, { correlatedPrompts: client.correlatedPrompts });
  const attach = client.attach;
  const contribute = client.contribute;
  if (attach !== undefined)
    Object.assign(entry, {
      attach: (context: ClientExtensionContextV1) =>
        invokeExtensionHook(() => attach(context)),
    });
  if (contribute !== undefined)
    Object.assign(entry, {
      contribute: (slot: ExtensionSlotV0 | undefined) =>
        contribute(viewExtension(contract, slot)),
    });
  return Object.freeze(parseClientEntryV1(entry));
}
