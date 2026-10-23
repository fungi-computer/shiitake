import * as JsonSchema from "effect/JsonSchema";
import * as Schema from "effect/Schema";

import {
  ModelPreferenceSchema,
  reasoningLevels,
  parseModelPreference,
  parseModelPreferenceJson,
} from "./internal/model-preference.js";
import type { ModelPreference } from "./internal/model-preference.js";

export {
  ModelPreferenceSchema,
  reasoningLevels as modelReasoningLevels,
  parseModelPreference,
  parseModelPreferenceJson,
};
export type { ModelPreference } from "./internal/model-preference.js";

/** JSON Schema generated from Shiitake's maintained model preference schema. */
export const modelPreferenceJsonSchema = JsonSchema.toDocumentDraft07(
  Schema.toJsonSchemaDocument(ModelPreferenceSchema, {
    onExcessProperty: "error",
  }),
).schema;

const modelAddressSchema = Schema.String.pipe(
  Schema.check(Schema.isPattern(/^[^/\s]+\/[^\s]+$/u)),
  Schema.check(Schema.isMaxLength(513)),
);

/** Parse the footer's provider/model spelling; model ids may contain slashes. */
export function parseModelAddress(input: unknown): ModelPreference {
  const address = Schema.decodeUnknownSync(modelAddressSchema)(input);
  const slash = address.indexOf("/");
  return Schema.decodeUnknownSync(ModelPreferenceSchema)({
    providerId: address.slice(0, slash),
    modelId: address.slice(slash + 1),
  });
}

/**
 * Resolve host-owned defaults for a future run. Hosts supply offered, logged-in
 * availability; an explicit choice never silently falls through on auth failure.
 */
export function resolveModelPreference(
  layers: Readonly<{
    session?: ModelPreference | null | undefined;
    agent?: ModelPreference | null | undefined;
    manual?: ModelPreference | null | undefined;
    team?: ModelPreference | null | undefined;
    available?: ModelPreference | null | undefined;
  }>,
): ModelPreference | null {
  return (
    layers.session ??
    layers.agent ??
    layers.manual ??
    layers.team ??
    layers.available ??
    null
  );
}
