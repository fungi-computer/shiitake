import type { SimpleStreamOptions } from "@fungi.computer/mule/effect";
import * as Schema from "effect/Schema";

const identifier = Schema.String.pipe(
  Schema.check(Schema.isMinLength(1)),
  Schema.check(Schema.isMaxLength(256)),
);
/** The thinking levels a model choice may name, shared by every model path. */
export const reasoningLevels = [
  "minimal",
  "low",
  "medium",
  "high",
  "xhigh",
  "max",
] as const satisfies readonly NonNullable<SimpleStreamOptions["reasoning"]>[];

/** Secret-free request to the host's maintained model catalogue. */
export const ModelPreferenceSchema = Schema.Struct({
  providerId: identifier,
  modelId: identifier,
  reasoning: Schema.optional(Schema.Literals(reasoningLevels)),
});

/** Null at the Session boundary means inherit the Agent default. */
export type ModelPreference = Schema.Schema.Type<typeof ModelPreferenceSchema>;

/** Decode the bounded client preference; availability remains model-owner policy. */
export const parseModelPreference = Schema.decodeUnknownSync(
  Schema.NullOr(ModelPreferenceSchema),
);

/** Decode the serialized preference with the same maintained schema. */
export const parseModelPreferenceJson = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.NullOr(ModelPreferenceSchema)),
);
