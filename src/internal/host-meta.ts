import * as Predicate from "effect/Predicate";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";

import { ShiitakeError } from "../errors.js";
import type { Json } from "../extensions.js";
import { JsonValueSchema } from "./json-value.js";

/** The most encoded JSON one message's host metadata may occupy. */
const MAX_HOST_META_BYTES = 4096;

const encoder = new TextEncoder();

/**
 * Host metadata as stored: any finite JSON value within the byte cap.
 * Shiitake stores and returns it and never branches on it.
 */
export const HostMetaSchema: Schema.Codec<Json> = JsonValueSchema.check(
  Schema.makeFilter(
    (value) =>
      encoder.encode(JSON.stringify(value)).byteLength <= MAX_HOST_META_BYTES,
  ),
);

/**
 * Any Standard Schema, such as a Zod 4 schema, whose `validate` settles
 * synchronously. Shiitake only asks whether a value is accepted.
 */
export type StandardHostMetaSchema = Readonly<{
  "~standard": Readonly<{
    /** Accept or refuse one value; must settle synchronously for Shiitake. */
    validate: (
      value: Json,
    ) => StandardHostMetaResult | PromiseLike<StandardHostMetaResult>;
  }>;
}>;

/** One Standard Schema validation outcome; only acceptance matters here. */
export type StandardHostMetaResult =
  | Readonly<{ issues?: undefined }>
  | Readonly<{ issues: readonly Readonly<{ message: string }>[] }>;

/**
 * The one schema a host supplies at Agent creation for the metadata it
 * attaches to prompts and operator lines: an Effect schema whose encoded side
 * is JSON, or a synchronous Standard Schema. Shiitake checks each value
 * against it before writing; it never reads the decoded form.
 */
export type HostMetaOptions = Readonly<{
  schema: Schema.Codec<unknown, Json> | StandardHostMetaSchema;
}>;

const decodeStoredMeta = Schema.decodeUnknownResult(HostMetaSchema);

/** Whether the host's schema accepts one stored value. */
function hostAccepts(schema: HostMetaOptions["schema"], meta: Json): boolean {
  if (Schema.isSchema(schema))
    return Result.isSuccess(Schema.decodeUnknownResult(schema)(meta));
  const result = schema["~standard"].validate(meta);
  if (Predicate.hasProperty(result, "then"))
    throw new ShiitakeError(
      "contract",
      "invalid",
      "Host metadata schema must validate synchronously",
    );
  return result.issues === undefined;
}

/**
 * Check one host-attached value: finite JSON within the cap, and accepted by
 * the host's schema when it supplied one. Fails as an invalid contract.
 */
export function checkHostMeta(
  options: HostMetaOptions | undefined,
  meta: Json,
): Json {
  const stored = decodeStoredMeta(meta);
  if (Result.isFailure(stored))
    throw new ShiitakeError(
      "contract",
      "invalid",
      "Host metadata is not JSON within 4096 bytes",
    );
  if (options !== undefined && !hostAccepts(options.schema, stored.success))
    throw new ShiitakeError(
      "contract",
      "invalid",
      "Host metadata does not match the host's schema",
    );
  return stored.success;
}

/** Whether two stored metadata values are the same canonical JSON. */
export function sameHostMeta(
  left: Json | undefined,
  right: Json | undefined,
): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}
