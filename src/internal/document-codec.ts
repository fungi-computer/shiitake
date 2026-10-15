import * as Schema from "effect/Schema";
import * as SchemaRepresentation from "effect/SchemaRepresentation";
import { ExtensionStateError } from "../extensions.js";
import type { DocumentsV0, DocumentSpecV0 } from "../session-documents.js";
import { JsonValueSchema } from "./json-value.js";

const Declaration = Schema.Struct({
  fork: Schema.Literals(["inherit-at-fork-point", "current", "fresh"]),
  maxBytes: Schema.Int.pipe(
    Schema.check(Schema.isGreaterThan(0)),
    Schema.check(Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)),
  ),
});

/** The declared schema owns typed values; the JSON decoder owns their encoding. */
export function encodeDocument<A, I>(
  spec: DocumentSpecV0<A, I>,
  value: A,
): string {
  const result = Schema.encodeUnknownResult(spec.schema)(value);
  if (result._tag === "Failure")
    throw new ExtensionStateError({
      reason: "encode",
      message: "Session document does not match its schema",
    });
  const json = Schema.decodeUnknownResult(JsonValueSchema)(result.success);
  if (json._tag === "Failure")
    throw new ExtensionStateError({
      reason: "encode",
      message: "Session document must encode finite JSON",
    });
  const text = JSON.stringify(json.success);
  if (new TextEncoder().encode(text).byteLength > spec.maxBytes)
    throw new ExtensionStateError({
      reason: "bound",
      message: "Session document exceeds maxBytes",
    });
  return text;
}

/** Validate policy, bound and initial state before admitting any extension. */
export function documentDescriptor(spec: DocumentsV0[string]) {
  const declaration = Schema.decodeUnknownSync(Declaration)(spec);
  return {
    ...declaration,
    schema: SchemaRepresentation.toJson(Schema.toRepresentation(spec.schema)),
    initial: encodeDocument(spec, spec.initial),
  };
}
