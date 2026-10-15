import * as Base64Url from "effect/encoding/Base64Url";
import * as Schema from "effect/Schema";

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const canonicalBase64Url = /^[A-Za-z0-9_-]+$/u;
const CursorValueSchema = Schema.Struct({
  v: Schema.Literal(1),
  generation: Schema.String.pipe(
    Schema.check(Schema.isMinLength(1)),
    Schema.check(Schema.isMaxLength(128)),
    Schema.check(
      Schema.makeFilter((value) => encoder.encode(value).byteLength <= 128),
    ),
  ),
  sequence: Schema.Int.pipe(
    Schema.check(Schema.isGreaterThan(0)),
    Schema.check(Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)),
  ),
});
const closed = { onExcessProperty: "error" } as const;
const decodeCursorValue = Schema.decodeUnknownResult(CursorValueSchema, closed);
const decodeCursorJson = Schema.decodeUnknownResult(
  Schema.fromJsonString(CursorValueSchema),
  closed,
);

/** Private durable position carried opaquely by the public cursor. */
export type WoodstockCursorPosition = Readonly<{
  generation: string;
  sequence: number;
}>;

/** Encode one validated durable position in the canonical public wire form. */
export function encodeWoodstockCursor(
  position: WoodstockCursorPosition,
): string | undefined {
  const decoded = decodeCursorValue({ v: 1, ...position });
  if (decoded._tag === "Failure") return undefined;
  const json = JSON.stringify(decoded.success);
  if (encoder.encode(json).byteLength > 384) return undefined;
  const encoded = Base64Url.encode(json);
  return encoder.encode(encoded).byteLength <= 512 ? encoded : undefined;
}

/** Decode only the strict canonical cursor spelling emitted by Woodstock. */
export function decodeWoodstockCursor(
  encoded: string,
): WoodstockCursorPosition | undefined {
  if (
    encoder.encode(encoded).byteLength > 512 ||
    !canonicalBase64Url.test(encoded)
  )
    return undefined;
  const bytes = Base64Url.decode(encoded);
  if (bytes._tag === "Failure" || bytes.success.byteLength > 384)
    return undefined;
  const json = decoder.decode(bytes.success);
  const decoded = decodeCursorJson(json);
  if (decoded._tag === "Failure") return undefined;
  const canonical = JSON.stringify(decoded.success);
  if (canonical !== json || Base64Url.encode(canonical) !== encoded)
    return undefined;
  return Object.freeze({
    generation: decoded.success.generation,
    sequence: decoded.success.sequence,
  });
}
