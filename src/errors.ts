import * as Schema from "effect/Schema";

/** Stable boundary that rejected a Shiitake operation. */
const ShiitakeErrorBoundarySchema = Schema.Literals([
  "contract",
  "extensions",
  "instructions",
  "model",
  "runtime",
  "store",
  "tools",
]);

/** Stable boundary that rejected a Shiitake operation. */
export type ShiitakeErrorBoundary = Schema.Schema.Type<
  typeof ShiitakeErrorBoundarySchema
>;

/** Safe failure category exposed by both Shiitake facades. */
const ShiitakeErrorCauseSchema = Schema.Literals([
  "closed",
  "conflict",
  "incompatible",
  "invalid",
  "rejected",
  "unavailable",
  /** Owner storage is near its budget; new Sessions and prompts wait. */
  "storage_pressure",
  /** One model context read is larger than the owner can safely decode. */
  "context_too_large",
]);
const requestIdSchema = Schema.String.pipe(
  Schema.check(Schema.isMinLength(1)),
  Schema.check(Schema.isMaxLength(256)),
);

/** Safe failure category exposed by both Shiitake facades. */
export type ShiitakeErrorCause = Schema.Schema.Type<
  typeof ShiitakeErrorCauseSchema
>;

/** Canonical serialized Shiitake failure. */
export const ShiitakeErrorWireSchema = Schema.Struct({
  _tag: Schema.Literal("ShiitakeError"),
  boundary: ShiitakeErrorBoundarySchema,
  cause: ShiitakeErrorCauseSchema,
  message: Schema.String,
  requestId: Schema.optionalKey(requestIdSchema),
});

/** Canonical serialized Shiitake failure. */
export type ShiitakeErrorWire = Schema.Schema.Type<
  typeof ShiitakeErrorWireSchema
>;

/** Sanitized failure from a parsed Shiitake boundary. */
export class ShiitakeError extends Error {
  /** Stable runtime tag for typed Shiitake failures. */
  public readonly _tag = "ShiitakeError" as const;

  /** Safe category describing why it was rejected. */
  public override readonly cause: ShiitakeErrorCause;

  /** Original cause retained only for server-side diagnosis. */
  public readonly underlying?: unknown;

  /** Construct a sanitized boundary failure. */
  public constructor(
    /** Boundary that rejected the operation. */
    public readonly boundary: ShiitakeErrorBoundary,
    /** Safe category describing why it was rejected. */
    category: ShiitakeErrorCause,
    /** Human-readable message safe for callers to display. */
    message: string,
    /** Request identity recoverable when a send may have reached the owner. */
    public readonly requestId?: string,
    /**
     * The original thrown value, kept for server-side diagnosis only. It is
     * never serialized: the wire encoder copies named fields only.
     */
    cause?: unknown,
  ) {
    super(message);
    this.name = "ShiitakeError";
    this.cause = category;
    this.underlying = cause;
  }
}

/** Sanitize one caught host failure for Shiitake's public wire. */
export function toShiitakeErrorWire(cause: unknown): ShiitakeErrorWire {
  const failure =
    cause instanceof ShiitakeError
      ? cause
      : new ShiitakeError(
          "runtime",
          "unavailable",
          "Shiitake operation failed",
        );
  const wire = {
    _tag: failure._tag,
    boundary: failure.boundary,
    cause: failure.cause,
    message: failure.message,
  };
  if (failure.requestId !== undefined)
    Object.assign(wire, { requestId: failure.requestId });
  return Object.freeze(wire);
}
