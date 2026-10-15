import * as Schema from "effect/Schema";

const identity = Schema.NonEmptyString.check(Schema.isMaxLength(256));
const instant = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

/** An extension-owned request for human attention; expiry is owned by its timer. */
export const AttentionItemSchema = Schema.Struct({
  id: identity,
  kind: Schema.NonEmptyString.check(Schema.isMaxLength(64)),
  summary: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  since: instant,
  expiresAt: Schema.optional(instant),
});

/** Input to the granted attention door, scoped to the calling extension. */
export type AttentionItem = typeof AttentionItemSchema.Type;

/** Public identity includes the owner so two extensions can use the same id. */
export const SessionAttentionSchema = AttentionItemSchema.mapFields(
  (fields) => ({
    ...fields,
    extensionId: identity,
  }),
);

/** One open attention item in a Session observation. */
export type SessionAttention = typeof SessionAttentionSchema.Type;

/** Private resource ceiling for a complete, bounded Session observation. */
export const maximumSessionAttention = 64;

/** Private outstanding-resource ceiling per extension, released when items clear. */
export const maximumExtensionAttention = 4096;

/** All open items, ordered by extension and id; cleared only by their owner. */
export const SessionAttentionListSchema = Schema.Array(
  SessionAttentionSchema,
).check(Schema.isMaxLength(maximumSessionAttention));
