import type * as Schema from "effect/Schema";

/** Which source value a new Session starts with. */
export type DocumentForkPolicyV0 =
  "inherit-at-fork-point" | "current" | "fresh";

/** A typed JSON document, bounded by its encoded UTF-8 size. */
export type DocumentSpecV0<A, I = A> = Readonly<{
  schema: Schema.Codec<A, I>;
  initial: A;
  fork: DocumentForkPolicyV0;
  maxBytes: number;
}>;

/** Existential declarations; each initial value is decoded by its own schema. */
export type DocumentsV0 = Readonly<
  Record<
    string,
    Readonly<{
      schema: Schema.Codec<unknown>;
      initial: unknown;
      fork: DocumentForkPolicyV0;
      maxBytes: number;
    }>
  >
>;

/** Both operations require the declaring extension's active core transaction. */
export type DocumentHandleV0<A> = Readonly<{
  /** Read the value at the Session’s selected transcript leaf, or its initial value. */
  read: (sessionId: string) => A;
  /** Validate and append a new snapshot atomically with the enclosing transaction. */
  update: (sessionId: string, change: (value: A) => A) => void;
}>;

/** Handles preserve the decoded type of each declared document. */
export type DocumentHandlesV0<D extends DocumentsV0> = {
  readonly [K in keyof D]: DocumentHandleV0<D[K]["schema"]["Type"]>;
};
