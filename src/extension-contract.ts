import * as Schema from "effect/Schema";

/** Host extension protocol; client-entry protocol is versioned independently. */
export type ShiitakeExtensionProtocolV0 = "shiitake.extension/v0";

/** ASCII extension identity shared by the host definition and its clients. */
export const extensionIdentifier = Schema.String.pipe(
  Schema.check(Schema.isMinLength(1)),
  Schema.check(Schema.isMaxLength(128)),
  Schema.check(Schema.isPattern(/^[a-z][a-z0-9]*(?:[-_.][a-z0-9]+)*$/u)),
);
/** SemVer spelling used by host admission. */
export const extensionVersion = Schema.String.pipe(
  Schema.check(Schema.isMinLength(1)),
  Schema.check(Schema.isMaxLength(64)),
  Schema.check(
    Schema.isPattern(
      /^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/u,
    ),
  ),
);

/** ASCII extension identity shared by the host definition and its clients. */
export const ExtensionId = extensionIdentifier.pipe(
  Schema.brand("ExtensionId"),
);
/** ASCII extension identity shared by the server definition and its clients. */
export type ExtensionId = Schema.Schema.Type<typeof ExtensionId>;
/** Core digest of one normalized extension definition. */
export const extensionRevisionSchema = Schema.String.pipe(
  Schema.check(Schema.isPattern(/^sha256:[0-9a-f]{64}$/u)),
  Schema.brand("ExtensionRevision"),
);
/** Core digest of one revision and its sorted granted capabilities. */
export const extensionFingerprintSchema = Schema.String.pipe(
  Schema.check(Schema.isPattern(/^sha256:[0-9a-f]{64}$/u)),
  Schema.brand("ExtensionFingerprint"),
);
/** Closed, versioned, feature-free capability vocabulary. */
export type ExtensionCapabilityV0 =
  /** Namespaced schema-typed durable records. */
  | "state"
  /** Namespaced durable requests for human attention. */
  | "attention"
  /** Exactly-once delivery of settled runs from a core-owned cursor. */
  | "settled"
  /** The one accepted-command door. */
  | "send"
  /** Durable per-extension wake timers. */
  | "timers"
  /** Session- and agent-scope commands over the generic call surface. */
  | "commands"
  /** One encoded slot in every Session snapshot and frame. */
  | "projection"
  /** A Mycelium module attached to runs this extension participates in. */
  | "run_tools"
  /** Exclusive Knapsack Files tools captured once per run. */
  | "files"
  /** Named system-prompt sections, evaluated before each model request. */
  | "prompt"
  /** The single exclusive compaction handoff slot. */
  | "compaction_handoff";

/** Static identity and requested capabilities of one v0 extension. */
export type ExtensionManifestV0 = Readonly<{
  protocol: ShiitakeExtensionProtocolV0;
  id: ExtensionId;
  /** Semver; bump when behaviour changes outside config and schemas. */
  version: string;
  requests: readonly ExtensionCapabilityV0[];
}>;

/** One command's scope and its encoded input and output schemas. */
export type CommandSpecV0<
  S extends "session" | "agent",
  I,
  II,
  O,
  OI,
> = Readonly<{
  scope: S;
  input: Schema.Codec<I, II>;
  output: Schema.Codec<O, OI>;
}>;
/** Existential command spec. */
export type AnyCommandSpecV0 = Readonly<{
  scope: "session" | "agent";
  input: Schema.Codec<unknown>;
  output: Schema.Codec<unknown>;
}>;
/** Commands declared by one extension contract. */
export type CommandsV0 = Readonly<Record<string, AnyCommandSpecV0>>;
/** The encoded Session projection slot. */
export type ProjectionSpecV0<P, PI> = Readonly<{
  schema: Schema.Codec<P, PI>;
}>;

/** Shared by the host definition and the client binding: one schema set. */
export type ExtensionContractV0<C extends CommandsV0, P, PI> = Readonly<{
  id: ExtensionId;
  commands: C;
  projection: ProjectionSpecV0<P, PI> | null;
}>;

/** Activity already observed by the client, never an execution authority. */
export type ClientActivityV0 = "idle" | "working" | "blocked" | "unknown";
