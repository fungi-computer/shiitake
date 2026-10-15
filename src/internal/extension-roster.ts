import type { DocumentsV0 } from "../session-documents.js";
import { documentDescriptor } from "./document-codec.js";
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as SchemaRepresentation from "effect/SchemaRepresentation";

import { ShiitakeError } from "../errors.js";
import type {
  AnyExtensionDefinitionV0,
  ExtensionCapabilityV0,
  ExtensionFingerprint,
  ExtensionId,
  ExtensionManifestV0,
  ExtensionRosterV0,
  ExtensionRevision,
  Json,
} from "../extensions.js";
import { readDefinition } from "./extension-definitions.js";
import {
  extensionFingerprintSchema,
  extensionIdentifier,
  extensionRevisionSchema,
  extensionVersion,
} from "../extension-contract.js";

/** One validated roster member with its core-derived identity. */
export type AdmittedExtensionV0 = Readonly<{
  id: ExtensionId;
  manifest: ExtensionManifestV0;
  definition: AnyExtensionDefinitionV0;
  documents: DocumentsV0;
  grants: ReadonlySet<ExtensionCapabilityV0>;
  revision: ExtensionRevision;
  fingerprint: ExtensionFingerprint;
}>;

const maximumRoster = 64;
const Capability = Schema.Literals([
  "state",
  "attention",
  "settled",
  "send",
  "timers",
  "commands",
  "projection",
  "run_tools",
  "files",
  "prompt",
  "compaction_handoff",
]);
const Capabilities = Schema.Array(Capability).pipe(
  Schema.check(Schema.isMaxLength(10)),
);
const ManifestSchema = Schema.Struct({
  protocol: Schema.Literal("shiitake.extension/v0"),
  id: extensionIdentifier,
  version: extensionVersion,
  requests: Capabilities,
});
const GrantsSchema = Schema.Array(
  Schema.Struct({
    id: extensionIdentifier,
    capabilities: Capabilities,
    grantKey: Schema.optionalKey(extensionIdentifier),
  }),
).pipe(Schema.check(Schema.isMaxLength(maximumRoster)));
const CollectionName = Schema.String.pipe(
  Schema.check(Schema.isPattern(/^[a-z][A-Za-z0-9_]{0,63}$/u)),
);
const decodeManifest = Schema.decodeUnknownResult(ManifestSchema, {
  onExcessProperty: "error",
});
const decodeGrants = Schema.decodeUnknownResult(GrantsSchema, {
  onExcessProperty: "error",
});
const decodeCollectionName = Schema.decodeUnknownResult(CollectionName);

function invalid(message: string): ShiitakeError {
  return new ShiitakeError("extensions", "invalid", message);
}

function hasDuplicates(values: readonly string[]): boolean {
  return new Set(values).size !== values.length;
}

/** JSON with recursively sorted object keys, so equal values hash equally. */
function canonicalJson(value: RevisionDescriptor | Json): string {
  return JSON.stringify(value, (_key: string, nested: Json) =>
    Predicate.isObject(nested)
      ? Object.fromEntries(
          Object.entries(nested).sort(([left], [right]) =>
            left < right ? -1 : left > right ? 1 : 0,
          ),
        )
      : nested,
  );
}

/** Hex SHA-256 of UTF-8 text. */
function sha256Hex(value: string): Effect.Effect<string, ShiitakeError> {
  return Effect.tryPromise({
    try: async () => {
      const digest = await crypto.subtle.digest(
        "SHA-256",
        new TextEncoder().encode(value),
      );
      return Array.from(new Uint8Array(digest), (byte) =>
        byte.toString(16).padStart(2, "0"),
      ).join("");
    },
    catch: () =>
      new ShiitakeError("extensions", "rejected", "Extension digest failed"),
  });
}

/** The hashed description of one definition. */
type RevisionDescriptor = ReturnType<typeof revisionDescriptor>;

/**
 * Everything the revision covers: manifest, declared schemas and the hashed
 * config. Functions (keys, migrations, hooks) are covered by the version law.
 */
function revisionDescriptor(
  definition: AnyExtensionDefinitionV0,
  documents: DocumentsV0,
) {
  const { manifest, contract, state, config, catalog } = definition;
  const descriptor = {
    manifest: {
      protocol: manifest.protocol,
      id: manifest.id,
      version: manifest.version,
      requests: [...manifest.requests].sort(),
    },
    commands: Object.fromEntries(
      Object.entries(contract.commands).map(([name, spec]) => [
        name,
        {
          scope: spec.scope,
          input: SchemaRepresentation.toJson(
            Schema.toRepresentation(spec.input),
          ),
          output: SchemaRepresentation.toJson(
            Schema.toRepresentation(spec.output),
          ),
        },
      ]),
    ),
    projection:
      contract.projection === null
        ? null
        : SchemaRepresentation.toJson(
            Schema.toRepresentation(contract.projection.schema),
          ),
    state: {
      version: state.version,
      collections: Object.fromEntries(
        Object.entries(state.collections).map(([name, spec]) => [
          name,
          SchemaRepresentation.toJson(Schema.toRepresentation(spec.schema)),
        ]),
      ),
    },
    config,
  };
  // An extension without a catalog keeps the revision it had before catalogs.
  const withDocuments =
    Object.keys(documents).length === 0
      ? descriptor
      : {
          ...descriptor,
          documents: Object.fromEntries(
            Object.entries(documents).map(([name, spec]) => [
              name,
              documentDescriptor(spec),
            ]),
          ),
        };
  return catalog === undefined ? withDocuments : { ...withDocuments, catalog };
}

function validateDefinition(
  definition: AnyExtensionDefinitionV0,
): ExtensionManifestV0 {
  const decoded = decodeManifest(definition.manifest);
  if (decoded._tag === "Failure")
    throw invalid("Extension manifest is invalid");
  const manifest = definition.manifest;
  if (hasDuplicates(manifest.requests))
    throw invalid("Extension capability requests are invalid");
  if (definition.contract.id !== manifest.id)
    throw invalid("Extension contract identity does not match its manifest");
  if (
    !Number.isSafeInteger(definition.state.version) ||
    definition.state.version < 1
  )
    throw invalid("Extension state version is invalid");
  for (const name of Object.keys(definition.state.collections))
    if (
      name === "update" ||
      name === "read" ||
      name === "observe" ||
      decodeCollectionName(name)._tag === "Failure"
    )
      throw invalid("Extension collection name is invalid");
  if (
    definition.catalog !== undefined &&
    !manifest.requests.includes("commands")
  )
    throw invalid("An extension catalog requires the commands capability");
  return manifest;
}

/** Validate a host roster and grants, then derive each revision and fingerprint. */
export function admitExtensionRosterV0(
  input: ExtensionRosterV0 | undefined,
): Effect.Effect<readonly AdmittedExtensionV0[], ShiitakeError> {
  return Effect.gen(function* () {
    const roster = input?.roster ?? [];
    const grantRows = input?.grants ?? [];
    const staged = yield* Effect.try({
      try: () => {
        if (roster.length > maximumRoster)
          throw invalid("Extension roster is invalid");
        const definitions = roster.map((entry) => {
          const definition = readDefinition(entry);
          if (definition === undefined)
            throw invalid("Extension roster entry was not defined");
          const documents = definition.documents ?? {};
          for (const [name, spec] of Object.entries(documents)) {
            if (decodeCollectionName(name)._tag === "Failure")
              throw invalid("Session document name is invalid");
            documentDescriptor(spec);
          }
          const manifest = validateDefinition(definition);
          if (
            Object.keys(documents).length > 0 &&
            !manifest.requests.includes("state")
          )
            throw invalid("Session documents require the state capability");
          return { definition, documents, manifest };
        });
        const ids = definitions.map(({ manifest }) => manifest.id);
        if (hasDuplicates(ids))
          throw new ShiitakeError(
            "extensions",
            "conflict",
            "Extension identities conflict",
          );
        const grants = decodeGrants(grantRows);
        if (grants._tag === "Failure")
          throw invalid("Extension grants are invalid");
        if (hasDuplicates(grants.success.map(({ id }) => id)))
          throw invalid("Extension grant identities are invalid");
        const grantsById = new Map<string, readonly ExtensionCapabilityV0[]>();
        for (const grant of grants.success) {
          const member = definitions.find(
            ({ manifest }) => manifest.id === grant.id,
          );
          if (
            member === undefined ||
            hasDuplicates(grant.capabilities) ||
            grant.capabilities.some(
              (capability) => !member.manifest.requests.includes(capability),
            )
          )
            throw invalid("Extension grant is invalid");
          grantsById.set(grant.id, grant.capabilities);
        }
        const handoffOwners = definitions.filter(({ manifest }) =>
          grantsById.get(manifest.id)?.includes("compaction_handoff"),
        );
        if (handoffOwners.length > 1)
          throw new ShiitakeError(
            "extensions",
            "conflict",
            "Compaction handoff slot is already owned",
          );
        const filesOwners = definitions.filter(({ manifest }) =>
          grantsById.get(manifest.id)?.includes("files"),
        );
        if (filesOwners.length > 1)
          throw new ShiitakeError(
            "extensions",
            "conflict",
            "Files slot is already owned",
          );
        return definitions
          .map((member) => ({
            ...member,
            grants: grantsById.get(member.manifest.id) ?? [],
          }))
          .sort((left, right) =>
            left.manifest.id < right.manifest.id
              ? -1
              : left.manifest.id > right.manifest.id
                ? 1
                : 0,
          );
      },
      catch: (error) =>
        error instanceof ShiitakeError
          ? error
          : invalid("Extension roster is invalid"),
    });
    return yield* Effect.forEach(staged, (member) =>
      Effect.gen(function* () {
        const descriptor = yield* Effect.try({
          try: () =>
            canonicalJson(
              revisionDescriptor(member.definition, member.documents),
            ),
          catch: () => invalid("Extension schemas cannot be described"),
        });
        const revision = `sha256:${yield* sha256Hex(descriptor)}`;
        const granted = [...member.grants].sort();
        const fingerprint = `sha256:${yield* sha256Hex(
          canonicalJson([revision, granted]),
        )}`;
        return Object.freeze({
          id: member.manifest.id,
          manifest: member.manifest,
          definition: member.definition,
          documents: member.documents,
          grants: new Set(granted),
          revision: Schema.decodeUnknownSync(extensionRevisionSchema)(revision),
          fingerprint: Schema.decodeUnknownSync(extensionFingerprintSchema)(
            fingerprint,
          ),
        });
      }),
    );
  });
}

/**
 * Hex digest of the core run protocol a run is admitted under, plus the
 * captured Files direct-tool names. It changes when core's
 * run-contract semantics change or the Files tool set changes, never with a
 * roster: the branded Files names are captured separately from fingerprints.
 */
export function coreProtocolDigest(
  fileToolNames: readonly string[],
): Effect.Effect<string, ShiitakeError> {
  return sha256Hex(
    canonicalJson({
      protocol: "shiitake.extension/v0",
      runContract: 1,
      fileTools: [...fileToolNames].sort(),
    }),
  );
}
