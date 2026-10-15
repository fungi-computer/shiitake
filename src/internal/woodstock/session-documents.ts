import * as Schema from "effect/Schema";
import type { SqliteOwner } from "../contracts.js";
import type { DocumentForkPolicyV0 } from "../../session-documents.js";
import type { StoredSessionEntry } from "./session-entry.js";
import { ShiitakeError } from "../../errors.js";

const SpecRow = Schema.Struct({
  extension_id: Schema.String,
  name: Schema.String,
  initial_json: Schema.String,
  fork: Schema.Literals(["inherit-at-fork-point", "current", "fresh"]),
  max_bytes: Schema.Int.pipe(Schema.check(Schema.isGreaterThan(0))),
});
export type StoredDocumentSpec = Readonly<{
  extensionId: string;
  name: string;
  initialJson: string;
  fork: DocumentForkPolicyV0;
  maxBytes: number;
}>;

/** Policy is durable too, so a fork never depends on a process-local extension. */
export function prepareSessionDocuments(owner: SqliteOwner): void {
  owner.sql.exec(`CREATE TABLE IF NOT EXISTS session_document_specs (
    extension_id TEXT NOT NULL, name TEXT NOT NULL, initial_json TEXT NOT NULL,
    fork TEXT NOT NULL, max_bytes INTEGER NOT NULL,
    PRIMARY KEY (extension_id, name)
  )`);
}

/** Activation commits a validated declaration; it does not rewrite document history. */
export function declareSessionDocument(
  owner: SqliteOwner,
  spec: StoredDocumentSpec,
): void {
  owner.sql.exec(
    `INSERT INTO session_document_specs
    (extension_id, name, initial_json, fork, max_bytes) VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(extension_id, name) DO UPDATE SET initial_json = excluded.initial_json,
      fork = excluded.fork, max_bytes = excluded.max_bytes`,
    spec.extensionId,
    spec.name,
    spec.initialJson,
    spec.fork,
    spec.maxBytes,
  );
}

/** Baselines appended after the copied history in the same fork transaction. */
export function forkDocumentEntries(
  owner: SqliteOwner,
  /** Latest value on the source's current lineage, not a global last write. */
  currentValue: (extensionId: string, name: string) => string | undefined,
): readonly StoredSessionEntry[] {
  const specs = owner.sql
    .exec(
      "SELECT extension_id, name, initial_json, fork, max_bytes FROM session_document_specs ORDER BY extension_id, name",
    )
    .toArray();
  return specs.flatMap((raw) => {
    const spec = Schema.decodeUnknownSync(SpecRow)(raw);
    if (spec.fork === "inherit-at-fork-point") return [];
    const valueJson =
      spec.fork === "fresh"
        ? spec.initial_json
        : (currentValue(spec.extension_id, spec.name) ?? spec.initial_json);
    if (new TextEncoder().encode(valueJson).byteLength > spec.max_bytes)
      throw new ShiitakeError(
        "store",
        "rejected",
        "Session document exceeds its declared size bound",
      );
    return [
      {
        kind: "document" as const,
        workId: null,
        extensionId: spec.extension_id,
        name: spec.name,
        valueJson,
      },
    ];
  });
}
