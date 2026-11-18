import { expectTypeOf, it } from "vitest";

import type { RedactedOutput } from "../src/internal/redaction.js";
import type {
  PersistableSessionEntry,
  StoredSessionEntry,
} from "../src/internal/woodstock/session-entry.js";
import type {
  AppendCompactionInput,
  CompactionCheckpointInput,
  CompleteSessionLeafInput,
} from "../src/internal/woodstock/session-store.js";

type Entry<Kind extends StoredSessionEntry["kind"]> = Extract<
  StoredSessionEntry,
  { kind: Kind }
>;

/**
 * The store write is synchronous and the Vault matcher is not, so the type is
 * the guard: output reaches SQLite only after the matcher redacted it.
 * (`tsc` checks these; the runtime assertion is empty.)
 */
it("refuses unredacted output at every synchronous store write, by type", () => {
  expectTypeOf<Entry<"message">>().not.toExtend<PersistableSessionEntry>();
  expectTypeOf<
    Entry<"message_start">
  >().not.toExtend<PersistableSessionEntry>();
  expectTypeOf<Entry<"model_prompt">>().not.toExtend<PersistableSessionEntry>();
  expectTypeOf<Entry<"tool_intent">>().not.toExtend<PersistableSessionEntry>();
  expectTypeOf<Entry<"tool_outcome">>().not.toExtend<PersistableSessionEntry>();
  expectTypeOf<Entry<"compaction">>().not.toExtend<PersistableSessionEntry>();
  expectTypeOf<
    Entry<"compaction_checkpoint">
  >().not.toExtend<PersistableSessionEntry>();
  expectTypeOf<
    Entry<"sandbox_failure">
  >().not.toExtend<PersistableSessionEntry>();
  expectTypeOf<Entry<"message">>().not.toExtend<
    CompleteSessionLeafInput["entry"]
  >();
  expectTypeOf<string>().not.toExtend<AppendCompactionInput["summary"]>();
  expectTypeOf<string>().not.toExtend<CompactionCheckpointInput["summary"]>();

  // Redacted output, and intake or lifecycle facts with no output, are written.
  expectTypeOf<
    RedactedOutput<Entry<"tool_outcome">>
  >().toExtend<PersistableSessionEntry>();
  expectTypeOf<Entry<"command">>().toExtend<PersistableSessionEntry>();
  expectTypeOf<Entry<"run_terminal">>().toExtend<PersistableSessionEntry>();
});
