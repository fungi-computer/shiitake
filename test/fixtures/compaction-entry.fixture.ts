import type { Usage } from "@earendil-works/pi-ai";

import { compactionSummaryMessage } from "../../src/internal/compaction/utils.js";
import type {
  CompactionTrigger,
  StoredSessionEntry,
} from "../../src/internal/woodstock/session-entry.js";

/** A compaction entry in Pi's shape: `head`, the model's summary, `data`. */
export function compactionEntryFixture(
  input: Readonly<{
    workId: string;
    head: string;
    summary: string;
    usage: Usage;
    contextRevision: string;
    reason?: CompactionTrigger;
    resumesRun?: boolean;
    tokensBefore?: number;
    tokensAfter?: number;
    timestamp?: number;
  }>,
): Extract<StoredSessionEntry, { kind: "compaction" }> {
  return {
    kind: "compaction",
    workId: input.workId,
    head: input.head,
    model: [compactionSummaryMessage(input.summary, input.timestamp ?? 0)],
    data: {
      reason: input.reason ?? "threshold",
      resumesRun: input.resumesRun ?? false,
      tokensBefore: input.tokensBefore ?? 100,
      tokensAfter: input.tokensAfter ?? 40,
      usage: input.usage,
      contextRevision: input.contextRevision,
    },
  };
}
