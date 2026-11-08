import type { Message, MuleError } from "@fungi.computer/mule/effect";
import * as Effect from "effect/Effect";

import { ShiitakeError } from "../errors.js";
import type { SqliteOwner } from "./contracts.js";
import {
  readSettlementContent,
  readWorkSender,
  type PreparedSessionRun,
} from "./woodstock/index.js";

/**
 * One reply opportunity for this accepted work, shared across compaction
 * attempts. Admission owns sender provenance: person-typed work has no sender,
 * while delegated mail and extension continuations retain theirs.
 */
export function openReplyOwed(
  owner: SqliteOwner,
  prepared: PreparedSessionRun,
): () => Effect.Effect<readonly Message[], MuleError> {
  let asked = false;
  return () =>
    Effect.gen(function* () {
      if (asked) return [];
      const sender = yield* Effect.try({
        try: () =>
          readWorkSender(
            owner,
            prepared.command.sessionId,
            prepared.command.entry.workId,
          ),
        catch: () =>
          new ShiitakeError(
            "store",
            "unavailable",
            "Work sender is unavailable",
          ),
      });
      if (sender === undefined) return [];
      const content = yield* readSettlementContent(owner, {
        sessionId: prepared.command.sessionId,
        commandFactId: prepared.command.id,
      });
      if (content !== "") return [];
      asked = true;
      return [
        {
          role: "user" as const,
          content: `Your turn ended without a reply to ${sender.name}. Reply now with your result in at most 5 lines.`,
          timestamp: Date.now(),
        },
      ];
    }).pipe(
      Effect.mapError((): MuleError => ({
        _tag: "MuleError",
        phase: "control",
        message: "Delegated reply state is unavailable",
      })),
    );
}
