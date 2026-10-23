import type {
  MuleResume,
  MuleToolCheckpoint,
  ToolResultMessage,
} from "@fungi.computer/mule/effect";
import type { StoredSessionRow } from "./session-store.js";

/** Derive continuation from facts, without introducing a second checkpoint store. */
export function recoverAssistantTurn(
  rows: readonly StoredSessionRow[],
): MuleResume | undefined {
  const assistant = [...rows]
    .reverse()
    .find(
      ({ entry }) =>
        entry.kind === "message" && entry.value.role === "assistant",
    );
  if (
    assistant?.entry.kind !== "message" ||
    assistant.entry.value.role !== "assistant"
  )
    return undefined;
  const after = rows.filter(({ sequence }) => sequence > assistant.sequence);
  // A later request was already dispatched. Resend that request from the
  // completed transcript, not the previous assistant turn.
  if (
    after.some(
      ({ entry }) =>
        entry.kind === "turn_started" ||
        (entry.kind === "compaction" && entry.data.resumesRun) ||
        (entry.kind === "live_retry" &&
          entry.event.type === "auto_retry_start"),
    )
  )
    return undefined;
  const toolResults: ToolResultMessage[] = after.flatMap(({ entry }) =>
    entry.kind === "message" && entry.value.role === "toolResult"
      ? [entry.value]
      : [],
  );
  const tools: MuleToolCheckpoint[] =
    assistant.entry.value.content.flatMap<MuleToolCheckpoint>((call) => {
      if (call.type !== "toolCall") return [];
      const outcome = [...after]
        .reverse()
        .find(
          ({ entry }) =>
            entry.kind === "tool_outcome" && entry.call.id === call.id,
        )?.entry;
      if (outcome?.kind === "tool_outcome")
        return [
          {
            call,
            state: "completed",
            result:
              outcome.data === undefined
                ? outcome.result
                : { ...outcome.result, data: outcome.data },
            message: toolResults.find(
              (result) => result.toolCallId === call.id,
            ),
          },
        ];
      const intent = [...after]
        .reverse()
        .find(
          ({ entry }) =>
            entry.kind === "tool_intent" && entry.call.id === call.id,
        )?.entry;
      return [
        intent?.kind === "tool_intent"
          ? { call, state: "started", replay: intent.replay }
          : { call, state: "pending" },
      ];
    });
  return { message: assistant.entry.value, tools };
}

/** One visibility rule for model context and compaction, including recovered runs. */
export function contextMessageIds(
  path: readonly StoredSessionRow[],
  activeWorkId?: string,
): ReadonlySet<string> {
  const terminalWork = new Set(
    path.flatMap(({ entry }) =>
      entry.kind === "run_terminal" ? [entry.workId] : [],
    ),
  );
  const messages = new Set<string>();
  const lastWorkAssistant = new Map<string, string>();
  for (const row of path) {
    const entry = row.entry;
    if (entry.kind === "message") {
      if (entry.value.role === "assistant") {
        lastWorkAssistant.set(entry.workId, row.id);
      }
      if (terminalWork.has(entry.workId) || entry.workId === activeWorkId)
        messages.add(row.id);
    }
    if (
      (entry.kind === "compaction" && entry.data.resumesRun) ||
      (entry.kind === "live_retry" && entry.event.type === "auto_retry_start")
    ) {
      const failed = lastWorkAssistant.get(entry.workId);
      if (failed !== undefined) messages.delete(failed);
    }
  }
  return messages;
}

/** Returned assistant failures still pass through compaction; abnormal exits do not. */
export function abnormalTerminal(
  rows: readonly StoredSessionRow[],
): "failed" | "cancelled" | undefined {
  const reversed = [...rows].reverse();
  const terminal = reversed.find(({ entry }) => entry.kind === "run_terminal");
  if (terminal?.entry.kind !== "run_terminal") return undefined;
  if (
    rows.some(
      ({ sequence, entry }) =>
        sequence > terminal.sequence &&
        (entry.kind === "run_started" ||
          (entry.kind === "compaction" && entry.data.resumesRun)),
    )
  )
    return undefined;
  if (terminal.entry.outcome === "cancelled") return "cancelled";
  if (terminal.entry.outcome !== "failed") return undefined;
  const turn = reversed.find(({ entry }) => entry.kind === "turn_started");
  const assistant = reversed.find(
    ({ sequence, entry }) =>
      entry.kind === "message" &&
      entry.value.role === "assistant" &&
      sequence > (turn?.sequence ?? 0),
  );
  return assistant?.entry.kind === "message" &&
    assistant.entry.value.role === "assistant" &&
    assistant.entry.value.stopReason === "error" &&
    !rows.some(
      ({ sequence, entry }) =>
        sequence > assistant.sequence &&
        entry.kind === "live_retry" &&
        entry.event.type === "auto_retry_start",
    )
    ? undefined
    : "failed";
}
