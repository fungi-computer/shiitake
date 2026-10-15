import type { WatchdogQueueHead } from "@fungi.computer/watchdog/effect";

import type {
  AgentRunningWork,
  SessionActivity,
  SessionProgress,
} from "../client.js";
import type { SqliteOwner } from "./contracts.js";
import {
  readProcessProgressForWork,
  readProcessCompaction,
  readLatestLiveRetryForWork,
  readActiveToolForWork,
  readLastModelChoiceSequence,
} from "./woodstock/index.js";

/** Public activity facts plus the process-local progress read with them. */
export type ObservedActivity = Readonly<{
  facts: SessionActivity;
  progress: SessionProgress | null;
}>;

/** Join Watchdog authority with Woodstock facts and process-local progress. */
export function readSessionActivity(
  owner: SqliteOwner,
  sessionId: string,
  head: WatchdogQueueHead,
  runningWork: AgentRunningWork | null,
): ObservedActivity {
  const lastModelChoiceSequence = readLastModelChoiceSequence(owner, sessionId);
  if (head.state === "idle")
    return {
      facts: {
        queue: { state: "idle" },
        cancellationRequested: false,
        retry: null,
        activeTool: null,
        lastModelChoiceSequence,
        compaction: null,
      },
      progress: null,
    };
  if (head.state === "queued")
    // The queue job id is the admitted command id: the one identity a person
    // may cancel while this Session's work waits for the Agent.
    return {
      facts: {
        queue: { state: "queued", commandId: head.jobId, runningWork },
        cancellationRequested: head.cancellationRequested,
        retry: null,
        activeTool: null,
        lastModelChoiceSequence,
        compaction: null,
      },
      progress: null,
    };

  const latestRetry = readLatestLiveRetryForWork(owner, sessionId, head.jobId);
  const retry =
    latestRetry?.event.type === "auto_retry_start"
      ? {
          attempt: latestRetry.event.attempt,
          maxAttempts: latestRetry.event.maxAttempts,
          delayMs: latestRetry.event.delayMs,
          errorMessage: latestRetry.event.errorMessage,
          scheduledAt: latestRetry.timestamp,
        }
      : null;
  return {
    facts: {
      queue: { state: "running", commandId: head.jobId },
      cancellationRequested: head.cancellationRequested,
      retry,
      activeTool: readActiveToolForWork(owner, sessionId, head.jobId) ?? null,
      lastModelChoiceSequence,
      compaction: readProcessCompaction(owner, sessionId),
    },
    progress: readProcessProgressForWork(owner, sessionId, head.jobId),
  };
}
