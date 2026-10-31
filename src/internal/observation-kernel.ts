import * as Effect from "effect/Effect";

import { ShiitakeError } from "../errors.js";
import type { SqliteOwner } from "./contracts.js";
import { reportCatchFailure } from "./failure.js";

const MAX_OBSERVATIONS = 64;
const MAX_PENDING_HINTS = 256;

/** Finite process-local signal; durable owner state remains authoritative. */
export type ObservationSignal =
  "closed" | "reset" | "durable" | "activity" | "progress";

type Wake = (signal: ObservationSignal) => void;
export type ObservationReason =
  "durable" | "activity" | "progress" | "settlement";
type ObservationHint = Exclude<ObservationReason, "settlement">;
type OwnerPublication =
  | Readonly<{
      kind: "session";
      sessionId: string | null;
      reason: ObservationReason;
    }>
  | Readonly<{ kind: "agent-execution" }>;
type OwnerPublisher = Readonly<{
  publish: (publication: OwnerPublication) => void;
  observes: (sessionId: string) => boolean;
}>;
type Listener = {
  readonly sessionId: string;
  readonly settlementOnly: boolean;
  closed: boolean;
  pendingCount: number;
  pendingReason: ObservationHint | undefined;
  queued: boolean;
  reset: boolean;
  wake: Wake | undefined;
};

const ownerPublishers = new WeakMap<SqliteOwner, Set<OwnerPublisher>>();

/** Actual live-view demand, excluding exact-command settlement waiters. */
export function hasOwnerObservers(
  store: SqliteOwner,
  sessionId: string,
): boolean {
  for (const publisher of ownerPublishers.get(store) ?? [])
    if (publisher.observes(sessionId)) return true;
  return false;
}

/** One scoped process-local observation registration. */
export type ObservationRegistration = Readonly<{
  /** Inspect and consume one pending finite signal without waiting. */
  pollSignal: Effect.Effect<ObservationSignal | undefined>;
  /** Await and consume one finite observation signal. */
  awaitSignal: Effect.Effect<ObservationSignal>;
  /** Release this registration without affecting durable work. */
  close: () => void;
  /** Track whether this Session's current queue head is waiting on the Agent. */
  setQueued: (queued: boolean) => void;
}>;

/** Process-local wake coordination; durable facts remain authoritative. */
export type ObservationKernel = Readonly<{
  /** Register before the caller's first durable read. */
  register: (
    sessionId: string,
    interest?: "settlement",
  ) => Effect.Effect<ObservationRegistration, ShiitakeError>;
  /** Close every registration and release this agent's owner publisher. */
  close: Effect.Effect<void>;
}>;

/** Combine coalesced reasons without allowing a progress hint to hide a commit. */
export function mergeObservationSignals(
  first: ObservationSignal,
  second: ObservationSignal,
): ObservationSignal {
  if (first === "closed" || second === "closed") return "closed";
  if (first === "reset" || second === "reset") return "reset";
  if (first === "durable" || second === "durable") return "durable";
  if (first === "activity" || second === "activity") return "activity";
  return "progress";
}

function queueObservation(listener: Listener, reason: ObservationHint): void {
  if (listener.pendingCount >= MAX_PENDING_HINTS) {
    listener.pendingCount = 0;
    listener.pendingReason = undefined;
    listener.reset = true;
    return;
  }
  listener.pendingCount += 1;
  listener.pendingReason =
    listener.pendingReason === "durable" || reason === "durable"
      ? "durable"
      : listener.pendingReason === "activity" || reason === "activity"
        ? "activity"
        : "progress";
}

function deliverObservation(listener: Listener, reason: ObservationHint): void {
  if (listener.closed || listener.reset) return;
  const wake = listener.wake;
  if (wake === undefined) {
    queueObservation(listener, reason);
    return;
  }
  listener.wake = undefined;
  try {
    wake(reason);
  } catch (cause) {
    reportCatchFailure("observation-delivery", cause);
    listener.pendingCount = 0;
    listener.pendingReason = undefined;
    listener.reset = true;
  }
}

/** Queued views reconcile occupancy without replaying another Session's facts. */
function notifyQueuedViews(
  listeners: Iterable<Listener>,
  sourceSessionId: string | null,
): void {
  for (const listener of listeners)
    if (
      listener.queued &&
      !listener.settlementOnly &&
      listener.sessionId !== sourceSessionId
    )
      deliverObservation(listener, "activity");
}

/** Notify observers of one Session's durable or process-local change. */
export function publishOwnerObservation(
  store: SqliteOwner,
  sessionId: string | null,
  reason: ObservationReason = "durable",
): void {
  const publishers = ownerPublishers.get(store);
  if (publishers === undefined) return;
  for (const publisher of publishers) {
    try {
      publisher.publish({ kind: "session", sessionId, reason });
    } catch (cause) {
      reportCatchFailure("owner-observation-publish", cause);
      continue;
    }
  }
}

/** Wake queued Sessions when this Agent's execution slot changes. */
export function publishAgentExecutionChange(store: SqliteOwner): void {
  const publishers = ownerPublishers.get(store);
  if (publishers === undefined) return;
  for (const publisher of publishers) {
    try {
      publisher.publish({ kind: "agent-execution" });
    } catch (cause) {
      reportCatchFailure("owner-execution-change-publish", cause);
    }
  }
}

/** Open one coalescing process-local observation kernel per agent. */
export function openObservationKernel(store: SqliteOwner): ObservationKernel {
  const sessions = new Map<string, Set<Listener>>();
  const active = new Set<Listener>();
  const publishers = ownerPublishers.get(store) ?? new Set<OwnerPublisher>();
  let released = false;

  const takeSignal = (listener: Listener): ObservationSignal | undefined => {
    if (listener.closed) return "closed";
    if (listener.reset) {
      listener.reset = false;
      return "reset";
    }
    if (listener.pendingCount === 0) return undefined;
    listener.pendingCount = 0;
    const reason = listener.pendingReason;
    listener.pendingReason = undefined;
    return reason ?? "progress";
  };

  const closeListener = (listener: Listener): void => {
    if (listener.closed) return;
    listener.closed = true;
    listener.pendingCount = 0;
    listener.pendingReason = undefined;
    listener.reset = false;
    const wake = listener.wake;
    listener.wake = undefined;
    active.delete(listener);
    const listeners = sessions.get(listener.sessionId);
    listeners?.delete(listener);
    if (listeners?.size === 0) sessions.delete(listener.sessionId);
    if (wake !== undefined) {
      try {
        wake("closed");
      } catch (cause) {
        reportCatchFailure("observation-listener-close", cause);
      }
    }
  };

  const publish = (publication: OwnerPublication): void => {
    if (publication.kind === "agent-execution") {
      notifyQueuedViews(active, null);
      return;
    }
    // Reuse bounded activity hints for the occupant's changing phase.
    if (publication.sessionId !== null && publication.reason !== "settlement")
      notifyQueuedViews(active, publication.sessionId);
    const listeners =
      publication.sessionId === null
        ? active
        : sessions.get(publication.sessionId);
    if (listeners === undefined) return;
    for (const listener of listeners) {
      if (listener.settlementOnly && publication.reason !== "settlement")
        continue;
      deliverObservation(
        listener,
        publication.reason === "settlement" ? "durable" : publication.reason,
      );
    }
  };
  const publisher: OwnerPublisher = {
    publish,
    observes: (sessionId) =>
      [...(sessions.get(sessionId) ?? [])].some(
        (listener) => !listener.closed && !listener.settlementOnly,
      ),
  };
  publishers.add(publisher);
  ownerPublishers.set(store, publishers);

  return Object.freeze({
    register: (sessionId, interest) =>
      Effect.suspend(() => {
        if (released)
          return Effect.fail(
            new ShiitakeError("runtime", "closed", "Shiitake is closed"),
          );
        if (active.size >= MAX_OBSERVATIONS)
          return Effect.fail(
            new ShiitakeError(
              "runtime",
              "rejected",
              "Observation capacity is exhausted",
            ),
          );

        const listener: Listener = {
          sessionId,
          settlementOnly: interest === "settlement",
          closed: false,
          pendingCount: 0,
          pendingReason: undefined,
          queued: true,
          reset: false,
          wake: undefined,
        };
        const listeners = sessions.get(sessionId) ?? new Set<Listener>();
        listeners.add(listener);
        sessions.set(sessionId, listeners);
        active.add(listener);

        const pollSignal = Effect.sync(() => takeSignal(listener));
        const awaitSignal = Effect.callback<ObservationSignal>((resume) => {
          const signal = takeSignal(listener);
          if (signal !== undefined) {
            resume(Effect.succeed(signal));
            return;
          }
          const wake = (next: ObservationSignal): void => {
            resume(Effect.succeed(next));
          };
          listener.wake = wake;
          return Effect.sync(() => {
            if (listener.wake === wake) listener.wake = undefined;
          });
        });

        return Effect.succeed(
          Object.freeze({
            pollSignal,
            awaitSignal,
            close: () => {
              closeListener(listener);
            },
            setQueued: (queued) => {
              if (!listener.closed) listener.queued = queued;
            },
          }),
        );
      }),
    close: Effect.sync(() => {
      if (released) return;
      released = true;
      publishers.delete(publisher);
      if (publishers.size === 0) ownerPublishers.delete(store);
      for (const listener of active) closeListener(listener);
    }),
  });
}
