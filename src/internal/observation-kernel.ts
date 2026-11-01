import * as Effect from "effect/Effect";
import type * as Cause from "effect/Cause";
import * as Pull from "effect/Pull";
import * as Queue from "effect/Queue";

import type { SessionFrame } from "../client.js";
import { ShiitakeError } from "../errors.js";
import type { SqliteOwner } from "./contracts.js";
import { reportCatchFailure } from "./failure.js";

const MAX_OBSERVATIONS = 64;
const MAX_PENDING_HINTS = 256;

const encodedFrames = new WeakMap<
  SessionFrame,
  Readonly<{ data: string; bytes: number }>
>();
const frameEncoder = new TextEncoder();

/** The feed shares one frame's JSON and byte accounting across its watchers. */
export function encodeSessionFrame(
  frame: SessionFrame,
): Readonly<{ data: string; bytes: number }> {
  const cached = encodedFrames.get(frame);
  if (cached !== undefined) return cached;
  const data = JSON.stringify(frame);
  const encoded = Object.freeze({
    data,
    bytes: frameEncoder.encode(data).byteLength,
  });
  encodedFrames.set(frame, encoded);
  return encoded;
}

/** One watcher has its own bounded backlog, not its own projection producer. */
export type SessionFrameQueue = Readonly<{
  take: Effect.Effect<SessionFrame | "reset" | "closed", ShiitakeError>;
  /** Record the join before reading; retain later frames for cursor deduplication. */
  checkpoint: (revision?: number) => void;
  close: () => void;
}>;

type FrameListener = {
  frames: Queue.Queue<SessionFrame | "reset", Cause.Done>;
  bytes: number;
  lagged: boolean;
  closed: boolean;
  observed: number;
  includeSandboxFailures: boolean;
};

function withoutSandboxFailures(frame: SessionFrame): SessionFrame {
  if (frame.kind === "update" && frame.sandboxFailures !== undefined) {
    const value = { ...frame };
    delete value.sandboxFailures;
    return value;
  }
  if (frame.kind === "reset" && frame.baseline.sandboxFailures !== undefined) {
    const baseline = { ...frame.baseline };
    delete baseline.sandboxFailures;
    return { ...frame, baseline };
  }
  return frame;
}

/** Bounded dropping fan-out; Woodstock, never this queue, owns replay. */
export function openSessionFrameFeed(onIdle: () => void) {
  const listeners = new Set<FrameListener>();
  let ended = false;
  let failure: ShiitakeError | undefined;
  const clear = (listener: FrameListener): void => {
    Effect.runSync(Queue.clear(listener.frames));
    listener.bytes = 0;
  };
  return {
    register(includeSandboxFailures = false): SessionFrameQueue {
      const listener: FrameListener = {
        frames: Effect.runSync(
          Queue.dropping<SessionFrame | "reset", Cause.Done>(256),
        ),
        bytes: 0,
        lagged: false,
        closed: false,
        observed: -1,
        includeSandboxFailures,
      };
      listeners.add(listener);
      const take: SessionFrameQueue["take"] = Effect.suspend(() => {
        if (listener.closed) return Effect.succeed("closed" as const);
        if (failure !== undefined) return Effect.fail(failure);
        if (listener.lagged) return Effect.succeed("reset" as const);
        if (ended) return Effect.succeed("closed" as const);
        return Queue.take(listener.frames).pipe(
          Pull.catchDone(() => Effect.succeed("closed" as const)),
          Effect.flatMap((frame) => {
            if (failure !== undefined) return Effect.fail(failure);
            if (frame !== "reset" && frame !== "closed")
              listener.bytes -= encodeSessionFrame(frame).bytes;
            return Effect.succeed(frame);
          }),
        );
      });
      return Object.freeze({
        take,
        checkpoint: (revision = -1) => {
          clear(listener);
          listener.lagged = false;
          listener.observed = revision;
        },
        close: () => {
          if (listener.closed) return;
          listener.closed = true;
          clear(listener);
          listeners.delete(listener);
          Queue.endUnsafe(listener.frames);
          if (listeners.size === 0) onIdle();
        },
      });
    },
    publish(frame: SessionFrame, revision = Infinity): void {
      if (ended) return;
      const publicFrame = withoutSandboxFailures(frame);
      for (const listener of listeners) {
        if (listener.closed || listener.lagged || revision <= listener.observed)
          continue;
        const value = listener.includeSandboxFailures ? frame : publicFrame;
        const { bytes } = encodeSessionFrame(value);
        if (
          listener.bytes + bytes > 1_048_576 ||
          !Queue.offerUnsafe(listener.frames, value)
        ) {
          clear(listener);
          // Keep exactly one reset obligation; no incoming backlog can grow it.
          listener.lagged = true;
          Queue.offerUnsafe(listener.frames, "reset");
        } else {
          listener.bytes += bytes;
        }
      }
    },
    end(error?: ShiitakeError): void {
      if (ended) return;
      ended = true;
      failure = error;
      for (const listener of listeners) {
        clear(listener);
        listener.lagged = false;
        Queue.endUnsafe(listener.frames);
      }
    },
  };
}

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
  revision: number;
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
  /** Owner-hint revision for the snapshot/live join, never a wire cursor. */
  revision: number;
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
  if (listener.closed) return;
  listener.revision += 1;
  if (listener.reset) return;
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
          revision: 0,
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
            get revision() {
              return listener.revision;
            },
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
