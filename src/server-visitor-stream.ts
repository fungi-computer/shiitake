import type { SessionSnapshot, SessionWatch } from "./client.js";
import { SSEStreamingApi } from "hono/streaming";
import { visitorSnapshot } from "./client-visitor.js";

export type VisitorStreamError =
  | "channel_visitor_stream_expiry_failed"
  | "channel_visitor_stream_close_failed"
  | "channel_visitor_stream_heartbeat_failed"
  | "channel_visitor_stream_failed";

export function visitorStream(
  options: Readonly<{
    watch: SessionWatch;
    visitorId: string;
    filter: (snapshot: SessionSnapshot) => SessionSnapshot;
    expiresAt: number;
    now: () => number;
    release: () => void;
    canRead: () => Promise<boolean>;
    read: () => Promise<SessionSnapshot>;
    onError: (event: VisitorStreamError, cause: unknown) => void;
  }>,
): ReadableStream<Uint8Array> {
  const { readable, writable } = new TransformStream<Uint8Array>();
  const stream = new SSEStreamingApi(writable, readable);
  let closing: Promise<void> | undefined;
  let ending = false;
  let heartbeat: ReturnType<typeof setTimeout> | undefined;
  const expiry = setTimeout(
    () => {
      void close().catch((cause: unknown) => {
        options.onError("channel_visitor_stream_expiry_failed", cause);
      });
    },
    Math.max(0, options.expiresAt - options.now()),
  );
  const close = (): Promise<void> => {
    if (closing !== undefined) return closing;
    ending = true;
    clearTimeout(expiry);
    clearTimeout(heartbeat);
    options.release();
    closing = Promise.resolve().then(async () => {
      stream.abort();
      try {
        await options.watch.close();
      } finally {
        await stream.close();
      }
    });
    return closing;
  };
  stream.onAbort(() => {
    void close().catch((cause: unknown) => {
      options.onError("channel_visitor_stream_close_failed", cause);
    });
  });
  const admitted = async () =>
    !ending &&
    options.now() < options.expiresAt &&
    (await options.canRead()) &&
    !ending;
  const beat = async () => {
    if (!(await admitted())) {
      await close();
      return;
    }
    await stream.write(": heartbeat\n\n");
    if (!ending)
      heartbeat = setTimeout(() => {
        void beat().catch((cause: unknown) => {
          options.onError("channel_visitor_stream_heartbeat_failed", cause);
          void close().catch((closeCause: unknown) => {
            options.onError("channel_visitor_stream_close_failed", closeCause);
          });
        });
      }, 30_000);
  };
  const publish = async (snapshot: SessionSnapshot, progressOnly: boolean) => {
    if (!(await admitted())) return false;
    const baseline = visitorSnapshot(
      options.filter(snapshot),
      options.visitorId,
    );
    await stream.writeSSE(
      progressOnly
        ? {
            event: "frame",
            data: JSON.stringify({
              kind: "progress",
              progress: baseline.progress,
              activity: baseline.activity,
            }),
          }
        : { event: "session", data: JSON.stringify(baseline) },
    );
    return !ending;
  };
  const pump = async () => {
    try {
      let snapshot = options.watch.snapshot;
      if (!(await publish(snapshot, false))) return;
      heartbeat = setTimeout(() => {
        void beat().catch((cause: unknown) => {
          options.onError("channel_visitor_stream_heartbeat_failed", cause);
          void close().catch((closeCause: unknown) => {
            options.onError("channel_visitor_stream_close_failed", closeCause);
          });
        });
      }, 30_000);
      for await (const frame of options.watch.changes) {
        if (ending) break;
        if (frame.kind === "tool_output") continue;
        snapshot =
          frame.kind === "progress"
            ? {
                ...snapshot,
                progress: frame.progress,
                activity: frame.activity,
              }
            : frame.kind === "activity"
              ? { ...snapshot, activity: frame.activity }
              : frame.kind === "reset"
                ? frame.baseline
                : await options.read();
        if (!(await publish(snapshot, frame.kind === "progress"))) break;
      }
    } finally {
      await close();
    }
  };
  void pump().catch((cause: unknown) => {
    options.onError("channel_visitor_stream_failed", cause);
  });
  return stream.responseReadable.pipeThrough(
    new TransformStream<Uint8Array, Uint8Array>(),
  );
}
