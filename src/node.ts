import {
  closeSync,
  constants,
  fchmodSync,
  fstatSync,
  mkdirSync,
  openSync,
  realpathSync,
} from "node:fs";
import { basename, dirname, join, resolve } from "node:path";
import { DatabaseSync } from "node:sqlite";
import * as Effect from "effect/Effect";
import { fromNodeSqlite } from "@fungi.computer/watchdog/node-sqlite";
import type { Hono } from "hono";

import {
  createShiitakeHttpRoutes,
  type ShiitakeHttpEnv,
} from "./http-routes.js";
import { Shiitake } from "./index.js";
import type {
  RuntimeHost,
  ShiitakeOptions,
  ShiitakeOwnerAgent,
} from "./internal/contracts.js";

/** A retry hint after a failed or still-pending reconciliation pass. */
const retryAfterMs = 5_000;

type BoundRuntime = Awaited<ReturnType<RuntimeHost["bind"]>>;

/** The event-loop runtime host one Node process gives one durable Agent. */
export type NodeRuntimeHost = Readonly<{
  /** Pass as `runtime` to `Shiitake.make`. */
  runtime: RuntimeHost;
  /** Ask for a reconciliation at an epoch-millisecond time; null clears it. */
  scheduleWake: (at: number | null) => void;
  /** Stop waking and await the reconciliation in flight. */
  close: () => Promise<void>;
}>;

/**
 * The Node event loop as Shiitake's runtime host: one reconciliation at a
 * time, re-run when woken during a pass, and a timer for the earliest due
 * retry. It has no queue policy; Watchdog owns execution and retries.
 * `observeFailure` sees a failed pass, which is retried after five seconds;
 * if it throws, the throw is logged and the retry still happens.
 */
export function nodeRuntimeHost(
  observeFailure: (cause: unknown) => void,
): NodeRuntimeHost {
  // A throwing observer must never cost the retry or crash the process.
  const reportFailure = (cause: unknown): void => {
    try {
      observeFailure(cause);
    } catch (observerCause) {
      Effect.runFork(
        Effect.logError("shiitake_failure_observer_threw", {
          cause: observerCause,
          failure: String(cause),
        }),
      );
    }
  };
  let bound: BoundRuntime | undefined;
  let wakeTimer: NodeJS.Timeout | undefined;
  let wakeAt: number | undefined;
  let closed = false;
  const scheduleWake = (at: number | null): void => {
    if (closed) return;
    if (at === null) {
      if (wakeTimer !== undefined) clearTimeout(wakeTimer);
      wakeTimer = undefined;
      wakeAt = undefined;
      return;
    }
    // Keep the earliest retry hint; later requests must not postpone it.
    if (wakeTimer !== undefined && wakeAt !== undefined && at >= wakeAt) return;
    if (wakeTimer !== undefined) clearTimeout(wakeTimer);
    wakeAt = at;
    wakeTimer = setTimeout(
      () => {
        wakeTimer = undefined;
        wakeAt = undefined;
        void bound?.requestWake().catch(reportFailure);
      },
      Math.max(0, at - Date.now()),
    );
  };
  const runtime: RuntimeHost = Object.freeze({
    bind: ({ reconcile, pendingWork }: Parameters<RuntimeHost["bind"]>[0]) => {
      if (bound !== undefined)
        return Promise.reject(
          new Error("Shiitake Node runtime is already bound"),
        );
      let active: Promise<void> | undefined;
      let pending: NodeJS.Immediate | undefined;
      let rerun = false;
      let closing: Promise<void> | undefined;

      const run = (): void => {
        if (active !== undefined) return;
        const task = Promise.resolve().then(async () => {
          try {
            await reconcile();
            const work = await pendingWork();
            scheduleWake(
              work.pending ? Date.now() + retryAfterMs : work.nextWakeAt,
            );
          } catch (cause) {
            reportFailure(cause);
            scheduleWake(Date.now() + retryAfterMs);
          } finally {
            if (active === task) active = undefined;
            if (rerun) {
              rerun = false;
              // A wake admitted before close still drains its one rerun.
              if (closed) run();
              else schedule();
            }
          }
        });
        active = task;
      };

      const schedule = (): void => {
        if (closed || active !== undefined || pending !== undefined) return;
        pending = setImmediate(() => {
          pending = undefined;
          if (!closed) run();
        });
      };

      const handle: BoundRuntime = Object.freeze({
        commit: (mutation) => mutation(),
        requestWake: () => {
          if (closed)
            return Promise.reject(new Error("Shiitake Node runtime is closed"));
          if (active !== undefined) rerun = true;
          else schedule();
          return Promise.resolve();
        },
        close: () => {
          closing ??= (async () => {
            closed = true;
            if (wakeTimer !== undefined) clearTimeout(wakeTimer);
            wakeTimer = undefined;
            wakeAt = undefined;
            if (pending !== undefined) clearImmediate(pending);
            pending = undefined;
            while (active !== undefined) await active;
          })();
          return closing;
        },
      });

      schedule();
      bound = handle;
      return Promise.resolve(handle);
    },
  });
  return Object.freeze({
    runtime,
    scheduleWake,
    close: () => bound?.close() ?? Promise.resolve(),
  });
}

/** An exclusive, process-held claim on one store file. */
export type NodeStoreLease = Readonly<{
  /** End the claim; later calls do nothing. */
  release: () => void;
}>;

/**
 * Claim one store for this process: an exclusive transaction held open on a
 * sidecar file, so a second owner of the same store is refused at once and
 * the claim ends with the process, never with a missed heartbeat.
 */
export function acquireNodeStoreLease(sidecarPath: string): NodeStoreLease {
  const database = new DatabaseSync(sidecarPath, { timeout: 0 });
  try {
    database.exec("PRAGMA busy_timeout = 0");
    database.exec("PRAGMA journal_mode = DELETE");
    database.exec("BEGIN EXCLUSIVE");
  } catch (cause) {
    database.close();
    throw cause;
  }
  let released = false;
  return Object.freeze({
    release: () => {
      if (released) return;
      released = true;
      try {
        database.exec("ROLLBACK");
      } finally {
        database.close();
      }
    },
  });
}

/**
 * Create or open one store file for this process alone: never through a
 * symbolic link, never a file with a second hard link (either would let a
 * second owner claim the same bytes under another name), and readable by
 * this user only, whatever the umask.
 */
function claimPrivateFile(path: string): void {
  const descriptor = openSync(
    path,
    constants.O_RDWR | constants.O_CREAT | constants.O_NOFOLLOW,
    0o600,
  );
  try {
    const metadata = fstatSync(descriptor);
    if (!metadata.isFile() || metadata.nlink !== 1)
      throw new Error(
        `Shiitake Node store must be one regular file with one name: ${path}`,
      );
    fchmodSync(descriptor, 0o600);
  } finally {
    closeSync(descriptor);
  }
}

/**
 * The one name every opener of a store agrees on: its real directory (created
 * private when missing) and its own file name, so a linked directory still
 * meets the same lease.
 */
function canonicalStorePath(path: string): string {
  const directory = dirname(resolve(path));
  mkdirSync(directory, { recursive: true, mode: 0o700 });
  return join(realpathSync(directory), basename(path));
}

/** Everything a Node host supplies except the store and runtime it owns. */
export type NodeShiitakeOptions = Omit<ShiitakeOptions, "store" | "runtime"> &
  Readonly<{
    /**
     * The SQLite file this Agent lives in; created when missing, private to
     * this user. It may not be a symbolic link or have another hard link.
     */
    path: string;
    /**
     * Observe a failed reconciliation pass; it is retried after 5 seconds.
     * Absent, the failure goes to Effect's logger.
     */
    onRuntimeFailure?: (cause: unknown) => void;
  }>;

/** One durable Agent in a SQLite file, owned by this Node process. */
export type NodeShiitake = Readonly<{
  /** The in-process owner: sends, metadata views and grant deliveries. */
  agent: ShiitakeOwnerAgent;
  /**
   * Shiitake's Session routes as a Hono app. Serve it with any Node or Bun
   * server, behind host middleware that alone sets `shiitakeMeta`.
   */
  routes: Hono<ShiitakeHttpEnv>;
  /** Stop waking, close the Agent, then release the store. */
  close: () => Promise<void>;
}>;

/**
 * Open one durable Agent in a SQLite file on disk. The process holds an
 * exclusive lease on the store until `close`, so a second opener is refused.
 * The store and its lease file are private to this user (0600), and a store
 * reached through a symbolic link or with a second hard link is refused.
 * Accepted work survives a restart: reopening the file resumes it.
 */
export async function openNodeShiitake(
  options: NodeShiitakeOptions,
): Promise<NodeShiitake> {
  const { path: requested, onRuntimeFailure, ...shiitakeOptions } = options;
  const path = canonicalStorePath(requested);
  const sidecarPath = `${path}.owner.sqlite`;
  claimPrivateFile(sidecarPath);
  const lease = acquireNodeStoreLease(sidecarPath);
  let database: DatabaseSync | undefined;
  let host: NodeRuntimeHost | undefined;
  try {
    // Checked under the lease: a link made after this is refused by the next
    // opener's own check, before it can open the bytes.
    claimPrivateFile(path);
    const opened = new DatabaseSync(path, { timeout: 0 });
    database = opened;
    opened.exec("PRAGMA foreign_keys = ON");
    const runtime = nodeRuntimeHost(
      onRuntimeFailure ??
        ((cause: unknown) => {
          Effect.runFork(
            Effect.logError("shiitake_reconciliation_failed").pipe(
              Effect.annotateLogs({ cause: String(cause) }),
            ),
          );
        }),
    );
    host = runtime;
    const agent = await Shiitake.make({
      ...shiitakeOptions,
      store: { ...fromNodeSqlite(opened), storeName: `Store file ${path}` },
      runtime: runtime.runtime,
    });
    const routes = createShiitakeHttpRoutes(
      (sessionId, meta) =>
        (meta === undefined ? agent : agent.withMeta(meta)).session(sessionId),
      agent.listSessions,
      agent.createSession,
      agent.deleteSession,
      () => agent.extensions,
    );
    let closing: Promise<void> | undefined;
    return Object.freeze({
      agent,
      routes,
      close: () => {
        closing ??= (async () => {
          try {
            await agent.close();
            await runtime.close();
          } finally {
            opened.close();
            lease.release();
          }
        })();
        return closing;
      },
    });
  } catch (cause) {
    await host?.close();
    database?.close();
    lease.release();
    throw cause;
  }
}
