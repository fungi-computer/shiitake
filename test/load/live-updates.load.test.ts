/**
 * Explicit-only Local load test. Never opens ~/.fungi: each Agent owns a
 * distinct node:sqlite :memory: database. LOAD_* overrides are diagnostic
 * miniatures, NOT acceptance measurements.
 *
 * Retained laws: Botanical 71ccd9e9
 * packages/fungi-computer/do-host/test/commits-bus.test.ts, Tests 30/32/36.
 * The durable owner here is Woodstock, not the old second event-log table.
 * The 0-watcher control records streaming p99, not an absolute S5 pass bar.
 * Watcher p99 must be at most control + 10 ms: S5 targets small added loop
 * lag under fan-out, without judging the host's unloaded timer latency.
 * S5's design-set absolute bars remain on the watcher-load phase only.
 * The control must still have a sampled streaming window and p99 in (0, 50)
 * ms, or the relative comparison has no trustworthy baseline.
 */
import { monitorEventLoopDelay } from "node:perf_hooks";

import {
  createAssistantMessageEventStream,
  type AssistantMessage,
} from "@earendil-works/pi-ai";
import {
  fauxAssistantMessage,
  fauxText,
  fauxThinking,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { directSandbox } from "@fungi.computer/mycelium/testing";
import { Mycelium } from "@fungi.computer/mycelium";
import { fauxModel } from "@fungi.computer/mule/testing";
import type { MuleEvent } from "@fungi.computer/mule";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import { afterEach, expect, it } from "vitest";
import { z } from "zod";

import type { SessionFrame, SqliteOwner } from "../../src/index.js";
import {
  createAgentControlSource,
  type AgentControlDirectory,
} from "../../src/agent-control.js";
import { createShiitakeHttpRoutes } from "../../src/http-routes.js";
import { memoryHost, type MemoryHost } from "../../src/testing.js";
import { createNativeSqliteOwnerFixture } from "../fixtures/native-sqlite-owner.fixture.js";
import { openToolProgress } from "../../src/internal/woodstock/progress.js";
import { decodeWoodstockCursor } from "../../src/internal/woodstock/cursor.js";

const historyTurns = Number(process.env.LOAD_HISTORY_TURNS ?? 150);
const trials = z
  .number()
  .int()
  .min(1)
  .max(3)
  .parse(Number(process.env.LOAD_TRIALS ?? 3));
const thinkingChars = Number(process.env.LOAD_THINKING ?? 200_000);
const textChars = Number(process.env.LOAD_TEXT ?? 50_000);
const chunkChars = Number(process.env.LOAD_CHUNK ?? 8);
const historyReplyChars = Number(process.env.LOAD_HISTORY_REPLY ?? 25_500);
const encoder = new TextEncoder();
const openHosts: (() => Promise<void>)[] = [];

/** Pi's native stream with socket-like event-loop turns between faux chunks. */
function pacedReplyStream(
  message: AssistantMessage,
  onDelta: (last: boolean) => void,
) {
  const reason = message.stopReason;
  if (reason !== "stop")
    throw new Error("Load reply must be a successful faux message");
  const events = createAssistantMessageEventStream();
  void (async () => {
    const partial: AssistantMessage = {
      ...message,
      stopReason: "pending",
      content: [],
    };
    events.push({ type: "start", partial: { ...partial } });
    for (const [contentIndex, block] of message.content.entries()) {
      if (block.type !== "text" && block.type !== "thinking")
        throw new Error("Load reply must contain text or thinking");
      const field = block.type;
      const content =
        field === "text"
          ? { type: "text" as const, text: "" }
          : { type: "thinking" as const, thinking: "" };
      partial.content = [...partial.content, content];
      events.push({
        type: `${field}_start`,
        contentIndex,
        partial: { ...partial },
      });
      const value = field === "text" ? block.text : block.thinking;
      for (let offset = 0; offset < value.length; offset += chunkChars) {
        await new Promise<void>((resolve) => setImmediate(resolve));
        const delta = value.slice(offset, offset + chunkChars);
        onDelta(field === "text" && offset + chunkChars >= value.length);
        if (content.type === "text") content.text += delta;
        else content.thinking += delta;
        events.push({
          type: `${field}_delta`,
          contentIndex,
          delta,
          partial: { ...partial },
        });
      }
      events.push({
        type: `${field}_end`,
        contentIndex,
        content: value,
        partial: { ...partial },
      });
    }
    events.push({ type: "done", reason, message });
    events.end(message);
  })();
  return events;
}

afterEach(async () => {
  await Promise.all(
    openHosts
      .splice(0)
      .reverse()
      .map((close) => close()),
  );
});

function measuredHost() {
  const fixture = createNativeSqliteOwnerFixture();
  let statements = 0;
  let contextReads = 0;
  const lineageReads = new Map<string, number>();
  let failNextAppend = false;
  const owner: SqliteOwner = {
    transactionSync: (operation) => fixture.owner.transactionSync(operation),
    sql: {
      exec: (statement, ...values) => {
        statements += 1;
        if (
          statement.includes(
            "SELECT tip.state AS state, tip.sequence AS newest_sequence",
          )
        ) {
          const session = Schema.decodeUnknownSync(Schema.String)(values[0]);
          lineageReads.set(session, (lineageReads.get(session) ?? 0) + 1);
        }
        if (
          statement.includes("SELECT id FROM walk") &&
          statement.includes("state != 'streaming'")
        )
          contextReads += 1;
        if (
          failNextAppend &&
          /INSERT\s+(?:OR\s+\w+\s+)?INTO\s+session_entries/i.test(statement)
        ) {
          failNextAppend = false;
          throw new Error("simulated Woodstock append failure");
        }
        return fixture.owner.sql.exec(statement, ...values);
      },
    },
  };
  return {
    owner,
    counts: () => ({ statements, contextReads }),
    lineageReads: (sessionId: string) => lineageReads.get(sessionId) ?? 0,
    failAppend: () => {
      failNextAppend = true;
    },
    rows: (id: string) =>
      fixture.owner.sql
        .exec(
          "SELECT sequence, data_json FROM session_entries WHERE session_id = ? ORDER BY sequence",
          id,
        )
        .toArray(),
    close: fixture.close,
  };
}

async function makeAgent(
  historyReply = "warm",
  liveReply = fauxAssistantMessage("live"),
  warmSteps = historyTurns,
  paced = false,
) {
  const store = measuredHost();
  const warm = fauxModel(
    Array.from({ length: warmSteps }, () => fauxAssistantMessage(historyReply)),
    {
      models: [{ id: "load", contextWindow: 100_000_000 }],
      tokenSize: { min: 50_000, max: 50_000 },
    },
  );
  const live = fauxModel(
    Array.from({ length: 8 }, () => liveReply),
    {
      models: [{ id: "load", contextWindow: 100_000_000 }],
      tokenSize: { min: chunkChars, max: chunkChars },
    },
  );
  let streamed = 0;
  let onStreamDelta: (last: boolean) => void = () => undefined;
  try {
    const host = await memoryHost({
      owner: store.owner,
      model: {
        acquire: () =>
          Promise.resolve({
            model: live.model,
            stream: (...args) => {
              streamed += 1;
              return streamed <= warmSteps
                ? warm.stream(...args)
                : paced
                  ? pacedReplyStream(liveReply, (last) => {
                      onStreamDelta(last);
                    })
                  : live.stream(...args);
            },
            retryable: live.retryable,
            release: () => Promise.resolve(),
          }),
        describe: () => Promise.resolve({ contextWindow: 100_000_000 }),
      },
    });
    let closed = false;
    const close = async () => {
      if (closed) return;
      closed = true;
      await host.close();
      store.close();
    };
    openHosts.push(close);
    return {
      host,
      store,
      close,
      onStreamDelta: (listener: (last: boolean) => void) => {
        onStreamDelta = listener;
      },
    };
  } catch (error) {
    store.close();
    throw error;
  }
}

function routes(host: MemoryHost, onFrame?: (frame: SessionFrame) => void) {
  return createShiitakeHttpRoutes(
    (id) => {
      const session = host.agent.session(id);
      if (onFrame === undefined) return Promise.resolve(session);
      return Promise.resolve({
        ...session,
        subscribe: (options?: Parameters<typeof session.subscribe>[0]) =>
          (async function* () {
            for await (const frame of session.subscribe(options ?? {})) {
              onFrame(frame);
              yield frame;
            }
          })(),
        watch: async (options?: Parameters<typeof session.watch>[0]) => {
          const opened = await session.watch(options);
          return {
            ...opened,
            changes: (async function* () {
              for await (const frame of opened.changes) {
                onFrame(frame);
                yield frame;
              }
            })(),
          };
        },
      });
    },
    () => Promise.resolve({ sessions: [], next: undefined }),
    () => Promise.resolve("fresh"),
    () => Promise.resolve(false),
    () => {
      throw new Error("extensions unused");
    },
  );
}

/** A real SSE reader; do not buffer the whole reply in the test. */
async function sseReader(
  host: MemoryHost,
  id: string,
  mode: "tui" | "extra" | "slow",
) {
  let slowServerBytes = 0;
  const countSlowFrame =
    mode === "slow"
      ? (frame: SessionFrame) => {
          // Mirror the Hono frame event at the producer boundary: the slow
          // reader's pulled bytes alone omit the server's queued serialization.
          const id =
            frame.kind === "update" || frame.kind === "reset"
              ? `\nid: ${frame.cursor ?? ""}`
              : "";
          slowServerBytes += encoder.encode(
            `event: frame\ndata: ${JSON.stringify(frame)}${id}\n\n`,
          ).byteLength;
        }
      : undefined;
  const cursor =
    mode === "tui"
      ? null
      : (await host.agent.session(id).read({ limit: 200 })).cursor;
  const query =
    mode === "tui"
      ? "limit=200"
      : `mode=subscribe${cursor === null ? "" : `&after=${encodeURIComponent(cursor)}`}`;
  const response: Response = await routes(host, countSlowFrame).request(
    `http://shiitake.test/sessions/${id}/events?${query}`,
  );
  if (response.body === null) throw new Error("SSE body missing");
  const reader = response.body.getReader();
  let bytes = 0;
  let snapshotBytes = 0;
  let resetSeen = false;
  let recent = "";
  if (mode === "tui") {
    const snapshot = await reader.read();
    if (snapshot.done) throw new Error("TUI watch closed before its snapshot");
    const chunk: unknown = snapshot.value;
    if (!(chunk instanceof Uint8Array))
      throw new Error("SSE snapshot is not bytes");
    if (!new TextDecoder().decode(chunk).includes("event: snapshot"))
      throw new Error("TUI watch did not open with a snapshot");
    snapshotBytes = chunk.byteLength;
  }
  const cancellation = new AbortController();
  let wake: (() => void) | undefined;
  const stop = () => {
    cancellation.abort();
    wake?.();
  };
  const pump = (async () => {
    for (;;) {
      const result = await reader.read();
      if (result.done) return;
      const chunk: unknown = result.value;
      if (!(chunk instanceof Uint8Array))
        throw new Error("SSE chunk is not bytes");
      recent = (recent + new TextDecoder().decode(chunk)).slice(-512);
      if (recent.includes('"kind":"reset"')) resetSeen = true;
      if (mode !== "slow") {
        bytes += chunk.byteLength;
        continue;
      }
      // Read only 1 KiB of each received frame per second before pulling
      // another frame. Cancellation interrupts the throttle immediately.
      for (
        let offset = 0;
        offset < chunk.byteLength && !cancellation.signal.aborted;
        offset += 1024
      ) {
        bytes += Math.min(1024, chunk.byteLength - offset);
        await new Promise<void>((resolve) => {
          const timer = setTimeout(() => {
            wake = undefined;
            resolve();
          }, 1000);
          wake = () => {
            clearTimeout(timer);
            resolve();
          };
        });
      }
    }
  })();
  return {
    bytes: () => bytes,
    serializedBytes: () => (mode === "slow" ? slowServerBytes : bytes),
    snapshotBytes: () => snapshotBytes,
    resetSeen: () => resetSeen,
    close: async () => {
      stop();
      await reader.cancel();
      await pump;
    },
  };
}

async function watchFrames(
  host: MemoryHost,
  id: string,
  onFrame?: (frame: SessionFrame) => void,
) {
  const watch = await host.agent.session(id).watch();
  const seen: SessionFrame[] = [];
  const draining = (async () => {
    for await (const frame of watch.changes) {
      onFrame?.(frame);
      seen.push(frame);
    }
  })();
  return {
    seen,
    snapshot: watch.snapshot,
    close: async () => {
      await watch.close();
      await draining;
    },
  };
}

type LoadAgent = Awaited<ReturnType<typeof makeAgent>>;
type Command = Readonly<{ index: number; commandId: string }>;

/** Feed real progress production with Mule-shaped tool-tail events (no SQL writes). */
async function emitToolTails(
  agent: LoadAgent,
  command: Command,
): Promise<number> {
  const accepted = {
    sessionId: `s${String(command.index)}`,
    requestId: "load",
    commandId: command.commandId,
  };
  const run = { kind: "run", value: "load-run" } as const;
  const turn = { kind: "turn", value: "load-turn", parent: run } as const;
  const toolCall = {
    kind: "tool_call",
    value: `tail-${String(command.index)}`,
    parent: turn,
  } as const;
  const call = {
    ...fauxToolCall("build", {}),
    id: `tail-${String(command.index)}`,
  };
  let emitted = 0;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const tool = yield* openToolProgress(agent.store.owner, accepted);
        yield* tool.started({
          type: "tool_execution_start",
          replay: "safe",
          run,
          turn,
          toolCall,
          call,
        } satisfies Extract<MuleEvent, { type: "tool_execution_start" }>);
        for (let revision = 1; revision <= 20; revision++) {
          yield* tool.update({
            type: "tool_execution_update",
            run,
            turn,
            toolCall,
            call,
            result: {
              isError: false,
              content: [{ type: "text", text: `step ${String(revision)}` }],
              data: { totalBytes: revision * 8, droppedBytes: 0 },
            },
          } satisfies Extract<MuleEvent, { type: "tool_execution_update" }>);
          emitted += 1;
          yield* Effect.promise(
            () => new Promise<void>((resolve) => setImmediate(resolve)),
          );
        }
      }),
    ),
  );
  return emitted;
}

/** Execute the actual agents.wait capability, including its directory/open path. */
async function startWaits(
  agents: readonly LoadAgent[],
  commands: readonly Command[],
) {
  let cycles = 0;
  let progressFrames = 0;
  let signalOpen: () => void = () => {
    throw new Error("wait start was not initialized");
  };
  const opened = new Promise<void>((resolve) => {
    signalOpen = resolve;
  });
  const directory: AgentControlDirectory = {
    list: () => Promise.resolve([]),
    open: (agentId) => {
      const index = Number(agentId.slice(1));
      const target = agents[index];
      if (target === undefined || agentId !== `a${String(index)}`)
        throw new Error(`Unknown sub-agent ${agentId}`);
      const instrumented = {
        ...target.host.agent,
        session: (id: string) => {
          const session = target.host.agent.session(id);
          return {
            ...session,
            watch: async (options?: Parameters<typeof session.watch>[0]) => {
              const opened = await session.watch(options);
              return {
                ...opened,
                changes: (async function* () {
                  for await (const frame of opened.changes) {
                    if (frame.kind === "progress") progressFrames += 1;
                    yield frame;
                  }
                })(),
              };
            },
          };
        },
      };
      return Promise.resolve({
        agent: instrumented,
        release: () => Promise.resolve(),
      });
    },
  };
  const runtime = await Mycelium.make({
    sources: [createAgentControlSource(directory)],
    sandbox: directSandbox(async ({ agents: operations }) => {
      if (operations?.wait === undefined)
        throw new Error("agents.wait missing");
      const wait = operations.wait;
      const result = z.object({
        commandId: z.string(),
        outcome: z.enum(["completed", "cancelled", "failed", "interrupted"]),
      });
      const work = commands.map(async ({ index, commandId }) => {
        for (let cycle = 0; cycle < 20; cycle++) {
          cycles += 1;
          try {
            const settled = result.parse(
              await wait({
                agentId: `a${String(index)}`,
                sessionId: `s${String(index)}`,
                commandId,
                timeoutMs: 15_000,
              }),
            );
            expect(settled).toMatchObject({ commandId, outcome: "completed" });
            return;
          } catch (error) {
            if (error instanceof Error && error.message.includes("timed out"))
              continue;
            throw error;
          }
        }
        throw new Error(
          `agents.wait exhausted 20 cycles for a${String(index)}`,
        );
      });
      signalOpen();
      await Promise.all(work);
      return null;
    }),
  });
  const lease = await runtime.acquire();
  try {
    const prepared = await Effect.runPromise(
      lease.executeTool.prepare({
        type: "toolCall",
        id: "parent-waits",
        name: "execute",
        arguments: { code: "return await agents.wait(...)" },
      }),
    );
    const running = Effect.runPromise(Stream.runCollect(prepared.execute()));
    await opened;
    return {
      cycles: () => cycles,
      progressFrames: () => progressFrames,
      finish: async () => {
        try {
          const parts = await running;
          expect(parts[0]).toMatchObject({ result: { isError: false } });
        } finally {
          await lease.release();
          await runtime.close();
        }
      },
    };
  } catch (error) {
    await lease.release();
    await runtime.close();
    throw error;
  }
}

it("S0 retained Test 30: sibling sequence churn cannot erase a quiet Session's Woodstock replay", async () => {
  const { host, store } = await makeAgent(
    "warm",
    fauxAssistantMessage("live"),
    303,
  );
  const a = host.agent.session("quiet");
  const b = host.agent.session("noisy");
  for (let n = 0; n < 50; n++) {
    await a.send(`A-${String(n)}`, { requestId: `A-${String(n)}` });
    await host.settle("quiet");
  }
  const before = store.rows("quiet");
  const cursor = (await a.read()).cursor;
  expect(cursor).not.toBeNull();
  for (let n = 0; n < 250; n++) {
    await b.send(`B-${String(n)}`, { requestId: `B-${String(n)}` });
    await host.settle("noisy");
  }
  for (let n = 0; n < 3; n++) {
    await a.send(`A-new-${String(n)}`, { requestId: `A-new-${String(n)}` });
    await host.settle("quiet");
  }
  const after = store.rows("quiet");
  expect(after.slice(0, before.length)).toEqual(before);
  expect(after.length).toBeGreaterThan(before.length);
  expect((await a.read()).cursor).not.toEqual(cursor);
  if (cursor === null) throw new Error("quiet Session has no cursor");
  const replay = a.subscribe({ after: cursor })[Symbol.asyncIterator]();
  try {
    const newRequests: string[] = [];
    const lastCursor = (await a.read()).cursor;
    for (let page = 0; page < 32; page++) {
      const next = await replay.next();
      if (next.done) throw new Error("Replay ended before the new A rows");
      const update = z
        .object({
          kind: z.literal("update"),
          cursor: z.string().nullable(),
          messages: z.array(z.object({ requestId: z.string().optional() })),
        })
        .parse(next.value);
      newRequests.push(
        ...update.messages.flatMap(({ requestId }) =>
          requestId?.startsWith("A-new-") ? [requestId] : [],
        ),
      );
      if (update.cursor === lastCursor) break;
    }
    expect(newRequests).toEqual(["A-new-0", "A-new-1", "A-new-2"]);
  } finally {
    await replay.return?.();
  }
});

it("S0 retained Test 32: failed Woodstock append is loud, never replaced by a process-only commit", async () => {
  const { host, store } = await makeAgent();
  const session = host.agent.session("failed");
  store.failAppend();
  await session.send("cannot commit", { requestId: "failed-append" });
  const failed = await host.settle("failed");
  expect(failed.terminals).toMatchObject([
    { outcome: "failed", diagnostic: "store:rejected" },
  ]);
  expect(failed.messages).toEqual([]);
  const failedRows = store.rows("failed");
  expect(failedRows.length).toBeGreaterThan(0); // durable failure, not an in-memory success
  const accepted = await session.send("recovered", { requestId: "recovered" });
  const recovered = await host.settle("failed");
  expect(recovered.terminals).toContainEqual(
    expect.objectContaining({
      commandId: accepted.commandId,
      outcome: "completed",
    }),
  );
  expect(
    store
      .rows("failed")
      .slice(failedRows.length)
      .some((row) =>
        Schema.decodeUnknownSync(Schema.String)(row.data_json).includes(
          "recovered",
        ),
      ),
  ).toBe(true);
});

it("S5 retained Test 36: public feed publishes a real cursor only after the Woodstock row commits", async () => {
  const { host, store } = await makeAgent();
  const session = host.agent.session("commit");
  const observedSequences: number[] = [];
  const watch = await watchFrames(host, "commit", (frame) => {
    if (frame.kind !== "update" || frame.cursor === null) return;
    const position = decodeWoodstockCursor(frame.cursor);
    if (position === undefined)
      throw new Error("feed emitted an invalid cursor");
    const committedSequences = store.rows("commit").map((row) => row.sequence);
    expect(committedSequences).toContain(position.sequence);
    // An activity/pending projection may repeat a cursor without a new
    // durable entry; only cursor-moving commits belong to this retained law.
    if (observedSequences.at(-1) !== position.sequence)
      observedSequences.push(position.sequence);
  });
  try {
    await session.send("one", { requestId: "one" });
    await host.settle("commit");
    const durable = await session.read();
    const updates = watch.seen.filter(
      (frame): frame is Extract<SessionFrame, { kind: "update" }> =>
        frame.kind === "update" && frame.cursor !== null,
    );
    expect(updates.length).toBeGreaterThan(0);
    expect(observedSequences.length).toBeGreaterThan(0);
    expect(observedSequences).toEqual(
      [...new Set(observedSequences)].sort((left, right) => left - right),
    );
    expect(updates.at(-1)?.cursor).toEqual(durable.cursor);
    // The retained bus's push tail must not precede its append. S5 also
    // needs to make push independent of per-subscriber replay reads.
    expect(store.rows("commit").length).toBeGreaterThan(0);
  } finally {
    await watch.close();
  }
});

it("S3: a sibling job transition does not replay an idle Session", async () => {
  const { host, store } = await makeAgent();
  await host.agent.session("quiet").send("quiet", { requestId: "quiet" });
  await host.settle("quiet");
  const watch = await watchFrames(host, "quiet");
  try {
    const before = store.lineageReads("quiet");
    await host.agent.session("busy").send("work", { requestId: "work" });
    await host.settle("busy");
    await new Promise<void>((resolve) => setImmediate(resolve));
    const replayReads = store.lineageReads("quiet") - before;
    process.stderr.write(
      `LIVE_UPDATES_S3 ${JSON.stringify({ replayReads })}\n`,
    );
    expect.soft(replayReads, "S3: sibling Session replay reads").toBe(0);
  } finally {
    await watch.close();
  }
});

it(
  "S4: producer CPU scales linearly with reply length",
  { timeout: 1_200_000 },
  async () => {
    const samples: { small: number; large: number }[] = [];
    for (let trial = 0; trial < 1; trial++) {
      const cpu = async (length: number) => {
        const agent = await makeAgent(
          "warm",
          fauxAssistantMessage(fauxText("x".repeat(length))),
          0,
          true,
        );
        try {
          // Serialize the whole-text progress frames over the actual HTTP SSE
          // path; without a draining reader this measures only string growth.
          const reader = await sseReader(agent.host, "cpu", "tui");
          try {
            const started = process.cpuUsage();
            await agent.host.agent.session("cpu").send(`cpu-${String(length)}`);
            await agent.host.settle("cpu");
            const elapsed = process.cpuUsage(started);
            return (elapsed.user + elapsed.system) / 1000;
          } finally {
            await reader.close();
          }
        } finally {
          await agent.close();
        }
      };
      samples.push({ small: await cpu(250_000), large: await cpu(500_000) });
    }
    const small = samples[0]?.small;
    const large = samples[0]?.large;
    if (small === undefined || large === undefined)
      throw new Error("Missing producer CPU sample");
    process.stderr.write(
      `LIVE_UPDATES_S4_CPU ${JSON.stringify({ samples, ratioToLinear: large / (2 * small) })}\n`,
    );
    expect
      .soft(large, "S4: 500 KB producer CPU vs 250 KB")
      .toBeLessThanOrEqual(1.2 * 2 * small);
  },
);

type Metrics = {
  turnMs: number;
  p99Ms: number;
  maxLagMs: number;
  histogramMaxLagMs: number;
  histogramSamples: number;
  histogramCoverageMs: number;
  streamingMs: number;
  streamingP99Ms: number;
  streamingMaxLagMs: number;
  streamingHistogramSamples: number;
  streamingHistogramCoverageMs: number;
  sampledMaxLagMs: number;
  peakRss: number;
  peakHeap: number;
  initialRss: number;
  bytesSerialized: number;
  snapshotBytes: number;
  slowClientBytes: number;
  slowClientReset: boolean;
  sseWatchers: number;
  contextReads: number;
  contextReadsBySession: readonly number[];
  historyChangesBySession: readonly number[];
  sqliteStatements: number;
  framesToWaits: number;
  waitCycles: number;
  toolTailUpdates: number;
};

function assertStreamingWindow(
  phase: Pick<
    Metrics,
    | "streamingMs"
    | "streamingP99Ms"
    | "streamingHistogramSamples"
    | "streamingHistogramCoverageMs"
  >,
  name: "zero-watcher" | "watched",
) {
  expect(
    phase.streamingHistogramSamples,
    `S0: ${name} streaming histogram has samples`,
  ).toBeGreaterThan(0);
  expect(
    phase.streamingHistogramCoverageMs,
    `S0: ${name} streaming histogram covers >=90% of streaming`,
  ).toBeGreaterThanOrEqual(0.9 * phase.streamingMs);
  if (name === "zero-watcher") {
    expect(
      phase.streamingP99Ms,
      "S0: control streaming p99 is measurable",
    ).toBeGreaterThan(0);
    expect(
      phase.streamingP99Ms,
      "S0: control streaming p99 <50 ms",
    ).toBeLessThan(50);
  }
}

async function measure(
  agents: Awaited<ReturnType<typeof makeAgent>>[],
  watched: boolean,
  turn: number,
): Promise<Metrics> {
  const readers: Awaited<ReturnType<typeof sseReader>>[] = [];
  const counts = agents.map(({ store }) => store.counts());
  const entryCounts = agents.map(
    ({ store }, index) => store.rows(`s${String(index)}`).length,
  );
  const commands = await Promise.all(
    agents.slice(1).map(async ({ host }, index) => {
      const id = index + 1;
      const accepted = await host.agent
        .session(`s${String(id)}`)
        .send(`final-${String(turn)}`, { requestId: `final-${String(turn)}` });
      return { index: id, commandId: accepted.commandId };
    }),
  );
  let delay: ReturnType<typeof monitorEventLoopDelay> | undefined;
  let streamDelay: ReturnType<typeof monitorEventLoopDelay> | undefined;
  let lagTimer: ReturnType<typeof setInterval> | undefined;
  let lastLagTick = 0;
  let sampledMaxLagMs = 0;
  let streamingMs = 0;
  let streamingP99Ms = 0;
  let streamingMaxLagMs = 0;
  let streamingHistogramSamples = 0;
  let streamingHistogramCoverageMs = 0;
  let streamingStart = 0;
  let streamingLastTick = 0;
  let streamingSampledMaxLagMs = 0;
  let streamingTimer: ReturnType<typeof setInterval> | undefined;
  const startedStreams = new Set<number>();
  const finishedStreams = new Set<number>();
  let peakRss = 0;
  let peakHeap = 0;
  const memory = setInterval(() => {
    const sample = process.memoryUsage();
    peakRss = Math.max(peakRss, sample.rss);
    peakHeap = Math.max(peakHeap, sample.heapUsed);
  }, 25);
  let waits: Awaited<ReturnType<typeof startWaits>> | undefined;
  let finished = false;
  try {
    // TUI-style parent + two sub-agent panes; two further independent HTTP
    // SSE watchers; a sixth client pulls at 1 KiB/s.
    if (watched) {
      for (const index of [0, 1, 2, 3, 4, 5]) {
        const agent = agents[index];
        if (agent === undefined) throw new Error("missing Agent");
        readers.push(
          await sseReader(
            agent.host,
            `s${String(index)}`,
            index < 3 ? "tui" : index === 5 ? "slow" : "extra",
          ),
        );
      }
      waits = await startWaits(agents, commands);
    }
    // The waits can still be opening when startWaits returns. Create the
    // histogram only after setup and require an on-time quiet tick to prime it.
    delay = monitorEventLoopDelay({ resolution: 10 });
    streamDelay = monitorEventLoopDelay({ resolution: 10 });
    delay.enable();
    streamDelay.enable();
    for (;;) {
      delay.reset();
      streamDelay.reset();
      const before = performance.now();
      await new Promise<void>((resolve) => setTimeout(resolve, 10));
      if (performance.now() - before < 20 && delay.count > 0) break;
    }
    for (let index = 1; index <= 5; index++) {
      agents[index]?.onStreamDelta((last) => {
        startedStreams.add(index);
        if (startedStreams.size === 5 && streamingStart === 0) {
          streamDelay?.reset();
          streamingStart = performance.now();
          streamingLastTick = streamingStart;
          streamingTimer = setInterval(() => {
            const now = performance.now();
            streamingSampledMaxLagMs = Math.max(
              streamingSampledMaxLagMs,
              now - streamingLastTick - 5,
            );
            streamingLastTick = now;
          }, 5);
        }
        if (last) finishedStreams.add(index);
        if (finishedStreams.size === 5 && streamingMs === 0) {
          streamingMs = performance.now() - streamingStart;
          streamingHistogramSamples = streamDelay?.count ?? 0;
          streamingHistogramCoverageMs =
            (streamingHistogramSamples * (streamDelay?.mean ?? 0)) / 1e6;
          streamingSampledMaxLagMs = Math.max(
            streamingSampledMaxLagMs,
            performance.now() - streamingLastTick - 5,
          );
          if (streamingTimer !== undefined) clearInterval(streamingTimer);
          streamingP99Ms = Math.max(
            0,
            (streamDelay?.percentile(99) ?? 0) / 1e6 - 10,
          );
          streamingMaxLagMs = Math.max(
            streamingSampledMaxLagMs,
            0,
            (streamDelay?.max ?? 0) / 1e6 - 10,
          );
          streamDelay?.disable();
        }
      });
    }
    const initialMemory = process.memoryUsage();
    const initialRss = initialMemory.rss;
    peakRss = initialMemory.rss;
    peakHeap = initialMemory.heapUsed;
    lastLagTick = performance.now();
    lagTimer = setInterval(() => {
      const now = performance.now();
      sampledMaxLagMs = Math.max(sampledMaxLagMs, now - lastLagTick - 5);
      lastLagTick = now;
    }, 5);
    const start = performance.now();
    const toolUpdates = agents.slice(1, 6).map((agent, index) => {
      const command = commands[index];
      if (command === undefined) throw new Error("missing tool command");
      return emitToolTails(agent, command);
    });
    let terminalCount = 0;
    let releaseReads: () => void = () => undefined;
    const readsAllowed = new Promise<void>((resolve) => {
      releaseReads = resolve;
    });
    let turnMs = 0;
    let histogramSamples = 0;
    let histogramCoverageMs = 0;
    let p99Ms = 0;
    let histogramMaxLagMs = 0;
    const settledPromise = Promise.all(
      agents.slice(1).map(({ host }, index) =>
        host.settle(`s${String(index + 1)}`, undefined, async () => {
          terminalCount += 1;
          if (terminalCount === 8) {
            // All eight terminals have arrived; exclude the harness's eight
            // full-context snapshot reads from both clock and lag histogram.
            turnMs = performance.now() - start;
            histogramSamples = delay?.count ?? 0;
            histogramCoverageMs =
              ((delay?.count ?? 0) * (delay?.mean ?? 0)) / 1e6;
            p99Ms = Math.max(0, (delay?.percentile(99) ?? 0) / 1e6 - 10);
            histogramMaxLagMs = Math.max(0, (delay?.max ?? 0) / 1e6 - 10);
            delay?.disable();
            if (lagTimer !== undefined) clearInterval(lagTimer);
            releaseReads();
          }
          await readsAllowed;
        }),
      ),
    );
    const [settled, emitted] = await Promise.all([
      settledPromise,
      Promise.all(toolUpdates),
    ]);
    expect(settled).toHaveLength(8);
    expect(emitted).toEqual([20, 20, 20, 20, 20]);
    if (waits !== undefined) await waits.finish();
    finished = true;
    if (streamingMs === 0)
      throw new Error("Five replies did not finish streaming");
    const now = process.memoryUsage();
    return {
      turnMs,
      p99Ms,
      maxLagMs: Math.max(histogramMaxLagMs, sampledMaxLagMs),
      histogramMaxLagMs,
      histogramSamples,
      histogramCoverageMs,
      streamingMs,
      streamingP99Ms,
      streamingMaxLagMs,
      streamingHistogramSamples,
      streamingHistogramCoverageMs,
      sampledMaxLagMs,
      peakRss: Math.max(peakRss, now.rss),
      peakHeap: Math.max(peakHeap, now.heapUsed),
      initialRss,
      bytesSerialized: readers.reduce(
        (sum, reader) => sum + reader.serializedBytes(),
        0,
      ),
      snapshotBytes: readers.reduce(
        (sum, reader) => sum + reader.snapshotBytes(),
        0,
      ),
      slowClientBytes: readers[5]?.bytes() ?? 0,
      slowClientReset: readers[5]?.resetSeen() ?? false,
      sseWatchers: readers.length,
      contextReads: agents.reduce(
        (sum, agent, index) =>
          sum +
          agent.store.counts().contextReads -
          (counts[index]?.contextReads ?? 0),
        0,
      ),
      contextReadsBySession: agents.map(
        ({ store }, index) =>
          store.counts().contextReads - (counts[index]?.contextReads ?? 0),
      ),
      historyChangesBySession: agents.map(
        ({ store }, index) =>
          store.rows(`s${String(index)}`).length - (entryCounts[index] ?? 0),
      ),
      sqliteStatements: agents.reduce(
        (sum, agent, index) =>
          sum +
          agent.store.counts().statements -
          (counts[index]?.statements ?? 0),
        0,
      ),
      framesToWaits: waits?.progressFrames() ?? 0,
      waitCycles: waits?.cycles() ?? 0,
      toolTailUpdates: emitted.reduce((sum, value) => sum + value, 0),
    };
  } finally {
    clearInterval(memory);
    if (lagTimer !== undefined) clearInterval(lagTimer);
    if (streamingTimer !== undefined) clearInterval(streamingTimer);
    delay?.disable();
    streamDelay?.disable();
    await Promise.all(readers.map((reader) => reader.close()));
    if (waits !== undefined && !finished) await waits.finish();
  }
}

/** Build equivalent fresh histories for both timed phases, never reuse a completed reply. */
async function seededAgents() {
  const agents = await Promise.all(
    Array.from({ length: 9 }, (_, i) =>
      makeAgent(
        "h".repeat(historyReplyChars),
        fauxAssistantMessage(
          i > 0 && i <= 5
            ? [
                fauxThinking("t".repeat(thinkingChars)),
                fauxText("x".repeat(textChars)),
              ]
            : "idle",
        ),
        historyTurns,
        i > 0 && i <= 5,
      ),
    ),
  );
  for (let turn = 0; turn < historyTurns; turn++) {
    await Promise.all(
      agents.map(({ host }, index) =>
        host.agent
          .session(`s${String(index)}`)
          .send(`history-${String(turn)}`, {
            requestId: `history-${String(turn)}`,
          }),
      ),
    );
    await Promise.all(
      agents.map(({ host }, index) => host.settle(`s${String(index)}`)),
    );
  }
  const history = agents.map(({ store }, i) => {
    const rows = store.rows(`s${String(i)}`);
    return {
      rows: rows.length,
      bytes: rows.reduce(
        (sum, row) =>
          sum +
          encoder.encode(Schema.decodeUnknownSync(Schema.String)(row.data_json))
            .byteLength,
        0,
      ),
    };
  });
  return { agents, history };
}

it("S0 control guard rejects an empty streaming histogram", () => {
  const histogram = monitorEventLoopDelay({ resolution: 10 });
  const p99Ms = Math.max(0, histogram.percentile(99) / 1e6 - 10);
  expect(histogram.count).toBe(0);
  expect(p99Ms).toBe(0);
  expect(() => {
    assertStreamingWindow(
      {
        streamingMs: 100,
        streamingP99Ms: p99Ms,
        streamingHistogramSamples: histogram.count,
        streamingHistogramCoverageMs: 0,
      },
      "zero-watcher",
    );
  }).toThrow(/S0: zero-watcher streaming histogram has samples/u);
});

it("S0 control guard rejects an undercovered streaming window", () => {
  expect(() => {
    assertStreamingWindow(
      {
        streamingMs: 1_000,
        streamingP99Ms: 20,
        streamingHistogramSamples: 10,
        streamingHistogramCoverageMs: 800,
      },
      "zero-watcher",
    );
  }).toThrow(/S0: zero-watcher streaming histogram covers >=90%/u);
});

it("S0 control guard rejects a clipped zero p99 despite full coverage", () => {
  expect(() => {
    assertStreamingWindow(
      {
        streamingMs: 1_000,
        streamingP99Ms: 0,
        streamingHistogramSamples: 100,
        streamingHistogramCoverageMs: 1_000,
      },
      "zero-watcher",
    );
  }).toThrow(/S0: control streaming p99 is measurable/u);
});

it("S0 control guard rejects a 400 ms p99 despite full histogram coverage", () => {
  expect(() => {
    assertStreamingWindow(
      {
        streamingMs: 1_000,
        streamingP99Ms: 400,
        streamingHistogramSamples: 100,
        streamingHistogramCoverageMs: 1_000,
      },
      "zero-watcher",
    );
  }).toThrow(/S0: control streaming p99 <50 ms/u);
});

it(
  "S0: median freeze baseline; S2–S5 and partial S6 bars (not S1/S7)",
  { timeout: 1_200_000 },
  async () => {
    const measurements: { idle: Metrics; busy: Metrics; ratio: number }[] = [];
    for (let trial = 0; trial < trials; trial++) {
      const baseline = await seededAgents();
      const idle = await measure(baseline.agents, false, 0);
      await Promise.all(baseline.agents.map((agent) => agent.close()));
      const observed = await seededAgents();
      const busy = await measure(observed.agents, true, 0);
      await Promise.all(observed.agents.map((agent) => agent.close()));
      const ratio = busy.turnMs / idle.turnMs;
      process.stderr.write(
        `LIVE_UPDATES_S0 ${JSON.stringify({ trial: trial + 1, historyIdle: baseline.history, historyBusy: observed.history, idle, busy, ratio })}\n`,
      );
      measurements.push({ idle, busy, ratio });
      if (
        process.env.LOAD_HISTORY_TURNS === undefined &&
        process.env.LOAD_HISTORY_REPLY === undefined
      )
        for (const session of [...baseline.history, ...observed.history]) {
          expect(session.rows, "S0: rows per Session").toBe(900);
          expect(session.bytes, "S0: bytes per Session").toBeGreaterThanOrEqual(
            4_000_000,
          );
        }
    }
    const median = [...measurements].sort(
      (left, right) => left.ratio - right.ratio,
    )[Math.floor(measurements.length / 2)];
    if (median === undefined) throw new Error("No load trials");
    const { idle, busy, ratio } = median;
    process.stderr.write(
      `LIVE_UPDATES_S0_MEDIAN ${JSON.stringify({ trials, idle, busy, ratio })}\n`,
    );
    for (const pair of measurements) {
      for (const [name, phase] of [
        ["zero-watcher", pair.idle],
        ["watched", pair.busy],
      ] as const) {
        expect(
          phase.histogramCoverageMs,
          "S0: histogram ticks cover >=90% of the timed phase",
        ).toBeGreaterThanOrEqual(0.9 * phase.turnMs);
        assertStreamingWindow(phase, name);
      }
    }
    expect(idle.sseWatchers, "S0: 0-watcher baseline").toBe(0);
    expect(busy.sseWatchers, "S0: 3 TUI + 2 SSE + 1 slow").toBe(6);
    expect(
      busy.waitCycles,
      "S0: all eight actual agents.wait calls",
    ).toBeGreaterThanOrEqual(8);
    expect(idle.toolTailUpdates, "S0: baseline tool-tail events").toBe(100);
    expect(busy.toolTailUpdates, "S0: watched tool-tail events").toBe(100);
    expect(
      busy.slowClientBytes,
      "S0: slow client consumed bytes",
    ).toBeGreaterThan(0);
    expect(
      busy.slowClientBytes,
      "S0: slow client reads at most 1 KiB/s",
    ).toBeLessThanOrEqual((Math.ceil(busy.turnMs / 1000) + 1) * 1024);
    // Fail rather than report a baseline that cannot reproduce the freeze.
    expect(ratio, "S0: watched/0-watcher turn slowdown").toBeGreaterThanOrEqual(
      10,
    );
    expect(idle.maxLagMs, "S0: low-lag zero-watcher control").toBeLessThan(500);
    expect(
      busy.histogramMaxLagMs,
      "S0: watcher loop lag",
    ).toBeGreaterThanOrEqual(500);
    // Future slice bars are assertions, not inferred from an absent implementation.
    for (let index = 0; index < 9; index++) {
      const extra =
        (busy.contextReadsBySession[index] ?? 0) -
        (idle.contextReadsBySession[index] ?? 0);
      expect
        .soft(extra, `S2: watcher-caused context reads for s${String(index)}`)
        .toBeLessThanOrEqual(
          index === 0 ? 0 : (busy.historyChangesBySession[index] ?? 0),
        );
    }
    expect
      .soft(busy.bytesSerialized, "S4: reply bytes with one bounded snapshot")
      .toBeLessThanOrEqual(
        2 * (thinkingChars + textChars) * 5 + busy.snapshotBytes,
      );
    expect
      .soft(busy.streamingMaxLagMs, "S5: streaming max loop lag")
      .toBeLessThan(50);
    expect
      .soft(busy.streamingP99Ms, "S5: streaming p99 loop lag")
      .toBeLessThan(20);
    expect
      .soft(busy.streamingP99Ms, "S5: watcher p99 vs 0-watcher control + 10 ms")
      .toBeLessThanOrEqual(idle.streamingP99Ms + 10);
    expect
      .soft(busy.slowClientReset, "S5: slow client reset after overflow")
      .toBe(true);
    expect
      .soft(
        busy.peakRss - busy.initialRss,
        "S5: RSS growth across watched phase",
      )
      .toBeLessThan(100 * 1024 * 1024);
    expect.soft(ratio, "S5: turn time vs baseline").toBeLessThanOrEqual(1.2);
    expect.soft(busy.framesToWaits, "S6: waits receive no progress").toBe(0);
  },
);
