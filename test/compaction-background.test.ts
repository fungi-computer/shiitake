import {
  fauxModel,
  type FauxResponseFactory,
} from "@fungi.computer/mule/testing";
// Runtime for every law here: Node 24, `node:sqlite`, Pi's faux provider and
// the real accepted run (`runAcceptedCommand` through the runtime host).
import { directSandbox } from "@fungi.computer/mycelium/testing";
import {
  getSystemMessageText,
  type AssistantMessage,
  type Context,
  type Usage,
  type UserMessage,
} from "@earendil-works/pi-ai";
import {
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import * as Effect from "effect/Effect";
import { expect, it } from "vitest";

import {
  defineExtension,
  ExtensionId,
  type RosteredExtensionV0,
} from "../src/extensions.js";
import {
  Shiitake,
  type BorrowedCompactionViewV0,
  type CompactionHandoffErrorV0,
  type CompactionHandoffResultV0,
  type ShiitakeOptions,
  type SqliteOwner,
} from "../src/index.js";
import {
  SUMMARIZATION_SYSTEM_PROMPT,
  serializeConversation,
} from "../src/internal/compaction/utils.js";
import {
  appendCompactionTransition,
  readProcessCompaction,
} from "../src/internal/woodstock/index.js";
import {
  readSelectedPath,
  type StoredSessionRow,
} from "../src/internal/woodstock/session-store.js";
import { requireAcceptedWorkId } from "./fixtures/accepted-work.fixture.js";
import {
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

const sessionId = "background-compaction";

/*
 * Budgets: a 60K window with the default 16384 reserve blocks above 43616
 * tokens; 23616 background tokens start a background compaction above 20000.
 * The seed run leaves about 10K tokens, the 28K-character prompt adds 8K
 * (18K: no pre-run start) and the first 14K-character tool result adds 4K
 * (21K: the first between-turn point starts one). With 6000 kept tokens the
 * cut lands on the prompt, so one history summary request covers the seed.
 */
const budgets = { reserveTokens: 16_384, backgroundTokens: 23_616 };

/**
 * The one SQL statement each context read, settled (`readSessionContextPath`,
 * which placement rereads) or prepared, issues for its kept window.
 */
const contextWindowRead =
  "WHERE id IN (SELECT id FROM walk) AND state != 'streaming'";

function deferred() {
  let resolve: () => void = () => undefined;
  const promise = new Promise<void>((settle) => {
    resolve = settle;
  });
  return { promise, resolve };
}

/** Pi's normalized context carries the system prompt as its first message. */
function isSummaryRequest(request: Context): boolean {
  const first = request.messages[0];
  return (
    first?.role === "system" &&
    getSystemMessageText(first) === SUMMARIZATION_SYSTEM_PROMPT
  );
}

function requestText(request: Context | undefined): string {
  const user = request?.messages.find(({ role }) => role === "user");
  if (user?.role !== "user") return "";
  return contentText(user.content);
}

function contentText(content: UserMessage["content"]): string {
  return Array.isArray(content)
    ? content
        .flatMap((part) => (part.type === "text" ? [part.text] : []))
        .join("")
    : content;
}

/** Whether a main request starts from a compaction summary (`model[0]`). */
function startsWithSummary(request: Context): boolean {
  return requestText(request).includes("<summary>");
}

function toolTurn(): AssistantMessage {
  return fauxAssistantMessage(fauxToolCall("execute", { code: "read()" }), {
    stopReason: "toolUse",
  });
}

/** Wait for `promise`, failing the law instead of hanging when it never settles. */
async function within<A>(promise: Promise<A>, label: string): Promise<A> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      reject(new Error(`Timed out waiting for ${label}`));
    }, 5_000);
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    clearTimeout(timer);
  }
}

/** Hold a summary request until `release`, or reject when its call aborts. */
function heldUntil(
  release: Promise<void>,
  signal: AbortSignal | undefined,
  onAbort: () => void,
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const abort = () => {
      onAbort();
      reject(new Error("summary request aborted"));
    };
    if (signal?.aborted === true) {
      abort();
      return;
    }
    signal?.addEventListener("abort", abort, { once: true });
    void release.then(() => {
      signal?.removeEventListener("abort", abort);
      resolve();
    });
  });
}

type Run = Readonly<{
  /** Main (non-summary) requests made so far, the seed run's included. */
  mainRequests: readonly Context[];
  summaryRequests: readonly Context[];
  /** Whether a summary request has arrived and is still held. */
  held: () => boolean;
  released: () => boolean;
  release: () => void;
  /** Main replies returned while a summary request was held. */
  heldReplies: () => number;
  owner: SqliteOwner;
}>;

type Script = (
  run: Run,
  request: Context,
  signal: AbortSignal | undefined,
) => AssistantMessage | Promise<AssistantMessage>;

type Consumed = {
  views: BorrowedCompactionViewV0[];
  /** Weak references to the handed-off prefix arrays (`weakViews`). */
  weak: WeakRef<readonly unknown[]>[];
  contextReadsAtConsume: number[];
};

function harness(
  input: Readonly<{
    keepRecentTokens?: number;
    /** The run-2 model turns; the seed run always answers with 40K characters. */
    script: Script;
    /** Whether the held summary request ends in a model error. */
    summaryFails?: boolean;
    /** The n-th summary request's text (1-based); short by default. */
    summaryText?: (request: number) => string;
    /** Whether the Zookeeper handoff refuses every view. */
    handoffFails?: boolean;
    /** Answer each view with `accepted` and the receipt `receipt-<n>`. */
    receipts?: boolean;
    /** Runs inside the handoff, before it returns (a write racing it). */
    onConsume?: (run: Run) => void;
    /**
     * Hold the handed-off arrays only weakly and return from the handoff a
     * job later, so a collection at the next context read shows whether the
     * run still reaches the summarized prefix.
     */
    weakViews?: boolean;
    /** Make the first head check after the release read a corrupt row. */
    corruptHeadCheckOnRelease?: boolean;
    /** Tool result text for the n-th tool call (1-based). */
    toolResult?: (call: number, run: Run) => string;
  }>,
) {
  const fixture = createNativeSqliteOwnerFixture();
  let contextReads = 0;
  let prefixReachableAtRead: boolean | "no gc" | undefined;
  let corruptArmed = false;
  const consumed: Consumed = { views: [], weak: [], contextReadsAtConsume: [] };
  // The store wrapper: counts every `readSessionContextPath` window read and,
  // at the first one after a weak handoff, collects garbage and checks
  // whether the handed-off prefix is still reachable.
  const owner: SqliteOwner = {
    ...fixture.owner,
    sql: {
      exec: (statement, ...bindings) => {
        if (
          corruptArmed &&
          /SELECT sequence\s+FROM session_entries/u.test(statement) &&
          statement.includes("AND id = ?")
        ) {
          corruptArmed = false;
          const row = { sequence: "corrupt" };
          return { toArray: () => [row], one: () => row };
        }
        if (statement.includes(contextWindowRead)) {
          contextReads += 1;
          if (consumed.weak.length > 0 && prefixReachableAtRead === undefined) {
            if (globalThis.gc === undefined) prefixReachableAtRead = "no gc";
            else {
              globalThis.gc();
              prefixReachableAtRead = consumed.weak.some(
                (reference) => reference.deref() !== undefined,
              );
            }
          }
        }
        return fixture.owner.sql.exec(statement, ...bindings);
      },
    },
  };
  const runtime = createCompatibilityRuntimeHost();
  const faux = fauxModel([], {
    models: [{ id: "background-1", contextWindow: 60_000, maxTokens: 100_000 }],
  });
  const model = faux.model;
  const hold = deferred();
  const summaryArrived = deferred();
  const mainRequests: Context[] = [];
  const summaryRequests: Context[] = [];
  let released = false;
  let heldReplies = 0;
  let active = 0;
  let peak = 0;
  let summaryAborted = false;
  let toolCalls = 0;
  let contextReadsAtFork: number | undefined;
  const run: Run = {
    mainRequests,
    summaryRequests,
    held: () => summaryRequests.length > 0 && !released,
    released: () => released,
    release: () => {
      released = true;
      if (input.corruptHeadCheckOnRelease === true) corruptArmed = true;
      hold.resolve();
    },
    heldReplies: () => heldReplies,
    owner,
  };
  const dispatch: FauxResponseFactory = async (context, options) => {
    if (isSummaryRequest(context)) {
      summaryRequests.push(context);
      contextReadsAtFork ??= contextReads;
      active += 1;
      peak = Math.max(peak, active);
      summaryArrived.resolve();
      try {
        await heldUntil(hold.promise, options?.signal, () => {
          summaryAborted = true;
        });
      } finally {
        active -= 1;
      }
      return input.summaryFails === true
        ? fauxAssistantMessage("", {
            stopReason: "error",
            errorMessage: "summarizer refused the request",
          })
        : fauxAssistantMessage(
            input.summaryText?.(summaryRequests.length) ??
              `background summary ${String(summaryRequests.length)}`,
          );
    }
    mainRequests.push(context);
    if (mainRequests.length === 1)
      return fauxAssistantMessage("d".repeat(40_000));
    const reply = await input.script(run, context, options?.signal);
    if (run.held()) heldReplies += 1;
    return reply;
  };
  faux.setResponses(Array.from({ length: 60 }, () => dispatch));
  const extensionId = ExtensionId.make("background-handoff");
  const handoff: RosteredExtensionV0 = defineExtension({
    id: extensionId,
    version: "1.0.0",
  }).define({
    compaction: () => ({
      consume: (view) => {
        consumed.contextReadsAtConsume.push(contextReads);
        if (input.weakViews === true) {
          consumed.weak.push(new WeakRef(view.messagesToSummarize));
          if (view.turnPrefixMessages.length > 0)
            consumed.weak.push(new WeakRef(view.turnPrefixMessages));
        } else consumed.views.push(view);
        input.onConsume?.(run);
        const answer: Effect.Effect<
          CompactionHandoffResultV0,
          CompactionHandoffErrorV0
        > =
          input.handoffFails === true
            ? Effect.fail({
                _tag: "CompactionHandoffError" as const,
                message: "archive refused",
              })
            : input.receipts === true
              ? Effect.succeed({
                  type: "accepted" as const,
                  receipt: `receipt-${String(consumed.contextReadsAtConsume.length)}`,
                })
              : Effect.succeed({ type: "discarded_by_policy" as const });
        return input.weakViews === true
          ? Effect.promise(
              () =>
                new Promise<void>((resolve) => {
                  setTimeout(resolve, 0);
                }),
            ).pipe(Effect.andThen(answer))
          : answer;
      },
    }),
  });
  const options: ShiitakeOptions = {
    store: owner,
    runtime: runtime.runtime,
    extensions: {
      roster: [handoff],
      grants: [{ id: extensionId, capabilities: ["compaction_handoff"] }],
    },
    instructions: {
      load: () =>
        Promise.resolve({ revision: "background-v1", text: "Answer." }),
    },
    model: {
      acquire: () =>
        Promise.resolve({
          model,
          retryable: () => false,
          stream: (streamModel, context, streamOptions) =>
            faux.stream(streamModel, context, streamOptions),
          release: () => Promise.resolve(),
        }),
    },
    tools: {
      sandbox: directSandbox(() => {
        toolCalls += 1;
        return Promise.resolve(
          input.toolResult?.(toolCalls, run) ??
            (toolCalls === 1 ? "t".repeat(14_000) : "ok"),
        );
      }),
    },
    compaction: {
      modelOverrides: {
        [`${model.provider}/${model.id}`]: {
          ...budgets,
          keepRecentTokens: input.keepRecentTokens ?? 6_000,
        },
      },
    },
  };
  return {
    fixture,
    owner,
    runtime,
    run,
    consumed,
    summaryArrived: summaryArrived.promise,
    peakSummaryCalls: () => peak,
    summaryAborted: () => summaryAborted,
    toolCalls: () => toolCalls,
    contextReads: () => contextReads,
    contextReadsAtFork: () => contextReadsAtFork,
    prefixReachableAtRead: () => prefixReachableAtRead,
    options,
  };
}

type Harness = ReturnType<typeof harness>;

async function settle(scenario: Harness) {
  while (scenario.runtime.snapshot().state === "pending")
    await scenario.runtime.runNext();
}

/** Seed one long exchange, then admit the run under test (`go`). */
async function startRun(scenario: Harness) {
  const agent = await Shiitake.make(scenario.options);
  const session = agent.session(sessionId);
  await session.command({
    type: "prompt",
    requestId: "seed",
    text: "Remember the plan.",
    delivery: "followUp",
  });
  await settle(scenario);
  await session.command({
    type: "prompt",
    requestId: "go",
    text: "p".repeat(28_000),
    delivery: "followUp",
  });
  return { agent, session, running: settle(scenario) };
}

function selected(scenario: Harness): readonly StoredSessionRow[] {
  return readSelectedPath(scenario.owner, { sessionId });
}

function compactionRows(scenario: Harness) {
  return selected(scenario).flatMap((row) =>
    row.entry.kind === "compaction" ? [{ row, entry: row.entry }] : [],
  );
}

function noticeRows(scenario: Harness) {
  return selected(scenario).flatMap(({ entry }) =>
    entry.kind === "compaction_notice" ? [entry] : [],
  );
}

/** Every notice must be a background `failed` one; returns their diagnostics. */
function failedDiagnostics(scenario: Harness): (string | undefined)[] {
  return noticeRows(scenario).map((notice) => {
    if (notice.result !== "failed" || notice.trigger !== "threshold")
      throw new Error(`Unexpected ${notice.trigger} ${notice.result} notice`);
    return notice.diagnostic;
  });
}

function isAssistantRow(row: StoredSessionRow): boolean {
  return row.entry.kind === "message" && row.entry.value.role === "assistant";
}

/**
 * A second writer commits a compaction whose head (the newest assistant
 * turn) is later than the background summary's head.
 */
function commitLaterCompaction(run: Run): void {
  const usage: Usage = {
    input: 1,
    output: 1,
    cacheRead: 0,
    cacheWrite: 0,
    totalTokens: 2,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
  };
  const path = readSelectedPath(run.owner, { sessionId });
  const leaf = path.at(-1);
  const later = path.filter(isAssistantRow).at(-1);
  if (leaf === undefined || later === undefined)
    throw new Error("Missing rows for the later compaction");
  appendCompactionTransition(run.owner, {
    sessionId,
    workId: requireAcceptedWorkId(run.owner, sessionId, "go"),
    expectedLeafId: leaf.id,
    head: later.id,
    summary: "external summary",
    trigger: "threshold",
    resumesRun: false,
    tokensBefore: 1,
    tokensAfter: 1,
    usage,
    contextRevision: leaf.id,
  });
}

/** The one stale notice a dropped summary leaves, with its spend. */
function expectOneStaleNotice(scenario: Harness): void {
  const notices = noticeRows(scenario);
  expect(notices).toEqual([
    expect.objectContaining({ result: "stale", trigger: "threshold" }),
  ]);
  expect(notices[0]).toHaveProperty("usage.totalTokens");
}

/**
 * The ordinary background script: tool turns while the summary is held,
 * release after two held replies, then finish once a request starts from the
 * placed summary (bounded, so a missing compaction fails instead of looping).
 */
function releaseAfterTwoHeldTurns(): Script {
  let afterRelease = 0;
  return (run, request) => {
    if (run.held() && run.heldReplies() >= 2) run.release();
    if (run.released()) afterRelease += 1;
    if (
      startsWithSummary(request) ||
      afterRelease > 4 ||
      run.mainRequests.length > 14
    )
      return fauxAssistantMessage("done");
    return toolTurn();
  };
}

it("(a) keeps the run going while the summary is held and places it at the next boundary", async () => {
  const scenario = harness({ script: releaseAfterTwoHeldTurns() });
  const { agent, running } = await startRun(scenario);
  try {
    await running;
    expect(scenario.run.summaryRequests).toHaveLength(1);
    expect(scenario.run.heldReplies()).toBeGreaterThanOrEqual(2);
    const path = selected(scenario);
    const compactions = compactionRows(scenario);
    expect(compactions).toHaveLength(1);
    const [placed] = compactions;
    if (placed === undefined) throw new Error("Missing compaction");
    const command = path.find(
      ({ entry }) => entry.kind === "command" && entry.requestId === "go",
    );
    // The head is the cut chosen at selection: the run's own prompt.
    expect(placed.entry.head).toBe(command?.id);
    const tail = path.find(
      ({ id }) => id === placed.entry.data.contextRevision,
    );
    if (tail === undefined) throw new Error("Missing selection tail");
    const firstToolResult = path.find(
      ({ entry }) =>
        entry.kind === "message" && entry.value.role === "toolResult",
    );
    // The selection tail is the first between-turn point (after tool result 1).
    expect(tail.sequence).toBeGreaterThanOrEqual(
      firstToolResult?.sequence ?? 0,
    );
    // Two or more assistant turns were recorded between selection and placement.
    expect(
      path.filter(
        (row) =>
          isAssistantRow(row) &&
          row.sequence > tail.sequence &&
          row.sequence < placed.row.sequence,
      ).length,
    ).toBeGreaterThanOrEqual(2);
    // Placed at a between-turn boundary: an assistant turn follows it.
    expect(
      path.some(
        (row) => isAssistantRow(row) && row.sequence > placed.row.sequence,
      ),
    ).toBe(true);
    // Mule's next request starts with the stored `model[0]`.
    const next = scenario.run.mainRequests.find(startsWithSummary);
    expect(requestText(next)).toContain("background summary 1");
    const firstModel = placed.entry.model[0];
    expect(contentText(firstModel.content)).toBe(requestText(next));
    expect(noticeRows(scenario)).toEqual([]);
  } finally {
    await agent.close();
    scenario.fixture.close();
  }
}, 30_000);

it("(b) drops a held summary as a stale notice when a later head was committed", async () => {
  let committed = false;
  const ordinary = releaseAfterTwoHeldTurns();
  const scenario = harness({
    script: (run, request, signal) => {
      if (run.held() && run.heldReplies() === 1 && !committed) {
        committed = true;
        commitLaterCompaction(run);
      }
      return ordinary(run, request, signal);
    },
  });
  const { agent, running } = await startRun(scenario);
  try {
    await running;
    expect(committed).toBe(true);
    expect(scenario.run.summaryRequests).toHaveLength(1);
    const compactions = compactionRows(scenario);
    expect(compactions).toHaveLength(1);
    const external = compactions[0]?.entry.model[0];
    expect(external && contentText(external.content)).toContain(
      "external summary",
    );
    const notices = noticeRows(scenario);
    expect(notices).toEqual([
      expect.objectContaining({ result: "stale", trigger: "threshold" }),
    ]);
    expect(notices[0]).toHaveProperty("usage.totalTokens");
    expect(scenario.consumed.views).toEqual([]);
  } finally {
    await agent.close();
    scenario.fixture.close();
  }
}, 30_000);

it("(c) cancelling the run leaves no entry, no notice and no activity", async () => {
  const cancelPoint = deferred();
  const scenario = harness({
    script: (run, _request, signal) => {
      if (run.held() && run.heldReplies() >= 2) {
        cancelPoint.resolve();
        return new Promise<AssistantMessage>((resolve) => {
          const aborted = () => {
            resolve(fauxAssistantMessage("", { stopReason: "aborted" }));
          };
          if (signal?.aborted === true) aborted();
          signal?.addEventListener("abort", aborted, { once: true });
        });
      }
      return run.mainRequests.length > 14
        ? fauxAssistantMessage("done")
        : toolTurn();
    },
  });
  const { agent, session, running } = await startRun(scenario);
  try {
    await within(cancelPoint.promise, "two turns with the summary held");
    expect(readProcessCompaction(scenario.owner, sessionId)).toEqual({
      trigger: "threshold",
    });
    await session.abort();
    await running;
    expect(scenario.summaryAborted()).toBe(true);
    expect(compactionRows(scenario)).toEqual([]);
    expect(noticeRows(scenario)).toEqual([]);
    expect(readProcessCompaction(scenario.owner, sessionId)).toBeNull();
    expect(scenario.consumed.views).toEqual([]);
    expect(
      scenario.fixture.owner.sql
        .exec(
          "SELECT outcome FROM watchdog_jobs WHERE job_id = ?",
          requireAcceptedWorkId(scenario.owner, sessionId, "go"),
        )
        .one(),
    ).toEqual({ outcome: "cancelled" });
    // The cancelled run left the owner's slot free: the next run starts a
    // background compaction of its own and places it.
    scenario.run.release();
    await session.command({
      type: "prompt",
      requestId: "after-cancel",
      text: "Carry on.",
      delivery: "followUp",
    });
    await settle(scenario);
    expect(scenario.run.summaryRequests).toHaveLength(2);
    expect(compactionRows(scenario)).toHaveLength(1);
  } finally {
    await running;
    await agent.close();
    scenario.fixture.close();
  }
}, 30_000);

it("(d) crossing the blocking threshold while the summary is held joins it: one summary request", async () => {
  let heldAtCrossing = false;
  let toolCallsAtSummary: number | undefined;
  const scenario = harness({
    script: (run, request) =>
      startsWithSummary(request) || run.mainRequests.length > 6
        ? fauxAssistantMessage("done")
        : toolTurn(),
    toolResult: (call, run) => {
      if (call === 1) return "t".repeat(14_000);
      if (call === 2) {
        heldAtCrossing = run.held();
        // Released only after the run has reached its blocking check.
        setTimeout(run.release, 150);
        return "h".repeat(90_000);
      }
      return "ok";
    },
  });
  void scenario.summaryArrived.then(() => {
    toolCallsAtSummary = scenario.toolCalls();
  });
  const { agent, running } = await startRun(scenario);
  try {
    await running;
    expect(scenario.run.summaryRequests).toHaveLength(1);
    // The one request is the background one, started before the crossing.
    expect(toolCallsAtSummary).toBe(1);
    expect(heldAtCrossing).toBe(true);
    expect(scenario.peakSummaryCalls()).toBe(1);
    const compactions = compactionRows(scenario);
    expect(compactions).toHaveLength(1);
    const path = selected(scenario);
    const lastToolResult = path
      .filter(
        ({ entry }) =>
          entry.kind === "message" && entry.value.role === "toolResult",
      )
      .at(-1);
    expect(compactions[0]?.row.sequence).toBeGreaterThan(
      lastToolResult?.sequence ?? Number.POSITIVE_INFINITY,
    );
    expect(
      startsWithSummary(scenario.run.mainRequests.at(-1) ?? { messages: [] }),
    ).toBe(true);
    expect(noticeRows(scenario)).toEqual([]);
  } finally {
    await agent.close();
    scenario.fixture.close();
  }
}, 30_000);

it("(e) reads no context path between fork and release", async () => {
  const scenario = harness({ script: releaseAfterTwoHeldTurns() });
  const { agent, running } = await startRun(scenario);
  try {
    await running;
    expect(scenario.run.summaryRequests).toHaveLength(1);
    expect(scenario.consumed.contextReadsAtConsume).toHaveLength(1);
    const atFork = scenario.contextReadsAtFork();
    if (atFork === undefined) throw new Error("The summary never forked");
    // Release follows the handoff at once: no read between fork and release.
    expect(scenario.consumed.contextReadsAtConsume[0]).toBe(atFork);
    // The rebuild read (placement step 4) comes only after the release.
    expect(scenario.contextReads()).toBeGreaterThan(atFork);
    expect(compactionRows(scenario)).toHaveLength(1);
  } finally {
    await agent.close();
    scenario.fixture.close();
  }
}, 30_000);

it("(f) hands the fork-time arrays to the Zookeeper handoff once", async () => {
  // 3000 kept tokens cut at the first assistant turn: a split turn, so the
  // borrowed view carries both arrays and two summary requests run in order.
  const scenario = harness({
    keepRecentTokens: 3_000,
    script: releaseAfterTwoHeldTurns(),
  });
  const { agent, running } = await startRun(scenario);
  try {
    await running;
    expect(scenario.consumed.views).toHaveLength(1);
    const [view] = scenario.consumed.views;
    if (view === undefined) throw new Error("Missing borrowed view");
    expect(view.messagesToSummarize.length).toBeGreaterThan(0);
    expect(view.turnPrefixMessages.length).toBeGreaterThan(0);
    expect(Object.isFrozen(view.messagesToSummarize)).toBe(true);
    expect(Object.isFrozen(view.turnPrefixMessages)).toBe(true);
    // The arrays are the ones the held requests serialized at fork time, and
    // no context read happened between that fork and the handoff.
    const [history, prefix] = scenario.run.summaryRequests;
    expect(requestText(history)).toContain(
      serializeConversation(view.messagesToSummarize),
    );
    expect(requestText(prefix)).toContain(
      serializeConversation(view.turnPrefixMessages),
    );
    expect(scenario.consumed.contextReadsAtConsume[0]).toBe(
      scenario.contextReadsAtFork(),
    );
    const [placed] = compactionRows(scenario);
    expect(Object.is(view.result.firstKeptEntryId, placed?.entry.head)).toBe(
      true,
    );
    expect(view.contextRevision).toBe(placed?.entry.data.contextRevision);
  } finally {
    await agent.close();
    scenario.fixture.close();
  }
}, 30_000);

it("a summary model error records the failed notice and keeps the context", async () => {
  const scenario = harness({
    summaryFails: true,
    script: releaseAfterTwoHeldTurns(),
  });
  const { agent, running } = await startRun(scenario);
  try {
    await running;
    expect(scenario.run.summaryRequests).toHaveLength(1);
    expect(scenario.consumed.views).toEqual([]);
    expect(compactionRows(scenario)).toEqual([]);
    expect(failedDiagnostics(scenario)).toEqual([
      "Summarization failed: summarizer refused the request",
    ]);
    expect(scenario.run.mainRequests.some(startsWithSummary)).toBe(false);
  } finally {
    await agent.close();
    scenario.fixture.close();
  }
}, 30_000);

it("a handoff failure records the failed notice, keeps the context and starts no second attempt", async () => {
  const scenario = harness({
    handoffFails: true,
    script: releaseAfterTwoHeldTurns(),
  });
  const { agent, running } = await startRun(scenario);
  try {
    await running;
    expect(scenario.consumed.views).toHaveLength(1);
    expect(compactionRows(scenario)).toEqual([]);
    expect(failedDiagnostics(scenario)).toEqual([
      "The compaction handoff failed; the conversation context is unchanged",
    ]);
    const completedUsage = scenario.consumed.views[0]?.result.usage;
    expect(completedUsage?.totalTokens).toBeGreaterThan(0);
    expect(noticeRows(scenario)[0]).toHaveProperty("usage", completedUsage);
    // Later boundaries stay above the background threshold; the run stopped
    // trying after the failure, so no second summary was requested.
    expect(scenario.run.summaryRequests).toHaveLength(1);
    expect(scenario.run.mainRequests.length).toBeGreaterThan(5);
    expect(scenario.run.mainRequests.some(startsWithSummary)).toBe(false);
    expect(readProcessCompaction(scenario.owner, sessionId)).toBeNull();
  } finally {
    await agent.close();
    scenario.fixture.close();
  }
}, 30_000);

it("a run that ends while its summary runs waits for it and places it after the terminal", async () => {
  const scenario = harness({
    script: (run) => {
      if (!run.held()) return toolTurn();
      // The run's last turn finishes while the summary is still held.
      setTimeout(run.release, 150);
      return fauxAssistantMessage("done");
    },
  });
  const { agent, running } = await startRun(scenario);
  try {
    await running;
    expect(scenario.run.summaryRequests).toHaveLength(1);
    const kinds = selected(scenario).map(({ entry }) => entry.kind);
    const compactions = compactionRows(scenario);
    expect(compactions).toHaveLength(1);
    expect(kinds.indexOf("compaction")).toBeGreaterThan(
      kinds.lastIndexOf("run_terminal"),
    );
    expect(compactions[0]?.entry.data).toMatchObject({
      reason: "threshold",
      resumesRun: false,
    });
    // It names what it summarized, not the leaf it was appended on.
    const leafBefore = selected(scenario).at(-2);
    expect(compactions[0]?.entry.data.contextRevision).not.toBe(leafBefore?.id);
    expect(scenario.consumed.views).toHaveLength(1);
    expect(noticeRows(scenario)).toEqual([]);
  } finally {
    await agent.close();
    scenario.fixture.close();
  }
}, 30_000);

/** Tool turns until the summary is held, then one overflowed reply. */
function overflowWhileHeld(): Script & { overflowed: () => boolean } {
  let overflowed = false;
  const script: Script = (run, request) => {
    if (startsWithSummary(request)) return fauxAssistantMessage("resumed");
    if (!run.held() || overflowed) return toolTurn();
    overflowed = true;
    setTimeout(run.release, 150);
    return fauxAssistantMessage("", {
      stopReason: "error",
      errorMessage: "prompt is too long: 70000 tokens > 60000 maximum",
    });
  };
  return Object.assign(script, { overflowed: () => overflowed });
}

function runOutcomes(scenario: Harness): string[] {
  return selected(scenario).flatMap(({ entry }) =>
    entry.kind === "run_terminal" ? [entry.outcome] : [],
  );
}

it("an overflow while a fitting summary runs joins it and resumes the run from it once", async () => {
  const script = overflowWhileHeld();
  const scenario = harness({ script });
  const { agent, running } = await startRun(scenario);
  try {
    await running;
    expect(script.overflowed()).toBe(true);
    expect(scenario.run.summaryRequests).toHaveLength(1);
    expect(noticeRows(scenario)).toEqual([]);
    const compactions = compactionRows(scenario);
    expect(compactions).toHaveLength(1);
    expect(compactions[0]?.entry.data).toMatchObject({
      reason: "overflow",
      resumesRun: true,
    });
    // The retry starts from the placed summary, without the failed reply.
    const retried = scenario.run.mainRequests.at(-1);
    expect(retried !== undefined && startsWithSummary(retried)).toBe(true);
    expect(JSON.stringify(retried)).not.toContain("prompt is too long");
    // The seed run's terminal, the overflowed attempt's, and the retry's.
    expect(
      selected(scenario).filter(({ entry }) => entry.kind === "run_terminal"),
    ).toHaveLength(3);
    expect(runOutcomes(scenario).at(-1)).toBe("completed");
  } finally {
    await agent.close();
    scenario.fixture.close();
  }
}, 30_000);

it("an overflow whose background summary would still be over is not placed: insufficient, then the ordinary recovery", async () => {
  const script = overflowWhileHeld();
  // The held summary alone is about 37K tokens: placed, the context would
  // still be over the 43616-token limit. The recovery's own summary is short.
  const scenario = harness({
    script,
    summaryText: (request) =>
      request === 1 ? "s".repeat(130_000) : "recovery summary",
  });
  const { agent, running } = await startRun(scenario);
  try {
    await running;
    expect(script.overflowed()).toBe(true);
    expect(scenario.run.summaryRequests).toHaveLength(2);
    const notices = noticeRows(scenario);
    expect(notices).toEqual([
      expect.objectContaining({ result: "insufficient", trigger: "threshold" }),
    ]);
    expect(notices[0]).toHaveProperty("usage.totalTokens");
    // The only entry is the ordinary recovery's, over the current context.
    const compactions = compactionRows(scenario);
    expect(compactions).toHaveLength(1);
    expect(compactions[0]?.entry.data).toMatchObject({
      reason: "overflow",
      resumesRun: true,
    });
    const placed = compactions[0]?.entry.model[0];
    expect(placed && contentText(placed.content)).toContain("recovery summary");
    // The handoff precedes the fresh read that measures tokensAfter, so the
    // unplaced background summary was handed off too, then the recovery.
    expect(scenario.consumed.views).toHaveLength(2);
    const retried = scenario.run.mainRequests.at(-1);
    expect(requestText(retried)).toContain("recovery summary");
    expect(requestText(retried)).not.toContain("s".repeat(1_000));
    expect(runOutcomes(scenario)).toEqual(["completed", "failed", "completed"]);
  } finally {
    await agent.close();
    scenario.fixture.close();
  }
}, 30_000);

it("a blocking join whose summary would still be over records insufficient, then the blocking compaction places its own", async () => {
  let heldAtCrossing = false;
  const scenario = harness({
    script: (run, request) =>
      startsWithSummary(request) || run.mainRequests.length > 6
        ? fauxAssistantMessage("done")
        : toolTurn(),
    toolResult: (call, run) => {
      if (call === 1) return "t".repeat(14_000);
      if (call === 2) {
        heldAtCrossing = run.held();
        // Released only after the run has reached its blocking check.
        setTimeout(run.release, 150);
        return "h".repeat(90_000);
      }
      return "ok";
    },
    // About 37K tokens: placed, the context would stay over the limit. The
    // blocking compaction's own summaries are short.
    summaryText: (request) =>
      request === 1 ? "s".repeat(130_000) : "blocking summary",
  });
  const { agent, running } = await startRun(scenario);
  try {
    await running;
    expect(heldAtCrossing).toBe(true);
    expect(scenario.peakSummaryCalls()).toBe(1);
    expect(scenario.run.summaryRequests.length).toBeGreaterThanOrEqual(2);
    const notices = noticeRows(scenario);
    expect(notices).toEqual([
      expect.objectContaining({ result: "insufficient", trigger: "threshold" }),
    ]);
    expect(notices[0]).toHaveProperty("usage.totalTokens");
    const compactions = compactionRows(scenario);
    expect(compactions).toHaveLength(1);
    expect(compactions[0]?.entry.data).toMatchObject({
      reason: "threshold",
      resumesRun: false,
    });
    const placed = compactions[0]?.entry.model[0];
    expect(placed && contentText(placed.content)).toContain("blocking summary");
    // Placed at that between-turn point: after the crossing tool result,
    // before the next assistant turn, which starts from it.
    const path = selected(scenario);
    const lastToolResult = path
      .filter(
        ({ entry }) =>
          entry.kind === "message" && entry.value.role === "toolResult",
      )
      .at(-1);
    expect(compactions[0]?.row.sequence).toBeGreaterThan(
      lastToolResult?.sequence ?? Number.POSITIVE_INFINITY,
    );
    const next = scenario.run.mainRequests.at(-1);
    expect(requestText(next)).toContain("blocking summary");
    expect(requestText(next)).not.toContain("s".repeat(1_000));
    expect(runOutcomes(scenario)).toEqual(["completed", "completed"]);
  } finally {
    await agent.close();
    scenario.fixture.close();
  }
}, 30_000);

it("a head that moves during the handoff is caught inside the append: stale, no entry", async () => {
  let committed = false;
  const scenario = harness({
    script: releaseAfterTwoHeldTurns(),
    onConsume: (run) => {
      committed = true;
      commitLaterCompaction(run);
    },
  });
  const { agent, running } = await startRun(scenario);
  try {
    await running;
    expect(committed).toBe(true);
    // The first head check passed, so the handoff ran once.
    expect(scenario.consumed.views).toHaveLength(1);
    const compactions = compactionRows(scenario);
    expect(compactions).toHaveLength(1);
    const external = compactions[0]?.entry.model[0];
    expect(external && contentText(external.content)).toContain(
      "external summary",
    );
    expectOneStaleNotice(scenario);
    expect(
      scenario.run.mainRequests.some((request) =>
        requestText(request).includes("background summary"),
      ),
    ).toBe(false);
  } finally {
    await agent.close();
    scenario.fixture.close();
  }
}, 30_000);

it("(e) the summarized prefix is unreachable when placement reads the context again", async () => {
  const scenario = harness({
    weakViews: true,
    script: releaseAfterTwoHeldTurns(),
  });
  const { agent, running } = await startRun(scenario);
  try {
    await running;
    expect(scenario.consumed.weak.length).toBeGreaterThan(0);
    expect(scenario.prefixReachableAtRead()).toBe(false);
    expect(compactionRows(scenario)).toHaveLength(1);
  } finally {
    await agent.close();
    scenario.fixture.close();
  }
}, 30_000);

it("a store failure in the head check is recorded with its own boundary", async () => {
  const scenario = harness({
    corruptHeadCheckOnRelease: true,
    script: releaseAfterTwoHeldTurns(),
  });
  const { agent, running } = await startRun(scenario);
  try {
    await running;
    expect(scenario.run.summaryRequests).toHaveLength(1);
    expect(scenario.consumed.views).toEqual([]);
    expect(compactionRows(scenario)).toEqual([]);
    expect(failedDiagnostics(scenario)).toEqual([
      "Compaction failed (store: unavailable)",
    ]);
  } finally {
    await agent.close();
    scenario.fixture.close();
  }
}, 30_000);

it("two triggers on one owner never make two concurrent summary requests", async () => {
  let triggersWhileHeld = 0;
  const scenario = harness({
    script: (run, request) => {
      // Each held turn ends above the background threshold: another trigger.
      if (run.held()) triggersWhileHeld += 1;
      if (run.held() && run.heldReplies() >= 3) run.release();
      return startsWithSummary(request) || run.mainRequests.length > 14
        ? fauxAssistantMessage("done")
        : toolTurn();
    },
  });
  const { agent, running } = await startRun(scenario);
  try {
    await running;
    expect(triggersWhileHeld).toBeGreaterThanOrEqual(3);
    expect(scenario.run.summaryRequests).toHaveLength(1);
    expect(scenario.peakSummaryCalls()).toBe(1);
    expect(compactionRows(scenario)).toHaveLength(1);
  } finally {
    await agent.close();
    scenario.fixture.close();
  }
}, 30_000);

it("an unclamped background threshold below zero is gated by keepRecentTokens, as in Pi (generation.ts:322-339)", async () => {
  // A 30K window with the default 16384 reserve blocks above 13616 tokens; the
  // default 32768 background tokens put the background threshold below zero.
  // Only the cut (4000 kept tokens here) decides whether one starts.
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const faux = fauxModel([], {
    models: [{ id: "small-1", contextWindow: 30_000, maxTokens: 100_000 }],
  });
  const model = faux.model;
  const summaryRequests: Context[] = [];
  const dispatch: FauxResponseFactory = (context) => {
    if (!isSummaryRequest(context)) return fauxAssistantMessage("ok");
    summaryRequests.push(context);
    return fauxAssistantMessage("small window summary");
  };
  faux.setResponses(Array.from({ length: 20 }, () => dispatch));
  const agent = await Shiitake.make({
    store: fixture.owner,
    runtime: runtime.runtime,
    extensions: { roster: [], grants: [] },
    instructions: {
      load: () => Promise.resolve({ revision: "small-v1", text: "Answer." }),
    },
    model: {
      acquire: () =>
        Promise.resolve({
          model,
          retryable: () => false,
          stream: (streamModel, context, options) =>
            faux.stream(streamModel, context, options),
          release: () => Promise.resolve(),
        }),
    },
    compaction: {
      modelOverrides: {
        [`${model.provider}/${model.id}`]: { keepRecentTokens: 4_000 },
      },
    },
  });
  const prompt = async (requestId: string, text: string) => {
    await agent.session(sessionId).command({
      type: "prompt",
      requestId,
      text,
      delivery: "followUp",
    });
    while (runtime.snapshot().state === "pending") await runtime.runNext();
  };
  const compactions = () =>
    readSelectedPath(fixture.owner, { sessionId }).filter(
      ({ entry }) => entry.kind === "compaction",
    );
  try {
    await prompt("small-1", "Hello.");
    await prompt("small-2", "Still small.");
    // Below keepRecentTokens: the cut keeps everything, nothing starts.
    expect(summaryRequests).toEqual([]);
    expect(compactions()).toEqual([]);
    // About 5700 tokens: past keepRecentTokens, still below blocking.
    await prompt("small-3", "p".repeat(20_000));
    expect(summaryRequests).toHaveLength(1);
    expect(compactions()).toHaveLength(1);
    expect(compactions()[0]?.entry).toMatchObject({
      data: { reason: "threshold" },
    });
  } finally {
    await agent.close();
    fixture.close();
  }
}, 30_000);

/** Every compaction entry's stored receipt, and the whole store's text. */
function storedReceipts(scenario: Harness) {
  const entries = compactionRows(scenario).map(
    ({ entry }) => entry.data.receipt,
  );
  const all = scenario.owner.sql
    .exec("SELECT data_json FROM session_entries")
    .toArray()
    .map((row) => JSON.stringify(row))
    .join("\n");
  return { entries, all };
}

it("stores the receipt in the entry that places a background summary", async () => {
  const scenario = harness({
    script: releaseAfterTwoHeldTurns(),
    receipts: true,
  });
  const { agent, running } = await startRun(scenario);
  try {
    await running;
    expect(scenario.consumed.views).toHaveLength(1);
    expect(storedReceipts(scenario).entries).toEqual(["receipt-1"]);
  } finally {
    await agent.close();
    scenario.fixture.close();
  }
}, 30_000);

it("stores the receipt in the entry that resumes an overflowed run", async () => {
  const script = overflowWhileHeld();
  const scenario = harness({ script, receipts: true });
  const { agent, running } = await startRun(scenario);
  try {
    await running;
    expect(script.overflowed()).toBe(true);
    expect(compactionRows(scenario)[0]?.entry.data).toMatchObject({
      reason: "overflow",
      resumesRun: true,
      receipt: "receipt-1",
    });
    expect(storedReceipts(scenario).entries).toEqual(["receipt-1"]);
  } finally {
    await agent.close();
    scenario.fixture.close();
  }
}, 30_000);

it("keeps no receipt for a summary dropped as insufficient; only the committed entry's is stored", async () => {
  const scenario = harness({
    script: overflowWhileHeld(),
    receipts: true,
    summaryText: (request) =>
      request === 1 ? "s".repeat(130_000) : "recovery summary",
  });
  const { agent, running } = await startRun(scenario);
  try {
    await running;
    expect(scenario.consumed.views).toHaveLength(2);
    expect(noticeRows(scenario)).toEqual([
      expect.objectContaining({ result: "insufficient" }),
    ]);
    const { entries, all } = storedReceipts(scenario);
    expect(entries).toEqual(["receipt-2"]);
    expect(all).not.toContain("receipt-1");
  } finally {
    await agent.close();
    scenario.fixture.close();
  }
}, 30_000);

it("keeps no receipt for a summary that goes stale inside the append", async () => {
  const scenario = harness({
    script: releaseAfterTwoHeldTurns(),
    receipts: true,
    onConsume: commitLaterCompaction,
  });
  const { agent, running } = await startRun(scenario);
  try {
    await running;
    expect(scenario.consumed.views).toHaveLength(1);
    expectOneStaleNotice(scenario);
    const { entries, all } = storedReceipts(scenario);
    expect(entries).toEqual([undefined]);
    expect(all).not.toContain("receipt-1");
  } finally {
    await agent.close();
    scenario.fixture.close();
  }
}, 30_000);
