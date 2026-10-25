import { fauxModel } from "@fungi.computer/mule/testing";
import { getSystemMessageText, type Context } from "@earendil-works/pi-ai";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import * as Effect from "effect/Effect";
import { expect, it } from "vitest";
import { requestMessages } from "./fixtures/model-context.js";

import {
  defineExtension,
  ExtensionId,
  type ExtensionHookError,
  type RosteredExtensionV0,
} from "../src/extensions.js";
import {
  Shiitake,
  type BorrowedCompactionViewV0,
  type CompactionHandoffErrorV0,
  type CompactionHandoffResultV0,
  type ShiitakeOptions,
} from "../src/index.js";
import {
  buildSessionContext,
  readSelectedPath,
} from "../src/internal/woodstock/session-store.js";
import { estimateContextTokens } from "@earendil-works/pi-ai/utils/estimate";
import { DEFAULT_COMPACTION_SETTINGS } from "../src/internal/compaction/utils.js";
import {
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";
import { requireAcceptedWorkId } from "./fixtures/accepted-work.fixture.js";

function compactionOptions(
  fixture: ReturnType<typeof createNativeSqliteOwnerFixture>,
  faux: ReturnType<typeof fauxModel>,
  runtime: ReturnType<typeof createCompatibilityRuntimeHost>,
  extensions: readonly RosteredExtensionV0[] = [],
): ShiitakeOptions {
  return {
    store: fixture.owner,
    runtime: runtime.runtime,
    extensions: handoffRoster(extensions),
    instructions: {
      load: () =>
        Promise.resolve({ revision: "real-compaction-v1", text: "Answer." }),
    },
    model: {
      acquire: () =>
        Promise.resolve({
          model: faux.model,
          retryable: () => false,
          stream: (model, context, options) =>
            faux.stream(model, context, options),
          release: () => Promise.resolve(),
        }),
    },
  };
}

/** Grant each roster member the compaction handoff it requests. */
function handoffRoster(extensions: readonly RosteredExtensionV0[]) {
  return {
    roster: extensions,
    grants: extensions.map(({ manifest }) => ({
      id: manifest.id,
      capabilities: ["compaction_handoff" as const],
    })),
  };
}

/** One agent-lifetime compaction handoff whose scope `release` closes. */
function compactionExtension(
  id: string,
  consume: (
    view: BorrowedCompactionViewV0,
  ) =>
    | CompactionHandoffResultV0
    | Promise<CompactionHandoffResultV0>
    | Effect.Effect<
        CompactionHandoffResultV0,
        CompactionHandoffErrorV0 | ExtensionHookError
      >,
  release?: () => void,
): RosteredExtensionV0 {
  const extensionId = ExtensionId.make(id);
  return defineExtension({ id: extensionId, version: "1.0.0" }).define({
    hooks: {
      start: () =>
        Effect.acquireRelease(Effect.void, () =>
          Effect.sync(() => release?.()),
        ),
    },
    compaction: () => ({ consume }),
  });
}

function largeCompactionResponses(faux: ReturnType<typeof fauxModel>) {
  faux.setResponses([
    fauxAssistantMessage("x".repeat(100_000)),
    fauxAssistantMessage("durable summary"),
  ]);
}

function createRealCompactionScenario() {
  const fixture = createNativeSqliteOwnerFixture();
  const faux = fauxModel([], {
    models: [{ id: "faux-1", contextWindow: 30_000, maxTokens: 100_000 }],
    tokenSize: { min: 100, max: 100 },
  });
  const requests: Context[] = [];
  let contextWindow = 30_000;
  let received: BorrowedCompactionViewV0 | undefined;
  const handoffLeafKinds: string[] = [];
  const handoff = {
    consume: async (view: BorrowedCompactionViewV0) => {
      await Promise.resolve();
      received = view;
      const path = readSelectedPath(fixture.owner, {
        sessionId: "real-compaction-caller",
      });
      handoffLeafKinds.push(path.at(-1)?.entry.kind ?? "missing");
      return { type: "discarded_by_policy" as const };
    },
  };
  const extension = compactionExtension(
    "real-compaction-handoff",
    handoff.consume,
  );
  const makeOptions = (
    runtime: ReturnType<typeof createCompatibilityRuntimeHost>,
    capture: boolean,
  ): ShiitakeOptions => ({
    store: fixture.owner,
    runtime: runtime.runtime,
    extensions: handoffRoster([extension]),
    instructions: {
      load: () =>
        Promise.resolve({ revision: "real-compaction-v1", text: "Answer." }),
    },
    model: {
      acquire: () => {
        const model = faux.model;
        model.contextWindow = contextWindow;
        return Promise.resolve({
          model,
          retryable: () => false,
          stream: (model, context, options) => {
            if (capture) requests.push(structuredClone(context));
            return faux.stream(model, context, options);
          },
          release: () => Promise.resolve(),
        });
      },
    },
  });
  return {
    fixture,
    faux,
    requests,
    get received() {
      return received;
    },
    handoffLeafKinds,
    setContextWindow(value: number) {
      contextWindow = value;
    },
    makeOptions,
  };
}

async function executeRealCompaction(
  scenario: ReturnType<typeof createRealCompactionScenario>,
) {
  const runtime = createCompatibilityRuntimeHost();
  scenario.faux.setResponses([
    fauxAssistantMessage("x".repeat(100_000)),
    fauxAssistantMessage("durable summary"),
  ]);
  const agent = await Shiitake.make(scenario.makeOptions(runtime, false));
  const session = agent.session("real-compaction-caller");
  await session.command({
    requestId: "real-compaction-first",
    delivery: "followUp",

    type: "prompt",
    text: "Build a durable context.",
  });
  await runtime.runNext();
  return { scenario, agent, session };
}

async function assertInitialCompaction(
  run: Awaited<ReturnType<typeof executeRealCompaction>>,
) {
  const path = readSelectedPath(run.scenario.fixture.owner, {
    sessionId: "real-compaction-caller",
  });
  const compactionIndex = path.findIndex(
    ({ entry }) => entry.kind === "compaction",
  );
  const terminalIndex = path.findIndex(
    ({ entry }) => entry.kind === "run_terminal",
  );
  expect(compactionIndex).toBeGreaterThan(terminalIndex);
  expect(run.scenario.handoffLeafKinds).toEqual(["run_terminal"]);
  expect(run.scenario.received?.sessionId).toBe("real-compaction-caller");
  expect(run.scenario.received?.messagesToSummarize).toBeDefined();
  expect(run.scenario.received?.result.summary).toContain("durable summary");
  // The client now shows full history, so the kept tail is located via the
  // compaction marker rather than assumed to start the message array.
  const snapshot = await run.session.read();
  const marker = snapshot.compactions?.[0];
  expect(marker).toBeDefined();
  const keptMessage = snapshot.messages.find(
    ({ sequence }) => sequence >= (marker?.sequence ?? 0),
  );
  expect(keptMessage?.value).toMatchObject({ role: "assistant" });
  expect(snapshot.messages[0]?.value).toMatchObject({ role: "user" });
}

async function reopenRealCompaction(
  scenario: ReturnType<typeof createRealCompactionScenario>,
) {
  scenario.faux.setResponses([fauxAssistantMessage("next answer")]);
  const runtime = createCompatibilityRuntimeHost();
  const reopened = await Shiitake.make(scenario.makeOptions(runtime, true));
  await reopened.session("real-compaction-caller").command({
    requestId: "real-compaction-second",
    delivery: "followUp",

    type: "prompt",
    text: "Continue the durable context.",
  });
  await runtime.runNext();
  return reopened;
}

function assertSummaryMessage(
  message: Context["messages"][number] | undefined,
) {
  if (message?.role !== "user" || !Array.isArray(message.content))
    throw new Error("Missing compaction summary message");
  const summaryText = message.content.find((part) => part.type === "text");
  expect(summaryText?.type).toBe("text");
  if (summaryText?.type !== "text") throw new Error("Missing summary text");
  expect(summaryText.text).toContain("durable summary\n</summary>");
}

function assertContinuationMessage(
  message: Context["messages"][number] | undefined,
) {
  expect(message?.role).toBe("user");
  if (message?.role !== "user") throw new Error("Missing continuation prompt");
  expect(message.content).toBe("Continue the durable context.");
}

function assertReopenedContext(requests: readonly Context[]) {
  const nextMessages = requestMessages(requests.at(-1));
  assertSummaryMessage(nextMessages[0]);
  expect(nextMessages[1]?.role).toBe("assistant");
  assertContinuationMessage(nextMessages[2]);
}

it("runs the admitted handoff after run_terminal and rebuilds the Pi horizon", async () => {
  const scenario = createRealCompactionScenario();
  const run = await executeRealCompaction(scenario);
  let agent: Awaited<ReturnType<typeof Shiitake.make>> | undefined = run.agent;
  let reopened: Awaited<ReturnType<typeof Shiitake.make>> | undefined;
  try {
    await assertInitialCompaction(run);
    const original = agent;
    agent = undefined;
    await original.close();
    reopened = await reopenRealCompaction(scenario);
    assertReopenedContext(scenario.requests);
  } finally {
    await agent?.close();
    await reopened?.close();
    scenario.fixture.close();
  }
});

it("excludes stale usage through the persisted Session caller", async () => {
  const scenario = createRealCompactionScenario();
  const run = await executeRealCompaction(scenario);
  let agent: Awaited<ReturnType<typeof Shiitake.make>> | undefined = run.agent;
  let reopened: Awaited<ReturnType<typeof Shiitake.make>> | undefined;
  let reopenedAgain: Awaited<ReturnType<typeof Shiitake.make>> | undefined;
  try {
    const path = readSelectedPath(scenario.fixture.owner, {
      sessionId: "real-compaction-caller",
    });
    expect(
      path.filter(({ entry }) => entry.kind === "compaction"),
    ).toHaveLength(1);
    const retained = path.find(
      ({ entry }) =>
        entry.kind === "message" && entry.value.role === "assistant",
    );
    expect(
      retained?.entry.kind === "message" &&
        retained.entry.value.role === "assistant"
        ? retained.entry.value.usage.totalTokens
        : 0,
    ).toBeGreaterThan(30_000 - DEFAULT_COMPACTION_SETTINGS.reserveTokens);
    const context = buildSessionContext(path);
    const estimate = estimateContextTokens(context);
    expect(estimate.tokens).toBeGreaterThan(
      30_000 - DEFAULT_COMPACTION_SETTINGS.reserveTokens,
    );

    const original = agent;
    agent = undefined;
    await original.close();
    const runtime = createCompatibilityRuntimeHost();
    scenario.setContextWindow(100_000);
    reopened = await Shiitake.make(scenario.makeOptions(runtime, false));
    const session = reopened.session("real-compaction-caller");
    const threshold = 100_000 - DEFAULT_COMPACTION_SETTINGS.reserveTokens;
    const prompt = "p".repeat(8_000);
    let preRunRequest = -1;
    for (let index = 0; index < 40; index += 1) {
      const pathBefore = readSelectedPath(scenario.fixture.owner, {
        sessionId: "real-compaction-caller",
      });
      const contextBefore = estimateContextTokens(
        buildSessionContext(pathBefore),
      );
      const crossesBeforeRun =
        contextBefore.lastUsageIndex !== null &&
        contextBefore.tokens + Math.ceil(prompt.length / 4) > threshold - 1_000;
      scenario.faux.setResponses(
        crossesBeforeRun
          ? [
              fauxAssistantMessage(`pre-run summary ${String(index)}`),
              fauxAssistantMessage("e".repeat(100_000), {
                stopReason: "error",
                errorMessage: "provider failed after pre-run compaction",
              }),
              fauxAssistantMessage(`stale second summary ${String(index)}`),
              fauxAssistantMessage(
                `stale second turn summary ${String(index)}`,
              ),
            ]
          : [fauxAssistantMessage(`small answer ${String(index)}`)],
      );
      await session.command({
        requestId: `real-compaction-growth-${String(index)}`,
        delivery: "followUp",

        type: "prompt",
        text: `${prompt}${String(index)}`,
      });
      await runtime.runNext();
      const count = readSelectedPath(scenario.fixture.owner, {
        sessionId: "real-compaction-caller",
      }).filter(({ entry }) => entry.kind === "compaction").length;
      if (count > 1) {
        preRunRequest = index;
        break;
      }
    }
    expect(preRunRequest).toBeGreaterThanOrEqual(0);
    expect(scenario.handoffLeafKinds.at(-1)).toBe("command");
    const afterFailure = readSelectedPath(scenario.fixture.owner, {
      sessionId: "real-compaction-caller",
    });
    expect(
      afterFailure.filter(({ entry }) => entry.kind === "compaction"),
    ).toHaveLength(2);
    expect(scenario.faux.getPendingResponseCount()).toBe(2);
    scenario.setContextWindow(30_000);
    const beforeFreshCompactions = afterFailure.filter(
      ({ entry }) => entry.kind === "compaction",
    ).length;

    scenario.faux.setResponses([
      fauxAssistantMessage("x".repeat(100_000)),
      fauxAssistantMessage("fresh history summary"),
      fauxAssistantMessage("fresh turn summary"),
    ]);
    await session.command({
      requestId: "real-compaction-fresh-follow-up",
      delivery: "followUp",

      type: "prompt",
      text: "Add fresh post-boundary context.",
    });
    await runtime.runNext();
    const afterFresh = readSelectedPath(scenario.fixture.owner, {
      sessionId: "real-compaction-caller",
    });
    expect(
      afterFresh.filter(({ entry }) => entry.kind === "compaction"),
    ).toHaveLength(beforeFreshCompactions + 1);

    const beforeReopen = [...afterFresh]
      .reverse()
      .find(
        ({ entry }) =>
          entry.kind === "message" &&
          entry.value.role === "assistant" &&
          entry.value.content.some(
            (part) => part.type === "text" && part.text.length === 100_000,
          ),
      );
    if (beforeReopen?.entry.kind !== "message")
      throw new Error("Missing fresh stored assistant");
    if (beforeReopen.entry.value.role !== "assistant")
      throw new Error("Missing fresh stored assistant");
    const freshUsage = beforeReopen.entry.value.usage;
    const beforeReopenCompactions = afterFresh.filter(
      ({ entry }) => entry.kind === "compaction",
    ).length;
    await reopened.close();
    reopened = undefined;
    const reopenRuntime = createCompatibilityRuntimeHost();
    reopenedAgain = await Shiitake.make(
      scenario.makeOptions(reopenRuntime, false),
    );
    scenario.faux.setResponses([fauxAssistantMessage("after reopen")]);
    await reopenedAgain.session("real-compaction-caller").command({
      requestId: "real-compaction-reopened-follow-up",
      delivery: "followUp",

      type: "prompt",
      text: "Continue after reopening.",
    });
    await reopenRuntime.runNext();
    const afterReopen = readSelectedPath(scenario.fixture.owner, {
      sessionId: "real-compaction-caller",
    });
    expect(
      afterReopen.filter(({ entry }) => entry.kind === "compaction"),
    ).toHaveLength(beforeReopenCompactions);
    expect(
      [...afterReopen]
        .reverse()
        .find(
          ({ entry }) =>
            entry.kind === "message" &&
            entry.value.role === "assistant" &&
            entry.value.content.some(
              (part) => part.type === "text" && part.text.length === 100_000,
            ),
        )?.entry,
    ).toMatchObject({
      kind: "message",
      value: { role: "assistant", usage: freshUsage },
    });
  } finally {
    await agent?.close();
    await reopened?.close();
    await reopenedAgain?.close();
    scenario.fixture.close();
  }
}, 30_000);

it("compacts without an installed handoff consumer", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const faux = fauxModel([], {
    models: [{ id: "faux-1", contextWindow: 30_000, maxTokens: 10_000 }],
    tokenSize: { min: 100, max: 100 },
  });
  largeCompactionResponses(faux);
  const runtime = createCompatibilityRuntimeHost();
  const agent = await Shiitake.make(compactionOptions(fixture, faux, runtime));
  try {
    const session = agent.session("no-handoff-compaction");
    await session.command({
      requestId: "no-handoff-first",
      delivery: "followUp",

      type: "prompt",
      text: "Compact without an extension.",
    });
    await runtime.runNext();
    expect(
      readSelectedPath(fixture.owner, {
        sessionId: "no-handoff-compaction",
      }).filter(({ entry }) => entry.kind === "compaction"),
    ).toHaveLength(1);
  } finally {
    await agent.close();
    fixture.close();
  }
});

it("recovers one length result without retrying compaction", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const faux = fauxModel([], {
    models: [{ id: "faux-1", contextWindow: 30_000, maxTokens: 100_000 }],
    tokenSize: { min: 100, max: 100 },
  });
  const failedText = "x".repeat(100_000);
  const firstLength = fauxAssistantMessage(failedText, {
    stopReason: "length",
  });
  faux.setResponses([
    firstLength,
    fauxAssistantMessage("length recovery summary"),
    fauxAssistantMessage("second length", { stopReason: "length" }),
    fauxAssistantMessage("must not run"),
  ]);
  const modelContexts: Context[] = [];
  const summaryContexts: Context[] = [];
  const runtime = createCompatibilityRuntimeHost();
  const base = compactionOptions(fixture, faux, runtime);
  const agent = await Shiitake.make({
    ...base,
    model: {
      acquire: () =>
        Promise.resolve({
          model: faux.model,
          retryable: () => false,
          stream: (model, context, options) => {
            const target = (context.messages[0]?.role === "system"
              ? getSystemMessageText(context.messages[0])
              : ""
            ).startsWith("You are a context summarization assistant")
              ? summaryContexts
              : modelContexts;
            target.push(structuredClone(context));
            return faux.stream(model, context, options);
          },
          release: () => Promise.resolve(),
        }),
    },
  });
  try {
    const session = agent.session("length-recovery-compaction");
    await session.command({
      requestId: "length-recovery-first",
      delivery: "followUp",

      type: "prompt",
      text: "Recover the truncated answer once.",
    });
    await runtime.runNext();

    const path = readSelectedPath(fixture.owner, {
      sessionId: "length-recovery-compaction",
    });
    expect(
      path.filter(({ entry }) => entry.kind === "compaction"),
    ).toHaveLength(1);
    expect(
      path.filter(({ entry }) => entry.kind === "run_terminal"),
    ).toHaveLength(2);
    expect(modelContexts).toHaveLength(2);
    expect(summaryContexts).toHaveLength(1);
    expect(
      modelContexts[1]?.messages.some(
        (message) =>
          message.role === "assistant" &&
          message.content.some(
            (part) => part.type === "text" && part.text === failedText,
          ),
      ),
    ).toBe(false);
    expect(
      modelContexts[1]?.messages.some(
        (message) =>
          message.role === "user" &&
          Array.isArray(message.content) &&
          message.content.some(
            (part) =>
              part.type === "text" &&
              part.text.includes(
                "The conversation history before this point was compacted into the following summary:",
              ) &&
              part.text.includes("length recovery summary"),
          ),
      ),
    ).toBe(true);
    expect(faux.state.callCount).toBe(3);
    expect(faux.getPendingResponseCount()).toBe(1);
  } finally {
    await agent.close();
    fixture.close();
  }
});

it("keeps the completed outcome when the handoff rejects", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const faux = fauxModel([], {
    models: [{ id: "faux-1", contextWindow: 30_000, maxTokens: 10_000 }],
    tokenSize: { min: 100, max: 100 },
  });
  largeCompactionResponses(faux);
  const runtime = createCompatibilityRuntimeHost();
  const extension = compactionExtension("failing-handoff", () =>
    Effect.fail({
      _tag: "CompactionHandoffError" as const,
      message: "handoff rejected",
    }),
  );
  const agent = await Shiitake.make(
    compactionOptions(fixture, faux, runtime, [extension]),
  );
  try {
    const session = agent.session("failed-handoff-compaction");
    await session.command({
      requestId: "failed-handoff-first",
      delivery: "followUp",

      type: "prompt",
      text: "Keep the completed answer.",
    });
    const workId = requireAcceptedWorkId(
      fixture.owner,
      "failed-handoff-compaction",
      "failed-handoff-first",
    );
    await runtime.runNext();
    const path = readSelectedPath(fixture.owner, {
      sessionId: "failed-handoff-compaction",
    });
    expect(path.some(({ entry }) => entry.kind === "run_terminal")).toBe(true);
    expect(path.some(({ entry }) => entry.kind === "compaction")).toBe(false);
    expect((await session.read()).messages).toHaveLength(2);
    expect(
      fixture.owner.sql
        .exec(
          "SELECT state, outcome FROM watchdog_jobs WHERE job_id = ?",
          workId,
        )
        .one(),
    ).toEqual({ state: "settled", outcome: "completed" });
  } finally {
    await agent.close();
    fixture.close();
  }
});

it("cancels a blocked post-terminal handoff through Watchdog", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const faux = fauxModel([], {
    models: [{ id: "faux-1", contextWindow: 30_000, maxTokens: 10_000 }],
    tokenSize: { min: 100, max: 100 },
  });
  faux.setResponses([
    fauxAssistantMessage("x".repeat(100_000)),
    fauxAssistantMessage("summary"),
  ]);
  let resolveStarted: () => void = () => undefined;
  const started = new Promise<void>((resolve) => {
    resolveStarted = resolve;
  });
  let interrupted = false;
  const trace: string[] = [];
  const runtime = createCompatibilityRuntimeHost();
  const extension = compactionExtension(
    "blocked-handoff",
    () =>
      Effect.callback<CompactionHandoffResultV0, CompactionHandoffErrorV0>(
        (resume) => {
          void resume;
          resolveStarted();
          return Effect.sync(() => {
            interrupted = true;
          });
        },
      ),
    () => trace.push("release"),
  );
  const agent = await Shiitake.make(
    compactionOptions(fixture, faux, runtime, [extension]),
  );
  let running: Promise<void> | undefined;
  try {
    const session = agent.session("blocked-handoff-compaction");
    await session.command({
      requestId: "blocked-handoff-first",
      delivery: "followUp",

      type: "prompt",
      text: "Cancel while handoff is blocked.",
    });
    const workId = requireAcceptedWorkId(
      fixture.owner,
      "blocked-handoff-compaction",
      "blocked-handoff-first",
    );
    running = runtime.runNext();
    await started;
    await session.abort();
    await running;
    running = undefined;
    expect(interrupted).toBe(true);
    expect(
      readSelectedPath(fixture.owner, {
        sessionId: "blocked-handoff-compaction",
      }).some(({ entry }) => entry.kind === "compaction"),
    ).toBe(false);
    expect(
      fixture.owner.sql
        .exec(
          "SELECT state, outcome FROM watchdog_jobs WHERE job_id = ?",
          workId,
        )
        .one(),
    ).toEqual({ state: "settled", outcome: "cancelled" });
    // The handoff is agent-lifetime: its scope outlives the cancelled run.
    expect(trace).toEqual([]);
    await agent.close();
    expect(trace).toEqual(["release"]);
  } finally {
    await running;
    await agent.close();
    fixture.close();
  }
});

it("keeps cancelled queued text out of the compaction request and later context", async () => {
  const scenario = createRealCompactionScenario();
  const runtime = createCompatibilityRuntimeHost();
  const agent = await Shiitake.make(scenario.makeOptions(runtime, true));
  try {
    const session = agent.session("real-compaction-caller");
    await session.send("WITHDRAWN: secret instruction never delivered");
    await session.abort();
    scenario.faux.setResponses([
      fauxAssistantMessage("x".repeat(100_000)),
      fauxAssistantMessage("durable summary"),
      fauxAssistantMessage("durable summary"),
    ]);
    await session.send("Build a durable context.");
    for (
      let pass = 0;
      pass < 10 && runtime.snapshot().state === "pending";
      pass++
    )
      await runtime.runNext();
    expect(scenario.received?.result.summary).toContain("durable summary");
    expect(scenario.requests.length).toBeGreaterThanOrEqual(2);
    expect(
      scenario.requests.some((request) =>
        JSON.stringify(request).includes("WITHDRAWN"),
      ),
    ).toBe(false);
    scenario.faux.setResponses([fauxAssistantMessage("continued")]);
    await session.send("Continue.");
    for (
      let pass = 0;
      pass < 10 && runtime.snapshot().state === "pending";
      pass++
    )
      await runtime.runNext();
    expect(
      scenario.requests.some((request) =>
        JSON.stringify(request).includes("WITHDRAWN"),
      ),
    ).toBe(false);
    expect(
      (await session.read()).messages.some(
        ({ value }) =>
          value.role === "user" &&
          JSON.stringify(value.content).includes("WITHDRAWN"),
      ),
    ).toBe(true);
  } finally {
    await agent.close();
    scenario.fixture.close();
  }
});

/** A context window large enough that no automatic trigger fires. */
function manualCompactionScenario(
  extensions: readonly RosteredExtensionV0[] = [],
) {
  const fixture = createNativeSqliteOwnerFixture();
  const faux = fauxModel([], {
    models: [{ id: "faux-1", contextWindow: 1_000_000, maxTokens: 100_000 }],
    tokenSize: { min: 100, max: 100 },
  });
  const open = async () => {
    const runtime = createCompatibilityRuntimeHost();
    const agent = await Shiitake.make(
      compactionOptions(fixture, faux, runtime, extensions),
    );
    return { runtime, agent };
  };
  return { fixture, faux, open };
}

const manualSession = "manual-compaction";

/** Two long exchanges: enough history to summarize, too little to trigger. */
async function buildHistory(
  scenario: ReturnType<typeof manualCompactionScenario>,
  opened: Awaited<
    ReturnType<ReturnType<typeof manualCompactionScenario>["open"]>
  >,
) {
  const session = opened.agent.session(manualSession);
  for (const [index, fill] of ["x", "y"].entries()) {
    scenario.faux.setResponses([fauxAssistantMessage(fill.repeat(100_000))]);
    await session.command({
      requestId: `manual-history-${String(index)}`,
      delivery: "followUp",
      type: "prompt",
      text: `Build history ${String(index)}.`,
    });
    await opened.runtime.runNext();
  }
  return session;
}

function compactionsIn(
  owner: ReturnType<typeof createNativeSqliteOwnerFixture>["owner"],
) {
  return readSelectedPath(owner, { sessionId: manualSession }).filter(
    ({ entry }) => entry.kind === "compaction",
  );
}

function summaryResponses(faux: ReturnType<typeof fauxModel>) {
  faux.setResponses([
    fauxAssistantMessage("manual summary"),
    fauxAssistantMessage("manual turn summary"),
  ]);
}

it("compacts an idle Session once as queued manual work, idempotent by request id", async () => {
  const scenario = manualCompactionScenario();
  const opened = await scenario.open();
  try {
    const session = await buildHistory(scenario, opened);
    const contextBefore = readSelectedPath(scenario.fixture.owner, {
      sessionId: manualSession,
    }).length;
    summaryResponses(scenario.faux);
    const accepted = await session.command({
      type: "compact",
      requestId: "manual-compact",
    });
    // The same request id replays the admission instead of queueing twice.
    expect(
      await session.command({ type: "compact", requestId: "manual-compact" }),
    ).toEqual(accepted);
    for (
      let pass = 0;
      pass < 5 && opened.runtime.snapshot().state === "pending";
      pass++
    )
      await opened.runtime.runNext();

    const compactions = compactionsIn(scenario.fixture.owner);
    expect(compactions).toHaveLength(1);
    // Manual: the entry belongs to the compaction request, never a run.
    expect(compactions[0]?.entry).toMatchObject({
      kind: "compaction",
      workId: accepted.commandId,
      data: { resumesRun: false },
    });
    const command = readSelectedPath(scenario.fixture.owner, {
      sessionId: manualSession,
    }).find(({ entry }) => entry.workId === accepted.commandId);
    expect(command?.entry).toMatchObject({ kind: "command", work: "compact" });
    expect(
      readSelectedPath(scenario.fixture.owner, {
        sessionId: manualSession,
      }).length,
    ).toBeGreaterThan(contextBefore);

    const snapshot = await session.read();
    // The request is never a message, a title or a queued prompt.
    expect(
      snapshot.messages.some(
        ({ commandId }) => commandId === accepted.commandId,
      ),
    ).toBe(false);
    expect(snapshot.pending.prompts).toEqual([]);
    expect(snapshot.compactions).toHaveLength(1);
    expect(
      snapshot.terminals.find(
        ({ commandId }) => commandId === accepted.commandId,
      ),
    ).toMatchObject({ outcome: "completed", diagnostic: null });
    expect((await opened.agent.listSessions()).sessions[0]).toMatchObject({
      title: "Build history 0.",
    });
    // The next model turn starts from the compacted context.
    expect(
      buildSessionContext(
        readSelectedPath(scenario.fixture.owner, { sessionId: manualSession }),
      )[0],
    ).toMatchObject({ role: "user" });
  } finally {
    await opened.agent.close();
    scenario.fixture.close();
  }
});

it("completes without a compaction and reports 'nothing to compact' as a notice", async () => {
  const scenario = manualCompactionScenario();
  const opened = await scenario.open();
  try {
    const session = opened.agent.session(manualSession);
    scenario.faux.setResponses([fauxAssistantMessage("short answer")]);
    await session.command({
      requestId: "manual-short",
      delivery: "followUp",
      type: "prompt",
      text: "Short.",
    });
    await opened.runtime.runNext();
    const accepted = await session.command({
      type: "compact",
      requestId: "manual-nothing",
    });
    await opened.runtime.runNext();
    expect(compactionsIn(scenario.fixture.owner)).toEqual([]);
    expect(scenario.faux.state.callCount).toBe(1);
    const snapshot = await session.read();
    const terminal = snapshot.terminals.find(
      ({ commandId }) => commandId === accepted.commandId,
    );
    expect(terminal?.outcome).toBe("completed");
    expect(snapshot.compactionNotices).toEqual([
      expect.objectContaining({
        trigger: "manual",
        result: "nothing_to_compact",
        diagnostic: null,
      }),
    ]);
  } finally {
    await opened.agent.close();
    scenario.fixture.close();
  }
});

it("fails the command and keeps the context when the handoff rejects", async () => {
  const scenario = manualCompactionScenario([
    compactionExtension("manual-failing-handoff", () =>
      Effect.fail({
        _tag: "CompactionHandoffError" as const,
        message: "archive refused",
      }),
    ),
  ]);
  const opened = await scenario.open();
  try {
    const session = await buildHistory(scenario, opened);
    const before = buildSessionContext(
      readSelectedPath(scenario.fixture.owner, { sessionId: manualSession }),
    );
    summaryResponses(scenario.faux);
    const accepted = await session.command({
      type: "compact",
      requestId: "manual-rejected",
    });
    await opened.runtime.runNext();
    expect(compactionsIn(scenario.fixture.owner)).toEqual([]);
    expect(
      buildSessionContext(
        readSelectedPath(scenario.fixture.owner, { sessionId: manualSession }),
      ),
    ).toEqual(before);
    const terminal = (await session.read()).terminals.find(
      ({ commandId }) => commandId === accepted.commandId,
    );
    expect(terminal?.outcome).toBe("failed");
    expect(terminal?.diagnostic).toContain("compaction handoff failed");
    expect(terminal?.diagnostic?.length).toBeLessThanOrEqual(200);
  } finally {
    await opened.agent.close();
    scenario.fixture.close();
  }
});

it("fails the command on a CAS conflict and distinguishes it from a handoff failure", async () => {
  let release: () => void = () => undefined;
  let started: () => void = () => undefined;
  const blocked = new Promise<void>((resolve) => {
    started = resolve;
  });
  const scenario = manualCompactionScenario([
    compactionExtension("manual-racing-handoff", async () => {
      started();
      await new Promise<void>((resolve) => {
        release = resolve;
      });
      return { type: "discarded_by_policy" as const };
    }),
  ]);
  const opened = await scenario.open();
  try {
    const session = await buildHistory(scenario, opened);
    summaryResponses(scenario.faux);
    const accepted = await session.command({
      type: "compact",
      requestId: "manual-conflict",
    });
    const running = opened.runtime.runNext();
    await blocked;
    // A preference write moves the selected leaf under the borrowed view.
    await session.modelPreference.set(null);
    release();
    await running;
    expect(compactionsIn(scenario.fixture.owner)).toEqual([]);
    const terminal = (await session.read()).terminals.find(
      ({ commandId }) => commandId === accepted.commandId,
    );
    expect(terminal?.outcome).toBe("failed");
    expect(terminal?.diagnostic).toContain("Session changed during compaction");
  } finally {
    await opened.agent.close();
    scenario.fixture.close();
  }
});

it("commits a discarded_by_policy handoff as a completed compaction", async () => {
  const scenario = manualCompactionScenario([
    compactionExtension("manual-discarding-handoff", () => ({
      type: "discarded_by_policy" as const,
    })),
  ]);
  const opened = await scenario.open();
  try {
    const session = await buildHistory(scenario, opened);
    summaryResponses(scenario.faux);
    const accepted = await session.command({
      type: "compact",
      requestId: "manual-discarded",
    });
    await opened.runtime.runNext();
    expect(compactionsIn(scenario.fixture.owner)).toHaveLength(1);
    expect(
      (await session.read()).terminals.find(
        ({ commandId }) => commandId === accepted.commandId,
      )?.outcome,
    ).toBe("completed");
  } finally {
    await opened.agent.close();
    scenario.fixture.close();
  }
});

/** The receipt stored in each committed compaction entry, and the store's text. */
function storedReceipts(scenario: ReturnType<typeof manualCompactionScenario>) {
  const entries = compactionsIn(scenario.fixture.owner).map(({ entry }) =>
    entry.kind === "compaction" ? entry.data.receipt : undefined,
  );
  const all = scenario.fixture.owner.sql
    .exec("SELECT data_json FROM session_entries")
    .toArray()
    .map((row) => JSON.stringify(row))
    .join("\n");
  return { entries, all };
}

it("stores the accepted handoff's receipt in the compaction entry that commits", async () => {
  const scenario = manualCompactionScenario([
    compactionExtension("manual-receipt-handoff", () => ({
      type: "accepted" as const,
      receipt: "archive-receipt-1",
    })),
  ]);
  const opened = await scenario.open();
  try {
    const session = await buildHistory(scenario, opened);
    summaryResponses(scenario.faux);
    await session.command({ type: "compact", requestId: "manual-receipt" });
    await opened.runtime.runNext();
    expect(storedReceipts(scenario).entries).toEqual(["archive-receipt-1"]);
  } finally {
    await opened.agent.close();
    scenario.fixture.close();
  }
});

it("fails the command with the handoff diagnostic for an over-long or empty receipt", async () => {
  for (const [label, receipt] of [
    ["over-long", "R".repeat(4097)],
    ["empty", ""],
  ] as const) {
    const scenario = manualCompactionScenario([
      compactionExtension(`manual-${label}-receipt-handoff`, () => ({
        type: "accepted" as const,
        receipt,
      })),
    ]);
    const opened = await scenario.open();
    try {
      const session = await buildHistory(scenario, opened);
      summaryResponses(scenario.faux);
      const before = (await session.read()).messages.length;
      const accepted = await session.command({
        type: "compact",
        requestId: `manual-${label}-receipt`,
      });
      await opened.runtime.runNext();
      const snapshot = await session.read();
      const terminal = snapshot.terminals.find(
        ({ commandId }) => commandId === accepted.commandId,
      );
      expect(terminal?.outcome, label).toBe("failed");
      expect(terminal?.diagnostic, label).toContain(
        "compaction handoff failed: it returned an invalid result",
      );
      expect(snapshot.messages, label).toHaveLength(before);
      const { entries, all } = storedReceipts(scenario);
      expect(entries, label).toEqual([]);
      expect(all, label).not.toContain("RRRR");
    } finally {
      await opened.agent.close();
      scenario.fixture.close();
    }
  }
});

it("stores no receipt for a discarded handoff", async () => {
  const scenario = manualCompactionScenario([
    compactionExtension("manual-no-receipt-handoff", () => ({
      type: "discarded_by_policy" as const,
    })),
  ]);
  const opened = await scenario.open();
  try {
    const session = await buildHistory(scenario, opened);
    summaryResponses(scenario.faux);
    await session.command({ type: "compact", requestId: "manual-no-receipt" });
    await opened.runtime.runNext();
    expect(storedReceipts(scenario).entries).toEqual([undefined]);
  } finally {
    await opened.agent.close();
    scenario.fixture.close();
  }
});

it("leaves no receipt anywhere when the append loses its CAS after an accepted handoff", async () => {
  let release: () => void = () => undefined;
  let started: () => void = () => undefined;
  const blocked = new Promise<void>((resolve) => {
    started = resolve;
  });
  const scenario = manualCompactionScenario([
    compactionExtension("manual-racing-receipt-handoff", async () => {
      started();
      await new Promise<void>((resolve) => {
        release = resolve;
      });
      return { type: "accepted" as const, receipt: "archive-receipt-lost" };
    }),
  ]);
  const opened = await scenario.open();
  try {
    const session = await buildHistory(scenario, opened);
    summaryResponses(scenario.faux);
    await session.command({ type: "compact", requestId: "manual-lost-cas" });
    const running = opened.runtime.runNext();
    await blocked;
    await session.modelPreference.set(null);
    release();
    await running;
    const { entries, all } = storedReceipts(scenario);
    expect(entries).toEqual([]);
    expect(all).not.toContain("archive-receipt-lost");
  } finally {
    await opened.agent.close();
    scenario.fixture.close();
  }
});

it("cancels a blocked manual compaction once through the existing cancel", async () => {
  let interrupted = 0;
  let started: () => void = () => undefined;
  const blocked = new Promise<void>((resolve) => {
    started = resolve;
  });
  const scenario = manualCompactionScenario([
    compactionExtension("manual-blocked-handoff", () =>
      Effect.callback<CompactionHandoffResultV0, CompactionHandoffErrorV0>(
        () => {
          started();
          return Effect.sync(() => {
            interrupted += 1;
          });
        },
      ),
    ),
  ]);
  const opened = await scenario.open();
  let running: Promise<void> | undefined;
  try {
    const session = await buildHistory(scenario, opened);
    summaryResponses(scenario.faux);
    const accepted = await session.command({
      type: "compact",
      requestId: "manual-cancelled",
    });
    running = opened.runtime.runNext();
    await blocked;
    await expect(
      session.cancel({ commandId: accepted.commandId }),
    ).resolves.toEqual(accepted);
    await running;
    running = undefined;
    expect(interrupted).toBe(1);
    expect(compactionsIn(scenario.fixture.owner)).toEqual([]);
    expect(
      (await session.read()).terminals.find(
        ({ commandId }) => commandId === accepted.commandId,
      )?.outcome,
    ).toBe("cancelled");
  } finally {
    await running;
    await opened.agent.close();
    scenario.fixture.close();
  }
});

it("runs a compaction request queued before a restart exactly once", async () => {
  const scenario = manualCompactionScenario();
  let opened = await scenario.open();
  try {
    const session = await buildHistory(scenario, opened);
    const accepted = await session.command({
      type: "compact",
      requestId: "manual-restart",
    });
    await opened.agent.close();
    opened = await scenario.open();
    summaryResponses(scenario.faux);
    for (
      let pass = 0;
      pass < 5 && opened.runtime.snapshot().state === "pending";
      pass++
    )
      await opened.runtime.runNext();
    expect(compactionsIn(scenario.fixture.owner)).toHaveLength(1);
    expect(compactionsIn(scenario.fixture.owner)[0]?.entry.workId).toBe(
      accepted.commandId,
    );
    // A retried request after the restart replays; it never compacts again.
    await opened.agent
      .session(manualSession)
      .command({ type: "compact", requestId: "manual-restart" });
    await opened.runtime.runNext();
    expect(compactionsIn(scenario.fixture.owner)).toHaveLength(1);
  } finally {
    await opened.agent.close();
    scenario.fixture.close();
  }
});
