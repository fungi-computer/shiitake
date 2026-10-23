// Runtime for every law here: Node 24, `node:sqlite`, Pi's faux provider.
import { directSandbox } from "@fungi.computer/mycelium/testing";
import {
  getSystemMessageText,
  type AssistantMessage,
  type Context,
  type Usage,
} from "@earendil-works/pi-ai";
import {
  fauxAssistantMessage,
  fauxProvider,
  fauxToolCall,
  type FauxResponseStep,
} from "@earendil-works/pi-ai/providers/faux";
import { describe, expect, it } from "vitest";

import { Shiitake, type ShiitakeOptions } from "../src/index.js";
import {
  compact,
  type CompactionPreparation,
} from "../src/internal/compaction/compaction.js";
import {
  DEFAULT_COMPACTION_SETTINGS,
  SUMMARIZATION_SYSTEM_PROMPT,
  serializeConversation,
} from "../src/internal/compaction/utils.js";
import { readSelectedPath } from "../src/internal/woodstock/session-store.js";
import * as Effect from "effect/Effect";
import {
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

const sessionId = "compaction-parity";

/** The text of one request's first user line, as the provider saw it. */
function userText(request: Context | undefined): string {
  const first = request?.messages.find(({ role }) => role === "user");
  if (first?.role !== "user") return "";
  return Array.isArray(first.content)
    ? first.content
        .flatMap((part) => (part.type === "text" ? [part.text] : []))
        .join("")
    : first.content;
}

/** Pi's normalized context carries the system prompt as its first message. */
function isSummaryRequest(request: Context): boolean {
  const first = request.messages[0];
  return (
    first?.role === "system" &&
    getSystemMessageText(first) === SUMMARIZATION_SYSTEM_PROMPT
  );
}

type Scenario = Readonly<{
  fixture: ReturnType<typeof createNativeSqliteOwnerFixture>;
  runtime: ReturnType<typeof createCompatibilityRuntimeHost>;
  faux: ReturnType<typeof fauxProvider>;
  requests: Context[];
  /** Most summary calls in flight at once. */
  peakSummaryCalls: () => number;
  options: (extra?: Partial<ShiitakeOptions>) => ShiitakeOptions;
  modelKey: string;
}>;

function scenario(contextWindow = 30_000): Scenario {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const faux = fauxProvider({
    models: [{ id: "parity-1", contextWindow, maxTokens: 100_000 }],
  });
  const requests: Context[] = [];
  let active = 0;
  let peak = 0;
  const model = faux.getModel();
  return {
    fixture,
    runtime,
    faux,
    requests,
    peakSummaryCalls: () => peak,
    modelKey: `${model.provider}/${model.id}`,
    options: (extra = {}) => ({
      store: fixture.owner,
      runtime: runtime.runtime,
      extensions: { roster: [], grants: [] },
      instructions: {
        load: () => Promise.resolve({ revision: "parity-v1", text: "Answer." }),
      },
      model: {
        acquire: () =>
          Promise.resolve({
            model: faux.getModel(),
            retryable: () => false,
            stream: (streamModel, context, options) => {
              requests.push(structuredClone(context));
              const stream = faux.provider.streamSimple(
                streamModel,
                context,
                options,
              );
              if (isSummaryRequest(context)) {
                active += 1;
                peak = Math.max(peak, active);
                void stream.result().then(() => {
                  active -= 1;
                });
              }
              return stream;
            },
            release: () => Promise.resolve(),
          }),
      },
      tools: {
        sandbox: directSandbox(() => Promise.resolve("y".repeat(60_000))),
      },
      ...extra,
    }),
  };
}

async function settle(run: Scenario) {
  while (run.runtime.snapshot().state === "pending")
    await run.runtime.runNext();
}

async function prompt(
  agent: Awaited<ReturnType<typeof Shiitake.make>>,
  run: Scenario,
  requestId: string,
  text: string,
) {
  await agent.session(sessionId).command({
    type: "prompt",
    requestId,
    text,
    delivery: "followUp",
  });
  await settle(run);
}

function entryKinds(run: Scenario): string[] {
  return readSelectedPath(run.fixture.owner, { sessionId }).map(
    ({ entry }) => entry.kind,
  );
}

describe("manual /compact (Node, node:sqlite, faux)", () => {
  it("passes custom instructions to the summary as Pi's additional focus", async () => {
    const run = scenario(1_000_000);
    run.faux.setResponses([
      fauxAssistantMessage("a".repeat(8_000)),
      fauxAssistantMessage("b".repeat(8_000)),
      fauxAssistantMessage("focused summary"),
      fauxAssistantMessage("turn prefix summary"),
    ]);
    const agent = await Shiitake.make(
      run.options({
        compaction: {
          modelOverrides: { [run.modelKey]: { keepRecentTokens: 1_000 } },
        },
      }),
    );
    try {
      await prompt(agent, run, "first", "Plan the API.");
      await prompt(agent, run, "second", "Write the API.");
      await agent.session(sessionId).command({
        type: "compact",
        requestId: "compact-focused",
        instructions: "keep every API name",
      });
      await settle(run);
      // The second turn splits: a history summary, then its turn prefix.
      // Pi adds the focus to the history summary only.
      const summary = run.requests.filter(isSummaryRequest);
      expect(summary).toHaveLength(2);
      expect(userText(summary[0])).toContain(
        "Additional focus: keep every API name",
      );
      expect(userText(summary[1])).not.toContain("Additional focus");
      const snapshot = await agent.session(sessionId).read();
      expect(snapshot.compactions?.[0]).toMatchObject({ trigger: "manual" });
    } finally {
      await agent.close();
      run.fixture.close();
    }
  });

  it("reports 'nothing to compact' as a typed notice, not a hidden diagnostic", async () => {
    const run = scenario(1_000_000);
    run.faux.setResponses([fauxAssistantMessage("short answer")]);
    const agent = await Shiitake.make(run.options());
    try {
      await prompt(agent, run, "only", "Hello.");
      const accepted = await agent.session(sessionId).command({
        type: "compact",
        requestId: "compact-nothing",
      });
      await settle(run);
      const snapshot = await agent.session(sessionId).read();
      expect(snapshot.compactionNotices).toEqual([
        expect.objectContaining({
          trigger: "manual",
          result: "nothing_to_compact",
        }),
      ]);
      expect(
        snapshot.terminals.find(
          ({ commandId }) => commandId === accepted.commandId,
        ),
      ).toMatchObject({ outcome: "completed" });
      expect(run.requests.filter(isSummaryRequest)).toHaveLength(0);
    } finally {
      await agent.close();
      run.fixture.close();
    }
  });
});

describe("per-model compaction budgets (Node, node:sqlite, faux)", () => {
  it("uses the model override before the ordinary setting and the default", async () => {
    const run = scenario(100_000);
    run.faux.setResponses([
      fauxAssistantMessage("c".repeat(120_000)),
      fauxAssistantMessage("override summary"),
    ]);
    // About 46K tokens of context is far below 100K - 16384, so only the
    // override's 80K reserve triggers a post-run compaction. The cut keeps
    // the long reply and summarizes the long request, so the context shrinks.
    const agent = await Shiitake.make(
      run.options({
        compaction: {
          reserveTokens: 16_384,
          keepRecentTokens: 1_000,
          modelOverrides: {
            [run.modelKey]: { reserveTokens: 80_000 },
            "other/model": { reserveTokens: 1 },
          },
        },
      }),
    );
    try {
      await prompt(agent, run, "big", `Write a lot. ${"r".repeat(40_000)}`);
      expect(entryKinds(run)).toContain("compaction");
      const snapshot = await agent.session(sessionId).read();
      const marker = snapshot.compactions?.[0];
      expect(marker).toMatchObject({ trigger: "threshold" });
      expect(marker?.tokensBefore).toBeGreaterThan(20_000);
      expect(marker?.tokensAfter).toBeLessThan(marker?.tokensBefore ?? 0);
    } finally {
      await agent.close();
      run.fixture.close();
    }
  });

  it("keeps the ordinary budget for other models", async () => {
    const run = scenario(100_000);
    run.faux.setResponses([fauxAssistantMessage("c".repeat(120_000))]);
    const agent = await Shiitake.make(
      run.options({
        compaction: {
          modelOverrides: { "other/model": { reserveTokens: 80_000 } },
        },
      }),
    );
    try {
      await prompt(agent, run, "big", "Write a lot.");
      expect(entryKinds(run)).not.toContain("compaction");
    } finally {
      await agent.close();
      run.fixture.close();
    }
  });

  it("refuses an invalid budget when the owner opens", async () => {
    const run = scenario();
    await expect(
      Shiitake.make(run.options({ compaction: { reserveTokens: -1 } })),
    ).rejects.toMatchObject({ cause: "invalid" });
    run.fixture.close();
  });
});

describe("mid-run compaction (Node, node:sqlite, faux)", () => {
  it("compacts between a tool result and the next assistant response", async () => {
    const run = scenario(30_000);
    run.faux.setResponses([
      fauxAssistantMessage("d".repeat(8_000)),
      fauxAssistantMessage(fauxToolCall("execute", { code: "big()" }), {
        stopReason: "toolUse",
      }),
      fauxAssistantMessage("history summary"),
      fauxAssistantMessage("turn prefix summary"),
      fauxAssistantMessage("final answer"),
    ]);
    const agent = await Shiitake.make(
      run.options({
        compaction: {
          // Blocking only: in a 30K window the default background budget
          // would start a background compaction before the run (see
          // compaction-background.test.ts for that path).
          modelOverrides: {
            [run.modelKey]: { keepRecentTokens: 1_000, backgroundTokens: 0 },
          },
        },
      }),
    );
    try {
      await prompt(agent, run, "seed", "Remember the plan.");
      await prompt(agent, run, "tool", "Read the big file, then answer.");
      const kinds = entryKinds(run);
      const toolResult = kinds.lastIndexOf("tool_outcome");
      const compaction = kinds.indexOf("compaction");
      const terminal = kinds.lastIndexOf("run_terminal");
      expect(compaction).toBeGreaterThan(toolResult);
      expect(compaction).toBeLessThan(terminal);
      expect(kinds.filter((kind) => kind === "compaction")).toHaveLength(1);
      // The answer after the tool reads the compacted context.
      const last = run.requests.at(-1);
      expect(userText(last)).toContain("history summary");
      expect(run.faux.getPendingResponseCount()).toBe(0);
      // Pi serializes the split-turn summaries (avoids 429s, keeps order).
      expect(run.peakSummaryCalls()).toBe(1);
      const summaries = run.requests.filter(isSummaryRequest);
      expect(userText(summaries[0])).toContain("<conversation>");
      expect(userText(summaries[1])).toContain("# Conversation");
    } finally {
      await agent.close();
      run.fixture.close();
    }
  });
});

describe("auto-compaction failures surface (Node, node:sqlite, faux)", () => {
  it("records a typed notice when the summarizer fails, and the run still completes", async () => {
    const run = scenario(100_000);
    const failedSummary: FauxResponseStep = fauxAssistantMessage("", {
      stopReason: "error",
      errorMessage: "summarizer offline",
    });
    run.faux.setResponses([
      fauxAssistantMessage("c".repeat(120_000)),
      failedSummary,
      failedSummary,
      failedSummary,
      failedSummary,
    ]);
    const agent = await Shiitake.make(
      run.options({
        compaction: {
          modelOverrides: {
            [run.modelKey]: { reserveTokens: 80_000, keepRecentTokens: 1_000 },
          },
        },
      }),
    );
    try {
      await prompt(agent, run, "big", "Write a lot.");
      const snapshot = await agent.session(sessionId).read();
      expect(snapshot.compactionNotices).toEqual([
        expect.objectContaining({ trigger: "threshold", result: "failed" }),
      ]);
      expect(snapshot.compactionNotices?.[0]?.diagnostic).toContain(
        "summarizer offline",
      );
      expect(snapshot.terminals.at(-1)).toMatchObject({ outcome: "completed" });
      expect(entryKinds(run)).not.toContain("compaction");
    } finally {
      await agent.close();
      run.fixture.close();
    }
  }, 30_000);
});

describe("summary serialization (Pi parity)", () => {
  it("leaves thinking blocks out of the summary input", () => {
    const text = serializeConversation([
      { role: "user", content: "question", timestamp: 1 },
      {
        role: "assistant",
        api: "pi",
        provider: "camel",
        model: "auto",
        content: [
          { type: "thinking", thinking: "private chain of thought" },
          { type: "text", text: "visible answer" },
        ],
        stopReason: "stop",
        timestamp: 2,
        usage: {
          input: 1,
          output: 1,
          cacheRead: 0,
          cacheWrite: 0,
          totalTokens: 2,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
        },
      },
    ]);
    expect(text).not.toContain("private chain of thought");
    expect(text).toContain("[Assistant]: visible answer");
  });
});

describe("split-turn summaries (Node, faux)", () => {
  const usage: Usage = {
    input: 1,
    output: 1,
    cacheRead: 0,
    cacheWrite: 0,
    totalTokens: 2,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
  };
  function preparation(
    previousSummary: string | undefined,
  ): CompactionPreparation {
    const message = (text: string) =>
      ({ role: "user", content: text, timestamp: 1 }) as const;
    const base = {
      firstKeptEntryId: "kept",
      messagesToSummarize: [],
      turnPrefixMessages: [message("prefix")],
      isSplitTurn: true,
      tokensBefore: 10,
      fileOps: {
        edited: new Set<string>(),
        read: new Set<string>(),
        written: new Set<string>(),
      },
      settings: DEFAULT_COMPACTION_SETTINGS,
    };
    return previousSummary === undefined ? base : { ...base, previousSummary };
  }

  it("keeps the previous summary when a split turn has no new history", async () => {
    const faux = fauxProvider({ models: [{ id: "split-1" }] });
    const answer: AssistantMessage = fauxAssistantMessage("prefix checkpoint");
    faux.setResponses([{ ...answer, usage }]);
    const result = await Effect.runPromise(
      compact(preparation("earlier summary"), faux.getModel(), (m, c, o) =>
        faux.provider.streamSimple(m, c, o),
      ),
    );
    expect(result.summary).toMatch(/^earlier summary\n\n---\n\n/u);
    expect(result.summary).toContain("prefix checkpoint");
  });

  it("refuses a summary that tries to call a tool", async () => {
    const faux = fauxProvider({ models: [{ id: "split-2" }] });
    faux.setResponses([
      fauxAssistantMessage(fauxToolCall("execute", { code: "x" }), {
        stopReason: "toolUse",
      }),
    ]);
    await expect(
      Effect.runPromise(
        compact(preparation(undefined), faux.getModel(), (m, c, o) =>
          faux.provider.streamSimple(m, c, o),
        ),
      ),
    ).rejects.toThrow(/attempted to call a tool/u);
  });
});
