import type { FauxResponseStep } from "@fungi.computer/mule/testing";
import {
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { expect } from "vitest";

import type { CompactionOptions } from "@fungi.computer/shiitake";
import type {
  CommandSettlement,
  SessionFrame,
  SessionSnapshot,
  SessionWatch,
  SubscribeOptions,
} from "@fungi.computer/shiitake/client";

/**
 * One unique, provider-shaped fake value. Each host's real Vault owner stores
 * it, so only that owner's port can teach Shiitake to find it.
 */
export const vaultCanary = "sk_live_vault_canary_s4_never_sent";

/** Byte forms a store scan must not find: raw, base64 (each offset) and hex. */
export function canaryForms(canary: string = vaultCanary): string[] {
  const bytes = new TextEncoder().encode(canary);
  const base64 = [0, 1, 2].map((shift) =>
    // Drop the edge characters that depend on neighboring bytes.
    base64Of(bytes.subarray(shift)).slice(2, -4),
  );
  const hex = Array.from(bytes, (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");
  return [canary, ...base64, hex];
}

function base64Of(bytes: Uint8Array): string {
  return btoa(String.fromCharCode(...bytes));
}

/** Bytes as text, one character per byte, for a plain substring scan. */
function latin1(bytes: Uint8Array): string {
  return new TextDecoder("latin1").decode(bytes);
}

/** The Session surface each host's law test drives; Hub and Oyster supply it. */
export type RedactionLawSession = Readonly<{
  send: (
    text: string,
    options: Readonly<{ requestId: string }>,
  ) => Promise<Readonly<{ commandId: string }>>;
  compact: (requestId: string) => Promise<Readonly<{ commandId: string }>>;
  read: () => Promise<SessionSnapshot>;
  readSettlement: (
    commandId: string,
    options: Readonly<{ timeoutMs: number }>,
  ) => Promise<CommandSettlement | undefined>;
  subscribe: (input: SubscribeOptions) => AsyncIterable<SessionFrame>;
  watch: () => Promise<SessionWatch>;
}>;

export type RedactionLawAgent = Readonly<{
  session: (sessionId: string) => RedactionLawSession;
  /** Every byte this host persisted for the Agent, read after settlement. */
  storedBytes: () => Promise<Uint8Array>;
  close: () => Promise<void>;
}>;

/**
 * Open the host's real Agent whose Vault owner stores `vaultCanary`. The
 * model follows `responses`; its one `execute` tool prints `toolOutput`.
 * While `portDown()` is true, the host's fingerprint port must reject.
 */
export type RedactionLawHost = (
  input: Readonly<{
    responses: readonly FauxResponseStep[];
    toolOutput: string;
    portDown: () => boolean;
    /** A budget small enough that a manual compaction has history to summarize. */
    compaction: CompactionOptions;
  }>,
) => Promise<RedactionLawAgent>;

const compaction: CompactionOptions = {
  enabled: false,
  keepRecentTokens: 1,
};

const absent = (label: string, text: string) => {
  for (const form of canaryForms())
    expect(text, `${label} carries a stored value`).not.toContain(form);
};

async function settled(
  session: RedactionLawSession,
  commandId: string,
): Promise<CommandSettlement> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    const settlement = await session.readSettlement(commandId, {
      timeoutMs: 100,
    });
    if (settlement !== undefined) return settlement;
  }
  throw new Error("The command did not settle");
}

/** Drain a watch's live frames in the background until it is closed. */
function collect(watch: SessionWatch) {
  const frames: SessionFrame[] = [];
  const done = (async () => {
    for await (const frame of watch.changes) frames.push(frame);
  })();
  return {
    frames,
    stop: async () => {
      await watch.close();
      // Closing ends the iteration; the drain itself must not have failed.
      expect(await Promise.allSettled([done])).toEqual([
        { status: "fulfilled", value: undefined },
      ]);
    },
  };
}

/** Read the full replay from the first entry, up to the settled cursor. */
async function replay(
  session: RedactionLawSession,
  cursor: string | null,
): Promise<SessionFrame[]> {
  const frames: SessionFrame[] = [];
  for await (const frame of session.subscribe({})) {
    frames.push(frame);
    if (frame.kind === "reset" && !frame.baseline.hasMore) break;
    if (frame.kind === "update" && frame.cursor === cursor) break;
  }
  return frames;
}

/**
 * A tool prints a stored value; the reply streams one too. The next model
 * request, every stored byte, the snapshot, replay, live progress frames and
 * the compaction summary carry only the mask.
 */
export async function proveSessionRedaction(host: RedactionLawHost) {
  const requests: string[] = [];
  const record =
    (reply: string): FauxResponseStep =>
    (context) => {
      requests.push(JSON.stringify(context));
      return fauxAssistantMessage(reply);
    };
  const agent = await host({
    responses: [
      fauxAssistantMessage(fauxToolCall("execute", { code: "cat .env" }), {
        stopReason: "toolUse",
      }),
      // A reply that repeats a value it learned elsewhere still streams masked.
      // Text before the value settles past the holdback and streams live.
      record(
        `${"I read the environment file you asked about. ".repeat(4)}Your key is ${vaultCanary} and the rest is fine.`,
      ),
      record("Summary: the user read their environment file."),
    ],
    toolOutput: `STRIPE_KEY=${vaultCanary}\nAUTH=${base64Of(new TextEncoder().encode(`user:${vaultCanary}`))}\n`,
    portDown: () => false,
    compaction,
  });
  try {
    const session = agent.session("vault-redaction");
    const live = collect(await session.watch());
    const accepted = await session.send("Show me my environment", {
      requestId: "show-env",
    });
    expect((await settled(session, accepted.commandId)).terminal.outcome).toBe(
      "completed",
    );
    const compacted = await session.compact("compact-env");
    await settled(session, compacted.commandId);
    const snapshot = await session.read();
    await live.stop();

    expect(requests).toHaveLength(2);
    absent("the next model request", requests[0] ?? "");
    expect(requests[0]).toContain("[REDACTED]");
    absent("the compaction request", requests[1] ?? "");
    absent("the snapshot", JSON.stringify(snapshot));
    absent("the live frames", JSON.stringify(live.frames));
    // Holdback: no live frame ever carried the start of the value either.
    expect(JSON.stringify(live.frames)).not.toContain(vaultCanary.slice(0, 16));
    expect(
      live.frames.some((frame) => frame.kind === "progress"),
      "the reply streamed live progress",
    ).toBe(true);
    absent(
      "the replay",
      JSON.stringify(await replay(session, snapshot.cursor)),
    );
    absent("the stored bytes", latin1(await agent.storedBytes()));
  } finally {
    await agent.close();
  }
}

/**
 * The port answers at the run's start, then goes down before the tool
 * finishes. The output is withheld, the run fails with a typed reason and the
 * model is never asked again.
 */
export async function proveRedactionFailsClosed(host: RedactionLawHost) {
  let down = false;
  let asked = 0;
  const agent = await host({
    responses: [
      () => {
        // The run's preparatory load has succeeded; the port now fails.
        down = true;
        return fauxAssistantMessage(
          fauxToolCall("execute", { code: "cat .env" }),
          { stopReason: "toolUse" },
        );
      },
      () => {
        asked += 1;
        return fauxAssistantMessage("never");
      },
    ],
    toolOutput: `STRIPE_KEY=${vaultCanary}\n`,
    portDown: () => down,
    compaction,
  });
  try {
    const session = agent.session("vault-down");
    const accepted = await session.send("Show me my environment", {
      requestId: "show-env-down",
    });
    const settlement = await settled(session, accepted.commandId);
    expect(settlement.terminal.outcome).toBe("failed");
    expect(settlement.terminal.diagnostic).toContain("redaction-unavailable");
    expect(asked).toBe(0);
    absent("the stored bytes", latin1(await agent.storedBytes()));
    // A read while the port is down is refused rather than served unredacted.
    await expect(session.read()).rejects.toThrow();
    down = false;
    const snapshot = await session.read();
    expect(JSON.stringify(snapshot)).toContain(
      "[output withheld: redaction unavailable]",
    );
  } finally {
    await agent.close();
  }
}
