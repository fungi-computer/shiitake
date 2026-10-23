import { directSandbox } from "@fungi.computer/mycelium/testing";
import { fauxModel } from "@fungi.computer/mule/testing";
import {
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import {
  Mycelium,
  type AccessRefusal,
  type Grants,
  type SandboxBinding,
  type SandboxValue,
} from "@fungi.computer/mycelium";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { expect, it } from "vitest";
import { z } from "zod";

import {
  Shiitake,
  ShiitakeError,
  type GrantsDeliveryResult,
  type ShiitakeOwnerAgent,
  type ToolAccess,
} from "../src/index.js";
import {
  defineExtension,
  ExtensionId,
  type ExtensionCapabilityV0,
  type ExtensionGrantV0,
} from "../src/extensions.js";
import { createShiitakeClient } from "../src/client.js";
import { createShiitakeHttpRoutes } from "../src/http-routes.js";
import {
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

const noteContract = {
  id: ExtensionId.make("test-note-tools"),
  commands: {},
  projection: null,
} as const;

/**
 * One note extension whose run_tools module has a read, a write and a
 * confirm operation. With `followUp`, it self-prompts once after each
 * Session's human run settles.
 */
function noteExtension(ran: string[], followUp: boolean) {
  const record = (sessionId: string, name: string) => () => {
    ran.push(`${sessionId}:${name}`);
    return Promise.resolve({ ok: true });
  };
  const noteModule = (sessionId: string) =>
    Mycelium.module({
      id: "test-note",
      name: "note",
      operations: {
        peek: Mycelium.operation({
          access: "read",
          input: z.object({}),
          output: z.object({ ok: z.boolean() }),
          execute: record(sessionId, "peek"),
        }),
        poke: Mycelium.operation({
          input: z.object({}),
          output: z.object({ ok: z.boolean() }),
          execute: record(sessionId, "poke"),
        }),
        send: Mycelium.operation({
          access: "read",
          confirm: true,
          input: z.object({}),
          output: z.object({ ok: z.boolean() }),
          execute: record(sessionId, "send"),
        }),
      },
    });
  const definition = defineExtension({
    id: noteContract.id,
    version: "1.0.0",
  });
  if (!followUp)
    return definition.define({
      tools: (_ctx, { sessionId }: { sessionId: string }) =>
        Effect.sync(() => Option.some(noteModule(sessionId))),
    });
  return definition.define({
    tools: (_ctx, { sessionId }: { sessionId: string }) =>
      Effect.sync(() => Option.some(noteModule(sessionId))),
    hooks: {
      onSettled: (ctx, page) =>
        Effect.forEach(
          page.filter((run) => run.origin === null),
          (run) =>
            ctx.send({
              sessionId: run.sessionId,
              when: "now",
              correlation: "follow-up",
              prompt: {
                text: "Keep going.",
                requestId: `follow-up:${run.workId}`,
                delivery: "followUp",
              },
            }),
          { discard: true },
        ),
    },
  });
}

type Harness = Readonly<{
  agent: ShiitakeOwnerAgent;
  ran: string[];
  /** Every value each run's three note calls returned, by Session. */
  results: Map<string, SandboxValue[]>;
  prompt: (sessionId: string) => Promise<void>;
  /** Settle every pending reconciliation, bounded. */
  drain: () => Promise<void>;
  close: () => Promise<void>;
}>;

/**
 * One Agent over the note extension. Each run calls peek, poke and send in
 * order; `between` runs after each call, as a host writing grants mid-run.
 */
async function openNotes(
  input: Readonly<{
    access?: ToolAccess;
    followUp?: boolean;
    /** The roster grant key for the note extension; its id by default. */
    grantKey?: string;
    runs: number;
    between?: (
      agent: ShiitakeOwnerAgent,
      sessionId: string,
      call: number,
    ) => void;
    fixture?: ReturnType<typeof createNativeSqliteOwnerFixture>;
  }>,
): Promise<Harness> {
  const fixture = input.fixture ?? createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const faux = fauxModel([]);
  const ran: string[] = [];
  const results = new Map<string, SandboxValue[]>();
  const current = { sessionId: "" };
  faux.setResponses(
    Array.from({ length: input.runs }).flatMap(() => [
      fauxAssistantMessage(fauxToolCall("execute", { code: "note" }), {
        stopReason: "toolUse",
      }),
      fauxAssistantMessage("done"),
    ]),
  );
  const capabilities: ExtensionCapabilityV0[] =
    input.followUp === true ? ["run_tools", "settled", "send"] : ["run_tools"];
  const noteGrant: ExtensionGrantV0 =
    input.grantKey === undefined
      ? { id: noteContract.id, capabilities }
      : { id: noteContract.id, capabilities, grantKey: input.grantKey };
  const agent = await Shiitake.make({
    store: fixture.owner,
    runtime: runtime.runtime,
    extensions: {
      roster: [noteExtension(ran, input.followUp === true)],
      grants: [noteGrant],
    },
    instructions: {
      load: ({ sessionId }) => {
        current.sessionId = sessionId;
        return Promise.resolve({ revision: "v1", text: "Use tools." });
      },
    },
    model: {
      acquire: () =>
        Promise.resolve({
          model: faux.model,
          retryable: () => false,
          stream: faux.stream,
          release: () => Promise.resolve(),
        }),
    },
    tools: {
      sandbox: directSandbox(async (bindings) => {
        const note = bindings.note;
        if (note === undefined) throw new Error("note is not offered");
        const values: SandboxValue[] = [];
        let call = 0;
        for (const name of ["peek", "poke", "send"]) {
          const bound: SandboxBinding | undefined = note[name];
          if (bound === undefined) throw new Error(`${name} missing`);
          values.push(await bound({}));
          call += 1;
          input.between?.(agent, current.sessionId, call);
        }
        results.set(current.sessionId, [
          ...(results.get(current.sessionId) ?? []),
          ...values,
        ]);
        return null;
      }),
      access: input.access ?? {},
    },
  });
  return {
    agent,
    ran,
    results,
    prompt: async (sessionId) => {
      await agent.session(sessionId).command({
        delivery: "followUp",
        type: "prompt",
        requestId: sessionId,
        text: "Use the note tools.",
      });
    },
    drain: async () => {
      for (let pass = 0; pass < 20; pass += 1) {
        if (runtime.snapshot().state !== "pending") return;
        await runtime.runNext();
      }
      throw new Error("Runs did not settle");
    },
    close: async () => {
      await agent.close();
      if (input.fixture === undefined) fixture.close();
    },
  };
}

/** The Shiitake error a synchronous owner delivery threw, for matching. */
function thrownBy(run: () => GrantsDeliveryResult): ShiitakeError {
  try {
    run();
  } catch (error) {
    if (error instanceof ShiitakeError) return error;
    throw error;
  }
  throw new Error("Expected the delivery to throw");
}

const refusal = (operation: string, reason: AccessRefusal["reason"]) => ({
  refused: true,
  module: noteContract.id,
  operation,
  reason,
});

it("runs every extension operation with no host access data, except confirm, which is refused", async () => {
  const notes = await openNotes({ runs: 1 });
  try {
    await notes.prompt("owner");
    await notes.drain();
    expect(notes.ran).toEqual(["owner:peek", "owner:poke"]);
    expect(notes.results.get("owner")?.[2]).toMatchObject(
      refusal("send", "confirmation-unavailable"),
    );
  } finally {
    await notes.close();
  }
});

it("grants an extension's modules by its extension id from each Session's stored grants", async () => {
  const refused: string[] = [];
  const asked: string[] = [];
  const notes = await openNotes({
    runs: 3,
    access: {
      confirm: (request) => {
        asked.push(
          `${String(request.sessionId)}:${request.module}.${request.operation}`,
        );
        return Promise.resolve(true);
      },
      onRefused: ({ operation, reason }, { sessionId }) => {
        refused.push(`${String(sessionId)}:${operation}:${reason}`);
      },
    },
  });
  try {
    for (const sessionId of ["owner", "reader", "bound"]) {
      // The prompt is queued before the host writes grants; the queued run
      // reads what is stored when it calls.
      await notes.prompt(sessionId);
      if (sessionId === "reader")
        notes.agent.grants.deliver(sessionId, {
          version: 1,
          grants: { [noteContract.id]: "read" },
        });
      if (sessionId === "bound")
        notes.agent.grants.deliver(sessionId, {
          version: 1,
          grants: { [noteContract.id]: "none" },
        });
      await notes.drain();
    }
    expect(notes.agent.grants.read("owner")).toBeNull();
    expect(notes.agent.grants.read("reader")).toEqual({
      version: 1,
      grants: { [noteContract.id]: "read" },
    });
    expect(notes.ran).toEqual([
      "owner:peek",
      "owner:poke",
      "owner:send",
      "reader:peek",
      "reader:send",
    ]);
    expect(asked).toEqual([
      `owner:${noteContract.id}.send`,
      `reader:${noteContract.id}.send`,
    ]);
    expect(notes.results.get("reader")?.[1]).toEqual({
      ...refusal("poke", "read-only"),
      message: `${noteContract.id}.poke changes something, and this Agent has read-only access to ${noteContract.id}.`,
    });
    expect(refused).toEqual([
      "reader:poke:read-only",
      "bound:peek:not-granted",
      "bound:poke:not-granted",
      "bound:send:not-granted",
    ]);
  } finally {
    await notes.close();
  }
});

it("lets a Session's stored grants override the Agent-wide default key by key", async () => {
  const notes = await openNotes({
    runs: 2,
    access: { grants: { [noteContract.id]: "none" } },
  });
  try {
    await notes.prompt("default");
    await notes.drain();
    await notes.prompt("widened");
    notes.agent.grants.deliver("widened", {
      version: 1,
      grants: { [noteContract.id]: "write" },
    });
    await notes.drain();
    expect(notes.ran).toEqual(["widened:peek", "widened:poke"]);
  } finally {
    await notes.close();
  }
});

it("never moves a delivery back, and lets the same version's newer computation replace it", async () => {
  const notes = await openNotes({ runs: 0 });
  try {
    const read = { [noteContract.id]: "read" } as const;
    // A delivery may arrive before the Session's first prompt.
    expect(
      notes.agent.grants.deliver("ordered", { version: 5, grants: read }),
    ).toBe("applied");
    expect(
      notes.agent.grants.deliver("ordered", { version: 5, grants: read }),
    ).toBe("unchanged");
    // The host recomputed at the same version (a deploy): the old grant goes.
    expect(
      notes.agent.grants.deliver("ordered", {
        version: 5,
        grants: { [noteContract.id]: "none" },
      }),
    ).toBe("applied");
    expect(
      thrownBy(() =>
        notes.agent.grants.deliver("ordered", {
          version: 4,
          grants: { [noteContract.id]: "write" },
        }),
      ),
    ).toMatchObject({ boundary: "contract", cause: "conflict" });
    expect(notes.agent.grants.read("ordered")).toEqual({
      version: 5,
      grants: { [noteContract.id]: "none" },
    });
    expect(
      notes.agent.grants.deliver("ordered", { version: 6, grants: read }),
    ).toBe("applied");
    const malformed = { [noteContract.id]: "read" } as const;
    Object.assign(malformed, { other: "admin" });
    const extra = { version: 7, grants: read };
    Object.assign(extra, { scope: "everything" });
    const tooMany: Grants = Object.fromEntries(
      Array.from({ length: 257 }, (_, index) => [
        `key-${String(index)}`,
        "read",
      ]),
    );
    const longKey: Grants = { ["k".repeat(161)]: "read" };
    for (const delivery of [
      { version: 7, grants: malformed },
      { version: -1, grants: read },
      { version: 1.5, grants: read },
      extra,
      { version: 7, grants: tooMany },
      { version: 7, grants: longKey },
    ])
      expect(
        thrownBy(() => notes.agent.grants.deliver("ordered", delivery)),
      ).toMatchObject({ boundary: "contract", cause: "invalid" });
    const atBound: Grants = Object.fromEntries(
      Array.from({ length: 256 }, (_, index) => [
        `key-${String(index)}`,
        "read",
      ]),
    );
    expect(
      notes.agent.grants.deliver("ordered", { version: 8, grants: atBound }),
    ).toBe("applied");
  } finally {
    await notes.close();
  }
});

it("copies a Session's delivery to its fork, so a fork never widens", async () => {
  const notes = await openNotes({ runs: 1 });
  try {
    await notes.prompt("source");
    notes.agent.grants.deliver("source", {
      version: 3,
      grants: { [noteContract.id]: "read" },
    });
    await notes.drain();
    const [entry] = (await notes.agent.session("source").read()).messages;
    if (entry === undefined) throw new Error("Missing source entry");
    const forked = await notes.agent
      .session("source")
      .command({ type: "fork", entryId: entry.id });
    expect(notes.agent.grants.read(forked.sessionId)).toEqual({
      version: 3,
      grants: { [noteContract.id]: "read" },
    });
  } finally {
    await notes.close();
  }
});

it("fails closed: a required key with no delivered grant is refused", async () => {
  const refused: string[] = [];
  const notes = await openNotes({
    runs: 2,
    access: {
      required: [noteContract.id],
      onRefused: ({ operation, reason }, { sessionId }) => {
        refused.push(`${String(sessionId)}:${operation}:${reason}`);
      },
    },
  });
  try {
    await notes.prompt("undelivered");
    await notes.drain();
    await notes.prompt("delivered");
    notes.agent.grants.deliver("delivered", {
      version: 1,
      grants: { [noteContract.id]: "read" },
    });
    await notes.drain();
    expect(notes.ran).toEqual(["delivered:peek"]);
    expect(refused).toEqual([
      "undelivered:peek:not-granted",
      "undelivered:poke:not-granted",
      "undelivered:send:not-granted",
      "delivered:poke:read-only",
      "delivered:send:confirmation-unavailable",
    ]);
  } finally {
    await notes.close();
  }
});

it("applies a grant the host writes mid-run to the very next call", async () => {
  const notes = await openNotes({
    runs: 1,
    between: (agent, sessionId, call) => {
      // The host revokes after the first call of the same run.
      if (call === 1)
        agent.grants.deliver(sessionId, {
          version: 2,
          grants: { [noteContract.id]: "none" },
        });
    },
  });
  try {
    await notes.prompt("revoked");
    await notes.drain();
    expect(notes.ran).toEqual(["revoked:peek"]);
    expect(notes.results.get("revoked")?.slice(1)).toMatchObject([
      refusal("poke", "not-granted"),
      refusal("send", "not-granted"),
    ]);
  } finally {
    await notes.close();
  }
});

it("holds stored grants for a run the extension self-prompted", async () => {
  const notes = await openNotes({ runs: 2, followUp: true });
  try {
    await notes.prompt("looping");
    notes.agent.grants.deliver("looping", {
      version: 1,
      grants: { [noteContract.id]: "read" },
    });
    await notes.drain();
    // Both the human run and the extension's follow-up run were read-only.
    expect(notes.ran).toEqual(["looping:peek", "looping:peek"]);
    const followUps = (
      await notes.agent.session("looping").read()
    ).messages.filter(({ origin }) => origin !== undefined);
    expect(followUps.map(({ origin }) => origin?.extensionId)).toEqual([
      noteContract.id,
    ]);
  } finally {
    await notes.close();
  }
});

it("holds stored grants for a run recovered after the owner reopens", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    const before = await openNotes({ runs: 0, fixture });
    await before.prompt("recovered");
    before.agent.grants.deliver("recovered", {
      version: 1,
      grants: { [noteContract.id]: "read" },
    });
    await before.close();
    const after = await openNotes({ runs: 1, fixture });
    try {
      await after.drain();
      expect(after.ran).toEqual(["recovered:peek"]);
    } finally {
      await after.close();
    }
  } finally {
    fixture.close();
  }
});

it("grants an extension's modules by the grant key its roster grant names", async () => {
  const notes = await openNotes({
    runs: 1,
    grantKey: "notes",
    access: { required: ["notes"] },
  });
  try {
    await notes.prompt("keyed");
    notes.agent.grants.deliver("keyed", {
      version: 1,
      // The extension id itself grants nothing; only its grant key counts.
      grants: { notes: "read", [noteContract.id]: "write" },
    });
    await notes.drain();
    expect(notes.ran).toEqual(["keyed:peek"]);
    expect(notes.results.get("keyed")?.[1]).toMatchObject({
      refused: true,
      module: "notes",
      reason: "read-only",
    });
  } finally {
    await notes.close();
  }
});

it("offers no HTTP route or remote client surface that writes grants", async () => {
  const notes = await openNotes({ runs: 0 });
  try {
    const routes = createShiitakeHttpRoutes(
      (id) => notes.agent.session(id),
      notes.agent.listSessions,
      notes.agent.createSession,
      notes.agent.deleteSession,
      () => notes.agent.extensions,
    );
    for (const method of ["POST", "PUT", "PATCH"])
      expect(
        (
          await routes.request(
            new Request("http://shiitake.test/sessions/s/grants", {
              method,
              headers: { "content-type": "application/json" },
              body: JSON.stringify({ version: 9, grants: { x: "write" } }),
            }),
          )
        ).status,
      ).toBe(404);
    const client = createShiitakeClient({
      baseUrl: "http://shiitake.test",
      fetch: (request, init) =>
        Promise.resolve(routes.request(new Request(request, init))),
    });
    expect("grants" in client).toBe(false);
    await client.close();
    expect(notes.agent.grants.read("s")).toBeNull();
  } finally {
    await notes.close();
  }
});
