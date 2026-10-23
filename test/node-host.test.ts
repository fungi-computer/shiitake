import {
  linkSync,
  mkdtempSync,
  rmSync,
  statSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";

import { fauxModel } from "@fungi.computer/mule/testing";
import { rejectingSandbox } from "@fungi.computer/mycelium/testing";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { Hono } from "hono";
import { afterEach, expect, it, vi } from "vitest";

import { createShiitakeClient } from "../src/client.js";
import type { ShiitakeHttpEnv } from "../src/http-routes.js";
import {
  nodeRuntimeHost,
  openNodeShiitake,
  type NodeShiitake,
  type NodeShiitakeOptions,
} from "../src/node.js";

const directories: string[] = [];

afterEach(() => {
  for (const directory of directories.splice(0))
    rmSync(directory, { recursive: true, force: true });
});

function storePath(): string {
  const directory = mkdtempSync(join(tmpdir(), "shiitake-node-"));
  directories.push(directory);
  return join(directory, "agent.sqlite");
}

/** A Node Agent whose model replies `reply` once per run. */
function options(path: string, reply: string): NodeShiitakeOptions {
  return {
    path,
    extensions: { roster: [], grants: [] },
    instructions: {
      load: () => Promise.resolve({ revision: "v1", text: "Reply briefly." }),
    },
    model: {
      acquire: () => {
        const faux = fauxModel([]);
        faux.setResponses([fauxAssistantMessage(reply)]);
        return Promise.resolve({
          model: faux.model,
          retryable: () => false,
          stream: faux.stream,
          release: () => Promise.resolve(),
        });
      },
    },
    tools: { sandbox: rejectingSandbox("No tools on this Agent") },
  };
}

/** Wait until the Session's last message is the Agent's reply, bounded. */
async function settledReply(host: NodeShiitake, sessionId: string) {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    const snapshot = await host.agent.session(sessionId).read();
    const last = snapshot.messages.at(-1)?.value;
    if (last?.role === "assistant" && snapshot.activity.queue.state === "idle")
      return last.content;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error("The run did not settle");
}

it("runs a prompt to completion on its own event loop, in a file on disk", async () => {
  const path = storePath();
  const configured = options(path, "hello from disk");
  const load = vi.fn(configured.instructions.load);
  const host = await openNodeShiitake({
    ...configured,
    instructions: { load },
  });
  try {
    const sessionId = await host.agent.createSession("first");
    const accepted = await host.agent
      .session(sessionId)
      .send("Hi", { requestId: "hi" });
    expect(await settledReply(host, sessionId)).toEqual([
      { type: "text", text: "hello from disk" },
    ]);
    expect(load).toHaveBeenCalledWith(
      expect.objectContaining({ sessionId, commandId: accepted.commandId }),
    );
  } finally {
    await host.close();
  }
  // The transcript is durable: a new owner of the same file reads it back.
  const reopened = await openNodeShiitake(options(path, "unused"));
  try {
    const page = await reopened.agent.listSessions();
    expect(page.sessions).toHaveLength(1);
  } finally {
    await reopened.close();
  }
});

it("refuses a second owner of the same store while the first holds it", async () => {
  const path = storePath();
  const host = await openNodeShiitake(options(path, "first owner"));
  try {
    await expect(
      openNodeShiitake(options(path, "second owner")),
    ).rejects.toThrow();
  } finally {
    await host.close();
  }
  // Closing releases the claim.
  const next = await openNodeShiitake(options(path, "next owner"));
  await next.close();
});

it("resumes accepted work after the owner reopens the file", async () => {
  const path = storePath();
  const first = await openNodeShiitake(options(path, "never ran"));
  const sessionId = await first.agent.createSession("resume");
  // Accept the prompt, then close before the event loop runs it.
  await first.agent
    .session(sessionId)
    .send("Pick this up later", { requestId: "later" });
  await first.close();
  const second = await openNodeShiitake(options(path, "picked up"));
  try {
    expect(await settledReply(second, sessionId)).toEqual([
      { type: "text", text: "picked up" },
    ]);
  } finally {
    await second.close();
  }
});

it("serves its Session routes behind host middleware that sets metadata", async () => {
  const path = storePath();
  const host = await openNodeShiitake(options(path, "over http"));
  const app = new Hono<ShiitakeHttpEnv>()
    .use(async (context, next) => {
      context.set("shiitakeMeta", { author: { id: "ada", name: "Ada" } });
      await next();
      return undefined;
    })
    .route("/", host.routes);
  const client = createShiitakeClient({
    baseUrl: "http://node.test",
    fetch: (input, init) =>
      Promise.resolve(app.request(new Request(input, init))),
  });
  try {
    const sessionId = await client.createSession("http");
    await client.session(sessionId).send("Hi", { requestId: "http-hi" });
    await settledReply(host, sessionId);
    const [prompt] = (await client.session(sessionId).read()).messages;
    expect(prompt?.meta).toEqual({ author: { id: "ada", name: "Ada" } });
  } finally {
    await client.close();
    await host.close();
  }
});

it("keeps the store and its lease file private to this user, whatever the umask", async () => {
  const path = storePath();
  const previous = process.umask(0o022);
  try {
    const host = await openNodeShiitake(options(path, "private"));
    await host.close();
  } finally {
    process.umask(previous);
  }
  expect(statSync(path).mode & 0o777).toBe(0o600);
  expect(statSync(`${path}.owner.sqlite`).mode & 0o777).toBe(0o600);
  // An existing store left readable is narrowed when it is opened.
  writeFileSync(join(path, "..", "loose.sqlite"), "", { mode: 0o644 });
  const loose = join(path, "..", "loose.sqlite");
  const reopened = await openNodeShiitake(options(loose, "narrowed"));
  await reopened.close();
  expect(statSync(loose).mode & 0o777).toBe(0o600);
});

it("refuses a second owner that reaches the store through a symbolic link", async () => {
  const path = storePath();
  const linked = join(path, "..", "linked.sqlite");
  const host = await openNodeShiitake(options(path, "first owner"));
  try {
    symlinkSync(path, linked);
    await expect(
      openNodeShiitake(options(linked, "second owner")),
    ).rejects.toThrow();
  } finally {
    await host.close();
  }
  // A link is refused even with no owner holding the store.
  await expect(openNodeShiitake(options(linked, "alone"))).rejects.toThrow();
});

it("refuses a second owner that reaches the store through a hard link", async () => {
  const path = storePath();
  const linked = join(path, "..", "linked.sqlite");
  const host = await openNodeShiitake(options(path, "first owner"));
  try {
    linkSync(path, linked);
    await expect(
      openNodeShiitake(options(linked, "second owner")),
    ).rejects.toThrow("one regular file with one name");
  } finally {
    await host.close();
  }
  // While the second name exists, neither name opens.
  await expect(openNodeShiitake(options(path, "either"))).rejects.toThrow(
    "one regular file with one name",
  );
});

/**
 * Let a pass's promise chain finish under fake timers. A fake immediate runs
 * on the next tick, so each turn moves the clock by one millisecond.
 */
async function settlePass(): Promise<void> {
  for (let turn = 0; turn < 10; turn += 1) await vi.advanceTimersByTimeAsync(1);
}

it("retries after a failed pass even when the failure observer throws", async () => {
  vi.useFakeTimers({
    toFake: [
      "setTimeout",
      "clearTimeout",
      "setImmediate",
      "clearImmediate",
      "Date",
    ],
  });
  try {
    let reconciles = 0;
    const host = nodeRuntimeHost(() => {
      throw new Error("observer broke");
    });
    await host.runtime.bind({
      reconcile: () => {
        reconciles += 1;
        return reconciles === 1
          ? Promise.reject(new Error("first pass fails"))
          : Promise.resolve();
      },
      pendingWork: () => Promise.resolve({ pending: false, nextWakeAt: null }),
    });
    await settlePass();
    expect(reconciles).toBe(1);
    await vi.advanceTimersByTimeAsync(5_000);
    await settlePass();
    expect(reconciles).toBe(2);
    await host.close();
  } finally {
    vi.useRealTimers();
  }
});

it("keeps the earliest requested wake when a later one arrives", async () => {
  vi.useFakeTimers({
    toFake: [
      "setTimeout",
      "clearTimeout",
      "setImmediate",
      "clearImmediate",
      "Date",
    ],
  });
  try {
    let reconciles = 0;
    const host = nodeRuntimeHost(() => undefined);
    await host.runtime.bind({
      reconcile: () => {
        reconciles += 1;
        return Promise.resolve();
      },
      pendingWork: () => Promise.resolve({ pending: false, nextWakeAt: null }),
    });
    await settlePass();
    expect(reconciles).toBe(1);
    host.scheduleWake(Date.now() + 1_000);
    host.scheduleWake(Date.now() + 60_000);
    await vi.advanceTimersByTimeAsync(1_000);
    await settlePass();
    expect(reconciles).toBe(2);
    await host.close();
  } finally {
    vi.useRealTimers();
  }
});

it("opens a store with one broken Session, refusing that Session by file", async () => {
  const path = storePath();
  const host = await openNodeShiitake(options(path, "kept"));
  let broken: string;
  let healthy: string;
  try {
    broken = await host.agent.createSession("broken");
    await host.agent.session(broken).send("Hi", { requestId: "hi" });
    await settledReply(host, broken);
    healthy = await host.agent.createSession("healthy");
  } finally {
    await host.close();
  }
  // A writer outside Woodstock drops its guards and removes a parent row.
  const raw = new DatabaseSync(path);
  try {
    for (const { name } of raw
      .prepare("SELECT name FROM sqlite_master WHERE type = 'trigger'")
      .all())
      raw.exec(`DROP TRIGGER "${String(name)}"`);
    raw
      .prepare(
        "DELETE FROM session_entries WHERE session_id = ? AND sequence = (SELECT MIN(sequence) + 1 FROM session_entries WHERE session_id = ?)",
      )
      .run(broken, broken);
  } finally {
    raw.close();
  }

  const reopened = await openNodeShiitake(options(path, "unused"));
  try {
    await expect(reopened.agent.session(broken).read()).rejects.toThrow(
      `Store file ${path}: Woodstock Session ${broken} holds a broken lineage (this store has 1 damaged Session, first ${broken}); a store never repairs itself — delete this Session with deleteSession("${broken}"), or reset the owning store:`,
    );
    await expect(reopened.agent.session(healthy).read()).resolves.toBeDefined();
  } finally {
    await reopened.close();
  }
});
