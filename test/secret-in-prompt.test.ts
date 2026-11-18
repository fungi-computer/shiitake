// Runtime: Node 24, `node:sqlite` through the Node host, with a scripted model.
import { mkdtempSync, readdirSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";

import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { fauxModel } from "@fungi.computer/mule/testing";
import { afterEach, describe, expect, it } from "vitest";

import { createShiitakeClient } from "../src/client.js";
import { createShiitakeHttpRoutes } from "../src/http-routes.js";
import type { SessionRedactor } from "../src/index.js";
import { openNodeShiitake } from "../src/node.js";

const directories: string[] = [];

afterEach(() => {
  for (const directory of directories.splice(0))
    rmSync(directory, { recursive: true, force: true });
});

/** A value the owner's Vault stores; no public pattern knows its shape. */
const storedCanary = `vault-canary-${crypto.randomUUID()}`;
/** Provider-shaped, never stored: only the maintained rule set finds it. */
const providerKey = ["ghp_", "7Zk9P2mQa4Wv6Xc8Bn0Ds3Fg5Hj1Lr9Ty2Ui"].join("");

const refused = { boundary: "contract", cause: "secret_in_prompt" } as const;

/**
 * The host's Vault port: a known-value matcher for `storedCanary` only. The
 * law is Shiitake's intake, so the host side is the smallest honest port.
 */
function vaultPort(): SessionRedactor {
  const redact = (text: string) =>
    Promise.resolve(
      text.includes(storedCanary)
        ? {
            text: text.replaceAll(storedCanary, "[REDACTED]"),
            status: "redacted" as const,
          }
        : { text, status: "clean" as const },
    );
  return {
    contains: (text) => Promise.resolve(text.includes(storedCanary)),
    redact,
    stream: () => {
      let held = "";
      return {
        write: (chunk) => {
          held += chunk;
          return Promise.resolve({ text: "", status: "clean" as const });
        },
        finish: () => redact(held),
      };
    },
  };
}

async function openAgent(
  redaction: Readonly<{ load: () => Promise<SessionRedactor> }> | undefined,
) {
  const directory = mkdtempSync(join(tmpdir(), "shiitake-secret-intake-"));
  directories.push(directory);
  const path = join(directory, "agent.sqlite");
  const faux = fauxModel([]);
  faux.setResponses([fauxAssistantMessage("Noted.")]);
  const options: Parameters<typeof openNodeShiitake>[0] = {
    path,
    extensions: { roster: [], grants: [] },
    instructions: {
      load: () => Promise.resolve({ revision: "v1", text: "Help." }),
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
  };
  const host = await openNodeShiitake(
    redaction === undefined ? options : { ...options, redaction },
  );
  return { host, directory, path };
}

/** Every byte the host persisted: the database and its WAL side files. */
function storeText(directory: string): string {
  return readdirSync(directory)
    .map((name) =>
      new TextDecoder("latin1").decode(readFileSync(join(directory, name))),
    )
    .join("\n");
}

function storedRows(path: string) {
  const database = new DatabaseSync(path, { readOnly: true });
  try {
    const count = (table: string) =>
      Number(database.prepare(`SELECT count(*) AS n FROM ${table}`).get()?.n);
    return {
      admissions: count("session_command_admissions"),
      entries: count("session_entries"),
    };
  } finally {
    database.close();
  }
}

function prompt(
  requestId: string,
  text: string,
  delivery: "followUp" | "steer" = "followUp",
) {
  return { type: "prompt" as const, requestId, text, delivery };
}

describe("secret_in_prompt intake refusal (Node, node:sqlite)", () => {
  it("refuses a stored value or a provider-shaped key before anything is admitted or stored", async () => {
    const { host, directory, path } = await openAgent({
      load: () => Promise.resolve(vaultPort()),
    });
    const agent = host.agent;
    const routes = createShiitakeHttpRoutes(
      (id) => agent.session(id),
      agent.listSessions,
      agent.createSession,
      agent.deleteSession,
      () => agent.extensions,
    );
    const client = createShiitakeClient({
      baseUrl: "http://shiitake.test",
      fetch: (input, init) =>
        Promise.resolve(routes.request(new Request(input, init))),
    });
    try {
      const sessionId = await agent.createSession("vault-paste");
      const session = agent.session(sessionId);
      await expect(
        session.command(prompt("stored", `use ${storedCanary} for deploys`)),
      ).rejects.toMatchObject(refused);
      await expect(
        session.command(prompt("shaped", `token: ${providerKey}`, "steer")),
      ).rejects.toMatchObject(refused);
      // The typed refusal crosses the HTTP wire intact.
      await expect(
        client
          .session(sessionId)
          .command(prompt("wire", `export GH=${providerKey}`)),
      ).rejects.toMatchObject(refused);
      expect((await session.read()).terminals).toEqual([]);
    } finally {
      await client.close();
      await host.close();
    }
    expect(storedRows(path)).toEqual({ admissions: 0, entries: 0 });
    const bytes = storeText(directory);
    expect(bytes).not.toContain(storedCanary);
    expect(bytes).not.toContain(providerKey);
  });

  it("refuses a secret in an operator or party line and in compaction focus, storing nothing", async () => {
    const { host, directory, path } = await openAgent({
      load: () => Promise.resolve(vaultPort()),
    });
    try {
      const sessionId = await host.agent.createSession("vault-takeover");
      const session = host.agent.session(sessionId);
      // A live takeover: the operator's reply and the visitor's own line.
      await expect(
        session.command({
          type: "operator",
          requestId: "operator",
          author: "operator",
          text: `the key is ${storedCanary}`,
        }),
      ).rejects.toMatchObject(refused);
      await expect(
        session.command({
          type: "operator",
          requestId: "party",
          author: "party",
          text: `here you go ${providerKey}`,
        }),
      ).rejects.toMatchObject(refused);
      await expect(
        session.command({
          type: "compact",
          requestId: "focus",
          instructions: `keep ${storedCanary} in the summary`,
        }),
      ).rejects.toMatchObject(refused);
      // A line with no secret is still recorded.
      await session.command({
        type: "operator",
        requestId: "plain",
        author: "party",
        text: "Thanks, that worked.",
      });
    } finally {
      await host.close();
    }
    expect(storedRows(path).admissions).toBe(0);
    const bytes = storeText(directory);
    expect(bytes).not.toContain(storedCanary);
    expect(bytes).not.toContain(providerKey);
  });

  it("admits a prompt that only talks about API keys", async () => {
    const { host, path } = await openAgent({
      load: () => Promise.resolve(vaultPort()),
    });
    try {
      const sessionId = await host.agent.createSession("vault-talk");
      const accepted = await host.agent
        .session(sessionId)
        .command(prompt("talk", "Where should I put my OpenAI API key?"));
      expect(accepted.status).toBe("accepted");
    } finally {
      await host.close();
    }
    expect(storedRows(path).admissions).toBe(1);
  });

  it("still refuses a provider-shaped key on a host without a Vault", async () => {
    const { host, path } = await openAgent(undefined);
    try {
      const sessionId = await host.agent.createSession("no-vault");
      await expect(
        host.agent
          .session(sessionId)
          .command(prompt("shaped", `key=${providerKey}`)),
      ).rejects.toMatchObject(refused);
    } finally {
      await host.close();
    }
    expect(storedRows(path)).toEqual({ admissions: 0, entries: 0 });
  });

  it("admits an unfocused compaction with the Vault port down, then fails it closed at run start", async () => {
    const { host } = await openAgent({
      load: () => Promise.reject(new Error("Vault is unreachable")),
    });
    try {
      const sessionId = await host.agent.createSession("compact-down");
      const session = host.agent.session(sessionId);
      // No person's text to check, so intake does not ask the port.
      const accepted = await session.command({
        type: "compact",
        requestId: "compact-down",
      });
      expect(accepted.status).toBe("accepted");
      const settled = await session.readSettlement(accepted.commandId, {
        timeoutMs: 5_000,
      });
      expect(settled?.terminal.outcome).toBe("failed");
      expect(settled?.terminal.diagnostic).toContain("redaction-unavailable");
    } finally {
      await host.close();
    }
  });

  it("refuses intake when the host's Vault port cannot load (fails closed)", async () => {
    const { host, path } = await openAgent({
      load: () => Promise.reject(new Error("Vault is unreachable")),
    });
    try {
      const sessionId = await host.agent.createSession("vault-down");
      await expect(
        host.agent
          .session(sessionId)
          .command(prompt("down", "plain words, no secret")),
      ).rejects.toMatchObject({ boundary: "contract", cause: "unavailable" });
      // The maintained patterns need no port: a recognisable key is still
      // refused as a key while the Vault is down.
      await expect(
        host.agent
          .session(sessionId)
          .command(prompt("down-key", `key=${providerKey}`)),
      ).rejects.toMatchObject(refused);
    } finally {
      await host.close();
    }
    expect(storedRows(path)).toEqual({ admissions: 0, entries: 0 });
  });
});
