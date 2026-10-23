import { Shiitake } from "@fungi.computer/shiitake";
import { memoryHost } from "@fungi.computer/shiitake/testing";
import { make as makeEffect } from "@fungi.computer/shiitake/effect";
import { DatabaseSync } from "node:sqlite";
import { mkdtempSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openNodeShiitake } from "@fungi.computer/shiitake/node";
import assert from "node:assert/strict";
import * as Schema from "effect/Schema";
import { ExtensionId } from "@fungi.computer/shiitake/extensions";
import {
  defineClientEntry,
  clientContributionSchemaV1,
} from "@fungi.computer/shiitake/client-entry";
import { rowSchemaV1 } from "@fungi.computer/gills";

const row = Schema.decodeUnknownSync(rowSchemaV1)({
  id: "public-row",
  title: "Public contribution",
  open: { params: [{ key: "session", value: "other" }] },
});
const entry = defineClientEntry(
  {
    id: ExtensionId.make("stranger"),
    commands: {},
    projection: { schema: Schema.String },
  },
  {
    contribute: (view) => ({
      chips: [],
      panel: view.status === "running" ? [row] : [],
      timeline: { cells: [], omitted: 0 },
      waiting: null,
    }),
  },
);
const contribution = Schema.decodeUnknownSync(clientContributionSchemaV1)(
  entry.contribute({
    status: "running",
    revision: "sha256:fixture",
    value: "ready",
  }),
);
assert.deepEqual(contribution.panel, [row]);
assert.deepEqual(
  entry.contribute({ status: "stopped", reason: "detached" }).panel,
  [],
);

if (Shiitake.make === undefined) {
  throw new Error("Shiitake stranger import did not expose its Promise facade");
}
if (makeEffect === undefined) {
  throw new Error("Shiitake stranger import did not expose its Effect facade");
}

const database = new DatabaseSync(":memory:");
const sql = {
  exec: (statement, ...bindings) => {
    const prepared = database.prepare(statement);
    const rows = /^\s*(?:SELECT|PRAGMA|WITH)\b/iu.test(statement)
      ? prepared.all(...bindings)
      : (prepared.run(...bindings), []);
    return {
      toArray: () => rows,
      one: () => {
        const row = rows[0];
        if (row === undefined) throw new Error("Expected one SQLite row");
        return row;
      },
    };
  },
};
const store = {
  sql,
  transactionSync: (operation) => {
    database.exec("BEGIN");
    try {
      const result = operation();
      database.exec("COMMIT");
      return result;
    } catch (error) {
      database.exec("ROLLBACK");
      throw error;
    }
  },
};
const model = {
  acquire: async () => ({
    selection: {
      provider: "stranger",
      model: "deterministic",
      catalogRevision: "catalog-1",
      definitionRevision: "definition-1",
      selectionPolicyRevision: "policy-1",
    },
    egress: {
      stream: async function* () {
        yield* [];
        return;
      },
    },
    release: async () => {
      await Promise.resolve();
    },
  }),
};
const instructions = {
  load: async () => ({ revision: "instructions-1", text: "" }),
};
const tools = { modules: [], sandbox: { execute: async () => undefined } };
const runtime = {
  bind: async () => ({
    commit: (mutation) => mutation(),
    requestWake: async () => Promise.resolve(),
    close: async () => Promise.resolve(),
  }),
};
const agent = await Shiitake.make({
  store,
  runtime,
  extensions: { roster: [], grants: [] },
  model,
  instructions,
  tools,
});
try {
  const accepted = await agent.session("stranger-session").command({
    requestId: "stranger-request",
    delivery: "followUp",

    type: "prompt",
    text: "open the garden",
  });
  if (accepted.status !== "accepted") {
    throw new Error("Shiitake stranger command was not accepted");
  }
} finally {
  await agent.close();
  database.close();
}

// Exercise the public helper from the packed package, without source fixtures.
const memory = await memoryHost();
try {
  const accepted = await memory.agent.session("memory-stranger").send("Hello", {
    requestId: "memory-stranger-prompt",
  });
  assert.equal(accepted.status, "accepted");
  const settled = await memory.settle("memory-stranger", 8);
  assert.equal(settled.activity.queue.state, "idle");
  assert.deepEqual(
    settled.terminals.map(({ outcome }) => outcome),
    ["completed"],
  );
  assert.deepEqual(settled.messages.at(-1)?.value.content, [
    { type: "text", text: "complete" },
  ]);
} finally {
  await memory.close();
}

// Open the packed Node host on a real file: private, durable, one owner.
const nodeDirectory = mkdtempSync(join(tmpdir(), "shiitake-stranger-node-"));
try {
  const nodePath = join(nodeDirectory, "agent.sqlite");
  const nodeOptions = {
    path: nodePath,
    extensions: { roster: [], grants: [] },
    instructions: {
      load: () => Promise.resolve({ revision: "v1", text: "Unused." }),
    },
    model: {
      acquire: () => Promise.reject(new Error("No model in this check")),
    },
  };
  const node = await openNodeShiitake(nodeOptions);
  try {
    await node.agent.createSession("stranger-node");
    await assert.rejects(openNodeShiitake(nodeOptions));
  } finally {
    await node.close();
  }
  assert.equal(statSync(nodePath).mode & 0o777, 0o600);
  const reopened = await openNodeShiitake(nodeOptions);
  try {
    assert.equal((await reopened.agent.listSessions()).sessions.length, 1);
  } finally {
    await reopened.close();
  }
} finally {
  rmSync(nodeDirectory, { recursive: true, force: true });
}
