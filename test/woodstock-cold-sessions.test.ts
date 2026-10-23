import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { expect, it } from "vitest";

import { Shiitake, type SqliteValue } from "../src/index.js";
import { readColdSessionPage } from "../src/internal/woodstock/index.js";
import { attentionExtension } from "./fixtures/attention.fixture.js";
import { openCore } from "./fixtures/extension-host.fixture.js";
import { grantAll } from "./fixtures/fixture-extensions.js";
import {
  compatibilityRunDependencies,
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

const encoder = new TextEncoder();
const decodeMeasured = Schema.decodeUnknownSync(
  Schema.Struct({ session_id: Schema.String, bytes: Schema.Int }),
);
const decodePayload = Schema.decodeUnknownSync(
  Schema.Struct({ payload: Schema.String }),
);

function prompt(requestId: string, text: string) {
  return {
    type: "prompt" as const,
    requestId,
    text,
    delivery: "followUp" as const,
  };
}

async function open() {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const agent = await Shiitake.make({
    ...compatibilityRunDependencies(),
    store: fixture.owner,
    runtime: runtime.runtime,
  });
  return { fixture, runtime, agent };
}

/** Independent oracle: the byte length of every stored payload of a Session. */
function storedBytes(
  fixture: ReturnType<typeof createNativeSqliteOwnerFixture>,
  sessionId: string,
): number {
  let total = 0;
  for (const [table, column] of [
    ["session_entries", "data_json"],
    ["session_command_admissions", "data_json"],
    ["session_attention", "value_json"],
    ["session_tool_grants", "grants_json"],
  ] as const)
    for (const row of fixture.owner.sql
      .exec(
        `SELECT ${column} AS payload FROM ${table} WHERE session_id = ?`,
        sessionId,
      )
      .toArray())
      total += encoder.encode(decodePayload(row).payload).byteLength;
  return total;
}

it("reports each Session's bytes equal to its stored row lengths", async () => {
  const { fixture, runtime, agent } = await open();
  try {
    const plain = await agent.createSession("plain");
    const wordy = await agent.createSession("wordy");
    await agent.session(plain).command(prompt("p1", "hi"));
    await runtime.runNext();
    await agent.session(wordy).command(prompt("w1", "héllo 🌱 ".repeat(200)));
    await runtime.runNext();

    const page = await Effect.runPromise(
      readColdSessionPage(fixture.owner, {}),
    );
    const byId = new Map(page.sessions.map((row) => [row.sessionId, row]));
    expect(byId.get(plain)?.bytes).toBe(storedBytes(fixture, plain));
    expect(byId.get(wordy)?.bytes).toBe(storedBytes(fixture, wordy));
    expect(byId.get(wordy)?.bytes).toBeGreaterThan(byId.get(plain)?.bytes ?? 0);
  } finally {
    await agent.close();
    fixture.close();
  }
});

it("reports a Session with a running run as not idle and a settled one as idle", async () => {
  const { fixture, runtime, agent } = await open();
  try {
    const settled = await agent.createSession("settled");
    const running = await agent.createSession("running");
    await agent.session(settled).command(prompt("s1", "done soon"));
    await runtime.runNext();
    await agent.session(running).command(prompt("r1", "still queued"));

    const page = await Effect.runPromise(
      readColdSessionPage(fixture.owner, {}),
    );
    const idle = new Map(page.sessions.map((row) => [row.sessionId, row.idle]));
    expect(idle.get(settled)).toBe(true);
    expect(idle.get(running)).toBe(false);
  } finally {
    await agent.close();
    fixture.close();
  }
});

it("returns no rows and no cursor for an empty store", async () => {
  const { fixture, agent } = await open();
  try {
    expect(
      await Effect.runPromise(readColdSessionPage(fixture.owner, {})),
    ).toEqual({ sessions: [] });
  } finally {
    await agent.close();
    fixture.close();
  }
});

it("bounds the page by its limit and walks every Session once through the cursor", async () => {
  const { fixture, agent } = await open();
  try {
    const ids = [
      await agent.createSession("a"),
      await agent.createSession("b"),
      await agent.createSession("c"),
    ];
    const first = await Effect.runPromise(
      readColdSessionPage(fixture.owner, { limit: 2 }),
    );
    expect(first.sessions).toHaveLength(2);
    const cursor = first.next;
    if (cursor === undefined) throw new Error("A full page has a cursor");
    const second = await Effect.runPromise(
      readColdSessionPage(fixture.owner, { limit: 2, after: cursor }),
    );
    expect(second.sessions).toHaveLength(1);
    expect(second.next).toBeUndefined();
    expect(
      [...first.sessions, ...second.sessions]
        .map((row) => row.sessionId)
        .sort(),
    ).toEqual([...ids].sort());
  } finally {
    await agent.close();
    fixture.close();
  }
});

it("ends the walk with no cursor when the last page fills the limit exactly", async () => {
  const { fixture, agent } = await open();
  try {
    await agent.createSession("a");
    await agent.createSession("b");
    const page = await Effect.runPromise(
      readColdSessionPage(fixture.owner, { limit: 2 }),
    );
    expect(page.sessions).toHaveLength(2);
    expect(page.next).toBeUndefined();
  } finally {
    await agent.close();
    fixture.close();
  }
});

it("refuses a limit past the page bound, a zero limit and a forged cursor", async () => {
  const { fixture, agent } = await open();
  try {
    const refused = { boundary: "contract", cause: "invalid" };
    for (const input of [
      { limit: 201 },
      { limit: 0 },
      { limit: 1.5 },
      { after: "not-a-cursor" },
    ])
      await expect(
        Effect.runPromise(readColdSessionPage(fixture.owner, input)),
      ).rejects.toMatchObject(refused);
    await expect(
      Effect.runPromise(readColdSessionPage(fixture.owner, { limit: 200 })),
    ).resolves.toEqual({ sessions: [] });
  } finally {
    await agent.close();
    fixture.close();
  }
});

it("lists the coldest Session first", async () => {
  const { fixture, agent } = await open();
  try {
    const older = await agent.createSession("older");
    const newer = await agent.createSession("newer");
    fixture.owner.sql.exec(
      "UPDATE session_inbox SET last_activity_at = 100 WHERE session_id = ?",
      older,
    );
    fixture.owner.sql.exec(
      "UPDATE session_inbox SET last_activity_at = 200 WHERE session_id = ?",
      newer,
    );
    const page = await Effect.runPromise(
      readColdSessionPage(fixture.owner, {}),
    );
    expect(
      page.sessions.map((row) => [row.sessionId, row.lastActivityAt]),
    ).toEqual([
      [older, 100],
      [newer, 200],
    ]);
  } finally {
    await agent.close();
    fixture.close();
  }
});

it("reports a Session waiting on a person as not idle", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const extension = attentionExtension();
  const opened = await openCore(fixture.owner, grantAll([extension]));
  try {
    const session = opened.core.agent.session("waiting");
    await Effect.runPromise(session.send("hello", { requestId: "hello" }));
    await opened.host.drain();
    const settled = await Effect.runPromise(
      readColdSessionPage(fixture.owner, {}),
    );
    expect(settled.sessions).toMatchObject([
      { sessionId: "waiting", idle: true },
    ]);

    await Effect.runPromise(
      session.extension(extension.manifest.id).call("set", {
        id: "choice",
        kind: "question",
        summary: "Choose a destination",
        since: 1,
        expiresAt: 2,
      }),
    );
    const waiting = await Effect.runPromise(
      readColdSessionPage(fixture.owner, {}),
    );
    expect(waiting.sessions).toMatchObject([
      { sessionId: "waiting", idle: false },
    ]);
  } finally {
    await Effect.runPromise(opened.core.agent.close);
    fixture.close();
  }
});

it("walks Sessions with equal last activity once each in a stable order", async () => {
  const { fixture, agent } = await open();
  try {
    const ids = [
      await agent.createSession("a"),
      await agent.createSession("b"),
      await agent.createSession("c"),
      await agent.createSession("d"),
    ];
    fixture.owner.sql.exec("UPDATE session_inbox SET last_activity_at = 500");
    const walked: string[] = [];
    let after: string | undefined;
    do {
      const page = await Effect.runPromise(
        readColdSessionPage(
          fixture.owner,
          after === undefined ? { limit: 1 } : { limit: 1, after },
        ),
      );
      walked.push(...page.sessions.map((row) => row.sessionId));
      after = page.next;
    } while (after !== undefined);
    expect(walked).toEqual([...ids].sort());
    const whole = await Effect.runPromise(
      readColdSessionPage(fixture.owner, {}),
    );
    expect(whole.sessions.map((row) => row.sessionId)).toEqual(walked);
  } finally {
    await agent.close();
    fixture.close();
  }
});

it("does not measure the discarded lookahead Session", async () => {
  const { fixture, runtime, agent } = await open();
  try {
    const ids = [
      await agent.createSession("a"),
      await agent.createSession("b"),
      await agent.createSession("c"),
    ];
    for (const [id, requestId] of [
      [ids[0], "p1"],
      [ids[1], "p2"],
      [ids[2], "p3"],
    ] as const) {
      if (id === undefined) throw new Error("Session was not created");
      await agent
        .session(id)
        .command(prompt(requestId, "héllo 🌱 ".repeat(50)));
      await runtime.runNext();
    }
    const measured: { sessionId: string; bytes: number }[] = [];
    const watching = {
      ...fixture.owner,
      sql: {
        exec: (statement: string, ...bindings: SqliteValue[]) => {
          const cursor = fixture.owner.sql.exec(statement, ...bindings);
          if (!statement.includes("session_inbox")) return cursor;
          for (const row of cursor.toArray()) {
            const decoded = decodeMeasured(row);
            measured.push({
              sessionId: decoded.session_id,
              bytes: decoded.bytes,
            });
          }
          return cursor;
        },
      },
    };

    const page = await Effect.runPromise(
      readColdSessionPage(watching, { limit: 2 }),
    );

    const returned = page.sessions.map((row) => row.sessionId);
    expect(returned).toHaveLength(2);
    expect(measured).toHaveLength(3);
    const lookahead = measured.at(-1);
    if (lookahead === undefined) throw new Error("The query read no rows");
    expect(returned).not.toContain(lookahead.sessionId);
    expect(storedBytes(fixture, lookahead.sessionId)).toBeGreaterThan(0);
    expect(lookahead.bytes).toBe(0);
    for (const row of page.sessions)
      expect(row.bytes).toBe(storedBytes(fixture, row.sessionId));
  } finally {
    await agent.close();
    fixture.close();
  }
});
