import { createPost } from "@fungi.computer/carrier/post";
import { Watchdog } from "@fungi.computer/watchdog";
import * as Effect from "effect/Effect";
import { expect, it } from "vitest";
import { sessionIds } from "../src/internal/woodstock/identities.js";
import { prepareWoodstockSchema } from "../src/internal/woodstock/index.js";
import { readWoodstockStoreProtocol } from "../src/effect.js";
import * as woodstock from "../src/internal/woodstock/session-store.js";
import {
  createNativeSqliteOwnerFixture,
  type NativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

type Owner = NativeSqliteOwnerFixture["owner"];

it("reads the shared compatibility marker without stamping or rewriting stores", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    expect(readWoodstockStoreProtocol(fixture.owner)).toBe("fresh");
    expect(
      fixture.owner.sql
        .exec("SELECT name FROM sqlite_master WHERE name = 'woodstock_schema'")
        .toArray(),
    ).toEqual([]);
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    expect(readWoodstockStoreProtocol(fixture.owner)).toBe("v0");
    fixture.owner.sql.exec("UPDATE woodstock_schema SET version = 3");
    expect(readWoodstockStoreProtocol(fixture.owner)).toBe("upgradable");
    fixture.owner.sql.exec("UPDATE woodstock_schema SET version = 0");
    expect(readWoodstockStoreProtocol(fixture.owner)).toBe("unmarked");
    expect(
      fixture.owner.sql.exec("SELECT version FROM woodstock_schema").one()
        .version,
    ).toBe(0);
  } finally {
    fixture.close();
  }
});

// One Agent owner holds both stores. A Watchdog table this build cannot read is
// refused whole: nothing loads and the retained rows are not rewritten.
it("refuses a Woodstock store whose Watchdog jobs have an unknown shape without rewriting either", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  let executions = 0;
  try {
    const { owner } = fixture;
    Effect.runSync(prepareWoodstockSchema(owner));
    woodstock.appendSessionEntry(owner, {
      sessionId: "retained-session",
      id: sessionIds.fact.generate(),
      entry: {
        kind: "operator",
        workId: null,
        requestId: "retained",
        author: "operator",
        message: { role: "user", content: "unchanged", timestamp: 1 },
      },
    });
    owner.sql.exec("UPDATE woodstock_schema SET version = 5");
    owner.sql.exec(
      "CREATE TABLE watchdog_jobs (job_id TEXT, state TEXT CHECK(state IN ('queued', 'running', 'settled')))",
    );
    owner.sql.exec("INSERT INTO watchdog_jobs VALUES ('retained', 'queued')");
    const retained = () => ({
      version: owner.sql.exec("SELECT version FROM woodstock_schema").one()
        .version,
      entries: owner.sql
        .exec("SELECT id, data_json FROM session_entries ORDER BY id")
        .toArray(),
      jobs: owner.sql.exec("SELECT * FROM watchdog_jobs").toArray(),
    });
    const before = retained();
    expect(before.jobs).toEqual([{ job_id: "retained", state: "queued" }]);

    expect(
      Effect.runSync(Effect.result(prepareWoodstockSchema(owner))),
    ).toMatchObject({
      _tag: "Failure",
      failure: { boundary: "store", cause: "incompatible" },
    });
    await expect(
      Watchdog.make({
        owner,
        execute: () => {
          executions += 1;
          return Promise.resolve({ kind: "completed" });
        },
        isAlive: () => Promise.resolve(false),
        wake: { recompute: () => Promise.resolve() },
      }),
    ).rejects.toMatchObject({ cause: "store_invalid" });

    expect(executions).toBe(0);
    expect(retained()).toEqual(before);
  } finally {
    fixture.close();
  }
});

// Fungi 0.14 Watchdog jobs, before the scheduler added its deadline columns.
const preSchedulerJobs = `CREATE TABLE watchdog_jobs (
  sequence INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id TEXT UNIQUE NOT NULL,
  queue_name TEXT NOT NULL,
  lane TEXT NOT NULL,
  priority INTEGER NOT NULL,
  payload_json TEXT NOT NULL,
  recovery_max INTEGER,
  recovery_window_ms INTEGER CHECK (recovery_window_ms IS NULL OR recovery_window_ms > 0),
  recovery_window_started_at INTEGER,
  state TEXT NOT NULL CHECK (state IN ('queued', 'running', 'completing', 'settled')),
  outcome TEXT CHECK (
    outcome IS NULL OR outcome IN ('completed', 'failed', 'cancelled', 'outcome_unknown', 'interrupted')
  ),
  claim_token TEXT,
  recovery_count INTEGER NOT NULL DEFAULT 0,
  cancellation_id TEXT UNIQUE,
  failure_reason TEXT,
  CHECK (recovery_max IS NULL OR recovery_max >= 0),
  CONSTRAINT watchdog_execution_state CHECK (
    (state = 'queued' AND outcome IS NULL AND claim_token IS NULL) OR
    (state = 'running' AND outcome IS NULL AND claim_token IS NOT NULL) OR
    (state = 'completing' AND outcome IS NOT NULL AND claim_token IS NOT NULL) OR
    (state = 'settled' AND outcome IS NOT NULL)
  )
)`;
// A Fungi 0.14 pending Post row: it carries the retired nextAttemptAt deadline.
const preSchedulerPost = (owner: Owner): void => {
  owner.sql.exec(
    "CREATE TABLE IF NOT EXISTS carrier_post_rows (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
  );
  owner.sql.exec(
    "INSERT INTO carrier_post_rows VALUES (?,?)",
    "post:retained",
    JSON.stringify({
      state: "pending",
      envelope: { key: "retained", kind: "team", address: {}, event: {} },
      attempts: 0,
      nextAttemptAt: 1,
      jobId: "retained-post",
    }),
  );
};

it("upgrades Woodstock v5 with pre-scheduler Watchdog jobs in place, and still refuses pre-scheduler Post rows", async () => {
  const agent = createNativeSqliteOwnerFixture();
  const posted = createNativeSqliteOwnerFixture();
  let executions = 0;
  const watchdog = (owner: Owner) =>
    Watchdog.make({
      owner,
      execute: () => {
        executions += 1;
        return Promise.resolve({ kind: "completed" });
      },
      isAlive: () => Promise.resolve(false),
      wake: { recompute: () => Promise.resolve() },
    });
  const snapshot = (owner: Owner, tables: readonly string[]) =>
    Object.fromEntries(
      tables.map((table) => [
        table,
        owner.sql.exec(`SELECT * FROM ${table}`).toArray(),
      ]),
    );
  try {
    // One Fungi 0.14 Agent owner: Woodstock v5, pre-scheduler jobs and Post.
    Effect.runSync(prepareWoodstockSchema(agent.owner));
    agent.owner.sql.exec("UPDATE woodstock_schema SET version = 5");
    agent.owner.sql.exec(preSchedulerJobs);
    agent.owner.sql.exec(
      "INSERT INTO watchdog_jobs (job_id, queue_name, lane, priority, payload_json, state) VALUES ('retained', 'agent', 'post', 0, '{}', 'queued')",
    );
    preSchedulerPost(agent.owner);
    const postBeforeUpgrade = snapshot(agent.owner, ["carrier_post_rows"]);
    // The jobs gain their deadline columns and keep the retained job.
    expect(
      Effect.runSync(Effect.result(prepareWoodstockSchema(agent.owner)))._tag,
    ).toBe("Success");
    expect(
      agent.owner.sql.exec("SELECT version FROM woodstock_schema").one()
        .version,
    ).toBe(7);
    expect(
      agent.owner.sql
        .exec(
          "SELECT job_id, state, not_before, attempt_count FROM watchdog_jobs",
        )
        .toArray(),
    ).toEqual([
      { job_id: "retained", state: "queued", not_before: 0, attempt_count: 0 },
    ]);
    // Post's own rows are another owner's: the upgrade leaves them as they were.
    expect(snapshot(agent.owner, ["carrier_post_rows"])).toEqual(
      postBeforeUpgrade,
    );

    // Post refuses its own retained rows even beside a current Watchdog.
    preSchedulerPost(posted.owner);
    const current = await watchdog(posted.owner);
    const postBefore = snapshot(posted.owner, ["carrier_post_rows"]);
    expect(() =>
      createPost({
        routes: {},
        owner: posted.owner,
        transactional: current.transactional,
        now: () => 1,
        scheduleWake: () => Promise.resolve(),
      }),
    ).toThrow(expect.objectContaining({ reason: "pre-scheduler-store" }));
    expect(snapshot(posted.owner, ["carrier_post_rows"])).toEqual(postBefore);
    expect(
      posted.owner.sql
        .exec("SELECT COUNT(*) AS count FROM watchdog_jobs")
        .one(),
    ).toEqual({ count: 0 });
    expect(executions).toBe(0);
  } finally {
    agent.close();
    posted.close();
  }
});
