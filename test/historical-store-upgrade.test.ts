import { copyFileSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Watchdog } from "@fungi.computer/watchdog";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { afterEach, describe, expect, it } from "vitest";
import { prepareWoodstockSchema } from "../src/internal/woodstock/index.js";
import { decodeStoredSessionEntry } from "../src/internal/woodstock/session-entry.js";
import { readSessionSummaries } from "../src/effect.js";
import type { SqliteValue } from "../src/internal/contracts.js";
import {
  readWoodstockStoreProtocol,
  upgradeWoodstockStore,
} from "../src/effect.js";
import {
  createNativeSqliteOwnerFixture,
  type NativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

/**
 * The fixtures are Agent stores a real Local host wrote at the last commit of
 * each Woodstock marker, with a stub model and a few synthetic Sessions (see
 * fixtures/historical-stores/README.md). Nothing in them is anyone's own data.
 */
const fixtureDirectory = join(
  dirname(fileURLToPath(import.meta.url)),
  "fixtures",
  "historical-stores",
);

const scratch: string[] = [];
const open: NativeSqliteOwnerFixture[] = [];
afterEach(() => {
  for (const fixture of open.splice(0)) fixture.close();
  for (const directory of scratch.splice(0))
    rmSync(directory, { recursive: true, force: true });
});

/** A private copy of one fixture, so a test never writes the committed bytes. */
function copyOf(marker: number): NativeSqliteOwnerFixture {
  const directory = mkdtempSync(join(tmpdir(), "woodstock-upgrade-"));
  scratch.push(directory);
  const path = join(directory, "agent.sqlite");
  copyFileSync(
    join(fixtureDirectory, `marker-${String(marker)}.agent.sqlite`),
    path,
  );
  const fixture = createNativeSqliteOwnerFixture(path);
  open.push(fixture);
  return fixture;
}

type Row = Record<string, SqliteValue>;
type Owner = NativeSqliteOwnerFixture["owner"];

function rowsOf(owner: Owner, statement: string): readonly Row[] {
  return owner.sql.exec(statement).toArray();
}

const decodeMarker = Schema.decodeUnknownSync(
  Schema.Struct({ version: Schema.Int }),
);
const decodeRetryFact = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({
      event: Schema.Struct({ delayMs: Schema.Number, dueAt: Schema.Number }),
    }),
  ),
);

/** Reads a column the test knows holds text. */
const text = Schema.decodeUnknownSync(Schema.String);

function marker(owner: Owner): number {
  return decodeMarker(
    owner.sql.exec("SELECT version FROM woodstock_schema").one(),
  ).version;
}

/** Everything a person could see of the store, in a form that compares. */
function readStore(owner: Owner) {
  return {
    identity: rowsOf(owner, "SELECT * FROM private_local_identity"),
    modelSettings: rowsOf(owner, "SELECT * FROM private_local_model_settings"),
    sessions: rowsOf(owner, "SELECT * FROM sessions ORDER BY id"),
    entries: rowsOf(
      owner,
      "SELECT id, session_id, parent_id, sequence, type, state, timestamp, data_json FROM session_entries ORDER BY session_id, sequence",
    ),
    inbox: rowsOf(owner, "SELECT * FROM session_inbox ORDER BY session_id"),
    jobs: rowsOf(
      owner,
      "SELECT sequence, job_id, queue_name, lane, priority, payload_json, state, outcome, claim_token, recovery_count, cancellation_id, failure_reason, recovery_max FROM watchdog_jobs ORDER BY sequence",
    ),
    records: rowsOf(
      owner,
      "SELECT * FROM shiitake_extension_records ORDER BY extension_id, collection, key",
    ),
  };
}

type Store = ReturnType<typeof readStore>;

type FactRow = Store["entries"][number];

const Unknown = Schema.Unknown;
const decodeOldToolOutcome = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({ result: Schema.Struct({ details: Unknown }) }),
  ),
);
const decodeNewData = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Struct({ data: Unknown })),
);
const decodeOldToolMessage = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({ value: Schema.Struct({ details: Unknown }) }),
  ),
);
const decodeOldCompaction = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({ firstKeptEntryId: Schema.String, summary: Schema.String }),
  ),
);
const decodeNewCompaction = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({
      head: Schema.String,
      model: Schema.Tuple([
        Schema.Struct({
          content: Schema.Tuple([Schema.Struct({ text: Schema.String })]),
        }),
      ]),
    }),
  ),
);

/** Each kind of change an upgrade makes to a stored fact, checked against what it replaced. */
function expectRewrittenFact(original: FactRow | undefined, entry: FactRow) {
  const before = text(original?.data_json);
  const after = text(entry.data_json);
  switch (entry.type) {
    case "live_retry": {
      const kept = decodeRetryFact(after);
      expect(kept.event.dueAt).toBe(
        Number(entry.timestamp) + kept.event.delayMs,
      );
      return;
    }
    case "tool_outcome":
      expect(after).not.toContain('"details"');
      expect(decodeNewData(after).data).toEqual(
        decodeOldToolOutcome(before).result.details,
      );
      return;
    case "message":
      expect(after).not.toContain('"details"');
      expect(decodeNewData(after).data).toEqual(
        decodeOldToolMessage(before).value.details,
      );
      return;
    case "compaction": {
      const old = decodeOldCompaction(before);
      const kept = decodeNewCompaction(after);
      expect(kept.head).toBe(old.firstKeptEntryId);
      expect(kept.model[0].content[0].text).toContain(old.summary);
      return;
    }
    default:
      throw new Error("Unexpected rewritten fact " + text(entry.type));
  }
}

/**
 * Every history fact survives in place and still reads; returns which kinds of
 * fact the upgrade had to rewrite, each checked against what it replaced.
 */
function rewrittenFacts(before: Store, after: Store): Map<string, number> {
  expect(after.entries).toHaveLength(before.entries.length);
  const changed = new Map<string, number>();
  for (const [index, entry] of after.entries.entries()) {
    const original = before.entries[index];
    expect(decodeStoredSessionEntry(text(entry.data_json))._tag).toBe(
      "Success",
    );
    if (entry.data_json === original?.data_json) continue;
    expectRewrittenFact(original, entry);
    expect({ ...entry, data_json: null }).toEqual({
      ...original,
      data_json: null,
    });
    const type = text(entry.type);
    changed.set(type, (changed.get(type) ?? 0) + 1);
  }
  return changed;
}

/** What each marker's builds stored in the old shape. */
function expectedRewrites(from: number): Map<string, number> {
  switch (from) {
    case 1:
      return new Map([
        ["compaction", 1],
        ["tool_outcome", 1],
        ["message", 1],
      ]);
    case 2:
      return new Map([
        ["tool_outcome", 1],
        ["message", 1],
      ]);
    case 3:
    case 4:
      return new Map([["live_retry", 1]]);
    default:
      return new Map();
  }
}

/** Every earlier job keeps its place; the timer is now one more queued job, due when it was. */
function expectTimerAsQueuedJob(
  owner: Owner,
  before: Store,
  after: Store,
  timersBefore: readonly Row[],
): void {
  expect(after.jobs.slice(0, before.jobs.length)).toEqual(before.jobs);
  const timers = rowsOf(
    owner,
    "SELECT extension_id, key, job_id FROM shiitake_extension_timers",
  );
  expect(timers).toHaveLength(1);
  const [timer] = timers;
  const [job] = rowsOf(
    owner,
    `SELECT * FROM watchdog_jobs WHERE job_id = '${text(timer?.job_id)}'`,
  );
  expect(job).toMatchObject({
    queue_name: timer?.job_id,
    lane: "extension_timer",
    state: "queued",
    outcome: null,
    claim_token: null,
    cancellation_id: null,
    not_before: timersBefore[0]?.due_at,
    attempt_count: 0,
    payload_json: JSON.stringify({
      extensionId: timer?.extension_id,
      key: timer?.key,
    }),
  });
  expect(after.jobs.length).toBe(before.jobs.length + 1);
}

const markers = [1, 2, 3, 4, 5] as const;

describe.each(markers)(
  "a real Agent store saved at Woodstock marker %s",
  (from) => {
    it("is recognised as upgradable, not refused", () => {
      const { owner } = copyOf(from);
      expect(marker(owner)).toBe(from);
      expect(readWoodstockStoreProtocol(owner)).toBe("upgradable");
    });

    it("upgrades in place and keeps its Agent, Sessions, history and work", () => {
      const { owner } = copyOf(from);
      const before = readStore(owner);
      const timersBefore = rowsOf(
        owner,
        "SELECT extension_id, key, due_at FROM shiitake_extension_timers",
      );
      expect(before.sessions).toHaveLength(3);
      expect(before.entries.length).toBeGreaterThan(30);
      expect(timersBefore).toHaveLength(1);

      expect(upgradeWoodstockStore(owner)).toBe("upgraded");

      expect(marker(owner)).toBe(6);
      expect(readWoodstockStoreProtocol(owner)).toBe("v0");
      const after = readStore(owner);
      expect(after.identity).toEqual(before.identity);
      expect(after.modelSettings).toEqual(before.modelSettings);
      expect(after.sessions).toEqual(before.sessions);
      expect(after.inbox).toEqual(before.inbox);
      expect(after.records).toEqual(before.records);

      expect(rewrittenFacts(before, after)).toEqual(expectedRewrites(from));
      expectTimerAsQueuedJob(owner, before, after, timersBefore);
    });

    it("ends with exactly the tables the current build creates, and Watchdog accepts them", async () => {
      const upgraded = copyOf(from);
      const fresh = createNativeSqliteOwnerFixture();
      open.push(fresh);
      const watchdog = (owner: Owner) =>
        Watchdog.make({
          owner,
          execute: () => Promise.resolve({ kind: "completed" }),
          isAlive: () => Promise.resolve(false),
          wake: { recompute: () => Promise.resolve() },
        });
      Effect.runSync(prepareWoodstockSchema(fresh.owner));
      await watchdog(fresh.owner);
      // Opening the Agent upgrades the store, then Watchdog prepares its indexes.
      Effect.runSync(prepareWoodstockSchema(upgraded.owner));
      await watchdog(upgraded.owner);

      const definitions = (owner: Owner) =>
        new Map(
          rowsOf(
            owner,
            "SELECT name, sql FROM sqlite_schema WHERE sql IS NOT NULL AND (name LIKE 'watchdog_%' OR name LIKE 'idx_watchdog_%' OR name LIKE 'shiitake_extension_timers%' OR name LIKE 'idx_shiitake_extension_timers%') ORDER BY name",
          ).map((row): [string, string] => [
            text(row.name),
            text(row.sql).replace(/\s+/gu, " "),
          ]),
        );
      expect(definitions(upgraded.owner)).toEqual(definitions(fresh.owner));
      expect(marker(upgraded.owner)).toBe(6);
    });

    it("lists its Sessions through the current reader once opened", async () => {
      const { owner } = copyOf(from);
      Effect.runSync(prepareWoodstockSchema(owner));
      const page = await Effect.runPromise(readSessionSummaries(owner));
      expect(page.sessions).toHaveLength(3);
      expect(page.sessions.every((row) => row.kind !== "unavailable")).toBe(
        true,
      );
    });

    it("is a no-op when run again", () => {
      const { owner } = copyOf(from);
      expect(upgradeWoodstockStore(owner)).toBe("upgraded");
      const once = readStore(owner);
      expect(upgradeWoodstockStore(owner)).toBe("unchanged");
      expect(
        Effect.runSync(Effect.result(prepareWoodstockSchema(owner)))._tag,
      ).toBe("Success");
      expect(readStore(owner)).toEqual(once);
    });
  },
);

describe("an upgrade that cannot complete", () => {
  it("rolls back the marker and rewrites when preparing current tables fails", () => {
    const { owner } = copyOf(1);
    owner.sql.exec(
      "CREATE TABLE idx_session_entries_ancestry (unexpected TEXT)",
    );
    const before = readStore(owner);
    const schema = rowsOf(
      owner,
      "SELECT name, sql FROM sqlite_schema ORDER BY name",
    );
    expect(() => upgradeWoodstockStore(owner)).toThrow();
    expect(marker(owner)).toBe(1);
    expect(readStore(owner)).toEqual(before);
    expect(
      rowsOf(owner, "SELECT name, sql FROM sqlite_schema ORDER BY name"),
    ).toEqual(schema);
  });

  it("leaves the store exactly as found, even after earlier steps ran", () => {
    const { owner } = copyOf(1);
    // Timers can only move to Watchdog jobs, so a store without the jobs table
    // cannot upgrade, after its compaction and tool data steps already ran.
    owner.sql.exec("DROP TABLE watchdog_jobs");
    const before = {
      schema: rowsOf(
        owner,
        "SELECT name, sql FROM sqlite_schema ORDER BY name",
      ),
      timers: rowsOf(owner, "SELECT * FROM shiitake_extension_timers"),
      entries: rowsOf(owner, "SELECT data_json FROM session_entries"),
    };
    expect(() => upgradeWoodstockStore(owner)).toThrow(
      expect.objectContaining({ boundary: "store", cause: "incompatible" }),
    );
    expect(marker(owner)).toBe(1);
    expect({
      schema: rowsOf(
        owner,
        "SELECT name, sql FROM sqlite_schema ORDER BY name",
      ),
      timers: rowsOf(owner, "SELECT * FROM shiitake_extension_timers"),
      entries: rowsOf(owner, "SELECT data_json FROM session_entries"),
    }).toEqual(before);
  });

  it.each([0, 7])(
    "refuses a store marked %s, untouched: no build wrote it",
    (version) => {
      // A real marker-3 store re-marked: no published or development build wrote these.
      const { owner } = copyOf(3);
      owner.sql.exec("UPDATE woodstock_schema SET version = ?", version);
      const before = readStore(owner);
      expect(readWoodstockStoreProtocol(owner)).toBe(
        version === 7 ? "newer" : "unmarked",
      );
      expect(upgradeWoodstockStore(owner)).toBe("unchanged");
      expect(
        Effect.runSync(Effect.result(prepareWoodstockSchema(owner))),
      ).toMatchObject({
        _tag: "Failure",
        failure: { boundary: "store", cause: "incompatible" },
      });
      expect(marker(owner)).toBe(version);
      expect(readStore(owner)).toEqual(before);
    },
  );
});

/** Tables of two Local features that no longer exist, as real stores carried them. */
const retiredLocalTables = [
  {
    name: "private_local_exec_rules",
    definition:
      "CREATE TABLE private_local_exec_rules (prefix TEXT PRIMARY KEY) STRICT",
    insert:
      "INSERT INTO private_local_exec_rules (prefix) VALUES ('git status'), ('pnpm test')",
  },
  {
    name: "local_birddog_outbox",
    definition:
      "CREATE TABLE local_birddog_outbox (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)",
    insert:
      "INSERT INTO local_birddog_outbox (key, value) VALUES ('mail:one', '{}')",
  },
] as const;

/** Every table the upgrade leaves alone, with its rows, so nothing else can change. */
function untouchedTables(owner: Owner) {
  const names = rowsOf(
    owner,
    "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name NOT IN ('watchdog_jobs', 'shiitake_extension_timers', 'session_entries', 'woodstock_schema', 'private_local_exec_rules', 'local_birddog_outbox') ORDER BY name",
  ).map((row) => text(row.name));
  return new Map(
    names.map((name): [string, readonly Row[]] => [
      name,
      rowsOf(owner, "SELECT * FROM " + name),
    ]),
  );
}

describe.each(markers)(
  "a marker %s store that still has the retired Local tables",
  (from) => {
    it("drops both tables and changes nothing else", () => {
      const { owner } = copyOf(from);
      for (const table of retiredLocalTables) {
        owner.sql.exec(table.definition);
        owner.sql.exec(table.insert);
      }
      const before = untouchedTables(owner);
      const entries = rowsOf(
        owner,
        "SELECT COUNT(*) AS count FROM session_entries",
      );
      expect(
        rowsOf(owner, "SELECT COUNT(*) AS count FROM private_local_exec_rules"),
      ).toEqual([{ count: 2 }]);
      expect(
        rowsOf(owner, "SELECT COUNT(*) AS count FROM local_birddog_outbox"),
      ).toEqual([{ count: 1 }]);

      expect(upgradeWoodstockStore(owner)).toBe("upgraded");

      expect(
        rowsOf(
          owner,
          "SELECT name FROM sqlite_schema WHERE name IN ('private_local_exec_rules', 'local_birddog_outbox')",
        ),
      ).toEqual([]);
      // Every table that was there keeps its rows; the upgrade may add the
      // tables a newer build creates, as opening the store would.
      const after = untouchedTables(owner);
      for (const [name, rows] of before) expect(after.get(name)).toEqual(rows);
      expect(
        rowsOf(owner, "SELECT COUNT(*) AS count FROM session_entries"),
      ).toEqual(entries);
    });
  },
);

describe("an extension stopped before the upgrade", () => {
  it("keeps its timer registered but cancels the wake, as stopping it does", () => {
    const { owner } = copyOf(3);
    owner.sql.exec(
      "UPDATE shiitake_extension_status SET status = 'stopped', reason = 'stopped for the test', stopped_at = 1 WHERE extension_id = 'botanical-session-loop'",
    );
    expect(upgradeWoodstockStore(owner)).toBe("upgraded");
    const [timer] = rowsOf(
      owner,
      "SELECT job_id FROM shiitake_extension_timers",
    );
    const [job] = rowsOf(
      owner,
      `SELECT cancellation_id, state FROM watchdog_jobs WHERE job_id = '${text(timer?.job_id)}'`,
    );
    expect(job).toEqual({
      cancellation_id: `${text(timer?.job_id)}:cancel`,
      state: "queued",
    });
  });
});
