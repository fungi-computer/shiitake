import { expect, it } from "vitest";
import * as Effect from "effect/Effect";

import { prepareWoodstockSchema } from "../src/internal/woodstock/index.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

it("uses only the retained sessions and session_entries conversation tables", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    const tables = fixture.owner.sql
      .exec(
        `SELECT name
           FROM sqlite_master
          WHERE type = 'table'
            AND (name = 'sessions'
              OR name = 'session_entries'
              OR name LIKE 'woodstock_%')
          ORDER BY name`,
      )
      .toArray();

    expect(tables).toEqual([
      { name: "session_entries" },
      { name: "sessions" },
      { name: "woodstock_schema" },
    ]);
  } finally {
    fixture.close();
  }
});

function tableNames(
  fixture: ReturnType<typeof createNativeSqliteOwnerFixture>,
) {
  return fixture.owner.sql
    .exec("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
    .toArray();
}

it("refuses a store that predates schema versioning, untouched", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    // A pre-versioning store already holds Sessions but no marker row.
    fixture.owner.sql.exec(`
      CREATE TABLE sessions (
        id TEXT PRIMARY KEY,
        cwd TEXT NOT NULL,
        parent_session TEXT,
        leaf_id TEXT,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        metadata_json TEXT
      )
    `);
    fixture.owner.sql.exec(
      "INSERT INTO sessions (id, cwd, created_at, updated_at) VALUES ('legacy', '/', 1, 1)",
    );

    expect(
      Effect.runSync(Effect.result(prepareWoodstockSchema(fixture.owner))),
    ).toMatchObject({
      _tag: "Failure",
      failure: { boundary: "store", cause: "incompatible" },
    });

    expect(tableNames(fixture)).toEqual([{ name: "sessions" }]);
    expect(fixture.owner.sql.exec("SELECT id FROM sessions").toArray()).toEqual(
      [{ id: "legacy" }],
    );
  } finally {
    fixture.close();
  }
});

it("refuses a store stamped with an unknown schema version, untouched", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    fixture.owner.sql.exec(
      "CREATE TABLE woodstock_schema (version INTEGER NOT NULL)",
    );
    fixture.owner.sql.exec(
      "INSERT INTO woodstock_schema (version) VALUES (999)",
    );

    expect(
      Effect.runSync(Effect.result(prepareWoodstockSchema(fixture.owner))),
    ).toMatchObject({
      _tag: "Failure",
      failure: { boundary: "store", cause: "incompatible" },
    });

    expect(tableNames(fixture)).toEqual([{ name: "woodstock_schema" }]);
    expect(
      fixture.owner.sql.exec("SELECT version FROM woodstock_schema").toArray(),
    ).toEqual([{ version: 999 }]);
  } finally {
    fixture.close();
  }
});
