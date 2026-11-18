import { copyFileSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import * as Schema from "effect/Schema";
import { afterEach, expect, it } from "vitest";

import { upgradeWoodstockStore } from "../src/effect.js";
import {
  createNativeSqliteOwnerFixture,
  type NativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

/**
 * Vault redaction runs where Session output is first written. The store
 * upgrade's raw UPDATEs (retry deadlines, tool details to `data`, and the
 * old compaction rewrite) only re-key a retained row's own JSON: every string
 * they leave was already in that row, apart from the summary's fixed wrapper.
 * So they write no new output that would need the matcher.
 */
const fixtures = join(
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

const decodeLeaves = Schema.decodeUnknownSync(
  Schema.Array(Schema.Struct({ id: Schema.String, value: Schema.String })),
);

/** Every string leaf of every entry, read by SQLite's own JSON parser. */
function entries(fixture: NativeSqliteOwnerFixture) {
  const leaves = new Map<string, Set<string>>();
  for (const { id, value } of decodeLeaves(
    fixture.owner.sql
      .exec(
        "SELECT e.id AS id, t.value AS value FROM session_entries e, json_tree(e.data_json) t WHERE t.type = 'text'",
      )
      .toArray(),
  )) {
    const row = leaves.get(id) ?? new Set<string>();
    row.add(value);
    leaves.set(id, row);
  }
  return leaves;
}

const summaryWrapper =
  /^The conversation history before this point was compacted into the following summary:\n\n<summary>\n([\s\S]*)\n<\/summary>$/u;
/** The summary message's own grammar literals: `role` and the part `type`. */
const grammar = new Set(["user", "text"]);

it.each([1, 2, 3, 4, 5])(
  "upgrading a seeded marker-%i store re-keys retained JSON without new output",
  (marker) => {
    const directory = mkdtempSync(join(tmpdir(), "woodstock-rekey-"));
    scratch.push(directory);
    const path = join(directory, "agent.sqlite");
    copyFileSync(join(fixtures, `marker-${String(marker)}.agent.sqlite`), path);
    const fixture = createNativeSqliteOwnerFixture(path);
    open.push(fixture);
    const before = entries(fixture);
    expect(before.size).toBeGreaterThan(0);

    expect(upgradeWoodstockStore(fixture.owner)).toBe("upgraded");

    for (const [id, after] of entries(fixture)) {
      const old = before.get(id);
      if (old === undefined) continue;
      for (const value of after) {
        const wrapped = summaryWrapper.exec(value)?.[1];
        expect(
          old.has(value) ||
            grammar.has(value) ||
            (wrapped !== undefined && old.has(wrapped)),
          `row ${id} gained ${JSON.stringify(value.slice(0, 80))}`,
        ).toBe(true);
      }
    }
  },
);
