import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it } from "vitest";

import type { SqliteOwner } from "../src/internal/contracts.js";
import { createWoodstockStorageConformance } from "../src/internal/woodstock/storage-conformance.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

const cases = createWoodstockStorageConformance((use) => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    use(fixture.owner);
  } finally {
    fixture.close();
  }
  return Promise.resolve();
});

for (const conformance of cases) it(conformance.name, conformance.run);

// Retain Pi 8efd314f's sqlite-storage.test.ts ReopeningStorage binding:
// close/reopen after a commit OR rollback, then run the same semantic laws.
describe("across file-backed SQLite transaction reopen", () => {
  const reopeningCases = createWoodstockStorageConformance((use) => {
    const directory = mkdtempSync(join(tmpdir(), "woodstock-reopen-"));
    const path = join(directory, "session.sqlite");
    let fixture = createNativeSqliteOwnerFixture(path);
    const owner: SqliteOwner = {
      sql: {
        exec: (...args) => fixture.owner.sql.exec(...args),
      },
      transactionSync: (operation) => {
        try {
          return fixture.owner.transactionSync(operation);
        } finally {
          fixture.close();
          fixture = createNativeSqliteOwnerFixture(path);
        }
      },
    };
    try {
      use(owner);
    } finally {
      fixture.close();
      rmSync(directory, { recursive: true, force: true });
    }
    return Promise.resolve();
  });

  for (const conformance of reopeningCases)
    it(conformance.name, conformance.run);
});
