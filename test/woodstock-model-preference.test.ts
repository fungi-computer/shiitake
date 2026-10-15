import { expect, it } from "vitest";
import {
  prepareSessionStore,
  setSessionModelPreference,
  readSessionModelPreference,
  selectSessionBranch,
  forkSessionAt,
} from "../src/internal/woodstock/session-store.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

it("restores nullable model preferences through the selected branch and fork without work admission", () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    const { owner } = fixture;
    prepareSessionStore(owner);
    const sessionId = "model-preference";
    expect(readSessionModelPreference(owner, sessionId)).toBeNull();
    const preference = {
      providerId: "faux",
      modelId: "first",
      reasoning: "low",
    } as const;
    const selected = setSessionModelPreference(owner, sessionId, preference);
    expect(readSessionModelPreference(owner, sessionId)).toEqual(preference);
    const cleared = setSessionModelPreference(owner, sessionId, null);
    expect(readSessionModelPreference(owner, sessionId)).toBeNull();
    selectSessionBranch(owner, { sessionId, entryId: selected.id });
    expect(readSessionModelPreference(owner, sessionId)).toEqual(preference);
    const fork = forkSessionAt(owner, { sessionId, entryId: selected.id });
    expect(readSessionModelPreference(owner, fork)).toEqual(preference);
    selectSessionBranch(owner, { sessionId, entryId: cleared.id });
    expect(readSessionModelPreference(owner, sessionId)).toBeNull();
    expect(readSessionModelPreference(owner, fork)).toEqual(preference);
    expect(
      owner.sql
        .exec("SELECT COUNT(*) AS count FROM session_command_admissions")
        .one().count,
    ).toBe(0);
    expect(
      owner.sql
        .exec(
          "SELECT COUNT(*) AS count FROM session_entries WHERE work_id IS NOT NULL",
        )
        .one().count,
    ).toBe(0);
  } finally {
    fixture.close();
  }
});
