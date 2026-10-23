// Runtime for every law here: Node 24, `node:sqlite`, the manual runtime host.
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { expect, it } from "vitest";

import { Shiitake } from "../src/index.js";
import type { SqliteOwner } from "../src/internal/contracts.js";
import { projectCommandSettlement } from "../src/internal/woodstock/client-projection.js";
import {
  prepareWoodstockSchema,
  readSessionEntriesForWork,
  readSettledRunFacts,
  readSettlementContent,
} from "../src/internal/woodstock/index.js";
import { damageOutsideWoodstock } from "./fixtures/lineage-damage.fixture.js";
import {
  compatibilityRunDependencies,
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

const decodeAdmission = Schema.decodeUnknownSync(
  Schema.Struct({ id: Schema.String, work_id: Schema.String }),
);

function admission(owner: SqliteOwner, sessionId: string) {
  return decodeAdmission(
    owner.sql
      .exec(
        "SELECT id, work_id FROM session_command_admissions WHERE session_id = ? ORDER BY sequence LIMIT 1",
        sessionId,
      )
      .one(),
  );
}

/** Every reader that derives content from one work's entries. */
function reads(owner: SqliteOwner, sessionId: string) {
  const work = admission(owner, sessionId);
  return {
    entries: () => readSessionEntriesForWork(owner, sessionId, work.work_id),
    settlement: () => projectCommandSettlement(owner, sessionId, work.id),
    content: () =>
      Effect.runSync(
        readSettlementContent(owner, { sessionId, commandFactId: work.id }),
      ),
    facts: () => readSettledRunFacts(owner, sessionId, work.id, work.work_id),
  };
}

it.each(["message", "whole work"])(
  "refuses every content read after deleting an earlier %s",
  async (deleted) => {
    const fixture = createNativeSqliteOwnerFixture();
    try {
      const runtime = createCompatibilityRuntimeHost();
      const agent = await Shiitake.make({
        ...compatibilityRunDependencies(),
        store: fixture.owner,
        runtime: runtime.runtime,
      });
      try {
        for (const sessionId of ["damaged", "healthy"]) {
          await agent.session(sessionId).command({
            requestId: `${sessionId}-1`,
            delivery: "followUp",
            type: "prompt",
            text: "one",
          });
          await runtime.runNext();
          if (deleted === "whole work") {
            await agent.session(sessionId).command({
              requestId: `${sessionId}-2`,
              delivery: "followUp",
              type: "prompt",
              text: "two",
            });
            await runtime.runNext();
          }
        }
      } finally {
        await agent.close();
      }
      // A later work still descends from the earlier work when every entry of
      // that earlier work is deleted. Its admission survives, but no entry row
      // remains on which a row decoder could check the Session's damage.
      if (deleted === "whole work") {
        const earlier = admission(fixture.owner, "damaged");
        damageOutsideWoodstock(
          fixture.owner,
          "DELETE FROM session_entries WHERE session_id = ? AND work_id = ?",
          "damaged",
          earlier.work_id,
        );
        expect(
          fixture.owner.sql
            .exec(
              "SELECT COUNT(*) AS entries FROM session_entries WHERE session_id = ? AND work_id = ?",
              "damaged",
              earlier.work_id,
            )
            .one(),
        ).toEqual({ entries: 0 });
      } else {
        damageOutsideWoodstock(
          fixture.owner,
          "DELETE FROM session_entries WHERE session_id = 'damaged' AND type = 'message'",
        );
      }
      Effect.runSync(prepareWoodstockSchema(fixture.owner));

      for (const [name, read] of Object.entries(
        reads(fixture.owner, "damaged"),
      ))
        expect
          .soft(read, name)
          .toThrow("Woodstock store: Woodstock Session damaged holds");
      const healthy = reads(fixture.owner, "healthy");
      expect(healthy.content()).toBe("complete");
      expect(healthy.settlement()?.content).toBe("complete");
      expect(healthy.facts().terminal).toBe("completed");
    } finally {
      fixture.close();
    }
  },
);
