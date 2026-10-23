// Runtime for every law here: Node 24, `node:sqlite`, the manual runtime host.
import * as Schema from "effect/Schema";
import { expect, it } from "vitest";

import { Shiitake } from "../src/index.js";
import type { SqliteOwner } from "../src/internal/contracts.js";
import { coreConsumer } from "../src/internal/extension-runtime.js";
import { readCursor } from "../src/internal/woodstock/index.js";
import { damageOutsideWoodstock } from "./fixtures/lineage-damage.fixture.js";
import {
  compatibilityRunDependencies,
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

const decodeWorkRow = Schema.decodeUnknownSync(
  Schema.Struct({ work_id: Schema.String }),
);

function workId(owner: SqliteOwner, sessionId: string, requestId: string) {
  return decodeWorkRow(
    owner.sql
      .exec(
        "SELECT work_id FROM session_command_admissions WHERE session_id = ? AND request_id = ?",
        sessionId,
        requestId,
      )
      .one(),
  ).work_id;
}

function job(owner: SqliteOwner, jobId: string) {
  return owner.sql
    .exec(
      "SELECT sequence, state, outcome, failure_reason FROM watchdog_jobs WHERE job_id = ?",
      jobId,
    )
    .one();
}

function prompt(requestId: string, text: string) {
  return {
    requestId,
    delivery: "followUp" as const,
    type: "prompt" as const,
    text,
  };
}

it("settles work queued for a Session the lineage audit refused, once", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    const first = createCompatibilityRuntimeHost();
    const agent = await Shiitake.make({
      ...compatibilityRunDependencies(),
      store: fixture.owner,
      runtime: first.runtime,
    });
    try {
      for (const sessionId of ["damaged", "healthy"]) {
        await agent.session(sessionId).command(prompt(`${sessionId}-1`, "one"));
        await first.runNext();
      }
      await agent.session("damaged").command(prompt("damaged-2", "two"));
    } finally {
      await agent.close();
    }
    const queued = workId(fixture.owner, "damaged", "damaged-2");
    damageOutsideWoodstock(
      fixture.owner,
      `DELETE FROM session_entries WHERE session_id = 'damaged'
         AND sequence = (SELECT MIN(sequence) + 1 FROM session_entries
                          WHERE session_id = 'damaged')`,
    );

    const second = createCompatibilityRuntimeHost();
    const reopened = await Shiitake.make({
      ...compatibilityRunDependencies(),
      store: fixture.owner,
      runtime: second.runtime,
    });
    try {
      for (
        let pass = 0;
        pass < 5 && second.snapshot().state === "pending";
        pass += 1
      )
        await second.runNext();

      // One terminal failure that names the Session and its reset, then idle:
      // nothing is left to retry, and the settled cursor moved past it.
      const settled = job(fixture.owner, queued);
      expect(settled).toEqual({
        sequence: settled.sequence,
        state: "settled",
        outcome: "failed",
        failure_reason:
          'store:unavailable: Woodstock store: Woodstock Session damaged holds a broken lineage; delete it with deleteSession("damaged"), or reset the owning store',
      });
      expect(second.snapshot().state).toBe("idle");
      expect(readCursor(fixture.owner, coreConsumer)).toBe(settled.sequence);

      // The healthy Session still runs and settles behind it.
      await reopened.session("healthy").command(prompt("healthy-2", "two"));
      await second.runNext();
      expect(
        job(fixture.owner, workId(fixture.owner, "healthy", "healthy-2")),
      ).toMatchObject({
        state: "settled",
        outcome: "completed",
      });
      expect((await reopened.session("healthy").read()).messages).toHaveLength(
        4,
      );
    } finally {
      await reopened.close();
    }
  } finally {
    fixture.close();
  }
});
