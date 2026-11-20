import { copyFileSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  prepareWoodstockSchema,
  readSessionEntriesForWork,
} from "../src/internal/woodstock/index.js";
import { repairDanglingToolCalls } from "../src/internal/woodstock/session-store.js";
import {
  createNativeSqliteOwnerFixture,
  type NativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

/**
 * A person's own store, written by the last shipped Woodstock marker, whose
 * newest turn stopped on a tool call that never got a result: the run died
 * with the tool still executing. It must load, answer that call once, and
 * stay answered.
 */
const fixtureDirectory = join(
  dirname(fileURLToPath(import.meta.url)),
  "fixtures",
  "historical-stores",
);
const scratch: string[] = [];
const open: NativeSqliteOwnerFixture[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const fixture of open.splice(0)) fixture.close();
  for (const directory of scratch.splice(0))
    rmSync(directory, { recursive: true, force: true });
});

const text = Schema.decodeUnknownSync(Schema.String);
const decodeDraft = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({
      value: Schema.Struct({
        role: Schema.String,
        content: Schema.Array(Schema.Unknown),
        stopReason: Schema.String,
      }),
    }),
  ),
);

const callId = "tool:old-store:dangling";

/** The marker-6 store with its last assistant turn left on an unanswered call. */
async function danglingStore() {
  const directory = mkdtempSync(join(tmpdir(), "woodstock-dangling-"));
  scratch.push(directory);
  const path = join(directory, "agent.sqlite");
  copyFileSync(join(fixtureDirectory, "marker-6.agent.sqlite"), path);
  const fixture = createNativeSqliteOwnerFixture(path);
  open.push(fixture);
  await Effect.runPromise(prepareWoodstockSchema(fixture.owner));
  const { owner } = fixture;
  const assistant = owner.sql
    .exec(
      "SELECT id, work_id, sequence, data_json FROM session_entries WHERE session_id = 'main' AND type = 'message' ORDER BY sequence DESC LIMIT 1",
    )
    .one();
  const id = text(assistant.id);
  const workId = text(assistant.work_id);
  const stored = decodeDraft(text(assistant.data_json));
  expect(stored.value.role).toBe("assistant");
  const original = Schema.decodeUnknownSync(
    Schema.fromJsonString(
      Schema.Struct({ value: Schema.Record(Schema.String, Schema.Unknown) }),
    ),
  )(text(assistant.data_json));
  const whole = Schema.decodeUnknownSync(
    Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
  )(text(assistant.data_json));
  const calling = JSON.stringify({
    ...whole,
    value: {
      ...original.value,
      content: [
        {
          type: "toolCall",
          id: callId,
          name: "execute",
          arguments: { code: "async () => 1" },
        },
      ],
      stopReason: "toolUse",
    },
  });
  // The run died in the tool: nothing after the call was ever written. The
  // store refuses to drop an entry that still has children, so newest first.
  const later = owner.sql
    .exec(
      "SELECT id FROM session_entries WHERE session_id = 'main' AND sequence > ? ORDER BY sequence DESC",
      Schema.decodeUnknownSync(Schema.Number)(assistant.sequence),
    )
    .toArray();
  for (const row of later)
    owner.sql.exec(
      "DELETE FROM session_entries WHERE session_id = 'main' AND id = ?",
      text(row.id),
    );
  owner.sql.exec(
    "UPDATE session_entries SET data_json = ? WHERE session_id = 'main' AND id = ?",
    calling,
    id,
  );
  owner.sql.exec("UPDATE sessions SET leaf_id = ? WHERE id = 'main'", id);
  return { owner, assistantId: id, workId };
}

const results = (owner: NativeSqliteOwnerFixture["owner"], workId: string) =>
  readSessionEntriesForWork(owner, "main", workId).flatMap(({ entry }) =>
    entry.kind === "message" && entry.value.role === "toolResult"
      ? [entry.value]
      : [],
  );

describe("a dangling tool call in an old stored Session", () => {
  it("is answered once with the interrupted result, then left answered", async () => {
    const { owner, workId } = await danglingStore();
    expect(results(owner, workId)).toEqual([]);
    expect(repairDanglingToolCalls(owner, "main", workId)).toBe(1);
    expect(results(owner, workId)).toMatchObject([
      {
        toolCallId: callId,
        toolName: "execute",
        isError: true,
        content: [
          {
            type: "text",
            text: `[interrupted] Tool "execute" (id: ${callId}) was still executing when the run was terminated.`,
          },
        ],
      },
    ]);
    // A second terminal or resume adds nothing.
    expect(repairDanglingToolCalls(owner, "main", workId)).toBe(0);
    expect(results(owner, workId)).toHaveLength(1);
    // The answer sits on the call's own branch: the Session's leaf follows it.
    const leaf = owner.sql
      .exec(
        "SELECT entry.type AS type, entry.parent_id AS parent FROM sessions JOIN session_entries AS entry ON entry.session_id = sessions.id AND entry.id = sessions.leaf_id WHERE sessions.id = 'main'",
      )
      .one();
    expect(text(leaf.type)).toBe("message");
  });

  it("logs once and answers nothing when the look-back stops before the call", async () => {
    const { owner, assistantId, workId } = await danglingStore();
    // More settled rows than the look-back visits, between the call and the leaf.
    let parent = assistantId;
    let sequence = Number(
      owner.sql
        .exec(
          "SELECT MAX(sequence) AS n FROM session_entries WHERE session_id = 'main'",
        )
        .one().n,
    );
    for (let index = 0; index < 250; index += 1) {
      sequence += 1;
      const id = `filler-${String(index)}`;
      owner.sql.exec(
        "INSERT INTO session_entries (id, session_id, parent_id, sequence, type, state, timestamp, work_id, data_json) VALUES (?, 'main', ?, ?, 'live_retry', 'complete', 0, ?, ?)",
        id,
        parent,
        sequence,
        workId,
        JSON.stringify({
          kind: "live_retry",
          workId,
          event: {
            type: "auto_retry_attempt",
            run: { kind: "run", value: "mule_run_filler" },
            attempt: 1,
          },
        }),
      );
      parent = id;
    }
    owner.sql.exec("UPDATE sessions SET leaf_id = ? WHERE id = 'main'", parent);
    const lines: string[] = [];
    for (const channel of ["log", "warn", "error"] as const)
      vi.spyOn(console, channel).mockImplementation((...line: unknown[]) => {
        lines.push(line.map(String).join(" "));
      });
    expect(repairDanglingToolCalls(owner, "main", workId)).toBe(0);
    await vi.waitFor(() => {
      expect(
        lines.filter((line) => line.includes("shiitake_dangling_tail_unfound")),
      ).toHaveLength(1);
    });
    expect(results(owner, workId)).toEqual([]);
  });
});
