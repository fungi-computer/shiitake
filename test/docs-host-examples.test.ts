import { clientContributionSchemaV1 } from "@fungi.computer/shiitake/client-entry";
import * as Schema from "effect/Schema";
import { expect, it } from "vitest";

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { firstSession } from "../examples/first-session.js";
import { nodeAgent } from "../examples/node-agent.js";
import bookmarkClient from "../examples/bookmarks/client.js";
import { bookmarkRoundTrip } from "../examples/bookmarks/run.js";

it("the first-Session docs example settles the accepted prompt", async () => {
  const { receipt, snapshot } = await firstSession();
  expect(receipt.status).toBe("accepted");
  expect(snapshot.activity.queue.state).toBe("idle");
  expect(snapshot.terminals).toMatchObject([{ outcome: "completed" }]);
  expect(snapshot.messages.at(-1)?.value).toMatchObject({
    role: "assistant",
    content: [{ type: "text", text: "Ready." }],
  });
});

it("pages 6–7: Bookmarks rows decode against the public row schema", async () => {
  const panel = await bookmarkRoundTrip();
  const rows = Schema.decodeUnknownSync(clientContributionSchemaV1)({
    chips: [],
    panel,
    timeline: { cells: [], omitted: 0 },
    waiting: null,
  });
  expect(rows.panel).toEqual([
    {
      id: "bookmark-0",
      title: "Fungi",
      detail: "https://fungi.computer/",
    },
    {
      id: "bookmark-1",
      title: "Shiitake docs",
      detail: "https://shiitake.shiit.app/",
    },
  ]);
});

it("pages 6–7: the Bookmarks client keeps long URLs and full pages in bounds", () => {
  const long = `https://example.com/${"a".repeat(1_900)}`;
  const rows = Schema.decodeUnknownSync(clientContributionSchemaV1)(
    bookmarkClient.contribute?.({
      status: "running",
      revision: "sha256:fixture",
      value: Array.from({ length: 32 }, (_, index) => ({
        url: index === 0 ? long : `https://example.com/${String(index)}`,
        title: `Bookmark ${String(index)}`,
        sessionId: "reading",
      })),
    }),
  );
  expect(rows.panel).toHaveLength(16);
  expect(rows.panel[0]?.detail).toHaveLength(1_024);
  expect(rows.panel[0]?.detail).toMatch(/…$/u);
});

it("the Run-it-on-Node docs example serves, runs and attributes a prompt", async () => {
  const directory = mkdtempSync(join(tmpdir(), "shiitake-node-docs-"));
  try {
    const snapshot = await nodeAgent(join(directory, "agent.sqlite"));
    expect(snapshot.terminals).toMatchObject([{ outcome: "completed" }]);
    expect(snapshot.messages[0]?.meta).toEqual({
      author: { id: "ada", name: "Ada" },
    });
    expect(snapshot.messages.at(-1)?.value).toMatchObject({
      role: "assistant",
      content: [{ type: "text", text: "Ready on Node." }],
    });
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
