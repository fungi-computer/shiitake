import assert from "node:assert/strict";
import { test } from "node:test";
import { memoryHost } from "@fungi.computer/shiitake/testing";
import client from "./client.js";
import { bookmarks } from "./extension.js";
import { readOnlySave } from "./grants.js";
import { bookmarkRoundTrip } from "./run.js";

await test("the model and a person both save links the client can draw", async () => {
  const rows = await bookmarkRoundTrip();
  assert.deepEqual(
    rows.map((row) => row.title),
    ["Fungi", "Shiitake docs"],
  );
});

await test("a host without the commands grant refuses saves", async () => {
  await assert.rejects(readOnlySave(), /Extension is unavailable/);
});

await test("each Session keeps its own bookmarks", async () => {
  const host = await memoryHost({ extensions: [bookmarks] });
  try {
    for (const id of ["first", "second"]) {
      const session = host.agent.session(id);
      await session.send("Start.", { requestId: id });
      await host.settle(id);
      await session.extension(bookmarks.manifest.id).call("add", {
        url: "https://fungi.computer/",
        title: id,
      });
    }
    for (const id of ["first", "second"]) {
      const slot = (await host.agent.session(id).read()).extensions.bookmarks;
      const rows = client.contribute?.(slot).panel ?? [];
      assert.deepEqual(
        rows.map((row) => row.title),
        [id],
      );
    }
  } finally {
    await host.close();
  }
});

await test("the client draws nothing from a broken projection", () => {
  const contribution = client.contribute?.({
    status: "running",
    revision: "sha256:fixture",
    value: [{ url: 42 }],
  });
  assert.deepEqual(contribution?.panel, []);
});
