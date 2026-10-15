import { expect, it } from "vitest";
import { z } from "zod";

import { promptSectionRun } from "../examples/ladder/01-prompt-section/run.js";
import { modelToolRun } from "../examples/ladder/02-model-tool/run.js";
import { storedStateRun } from "../examples/ladder/03-stored-state/run.js";
import { projectionRun } from "../examples/ladder/04-projection/run.js";
import { commandsRun } from "../examples/ladder/05-commands/run.js";

const textOf = (value: unknown): string =>
  z
    .object({ content: z.array(z.object({ text: z.string() })) })
    .parse(value)
    .content.map((part) => part.text)
    .join("");

it("page 1: the prompt section reaches the model", async () => {
  const snapshot = await promptSectionRun();
  expect(textOf(snapshot.messages.at(-1)?.value)).toContain(
    "[extension:house-style.voice]\nAnswer in one short sentence.",
  );
});

it("page 2: the model's tool call returns the link's host", async () => {
  const snapshot = await modelToolRun();
  expect(textOf(snapshot.messages.at(-2)?.value)).toBe(
    '{"ok":true,"host":"shiitake.shiit.app"}',
  );
  expect(textOf(snapshot.messages.at(-1)?.value)).toBe("That link is fine.");
});

it("page 3: a saved link appears in the next system prompt", async () => {
  const snapshot = await storedStateRun();
  expect(textOf(snapshot.messages.at(-1)?.value)).toContain(
    "[extension:bookmarks.saved]\nSaved links: Shiitake docs",
  );
});

it("page 4: the Session projection shows the saved link", async () => {
  const snapshot = await projectionRun();
  expect(snapshot.extensions.bookmarks).toMatchObject({
    status: "running",
    value: [
      {
        url: "https://shiitake.shiit.app/",
        title: "Shiitake docs",
        sessionId: "reading",
      },
    ],
  });
});

it("page 5: the model's tool and a person's command share one handler", async () => {
  const { added, snapshot } = await commandsRun();
  expect(added).toEqual({
    url: "https://fungi.computer/",
    title: "Fungi",
    sessionId: "reading",
  });
  expect(snapshot.extensions.bookmarks).toMatchObject({
    value: [
      { title: "Fungi", sessionId: "reading" },
      { title: "Shiitake docs", sessionId: "reading" },
    ],
  });
});
