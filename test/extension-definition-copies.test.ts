import { execFileSync } from "node:child_process";
import { copyFile, mkdir, mkdtemp, rm } from "node:fs/promises";
import { resolve } from "node:path";
import { URL, pathToFileURL } from "node:url";
import { expect, it } from "vitest";

it("reads the captured definition through a second loaded registry module", async () => {
  const area = resolve("../../.botanical");
  await mkdir(area, { recursive: true });
  const directory = await mkdtemp(resolve(area, "extension-reader-"));
  try {
    const copy = resolve(directory, "reader.mjs");
    await copyFile(
      new URL("../dist/internal/extension-definitions.js", import.meta.url),
      copy,
    );
    const entry = new URL("../dist/extensions.js", import.meta.url).href;
    const source = `
      import { defineExtension, ExtensionId } from ${JSON.stringify(entry)};
      import { readDefinition } from ${JSON.stringify(pathToFileURL(copy).href)};
      const extension = defineExtension({
        id: ExtensionId.make("reader-copy"), version: "1.0.0",
      }).define({});
      const definition = readDefinition(extension);
      process.stdout.write(String(
        definition?.manifest.id === "reader-copy" &&
        Object.keys(extension).join(",") === "manifest,contract" &&
        Object.isFrozen(extension)
      ));
    `;
    expect(
      execFileSync(process.execPath, ["--input-type=module", "-e", source], {
        encoding: "utf8",
        timeout: 10_000,
      }),
    ).toBe("true");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
