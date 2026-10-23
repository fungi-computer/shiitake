import { readFileSync } from "node:fs";
import { join } from "node:path";
import { expect, it } from "vitest";

it("keeps archive, R2 and file-system types out of the public handoff declarations", () => {
  for (const file of ["../src/extensions.ts", "../docs/api/extensions.md"]) {
    const text = readFileSync(join(import.meta.dirname, file), "utf8");
    expect(text).not.toMatch(/archiveYeet|R2Bucket|\bR2\b|node:fs/u);
  }
});
