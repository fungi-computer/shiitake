import { readdirSync, readFileSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, it } from "vitest";

const sourceRoot = join(dirname(fileURLToPath(import.meta.url)), "../src");

function sourceFiles(directory: string): readonly string[] {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) return sourceFiles(path);
    return entry.name.endsWith(".ts") ? [path] : [];
  });
}

const consoleCall = /\bconsole\s*\./u;

it("never logs with console; hosts provide the Logger", () => {
  const offenders = sourceFiles(sourceRoot)
    .map((path) => relative(sourceRoot, path))
    .filter((relativePath) => {
      const text = readFileSync(join(sourceRoot, relativePath), "utf8");
      return consoleCall.test(text);
    });
  expect(offenders).toEqual([]);
});
