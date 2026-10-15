import { readdirSync, readFileSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, it } from "vitest";

const sourceRoot = join(dirname(fileURLToPath(import.meta.url)), "../src");
const woodstockPrefix = "internal/woodstock/";

function sourceFiles(directory: string): readonly string[] {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) return sourceFiles(path);
    return entry.name.endsWith(".ts") ? [path] : [];
  });
}

function isOutsideWoodstock(relativePath: string): boolean {
  return !relativePath.startsWith(woodstockPrefix);
}

const deepWoodstockImport =
  /from\s+["'][^"']*\/woodstock\/(?!index\.js)([^"']+)["']/u;

it("routes every import of Woodstock through its index, never a deep file", () => {
  const offenders = sourceFiles(sourceRoot)
    .map((path) => relative(sourceRoot, path))
    .filter(isOutsideWoodstock)
    .filter((relativePath) => {
      const text = readFileSync(join(sourceRoot, relativePath), "utf8");
      return deepWoodstockImport.test(text);
    });
  expect(offenders).toEqual([]);
});

it("keeps `.sql.exec` a capability Woodstock alone calls", () => {
  const offenders = sourceFiles(sourceRoot)
    .map((path) => relative(sourceRoot, path))
    .filter(isOutsideWoodstock)
    .filter((relativePath) => {
      const text = readFileSync(join(sourceRoot, relativePath), "utf8");
      return text.includes(".sql.exec(");
    });
  expect(offenders).toEqual([]);
});
