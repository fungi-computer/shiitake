import { readFileSync } from "node:fs";
import { URL } from "node:url";
import ts from "typescript";
import { expect, it } from "vitest";

it("keeps all synchronous Effect execution inside runTransaction and forbids runPromise", () => {
  const source = ts.createSourceFile(
    "extension-runtime.ts",
    readFileSync(
      new URL("../src/internal/extension-runtime.ts", import.meta.url),
      "utf8",
    ),
    ts.ScriptTarget.Latest,
    true,
  );
  const violations: string[] = [];
  let transactionRuns = 0;
  const visit = (node: ts.Node, inTransaction: boolean): void => {
    const inside =
      inTransaction ||
      (ts.isFunctionDeclaration(node) && node.name?.text === "runTransaction");
    if (
      ts.isCallExpression(node) &&
      ts.isPropertyAccessExpression(node.expression)
    ) {
      const method = node.expression.name.text;
      if (method === "runPromise") violations.push(method);
      if (method === "runSyncExit") {
        if (!inside) violations.push(method);
        else transactionRuns += 1;
      }
    }
    ts.forEachChild(node, (child) => {
      visit(child, inside);
    });
  };
  visit(source, false);
  expect(transactionRuns).toBe(1);
  expect(violations).toEqual([]);
});
