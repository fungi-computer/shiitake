import * as Effect from "effect/Effect";
import { expect, it } from "vitest";

import { ShiitakeError, toShiitakeErrorWire } from "../src/errors.js";
import type { SqliteOwner } from "../src/effect.js";
import { prepareWoodstockSchema } from "../src/internal/woodstock/index.js";

it("keeps the original SQL failure on the store error without exposing it on the wire", async () => {
  const sqlFailure = new Error(
    "SQLITE_IOERR: disk I/O error near secret-table",
  );
  const owner: SqliteOwner = {
    sql: {
      exec: () => {
        throw sqlFailure;
      },
    },
    transactionSync: (operation) => operation(),
  };

  const error = await Effect.runPromise(
    Effect.flip(prepareWoodstockSchema(owner)),
  );

  expect(error).toBeInstanceOf(ShiitakeError);
  expect(error).toMatchObject({
    boundary: "store",
    cause: "rejected",
    message: "Native Session store operation failed",
  });
  expect(error.underlying).toBe(sqlFailure);

  const wire = JSON.stringify(toShiitakeErrorWire(error));
  expect(wire).not.toContain("underlying");
  expect(wire).not.toContain("disk I/O");
  expect(wire).not.toContain("secret-table");
});
