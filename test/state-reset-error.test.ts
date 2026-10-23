import * as Schema from "effect/Schema";
import { expect, it } from "vitest";
import {
  ShiitakeError,
  ShiitakeErrorWireSchema,
  toShiitakeErrorWire,
} from "../src/errors.js";

it("round-trips the typed state-reset diagnostic through the canonical JSON wire", () => {
  const error = new ShiitakeError(
    "extensions",
    "state_reset_required",
    "State reset required; an operator must authorize it.",
  );
  const decode = Schema.decodeSync(
    Schema.fromJsonString(ShiitakeErrorWireSchema),
  );
  expect(decode(JSON.stringify(toShiitakeErrorWire(error)))).toEqual({
    _tag: "ShiitakeError",
    boundary: "extensions",
    cause: "state_reset_required",
    message: "State reset required; an operator must authorize it.",
  });
});
