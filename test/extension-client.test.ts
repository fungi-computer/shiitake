import * as Schema from "effect/Schema";
import { expect, expectTypeOf, it } from "vitest";

import {
  bindAgentExtension,
  bindSessionExtension,
} from "../src/extension-client.js";
import { ExtensionId, type Json } from "../src/extensions.js";

const contract = {
  id: ExtensionId.make("client-boundary-proof"),
  commands: {
    measure: {
      scope: "session" as const,
      input: Schema.Struct({
        count: Schema.NumberFromString.check(
          Schema.isInt(),
          Schema.isGreaterThan(0),
        ),
      }),
      output: Schema.NumberFromString,
    },
    total: {
      scope: "agent" as const,
      input: Schema.Struct({}),
      output: Schema.NumberFromString,
    },
  },
  projection: null,
};

it("encodes a typed Session command once and decodes its foreign result", async () => {
  const sent: Json[] = [];
  const bound = bindSessionExtension(
    {
      extension: () => ({
        call: (_command, input) => {
          sent.push(input);
          return Promise.resolve("42");
        },
      }),
    },
    contract,
  );

  const result = bound.call("measure", { count: 7 });
  expectTypeOf(result).toEqualTypeOf<Promise<number>>();
  expectTypeOf(bound.call).parameter(0).toEqualTypeOf<"measure">();
  await expect(result).resolves.toBe(42);
  expect(sent).toEqual([{ count: "7" }]);

  await expect(bound.call("measure", { count: 0 })).rejects.toMatchObject({
    boundary: "contract",
    cause: "invalid",
    message: "Extension command input is invalid",
  });
  expect(sent).toHaveLength(1);
});

it("rejects malformed foreign output without exposing its contents", async () => {
  const bound = bindSessionExtension(
    {
      extension: () => ({
        call: () => Promise.resolve({ privateHostDetail: "operator-only" }),
      }),
    },
    contract,
  );
  await expect(bound.call("measure", { count: 1 })).rejects.toMatchObject({
    boundary: "contract",
    cause: "invalid",
    message: "Extension output does not match its contract",
  });
});

it("binds the Agent scope through the same typed encoding boundary", async () => {
  const sent: Readonly<{ extension: string; command: string; input: Json }>[] =
    [];
  const bound = bindAgentExtension(
    {
      extensions: {
        call: (extension, command, input) => {
          sent.push({ extension, command, input });
          return Promise.resolve("9");
        },
        list: () => Promise.resolve([]),
        resume: () => Promise.reject(new Error("Not used by this caller")),
      },
    },
    contract,
  );
  expectTypeOf(bound.call).parameter(0).toEqualTypeOf<"total">();
  await expect(bound.call("total", {})).resolves.toBe(9);
  expect(sent).toEqual([
    { extension: contract.id, command: "total", input: {} },
  ]);
});
