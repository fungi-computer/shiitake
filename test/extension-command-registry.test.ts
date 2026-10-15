import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { expect, expectTypeOf, it, vi } from "vitest";
import { ExtensionId, ExtensionRead, ExtensionTx } from "../src/extensions.js";
import { registerExtensionCommands } from "../src/internal/extension-command-registry.js";

it("captures each command's decoder, typed handler and encoder before dispatch", () => {
  const invoked = vi.fn();
  const identity = { extensionId: ExtensionId.make("registry-proof") };
  const services = Context.make(ExtensionTx, identity).pipe(
    Context.add(ExtensionRead, identity),
  );
  const commands = registerExtensionCommands(
    {
      increment: {
        scope: "session",
        input: Schema.Struct({ value: Schema.NumberFromString }),
        output: Schema.NumberFromString,
      },
      echo: { scope: "agent", input: Schema.String, output: Schema.String },
    },
    {
      increment: ({ sessionId, input }) => {
        expectTypeOf(sessionId).toEqualTypeOf<string>();
        expectTypeOf(input.value).toEqualTypeOf<number>();
        invoked(sessionId, input.value);
        return input.value + 1;
      },
      echo: ({ sessionId, input }) => {
        expectTypeOf(sessionId).toEqualTypeOf<null>();
        expectTypeOf(input).toEqualTypeOf<string>();
        return input;
      },
    },
  );
  const increment = commands.get("increment");
  const prepared = increment?.({
    sessionId: "session-one",
    input: { value: "12" },
  });
  expect(invoked).not.toHaveBeenCalled();
  if (prepared?._tag !== "Success")
    throw new Error("Expected a prepared command");
  expect(
    Effect.runSync(prepared.success().pipe(Effect.provide(services))),
  ).toBe("13");
  expect(invoked).toHaveBeenCalledExactlyOnceWith("session-one", 12);
  expect(increment?.({ sessionId: null, input: { value: "12" } })._tag).toBe(
    "Failure",
  );
  expect(
    increment?.({
      sessionId: "session-one",
      input: { value: "12", extra: true },
    })._tag,
  ).toBe("Failure");
  expect(invoked).toHaveBeenCalledTimes(1);
  const echo = commands.get("echo")?.({ sessionId: null, input: "hello" });
  if (echo?._tag !== "Success") throw new Error("Expected a prepared echo");
  expect(Effect.runSync(echo.success().pipe(Effect.provide(services)))).toBe(
    "hello",
  );
});
