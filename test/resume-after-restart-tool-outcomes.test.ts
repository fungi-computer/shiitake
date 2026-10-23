import { expect, it } from "vitest";
import {
  killed,
  finished,
  withDatabase,
  expectOutcome,
  toolResult,
} from "./fixtures/restart.fixture.js";
import { readFile } from "node:fs/promises";

it(
  "does not replay a formerly safe tool after its declaration becomes unsafe",
  () =>
    withDatabase(async (path) => {
      await killed(path, "crash-safe", "safe-started", "restart-mule");
      const snapshot = await finished(path, "downgraded", "restart-mule");
      expect(toolResult(snapshot, "read-call")).toMatchObject({
        role: "toolResult",
        isError: true,
      });
      expect(await readFile(path + ".calls", "utf8")).toBe("read\n");
    }),
  30_000,
);

it(
  "reuses a known unsafe outcome when killed before its transcript message",
  () =>
    withDatabase(async (path) => {
      await killed(path, "crash-outcome", "outcome-saved", "restart-mule");
      const snapshot = await finished(path, "finish", "restart-mule");
      expect(
        snapshot.messages.find(
          ({ value }) =>
            value.role === "toolResult" && value.toolCallId === "read-call",
        )?.data,
      ).toEqual({ display: "saved outcome display payload" });
      expect(toolResult(snapshot, "read-call")).toMatchObject({
        role: "toolResult",
        isError: false,
      });
      expect(await readFile(path + ".calls", "utf8")).toBe("read\n");
    }),
  30_000,
);

it(
  "preserves a saved tool termination decision after restart",
  () =>
    withDatabase(async (path) => {
      await killed(path, "crash-terminate", "outcome-saved", "restart-mule");
      expectOutcome(
        await finished(path, "finish-terminate", "restart-mule"),
        "completed",
      );
      expect(await readFile(path + ".calls", "utf8")).toBe("read\n");
    }),
  30_000,
);
