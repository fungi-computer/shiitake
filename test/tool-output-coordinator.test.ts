import { it } from "vitest";
import { proveToolOutput } from "./fixtures/tool-output-coordinator.fixture.js";

it("coalesces real Mule output with durable call identity through coordinator, replay and HTTP", async () => {
  await proveToolOutput(() => undefined);
});
