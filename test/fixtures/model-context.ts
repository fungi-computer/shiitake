import type { Context, Message } from "@earendil-works/pi-ai";
import { expect } from "vitest";

/** Pi 0.87 places the system instruction first in every provider request. */
export function requestMessages(
  context: Context | undefined,
): readonly Message[] {
  expect(context?.messages[0]).toMatchObject({ role: "system" });
  return context?.messages.slice(1) ?? [];
}
