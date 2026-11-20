import {
  createFingerprintRedactor,
  createFingerprintSet,
} from "@fungi.computer/veil/redaction";
import * as Effect from "effect/Effect";
import { afterEach, expect, it } from "vitest";

import {
  bindSessionRedaction,
  liveTextRedaction,
  loadSessionRedaction,
  toolResultRedaction,
} from "../src/internal/redaction.js";
import {
  createNativeSqliteOwnerFixture,
  type NativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

// A stored sign-in token: far longer than one live output snapshot.
const token = ["vault", "signin", "canary"].join("-").padEnd(1500, "q");
const fixtures: NativeSqliteOwnerFixture[] = [];

afterEach(() => {
  for (const fixture of fixtures.splice(0)) fixture.close();
});

/** An owner whose host stores `token`, as Local does, with its matcher loaded. */
async function ownerStoringToken() {
  const fixture = createNativeSqliteOwnerFixture();
  fixtures.push(fixture);
  const set = await createFingerprintSet(
    crypto.getRandomValues(new Uint8Array(32)),
    [token],
    { prefixes: true },
  );
  bindSessionRedaction(
    fixture.owner,
    Effect.promise(() => createFingerprintRedactor(set)),
  );
  await Effect.runPromise(loadSessionRedaction(fixture.owner));
  return fixture.owner;
}

const tail = (text: string) => ({
  isError: false,
  content: [{ type: "text" as const, text }],
});

it("shows a live tool output snapshot shorter than a stored value", async () => {
  const redact = toolResultRedaction(await ownerStoringToken(), {
    encode: JSON.stringify,
    decode: JSON.parse,
  });
  if (redact === undefined) throw new Error("The owner has no Vault redaction");
  for (const snapshot of [
    "build step 1/12\n",
    "build step 1/12\nwarn 1\nbuild step 2/12\n",
  ])
    expect(await Effect.runPromise(redact(tail(snapshot), "update"))).toEqual(
      tail(snapshot),
    );
  // A snapshot cut inside the stored value never shows any of it.
  const cut = await Effect.runPromise(
    redact(tail(`token: ${token.slice(0, 3)}`), "update"),
  );
  expect(cut).toEqual(tail("token: "));
});

it("shows live model reply text as it streams despite a long stored value", async () => {
  const write = liveTextRedaction(await ownerStoringToken());
  expect(await Effect.runPromise(write("The build "))).toBe("The build ");
  expect(await Effect.runPromise(write("finished."))).toBe("finished.");
  expect(await Effect.runPromise(write(` ${token.slice(0, 5)}`))).toBe(" ");
});
