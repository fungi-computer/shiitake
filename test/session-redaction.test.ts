import { mkdtempSync, readdirSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { fauxModel } from "@fungi.computer/mule/testing";
import { directSandbox } from "@fungi.computer/mycelium/testing";
import {
  createFingerprintRedactor,
  createFingerprintSet,
} from "@fungi.computer/veil/redaction";
import { afterEach, it } from "vitest";

import { openNodeShiitake } from "../src/node.js";
import {
  proveRedactionFailsClosed,
  proveSessionRedaction,
  vaultCanary,
  type RedactionLawHost,
} from "./fixtures/session-redaction-laws.js";

const directories: string[] = [];

afterEach(() => {
  for (const directory of directories.splice(0))
    rmSync(directory, { recursive: true, force: true });
});

/** The Node host with Veil's matcher behind a port the law can take down. */
const nodeHost: RedactionLawHost = async (input) => {
  const directory = mkdtempSync(join(tmpdir(), "shiitake-redaction-"));
  directories.push(directory);
  const faux = fauxModel([]);
  faux.setResponses([...input.responses]);
  const fingerprints = createFingerprintSet(
    crypto.getRandomValues(new Uint8Array(32)),
    [vaultCanary],
  );
  const host = await openNodeShiitake({
    path: join(directory, "agent.sqlite"),
    extensions: { roster: [], grants: [] },
    instructions: {
      load: () => Promise.resolve({ revision: "v1", text: "Help." }),
    },
    model: {
      acquire: () =>
        Promise.resolve({
          model: faux.model,
          retryable: () => false,
          stream: faux.stream,
          release: () => Promise.resolve(),
        }),
    },
    compaction: input.compaction,
    tools: { sandbox: directSandbox(() => Promise.resolve(input.toolOutput)) },
    // The owner gives this host only fingerprints of its stored value.
    redaction: {
      load: async () => {
        if (input.portDown()) throw new Error("Vault is unreachable");
        return createFingerprintRedactor(await fingerprints);
      },
    },
  });
  return {
    session: (sessionId) => {
      const session = host.agent.session(sessionId);
      return {
        send: (text, options) => session.send(text, options),
        compact: (requestId) => session.command({ type: "compact", requestId }),
        read: () => session.read(),
        readSettlement: (commandId, options) =>
          session.readSettlement(commandId, options),
        subscribe: (options) => session.subscribe(options),
        watch: () => session.watch(),
      };
    },
    storedBytes: () => {
      const files = readdirSync(directory).map((name): Uint8Array =>
        readFileSync(join(directory, name)),
      );
      const bytes = new Uint8Array(
        files.reduce((length, file) => length + file.byteLength, 0),
      );
      let offset = 0;
      for (const file of files) {
        bytes.set(file, offset);
        offset += file.byteLength;
      }
      return Promise.resolve(bytes);
    },
    close: () => host.close(),
  };
};

it("scrubs a stored value from the model request, store, reads, replay, live frames and compaction", async () => {
  await proveSessionRedaction(nodeHost);
});

it("withholds output and fails the run with a typed reason when the Vault port is down", async () => {
  await proveRedactionFailsClosed(nodeHost);
});
