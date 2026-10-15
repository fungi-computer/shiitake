import {
  directSandbox,
  rejectingSandbox,
} from "@fungi.computer/mycelium/testing";
import { fauxModel } from "@fungi.computer/mule/testing";
import {
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { Knapsack, type FilesPort } from "@fungi.computer/knapsack";
import type { Grants } from "@fungi.computer/mycelium";
import { expect, it } from "vitest";
import { defineExtension, ExtensionId } from "../src/extensions.js";

import { Shiitake, type ToolAccess } from "../src/index.js";
import {
  createCompatibilityRuntimeHost,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

/** A files.* host that records every attempted operation. */
function recordingFilesPort(invoked: string[]): FilesPort {
  return {
    read: (input) => {
      invoked.push("read");
      return Promise.resolve({
        path: input.path,
        content: "",
        offset: 0,
        nextOffset: null,
        truncated: false,
      });
    },
    list: (input) => {
      invoked.push("list");
      return Promise.resolve({
        path: input.path,
        entries: [],
        truncated: false,
      });
    },
    glob: (input) => {
      invoked.push("glob");
      return Promise.resolve({
        path: input.path,
        matches: [],
        truncated: false,
      });
    },
    grep: (input) => {
      invoked.push("grep");
      return Promise.resolve({
        path: input.path,
        matches: [],
        truncated: false,
      });
    },
    replace: (input) => Promise.resolve({ path: input.path, bytes: 0 }),
    patch: () => {
      invoked.push("patched");
      return Promise.resolve({
        status: "completed" as const,
        exitCode: 0,
        stdout: "",
        stderr: "",
        truncated: false,
      });
    },
  };
}

function fileExtensions(invoked: string[]) {
  const member = defineExtension({
    id: ExtensionId.make("test-files"),
    version: "1.0.0",
  }).define({
    files: () => Knapsack.files({ port: recordingFilesPort(invoked) }).tools,
  });
  return {
    roster: [member],
    grants: [{ id: member.manifest.id, capabilities: ["files"] as const }],
  };
}

/** One Agent over the recording Files extension; each Session runs one prompt. */
async function runFiles(
  input: Readonly<{
    access?: ToolAccess;
    /** The grants the host stores for each Session before its run. */
    grantsFor?: (sessionId: string) => Grants | undefined;
    /** Raw SQL the test runs against the owner before each run. */
    tamper?: (
      owner: ReturnType<typeof createNativeSqliteOwnerFixture>["owner"],
    ) => void;
    responses: ReturnType<typeof fauxAssistantMessage>[];
    sessions: readonly string[];
    invoked: string[];
    sandbox?: Parameters<typeof directSandbox>[0];
  }>,
) {
  const fixture = createNativeSqliteOwnerFixture();
  const runtime = createCompatibilityRuntimeHost();
  const faux = fauxModel([]);
  faux.setResponses(input.responses);
  const files = Knapsack.files({ port: recordingFilesPort(input.invoked) });
  const agent = await Shiitake.make({
    store: fixture.owner,
    runtime: runtime.runtime,
    extensions: fileExtensions(input.invoked),
    instructions: {
      load: () => Promise.resolve({ revision: "v1", text: "Use tools." }),
    },
    model: {
      acquire: () =>
        Promise.resolve({
          model: faux.model,
          stream: faux.stream,
          release: () => Promise.resolve(),
        }),
    },
    tools: {
      // An explicitly composed Files module is granted like any other module.
      modules: [files.module],
      sandbox:
        input.sandbox === undefined
          ? rejectingSandbox("Direct Files tools never call execute")
          : directSandbox(input.sandbox),
      access: input.access ?? {},
    },
  });
  try {
    const terminals: unknown[] = [];
    for (const sessionId of input.sessions) {
      const session = agent.session(sessionId);
      await session.command({
        delivery: "followUp",
        type: "prompt",
        requestId: sessionId,
        text: "Use Files.",
      });
      const grants = input.grantsFor?.(sessionId);
      if (grants !== undefined)
        agent.grants.deliver(sessionId, { version: 1, grants });
      input.tamper?.(fixture.owner);
      await runtime.runNext();
      terminals.push(...(await session.read()).terminals);
    }
    return terminals;
  } finally {
    await agent.close();
    fixture.close();
  }
}

it("runs Files tools when the host supplies no access data", async () => {
  const invoked: string[] = [];
  await runFiles({
    invoked,
    sessions: ["owner"],
    responses: [
      fauxAssistantMessage(fauxToolCall("read", { path: "a.txt" }), {
        stopReason: "toolUse",
      }),
      fauxAssistantMessage("done"),
    ],
  });
  expect(invoked).toEqual(["read"]);
});

it("refuses a Files call whose stored grants cannot be read, before the port runs", async () => {
  const invoked: string[] = [];
  await runFiles({
    invoked,
    sessions: ["owner"],
    grantsFor: () => ({ files: "write" }),
    // A corrupted row must fail the call closed, never open.
    tamper: (owner) => {
      owner.sql.exec(
        `UPDATE session_tool_grants SET grants_json = '{"files":"admin"}'`,
      );
    },
    responses: [
      fauxAssistantMessage(fauxToolCall("read", { path: "a.txt" }), {
        stopReason: "toolUse",
      }),
      fauxAssistantMessage("done"),
    ],
  });
  expect(invoked).toEqual([]);
});

it.each(["direct", "execute"])(
  "refuses Files %s writes under a read grant, leaving the file untouched",
  async (presentation) => {
    const invoked: string[] = [];
    const refused: string[] = [];
    const callPatch = () =>
      fauxAssistantMessage(
        presentation === "direct"
          ? fauxToolCall("apply_patch", {
              patch: "*** Begin Patch\n*** End Patch",
            })
          : fauxToolCall("execute", { code: "files.patch" }),
        { stopReason: "toolUse" },
      );
    await runFiles({
      invoked,
      sessions: ["owner-session", "bound-session"],
      grantsFor: (sessionId) =>
        sessionId === "owner-session" ? undefined : { files: "read" },
      access: {
        onRefused: ({ module, operation, reason }, { sessionId }) => {
          refused.push(`${String(sessionId)}:${module}.${operation}:${reason}`);
        },
      },
      responses: [
        callPatch(),
        fauxAssistantMessage("owner done"),
        callPatch(),
        fauxAssistantMessage("bound done"),
      ],
      sandbox: async (bindings) => {
        const patch = bindings.files?.patch;
        if (patch === undefined) throw new Error("Files patch not offered");
        return patch({ patch: "*** Begin Patch\n*** End Patch" });
      },
    });
    const operation = presentation === "direct" ? "apply_patch" : "patch";
    expect(refused).toEqual([`bound-session:files.${operation}:read-only`]);
    // Only the allowed call reached the Files host.
    expect(invoked).toEqual(["patched"]);
  },
);

it("keeps every read-only Files tool under a read grant and refuses each under none", async () => {
  const cases = [
    ["read", { path: "hello.txt" }],
    ["list", { path: "." }],
    ["glob", { path: ".", pattern: "*.txt" }],
    ["grep", { path: ".", pattern: "hello" }],
  ] as const;
  const responses = () =>
    cases.flatMap(([name, args]) => [
      fauxAssistantMessage(fauxToolCall(name, args), { stopReason: "toolUse" }),
      fauxAssistantMessage("done"),
    ]);
  const sessions = cases.map(([name]) => `${name}-session`);
  for (const [grant, expected] of [
    ["read", ["read", "list", "glob", "grep"]],
    ["none", []],
  ] as const) {
    const invoked: string[] = [];
    await runFiles({
      invoked,
      sessions,
      grantsFor: () => ({ files: grant }),
      responses: responses(),
    });
    expect(invoked).toEqual(expected);
  }
});
