import {
  memoryHost,
  fauxModelSource,
  type MemoryHost,
} from "@fungi.computer/shiitake/testing";
import { Shiitake } from "@fungi.computer/shiitake";
import { make as makeEffect } from "@fungi.computer/shiitake/effect";
import type {
  RuntimeHost,
  ShiitakeAgent,
  ShiitakeOptions,
} from "@fungi.computer/shiitake";
import type {
  RuntimeHostEffect,
  ShiitakeEffectAgent,
  ShiitakeEffectOptions,
} from "@fungi.computer/shiitake/effect";
import type {
  SessionCommand,
  SessionSnapshot,
} from "@fungi.computer/shiitake/client";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { ExtensionId } from "@fungi.computer/shiitake/extensions";
import { defineClientEntry } from "@fungi.computer/shiitake/client-entry";
import { rowSchemaV1, type RowV1 } from "@fungi.computer/gills";

const row: RowV1 = Schema.decodeUnknownSync(rowSchemaV1)({
  id: "public-row",
  title: "Public contribution",
  open: { kind: "session", agentId: "stranger", sessionId: "other" },
});
const clientEntry = defineClientEntry(
  {
    id: ExtensionId.make("stranger"),
    commands: {},
    projection: { schema: Schema.String },
  },
  {
    attach: ({ signal }) => {
      signal.throwIfAborted();
      return Promise.resolve();
    },
    contribute: (view) => ({
      chips: [],
      panel: view.status === "running" ? [row] : [],
      timeline: { cells: [], omitted: 0 },
      waiting: null,
    }),
  },
);
void clientEntry;

declare const options: Omit<ShiitakeOptions, "runtime">;
declare const effectOptions: Omit<ShiitakeEffectOptions, "runtime">;
declare const agent: ShiitakeAgent;
declare const effectAgent: ShiitakeEffectAgent;
declare const command: SessionCommand;
declare const snapshot: SessionSnapshot;

const runtime: RuntimeHost = {
  bind: () =>
    Promise.resolve({
      commit: (mutation) => mutation(),
      requestWake: async () => Promise.resolve(),
      close: async () => Promise.resolve(),
    }),
};
const effectRuntime: RuntimeHostEffect = {
  bind: () =>
    Effect.succeed({
      commit: (mutation) => mutation,
      requestWake: Effect.void,
      close: Effect.void,
    }),
};

void Shiitake.make({ ...options, runtime });
void makeEffect({ ...effectOptions, runtime: effectRuntime });
void agent.session("stranger-session").command(command);
void effectAgent.session("stranger-session").command(command);
void snapshot;

async function testingConsumer(): Promise<void> {
  const host: MemoryHost = await memoryHost({ model: fauxModelSource([]) });
  try {
    await host.agent.session("consumer").send("Hello");
    const snapshot: SessionSnapshot = await host.settle("consumer", 8);
    void snapshot;
    await host.reconcile();
  } finally {
    await host.close();
  }
}
void testingConsumer;

async function fleetConsumer(
  options: import("@fungi.computer/shiitake/fleet").FleetDelegationOptions,
  grant: import("@fungi.computer/shiitake/fleet").AgentGrant,
): Promise<void> {
  const { createFleetDelegation, createFleetSessions, createAgentDirectory } =
    await import("@fungi.computer/shiitake/fleet");
  const { agentExtensionMethods } =
    await import("@fungi.computer/shiitake/agar-extension");
  const outcome = await createFleetDelegation(options).delegate(grant, {
    requestId: "task",
    agentId: "child",
    text: "Work",
  });
  if (outcome.kind === "settled") {
    const terminal: import("@fungi.computer/shiitake/client").SessionTerminal =
      outcome.terminal;
    void terminal;
    void outcome.usage;
  }
  const sessions = createFleetSessions(options.open);
  await sessions.send(grant, "child", undefined, {
    requestId: "send",
    text: "Work",
  });
  void createAgentDirectory(options.storage.sql).list(grant);
  void agentExtensionMethods;
}
void fleetConsumer;
