import { DatabaseSync } from "node:sqlite";
import { fromNodeSqlite } from "@fungi.computer/watchdog/node-sqlite";
import { fauxModel, type FauxResponseStep } from "@fungi.computer/mule/testing";
import { rejectingSandbox } from "@fungi.computer/mycelium/testing";
import type {
  ExtensionRosterV0,
  Json,
  RosteredExtensionV0,
} from "./extensions.js";
import * as Schema from "effect/Schema";
import * as z from "zod";
import {
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { GuestThrew, type Sandbox } from "@fungi.computer/mycelium";
import {
  Shiitake,
  type ModelSource,
  type RuntimeHost,
  type SessionSnapshot,
  type ShiitakeOwnerAgent,
  type ShiitakeOptions,
  type SqliteOwner,
} from "./index.js";

const settlementBudgetSchema = z.int().min(1).max(1_000).default(1_000);

/** A deterministic model source for trusted tests; no provider account or I/O. */
export function fauxModelSource(
  replies: readonly FauxResponseStep[],
): ModelSource {
  return {
    acquire: ({ signal }) => {
      signal.throwIfAborted();
      const faux = fauxModel(replies);
      return Promise.resolve({
        model: faux.model,
        stream: faux.stream,
        retryable: faux.retryable,
        release: () => Promise.resolve(),
      });
    },
    describe: ({ signal }) => {
      signal.throwIfAborted();
      return Promise.resolve({
        contextWindow: fauxModel(replies).model.contextWindow,
      });
    },
  };
}

/** One scripted model tool call; the scripted sandbox runs exactly this call. */
export class ScriptedToolCall {
  constructor(
    /** `<alias>.<operation>` as the model would call it, e.g. `bookmarks.add`. */
    readonly path: string,
    readonly input: Json,
  ) {}
}

/** One model turn in a test script: a reply, a tool call, or a faux step. */
export type ScriptStep = string | ScriptedToolCall | FauxResponseStep;

/** Script one model turn that calls an operation with this input. */
export function callTool(path: string, input: Json = {}): ScriptedToolCall {
  return new ScriptedToolCall(path, input);
}

const systemMessage = z.object({
  role: z.literal("system"),
  content: z.string(),
});

/** Script one model turn that replies with the system prompt it received. */
export function echoSystemPrompt(): FauxResponseStep {
  return (context) => {
    const system = systemMessage.safeParse(context.messages[0]);
    return fauxAssistantMessage(
      system.success ? system.data.content : "(no system prompt)",
    );
  };
}

function scriptedCode(call: ScriptedToolCall): string {
  return `return await ${call.path}(${JSON.stringify(call.input)});`;
}

/**
 * A deterministic model plus a sandbox that runs only the scripted calls,
 * through the operations the host admitted. It evaluates no JavaScript.
 */
type ScriptedRun = Readonly<{ model: ModelSource; sandbox: Sandbox }>;

const isReplyText = Schema.is(Schema.String);
const thrownMessage = z.object({ message: z.string() });

function scriptedRun(steps: readonly ScriptStep[]): ScriptedRun {
  const calls = new Map<string, ScriptedToolCall>();
  const replies: FauxResponseStep[] = steps.map((step) => {
    if (isReplyText(step)) return fauxAssistantMessage(step);
    if (step instanceof ScriptedToolCall) {
      const code = scriptedCode(step);
      calls.set(code, step);
      return fauxAssistantMessage(fauxToolCall("execute", { code }), {
        stopReason: "toolUse",
      });
    }
    return step;
  });
  return {
    model: fauxModelSource(replies),
    sandbox: {
      execute: async (request, signal) => {
        signal.throwIfAborted();
        const call = calls.get(request.code);
        if (call === undefined)
          throw new Error("The scripted sandbox runs only scripted calls");
        const dot = call.path.indexOf(".");
        const operation =
          request.bindings[call.path.slice(0, dot)]?.[call.path.slice(dot + 1)];
        if (dot < 1 || operation === undefined)
          // Model code calling an operation this run lacks fails in the guest.
          throw new GuestThrew({
            name: "ReferenceError",
            message: `${call.path} is not available to this run`,
          });
        try {
          return await operation(call.input);
        } catch (cause) {
          // An uncaught rejection in model code is a guest exception.
          throw new GuestThrew({
            name: "Error",
            message:
              thrownMessage.safeParse(cause).data?.message ?? String(cause),
          });
        }
      },
    },
  };
}

/** Optional canonical Agent inputs and a borrowed owner; defaults are in-memory. */
export type MemoryHostOptions = Partial<
  Omit<ShiitakeOptions, "store" | "runtime" | "extensions">
> &
  Readonly<{
    /** Borrow this owner without closing it; otherwise own a Node memory database. */
    owner?: SqliteOwner;
    /**
     * An explicit roster and grants, or a plain list of extensions. A list is
     * granted exactly what each extension requests: test hosts only;
     * `Shiitake.make` always takes explicit grants.
     */
    extensions?: ExtensionRosterV0 | readonly RosteredExtensionV0[];
    /**
     * Script the model turns. Tool calls run through a sandbox that only runs
     * these calls. Use instead of `model` and `tools.sandbox`.
     */
    script?: readonly ScriptStep[];
  }>;

/** Grant each listed extension exactly the capabilities it requests. */
function requestedRoster(
  extensions: ExtensionRosterV0 | readonly RosteredExtensionV0[],
): ExtensionRosterV0 {
  if ("roster" in extensions) return extensions;
  return {
    roster: extensions,
    grants: extensions.map((extension) => ({
      id: extension.manifest.id,
      capabilities: extension.manifest.requests,
    })),
  };
}

/** Explicitly driven in-process host for trusted tests and runnable examples. */
export type MemoryHost = Readonly<{
  /** The canonical public Shiitake Agent. */
  agent: ShiitakeOwnerAgent;
  /** Invoke one canonical reconciliation tick; no timer runs in the background. */
  reconcile: () => Promise<void>;
  /** Drain pending Agent work with a 1–1,000 tick budget (default 1,000). */
  settle: (sessionId: string, maxTicks?: number) => Promise<SessionSnapshot>;
  /** Close the Agent and only the database owned by this helper. */
  close: () => Promise<void>;
}>;

/**
 * Compose the real Shiitake runtime over an optional borrowed SQLite owner.
 * Call reconcile/settle explicitly after acceptance. Defaults use a faux model
 * and rejecting sandbox; custom callbacks are NOT isolation and retain host
 * privileges. No submitted JavaScript is evaluated by this helper.
 */
export async function memoryHost(
  options: MemoryHostOptions = {},
): Promise<MemoryHost> {
  const { owner, script, extensions, ...agentOptions } = options;
  if (
    script !== undefined &&
    (options.model !== undefined || options.tools !== undefined)
  )
    throw new Error("Use script, or model and tools, not both");
  const scripted = script === undefined ? undefined : scriptedRun(script);
  const storage =
    owner === undefined ? openMemoryOwner() : { owner, close: () => undefined };
  let released = false;
  let bound: Parameters<RuntimeHost["bind"]>[0] | undefined;
  let closed = false;
  const runtime: RuntimeHost = {
    bind: (input) => {
      bound = input;
      return Promise.resolve({
        commit: (mutation) => mutation(),
        requestWake: () => Promise.resolve(),
        close: () => {
          closed = true;
          return Promise.resolve();
        },
      });
    },
  };
  const requireBound = () => {
    if (closed || bound === undefined)
      throw new Error("Memory host is closed or unbound");
    return bound;
  };
  try {
    const agent = await Shiitake.make({
      ...agentOptions,
      store: storage.owner,
      runtime,
      model:
        scripted?.model ??
        options.model ??
        fauxModelSource([fauxAssistantMessage("complete")]),
      instructions: options.instructions ?? {
        load: () =>
          Promise.resolve({
            revision: "memory-host-v1",
            text: "Complete this test run.",
          }),
      },
      extensions:
        extensions === undefined
          ? { roster: [], grants: [] }
          : requestedRoster(extensions),
      tools:
        scripted === undefined
          ? (options.tools ?? {
              sandbox: rejectingSandbox("Memory host has no direct tools"),
            })
          : { sandbox: scripted.sandbox },
    });
    return {
      agent,
      reconcile: async () => requireBound().reconcile(),
      settle: async (sessionId, maxTicks) => {
        const budget = settlementBudgetSchema.parse(maxTicks);
        for (let tick = 0; tick < budget; tick += 1) {
          const host = requireBound();
          if (!(await host.pendingWork()).pending)
            return agent.session(sessionId).read();
          await host.reconcile();
        }
        throw new Error(
          `Memory host did not settle within ${String(budget)} reconciliation ticks`,
        );
      },
      close: async () => {
        if (released) return;
        released = true;
        try {
          await agent.close();
        } finally {
          storage.close();
        }
      },
    };
  } catch (cause) {
    storage.close();
    throw cause;
  }
}

function openMemoryOwner() {
  const database = new DatabaseSync(":memory:");
  database.exec("PRAGMA foreign_keys = ON");
  return {
    owner: fromNodeSqlite(database),
    close: () => {
      database.close();
    },
  };
}
