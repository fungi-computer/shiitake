/**
 * Hosted Agent extensions: one `defineExtension` value served from another
 * isolate (a Fungi App backend) to an Agent that does not load its code.
 *
 * The wire is three JSON methods. The Agent's host calls them through its own
 * authority door; this module owns their grammar and the guest runtime that
 * answers them by activating the extension against a port with no Agent
 * transaction behind it.
 */
import {
  describeModule,
  invokeModuleOperation,
  remoteModule,
} from "@fungi.computer/mycelium/remote";
import type { InvocationContext } from "@fungi.computer/mycelium";
import { Mycelium } from "@fungi.computer/mycelium/define";
import { CapabilityFailed } from "@fungi.computer/mycelium/sandbox-failure";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import { z } from "zod";

import { agentExtensionMethodNames } from "@fungi.computer/agar/backend-definition";
import { ShiitakeError } from "./errors.js";
import { ExtensionId } from "./extension-contract.js";
import {
  AdmissionError,
  defineExtension,
  ExtensionRead,
  ExtensionStateError,
  type AnyAgentHooksV0,
  type ExtensionCapabilityV0,
  type ExtensionGrantV0,
  type ExtensionManifestV0,
  type PromptSectionV0,
  type RosteredExtensionV0,
  type RunIdentityV0,
} from "./extensions.js";
import type { ExtensionRuntimePortV0 } from "./internal/extension-context.js";
import { readDefinition } from "./internal/extension-definitions.js";
import { invokeExtensionHook } from "./internal/extension-hook.js";
import { admitExtensionRosterV0 } from "./internal/extension-roster.js";

/** Capabilities a hosted extension may request in this protocol version. */
export const hostedExtensionCapabilities: readonly ExtensionCapabilityV0[] =
  Object.freeze(["prompt", "run_tools"]);

const encoder = new TextEncoder();
const identity = z.string().min(1).max(256);
const boundedText = (bytes: number) =>
  z
    .string()
    .min(1)
    .refine((text) => encoder.encode(text).byteLength <= bytes, {
      message: `Text exceeds ${String(bytes)} bytes`,
    });
type JsonValue = z.output<ReturnType<typeof z.json>>;
const jsonObject = z.record(z.string(), z.json());
/**
 * Keywords a hosted schema may not use. Regexes could stall the Agent host,
 * and references expand exponentially when it renders or validates them.
 */
const refusedKeywords = new Set([
  "pattern",
  "patternProperties",
  "$ref",
  "$dynamicRef",
  "$recursiveRef",
  "$defs",
  "definitions",
]);
/** Keywords whose object keys are property names, not keywords. */
const nameMaps = new Set(["properties", "dependentSchemas"]);
/**
 * True when a guest JSON Schema stays within depth 16 and carries no refused
 * keyword. Without references, rendering and validation work stays linear in
 * the schema, which is itself bounded to 20000 bytes. The guest still
 * validates with its own schemas.
 */
function safeSchemaTree(value: JsonValue, depth = 0, names = false): boolean {
  if (depth > 16) return false;
  if (Array.isArray(value))
    return value.every((child) => safeSchemaTree(child, depth + 1));
  const object = jsonObject.safeParse(value);
  if (!object.success) return true;
  return Object.entries(object.data).every(
    ([key, child]) =>
      (names || !refusedKeywords.has(key)) &&
      safeSchemaTree(child, depth + 1, !names && nameMaps.has(key)),
  );
}
const boundedJson = z
  .json()
  .refine(
    (value) => encoder.encode(JSON.stringify(value)).byteLength <= 20_000,
    { message: "JSON Schema exceeds 20000 bytes" },
  )
  .refine((value) => safeSchemaTree(value), {
    message: "JSON Schema is too deep or uses a refused keyword",
  });
const operationPath = z
  .string()
  .max(160)
  .regex(/^[A-Za-z][A-Za-z0-9_]*(?:\.[A-Za-z][A-Za-z0-9_]*)*$/u);

/** The run a hosted call is made for; the host derives it, never the model. */
export const hostedRunSchema = z.strictObject({
  sessionId: identity,
  workId: identity,
  origin: z
    .strictObject({ extensionId: identity, correlation: identity.nullable() })
    .nullable(),
});
/** A parsed {@link hostedRunSchema} value. */
export type HostedRun = z.output<typeof hostedRunSchema>;

/** `agentExtensionPrompt` input: the run whose model request needs sections. */
export const hostedPromptInputSchema = z.strictObject({ run: hostedRunSchema });
/** A parsed {@link hostedPromptInputSchema} value. */
export type HostedPromptInput = z.output<typeof hostedPromptInputSchema>;
/** `agentExtensionPrompt` output: at most 16 named sections of at most 8 KiB each. */
export const hostedPromptOutputSchema = z.strictObject({
  sections: z
    .array(
      z.strictObject({
        name: z.string().regex(/^[a-z][A-Za-z0-9_-]{0,63}$/u),
        text: boundedText(8192),
      }),
    )
    .max(16),
});
/** A parsed {@link hostedPromptOutputSchema} value. */
export type HostedPromptOutput = z.output<typeof hostedPromptOutputSchema>;

/** A run's tool module as at most 64 dotted operations with bounded JSON Schemas. */
export const hostedModuleDescriptorSchema = z.strictObject({
  description: z.string().max(4096).optional(),
  operations: z
    .record(
      operationPath,
      z.strictObject({
        replay: z.enum(["safe", "unsafe"]),
        description: z.string().max(1000).optional(),
        input: boundedJson,
        output: boundedJson.nullable(),
      }),
    )
    .refine((operations) => Object.keys(operations).length <= 64, {
      message: "A hosted module declares at most 64 operations",
    }),
});
/** A parsed {@link hostedModuleDescriptorSchema} value. */
export type HostedModuleDescriptor = z.output<
  typeof hostedModuleDescriptorSchema
>;

/** `agentExtensionTools` input: the run the tools are captured for. */
export const hostedToolsInputSchema = hostedPromptInputSchema;
/** A parsed {@link hostedToolsInputSchema} value. */
export type HostedToolsInput = z.output<typeof hostedToolsInputSchema>;
/** `agentExtensionTools` output: the run's module, or null when it has none. */
export const hostedToolsOutputSchema = z.strictObject({
  module: hostedModuleDescriptorSchema.nullable(),
});
/** A parsed {@link hostedToolsOutputSchema} value. */
export type HostedToolsOutput = z.output<typeof hostedToolsOutputSchema>;

/** `agentExtensionCall` input: one operation, its input, and the host's call identity. */
export const hostedCallInputSchema = z.strictObject({
  run: hostedRunSchema,
  operation: operationPath,
  input: z.json(),
  invocation: z.strictObject({
    executionId: identity,
    toolCallId: identity.optional(),
    deadlineAt: z.number().int().nonnegative().nullable(),
  }),
});
/** A parsed {@link hostedCallInputSchema} value. */
export type HostedCallInput = z.output<typeof hostedCallInputSchema>;
/** `agentExtensionCall` output: the operation's JSON result. */
export const hostedCallOutputSchema = z.strictObject({ value: z.json() });
/** A parsed {@link hostedCallOutputSchema} value. */
export type HostedCallOutput = z.output<typeof hostedCallOutputSchema>;

/**
 * The guest side of one hosted extension. Inputs are already parsed by the
 * App method door with the schemas above; outputs are bounded here.
 */
export type HostedAgentExtension = Readonly<{
  manifest: ExtensionManifestV0;
  /** Build the run's prompt sections. */
  prompt: (input: HostedPromptInput) => Promise<HostedPromptOutput>;
  /** Describe the run's tool module. */
  tools: (input: HostedToolsInput) => Promise<HostedToolsOutput>;
  /** Run one operation of the run's tool module. */
  call: (input: HostedCallInput) => Promise<HostedCallOutput>;
}>;

const unavailable = (what: string) =>
  new ExtensionStateError({
    reason: "unavailable",
    message: `${what} is unavailable to a hosted extension`,
  });

/** No Agent transaction stands behind a hosted extension; every door says so. */
function hostedPort(
  manifest: ExtensionManifestV0,
  revision: ExtensionRuntimePortV0["context"]["revision"],
  grants: readonly ExtensionCapabilityV0[],
): ExtensionRuntimePortV0 {
  return Object.freeze({
    collection: () => {
      throw unavailable("State");
    },
    updateCollection: () => {
      throw unavailable("State");
    },
    document: () => {
      throw unavailable("A Session document");
    },
    update: () => {
      throw unavailable("A state update");
    },
    observeUpdate: () => {
      throw unavailable("Observation");
    },
    context: Object.freeze({
      manifest,
      revision,
      grants: Object.freeze([...grants]),
      send: () =>
        Effect.fail(
          new AdmissionError({
            reason: "closed",
            message: "Send is unavailable to a hosted extension",
          }),
        ),
      timers: Object.freeze({
        set: () => Effect.fail(unavailable("A timer")),
        cancel: () => Effect.fail(unavailable("A timer")),
        read: () => Effect.fail(unavailable("A timer")),
      }),
      attention: Object.freeze({
        set: () => Effect.fail(unavailable("Attention")),
        awaitsDecision: () => Effect.fail(unavailable("Attention")),
        clear: () => Effect.fail(unavailable("Attention")),
      }),
      hasPendingSteer: () => Effect.fail(unavailable("Steering")),
      workSender: () => Effect.fail(unavailable("Run sender")),
      sessionParent: () => Effect.fail(unavailable("Session parent")),
      sessionMaker: () => Effect.fail(unavailable("Session maker")),
      recipientStatus: () => Effect.succeed("unknown" as const),
      recipientParent: () => Effect.fail(unavailable("Recipient parent")),
      sessionPosition: () => Effect.fail(unavailable("Session timeline")),
      hasSuccessfulToolResult: () => Effect.fail(unavailable("A tool result")),
      observe: () => Effect.die(unavailable("Observation")),
      isObserved: () => false,
      transact: () =>
        Effect.fail(
          new ShiitakeError(
            "extensions",
            "rejected",
            "A hosted extension has no Agent transaction",
          ),
        ),
    }),
  });
}

/** The guest's own context for one forwarded call; the deadline is the host's. */
function invocationContext(
  invocation: HostedCallInput["invocation"],
): InvocationContext {
  const signal =
    invocation.deadlineAt === null
      ? new AbortController().signal
      : AbortSignal.timeout(Math.max(0, invocation.deadlineAt - Date.now()));
  const context: InvocationContext = {
    executionId: invocation.executionId,
    deadlineAt: invocation.deadlineAt,
    signal,
  };
  return invocation.toolCallId === undefined
    ? context
    : { ...context, toolCallId: invocation.toolCallId };
}

function runIdentity(run: HostedRun): RunIdentityV0 {
  return {
    sessionId: run.sessionId,
    workId: run.workId,
    origin:
      run.origin === null
        ? null
        : {
            extensionId: ExtensionId.make(run.origin.extensionId),
            correlation: run.origin.correlation,
          },
  };
}

/**
 * Serve one extension's prompt sections and tools to a remote Agent. Throws
 * when the extension requests a capability hosting does not provide yet, so a
 * build fails instead of silently dropping it.
 */
export function hostAgentExtension(
  rostered: RosteredExtensionV0,
): HostedAgentExtension {
  const definition = readDefinition(rostered);
  if (definition === undefined)
    throw new Error("Hosted extension was not defined");
  const manifest = definition.manifest;
  const unsupported = manifest.requests.filter(
    (capability) => !hostedExtensionCapabilities.includes(capability),
  );
  if (unsupported.length > 0)
    throw new Error(
      `Hosted extensions do not support: ${unsupported.join(", ")}`,
    );
  const admitted = Effect.runPromise(
    admitExtensionRosterV0({
      roster: [rostered],
      grants: [{ id: manifest.id, capabilities: manifest.requests }],
    }),
  ).then((members) => {
    const member = members[0];
    if (member === undefined) throw new Error("Hosted extension not admitted");
    return member;
  });
  const read = { extensionId: manifest.id };
  const withHooks = async <A>(
    use: (hooks: AnyAgentHooksV0) => Effect.Effect<A, unknown, ExtensionRead>,
  ): Promise<A> => {
    const member = await admitted;
    const exit = await Effect.runPromiseExit(
      Effect.scoped(
        definition
          .activate(hostedPort(manifest, member.revision, [...member.grants]))
          .pipe(Effect.flatMap(use)),
      ).pipe(Effect.provideService(ExtensionRead, read)),
    );
    if (Exit.isSuccess(exit)) return exit.value;
    throw new Error("Hosted extension hook failed", { cause: exit.cause });
  };
  const moduleFor = (hooks: AnyAgentHooksV0, run: RunIdentityV0) =>
    hooks.moduleForRun === undefined
      ? Effect.succeed(Option.none())
      : hooks.moduleForRun(run);
  return Object.freeze({
    manifest,
    prompt: async ({ run }) => {
      const sections = await withHooks((hooks) => {
        const hook = hooks.prompt;
        return hook === undefined
          ? Effect.succeed([])
          : invokeExtensionHook(() => hook(runIdentity(run)));
      });
      return hostedPromptOutputSchema.parse({ sections });
    },
    tools: async ({ run }) => {
      const registration = await withHooks((hooks) =>
        moduleFor(hooks, runIdentity(run)),
      );
      return hostedToolsOutputSchema.parse({
        module: Option.isSome(registration)
          ? describeModule(registration.value)
          : null,
      });
    },
    call: async (request) => {
      const value = await withHooks((hooks) =>
        moduleFor(hooks, runIdentity(request.run)).pipe(
          Effect.flatMap((registration) =>
            Option.isNone(registration)
              ? Effect.fail(new Error("Hosted extension has no tools"))
              : Effect.tryPromise(() =>
                  invokeModuleOperation(
                    registration.value,
                    request.operation,
                    request.input,
                    invocationContext(request.invocation),
                  ),
                ),
          ),
        ),
      );
      return hostedCallOutputSchema.parse({ value });
    },
  });
}

/** The one roster member that brings attached hosted extensions to a run. */
export const hostedAgentExtensionsId = ExtensionId.make(
  "hosted-agent-extensions",
);
/** The host grant for {@link hostedAppExtensions}; a host may add a grant key. */
export const hostedAgentExtensionsGrant: ExtensionGrantV0 = {
  id: hostedAgentExtensionsId,
  capabilities: ["prompt", "run_tools"],
};

const maximumSections = 16;
const maximumCached = 256;
const sectionBytes = 8192;
const sectionName = /^[a-z][A-Za-z0-9_-]{0,63}$/u;

/** One hosted extension a host attached to this Agent, as the host decided it. */
export type HostedAppAttachment = Readonly<{
  /** The App's binding name; the run sees it as `env.extensions.<NAME>`. */
  bindingName: string;
  /** The installation the extension's tool module is named for. */
  installationId: string;
  /** Prompt sections reach every level; tools reach `write` only. */
  level: "read" | "write";
  /** Changes whenever the attachment's authority or Release changes. */
  cacheKey: string;
}>;

/** One guest call's answer from the host's App door; the composer parses `value`. */
export type HostedAppCallResult =
  | Readonly<{ kind: "ok"; value: unknown }>
  | Readonly<{ kind: "refused"; reason: string }>;

/** A guest failure the host records; none of them fails the run. */
export type HostedAppReport<Attachment extends HostedAppAttachment> =
  | Readonly<{ kind: "prompt_failed"; cause: unknown }>
  | Readonly<{ kind: "tools_failed"; attachment: Attachment; cause: unknown }>
  | Readonly<{
      kind: "tools_skipped";
      attachment: Attachment;
      outcome: string;
    }>;

/** The host's side of {@link hostedAppExtensions}. */
export type HostedAppExtensionsPorts<Attachment extends HostedAppAttachment> =
  Readonly<{
    /** The Session's attached extensions, or null when it may use none. */
    attachments: (sessionId: string) => Promise<readonly Attachment[] | null>;
    /** One guest call through the host's own App authority door. */
    call: (
      attachment: Attachment,
      method: string,
      input: z.output<ReturnType<typeof z.json>>,
      deadlineAt: number | null,
    ) => Promise<HostedAppCallResult>;
    /** Records a guest failure; none of them fails the run. */
    report: (report: HostedAppReport<Attachment>) => void;
    /**
     * Wall budget for one prompt or tools call. It travels as a deadline to
     * the App's owner, which ends the call and aborts the guest isolate.
     */
    guestBudgetMs: number;
  }>;

function wireRun(run: RunIdentityV0): HostedRun {
  return {
    sessionId: run.sessionId,
    workId: run.workId,
    origin:
      run.origin === null
        ? null
        : {
            extensionId: run.origin.extensionId,
            correlation: run.origin.correlation,
          },
  };
}

/** The host's identity and deadline for one forwarded call; the signal stays here. */
function wireInvocation(invocation: InvocationContext) {
  const forwarded = {
    executionId: invocation.executionId,
    deadlineAt: invocation.deadlineAt,
  };
  return invocation.toolCallId === undefined
    ? forwarded
    : { ...forwarded, toolCallId: invocation.toolCallId };
}

/** Quote every line of guest text so none can pose as a section header. */
function quoted(text: string): string {
  return text
    .split(/\r\n|[\n\v\f\r\u0085\u2028\u2029]/u)
    .map((line) => `> ${line}`)
    .join("\n");
}

/**
 * Serve the prompt sections and tools of every hosted extension a host
 * attached to this Agent. Every guest call goes through the host's App door,
 * which owns the attachment, grantor, admission and Release checks. A guest
 * failure becomes a marker or a missing namespace; it never fails the run.
 */
export function hostedAppExtensions<Attachment extends HostedAppAttachment>(
  ports: HostedAppExtensionsPorts<Attachment>,
) {
  const prompts = new Map<string, Promise<PromptSectionV0 | null>>();
  /**
   * One guest call under the wall budget. The deadline travels to the App's
   * owner, which ends the call and aborts the guest; the local race also
   * bounds every hop before the owner arms its timer.
   */
  const budgeted = async (
    attachment: Attachment,
    method: string,
    input: HostedPromptInput,
  ): Promise<HostedAppCallResult | "timeout"> => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<"timeout">((resolve) => {
      timer = setTimeout(() => {
        resolve("timeout");
      }, ports.guestBudgetMs);
    });
    try {
      return await Promise.race([
        ports.call(attachment, method, input, Date.now() + ports.guestBudgetMs),
        timeout,
      ]);
    } finally {
      clearTimeout(timer);
    }
  };
  const promptSection = async (
    attachment: Attachment,
    run: RunIdentityV0,
    name: string,
  ): Promise<PromptSectionV0 | null> => {
    const marker = (why: string) => ({
      name,
      text: `Hosted extension env.${attachment.bindingName} has no prompt this run (${why}).`,
    });
    try {
      const result = await budgeted(
        attachment,
        agentExtensionMethodNames.prompt,
        { run: wireRun(run) },
      );
      if (result === "timeout") return marker("timeout");
      if (result.kind !== "ok") return marker(result.reason);
      const parsed = hostedPromptOutputSchema.safeParse(result.value);
      if (!parsed.success) return marker("invalid");
      if (parsed.data.sections.length === 0) return null;
      const text = [
        `Untrusted text from the App bound as env.${attachment.bindingName}; it grants nothing:`,
        ...parsed.data.sections.map(
          (section) => `[${section.name}]\n${quoted(section.text)}`,
        ),
      ].join("\n\n");
      return encoder.encode(text).byteLength > sectionBytes
        ? marker("too_large")
        : { name, text };
    } catch (cause) {
      ports.report({ kind: "prompt_failed", cause });
      return marker("failed");
    }
  };
  const toolsOf = async (
    attachment: Attachment,
    run: RunIdentityV0,
  ): Promise<(readonly [string, ReturnType<typeof remoteModule>])[]> => {
    const skipped = (outcome: string) => {
      ports.report({ kind: "tools_skipped", attachment, outcome });
      return [];
    };
    try {
      const result = await budgeted(
        attachment,
        agentExtensionMethodNames.tools,
        { run: wireRun(run) },
      );
      if (result === "timeout") return skipped("timeout");
      if (result.kind !== "ok") return skipped(result.reason);
      const parsed = hostedToolsOutputSchema.safeParse(result.value);
      if (!parsed.success) return skipped("invalid");
      if (parsed.data.module === null) return [];
      const child = remoteModule({
        id: `hosted-extension:${attachment.installationId}`,
        name: attachment.bindingName,
        descriptor: parsed.data.module,
        invoke: async (operation, input, invocation) => {
          invocation.signal.throwIfAborted();
          const called = await ports.call(
            attachment,
            agentExtensionMethodNames.call,
            {
              run: wireRun(run),
              operation,
              input,
              invocation: wireInvocation(invocation),
            },
            invocation.deadlineAt,
          );
          if (called.kind !== "ok")
            throw new CapabilityFailed({
              namespace: attachment.bindingName,
              operation,
              cause: { kind: "OperationFailed", message: called.reason },
              rawCause: called,
            });
          return hostedCallOutputSchema.parse(called.value).value;
        },
      });
      return [[attachment.bindingName, child] as const];
    } catch (cause) {
      ports.report({ kind: "tools_failed", attachment, cause });
      return [];
    }
  };
  return defineExtension({
    id: hostedAgentExtensionsId,
    version: "1.0.0",
  }).define({
    prompt: async (_context, run) => {
      const attachments = await ports.attachments(run.sessionId);
      if (attachments === null) return [];
      const used = new Set<string>();
      const pending = attachments
        .slice(0, maximumSections)
        .map((attachment, index) => {
          const preferred = `app-${attachment.bindingName}`;
          const name =
            sectionName.test(preferred) && !used.has(preferred)
              ? preferred
              : `app-${String(index)}`;
          used.add(name);
          const key = JSON.stringify([
            run.sessionId,
            run.workId,
            attachment.cacheKey,
          ]);
          const cached = prompts.get(key);
          if (cached !== undefined) return cached;
          const section = promptSection(attachment, run, name);
          prompts.set(key, section);
          for (const old of prompts.keys()) {
            if (prompts.size <= maximumCached) break;
            prompts.delete(old);
          }
          return section;
        });
      const sections = await Promise.all(pending);
      return sections.filter((section) => section !== null);
    },
    tools: async (_context, run) => {
      const attachments = await ports.attachments(run.sessionId);
      if (attachments === null) return null;
      const children = await Promise.all(
        attachments
          .filter((attachment) => attachment.level === "write")
          .map((attachment) => toolsOf(attachment, run)),
      );
      const operations = Object.fromEntries(children.flat());
      if (Object.keys(operations).length === 0) return null;
      return Mycelium.module({
        id: hostedAgentExtensionsId,
        name: "extensions",
        description:
          "Tools of the hosted extensions attached to this Agent, one namespace per App binding.",
        operations,
      });
    },
  });
}
