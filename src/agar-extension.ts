import type { RosteredExtensionV0 } from "./extensions.js";
import {
  agentExtensionMethodNames,
  hostAgentExtension,
  hostedCallInputSchema,
  hostedCallOutputSchema,
  hostedPromptInputSchema,
  hostedPromptOutputSchema,
  hostedToolsInputSchema,
  hostedToolsOutputSchema,
} from "./hosted.js";
import {
  defineAppMethod,
  type AppBackendMethod,
} from "@fungi.computer/agar/backend-definition";

/**
 * Serve one Shiitake extension from this App backend. Spread the result into
 * `defineAppBackend({ methods })`; the build writes the three methods into the
 * Release's API, which is how a Release declares itself an Agent extension.
 * An extension requesting a capability hosting lacks fails here, at build.
 */
export function agentExtensionMethods<Context = void>(
  extension: RosteredExtensionV0,
): Readonly<Record<string, AppBackendMethod<Context>>> {
  const hosted = hostAgentExtension(extension);
  const id = hosted.manifest.id;
  return Object.freeze({
    [agentExtensionMethodNames.prompt]: defineAppMethod<
      typeof hostedPromptInputSchema,
      typeof hostedPromptOutputSchema,
      Context
    >({
      description: `Prompt sections of the ${id} Agent extension.`,
      effect: "read",
      requires: [],
      input: hostedPromptInputSchema,
      output: hostedPromptOutputSchema,
      handler: (input) => hosted.prompt(input),
    }),
    [agentExtensionMethodNames.tools]: defineAppMethod<
      typeof hostedToolsInputSchema,
      typeof hostedToolsOutputSchema,
      Context
    >({
      description: `Tools of the ${id} Agent extension for one run.`,
      effect: "read",
      requires: [],
      input: hostedToolsInputSchema,
      output: hostedToolsOutputSchema,
      handler: (input) => hosted.tools(input),
    }),
    [agentExtensionMethodNames.call]: defineAppMethod<
      typeof hostedCallInputSchema,
      typeof hostedCallOutputSchema,
      Context
    >({
      description: `Run one tool of the ${id} Agent extension.`,
      effect: "write",
      requires: [],
      input: hostedCallInputSchema,
      output: hostedCallOutputSchema,
      handler: (input) => hosted.call(input),
    }),
  });
}
