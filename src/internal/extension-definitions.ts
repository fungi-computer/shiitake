import type {
  AnyExtensionDefinitionV0,
  RosteredExtensionV0,
} from "../extensions.js";

/** The typed reader remains usable across two loaded copies of Shiitake. */
export const definitionReader = Symbol.for(
  "@fungi.computer/shiitake/extension-definition-reader",
);

/** Read the definition captured when the author registered this roster entry. */
export function readDefinition(
  rostered: RosteredExtensionV0,
): AnyExtensionDefinitionV0 | undefined {
  return rostered[definitionReader]?.();
}
