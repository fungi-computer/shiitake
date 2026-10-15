import type { ExtensionRosterV0 } from "@fungi.computer/shiitake/extensions";
import { bookmarks } from "./extension.js";

/** The host's decision: this roster entry, and exactly these capabilities. */
export const bookmarkRoster: ExtensionRosterV0 = {
  roster: [bookmarks],
  grants: [
    {
      id: bookmarks.manifest.id,
      capabilities: ["state", "prompt", "commands", "run_tools", "projection"],
    },
  ],
};
