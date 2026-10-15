import {
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import type { SandboxValue } from "@fungi.computer/mycelium";
import { directSandbox } from "@fungi.computer/mycelium/testing";
import { fauxModelSource, memoryHost } from "@fungi.computer/shiitake/testing";
import { bookmarkContract } from "./contract.js";
import { bookmarkRoster } from "./roster.js";

/**
 * The host grants this Session read-only use of Bookmarks' tools, and the
 * model tries to save a link. `bookmarks.add` changes something, so it is
 * refused before it runs; the refusal comes back as the call's result.
 */
export async function readGrantAdd(): Promise<SandboxValue> {
  const results: SandboxValue[] = [];
  const host = await memoryHost({
    extensions: bookmarkRoster,
    model: fauxModelSource([
      fauxAssistantMessage(fauxToolCall("execute", { code: "bookmarks.add" }), {
        stopReason: "toolUse",
      }),
      fauxAssistantMessage("I could not save it."),
    ]),
    tools: {
      // A trusted test sandbox that makes exactly the model's one call.
      sandbox: directSandbox(async (bindings) => {
        const add = bindings.bookmarks?.add;
        if (add === undefined) throw new Error("bookmarks.add is not offered");
        results.push(
          await add({ url: "https://fungi.computer/", title: "Fungi" }),
        );
        return null;
      }),
      // Every Bookmarks tool needs a grant; a Session without one gets none.
      access: { required: [bookmarkContract.id] },
    },
  });
  try {
    // The host's own decision, delivered as versioned data for one Session.
    host.agent.grants.deliver("reading", {
      version: 1,
      grants: { [bookmarkContract.id]: "read" },
    });
    await host.agent.session("reading").send("Save Fungi.", {
      requestId: "save",
    });
    await host.settle("reading");
    return results[0] ?? null;
  } finally {
    await host.close();
  }
}
