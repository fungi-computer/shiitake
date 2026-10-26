import {
  SettledBirdDogReplySchema,
  UndeliverableBirdDogReplySchema,
} from "@fungi.computer/bird-dog/effect";
import type { UserMessage } from "@fungi.computer/mule/effect";
import * as Schema from "effect/Schema";

const Identifier = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(256),
);

/**
 * A delegated Agent's reply after the admitting host authenticated it against
 * the mail it sent: the replying Agent and Session plus BirdDog's settlement
 * facts. Only `acceptMail` stores it, and only the host passes it.
 */
export const VerifiedAgentReplySchema = Schema.Union([
  Schema.Struct({
    agentId: Identifier,
    sessionId: Identifier,
    ...SettledBirdDogReplySchema.fields,
  }),
  Schema.Struct({
    agentId: Identifier,
    sessionId: Schema.optionalKey(Identifier),
    ...UndeliverableBirdDogReplySchema.fields,
  }),
]);
/** A delegated Agent's reply after the admitting host authenticated it. */
export type VerifiedAgentReply = typeof VerifiedAgentReplySchema.Type;

/** The tag that marks a host-verified Agent reply in model context. */
export const AGENT_REPLY_TAG = "fungi_agent_reply";

// `<` before the tag name, allowing the spacing and case a model would still read as the tag.
const tagOpening = new RegExp(`<(?=\\s*/?\\s*${AGENT_REPLY_TAG})`, "giu");

/** Make untrusted text unable to open or close the verified-reply tag. */
function neutralize(text: string): string {
  return text.replace(tagOpening, "&lt;");
}

function attribute(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll('"', "&quot;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function opening(reply: VerifiedAgentReply): string {
  const attributes: [string, string][] = [["agent_id", reply.agentId]];
  if (reply.sessionId !== undefined)
    attributes.push(["session_id", reply.sessionId]);
  if (reply.outcome !== "undeliverable")
    attributes.push(["command_id", reply.commandId]);
  attributes.push(["outcome", reply.outcome]);
  if (reply.outcome !== "undeliverable" && reply.diagnostic !== undefined)
    attributes.push(["diagnostic", reply.diagnostic]);
  if (reply.outcome === "undeliverable")
    attributes.push(["reason", reply.reason]);
  const rendered = attributes
    .map(([name, value]) => `${name}="${attribute(value)}"`)
    .join(" ");
  return `<${AGENT_REPLY_TAG} ${rendered}>`;
}

/**
 * What a model sees of one stored user-role line. Every typed, mailed or
 * operator text is neutralized, so only Shiitake can open the verified-reply
 * tag; a line the host admitted with a verified reply is wrapped in it. The
 * provider role stays `user`.
 */
export function userContextMessage(
  message: UserMessage,
  reply?: VerifiedAgentReply,
): UserMessage {
  const content = Array.isArray(message.content)
    ? message.content.map((part) =>
        part.type === "text" ? { ...part, text: neutralize(part.text) } : part,
      )
    : neutralize(message.content);
  if (reply === undefined) return { ...message, content };
  const open = opening(reply);
  const close = `</${AGENT_REPLY_TAG}>`;
  return {
    ...message,
    content: Array.isArray(content)
      ? [
          { type: "text", text: open },
          ...content,
          { type: "text", text: close },
        ]
      : `${open}\n${content}\n${close}`,
  };
}
