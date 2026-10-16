import { SessionTerminalSchema } from "./client-wire.js";
import {
  usage,
  createFleetDirectoryStore,
  createFleetDelegationStore,
  type VerifiedAgentReply,
} from "./internal/woodstock/index.js";
import type { CommandSettlement } from "./internal/contracts.js";
import { pending, type Pending } from "@fungi.computer/agar";
import type { BirdDogMail } from "@fungi.computer/bird-dog";
import { BirdDogMailSchema } from "@fungi.computer/bird-dog/effect";
import * as Schema from "effect/Schema";
import { parseAccepted, type Accepted, type ShiitakeAgent } from "./client.js";
import type { Json } from "./extensions.js";
import { checkHostMeta } from "./internal/host-meta.js";
import type { WatchdogSqliteOwner } from "@fungi.computer/watchdog";
import { defineIdFamily, type CairnId } from "@fungi.computer/cairn/id";
import { z } from "zod";
import {
  authorMeta,
  FungiMessageMetaSchema,
  type FungiAuthor,
} from "@fungi.computer/agar/agents";

const identity = z.string().min(1).max(256);
/** Operations explicitly admitted by the host. */
export const agentActionSchema = z.enum([
  "create",
  "manage",
  "read",
  "send",
  "delegate",
]);
/** One host-admitted fleet operation. */
export type AgentAction = z.infer<typeof agentActionSchema>;
/** Attribution supplied by the admitting host, without Team semantics. */
export const agentProvenanceSchema = z.strictObject({
  actor: identity,
  workflowRunId: identity.optional(),
  /** Host fingerprint of the enclosing model program, frozen with replay input. */
  toolCallInputDigest: z
    .string()
    .regex(/^[a-f0-9]{64}$/)
    .optional(),
  session: z
    .strictObject({ agentId: identity, sessionId: identity })
    .optional(),
});
/** Fleet authority is supplied, never looked up in host policy. */
export const agentGrantSchema = z.strictObject({
  agents: z.union([z.literal("all"), z.array(identity)]),
  actions: z.array(agentActionSchema).max(5),
  expiresAt: z.number().int().nonnegative(),
  provenance: agentProvenanceSchema,
  meta: z.json().optional(),
  /** Delegation abuse bound; inherited paths may only narrow it. */
  maxDepth: z.number().int().min(1).max(8).optional(),
});
/** A trusted host's bounded, addressed authority for fleet operations. */
export type AgentGrant = z.infer<typeof agentGrantSchema>;
/** A committed directory transition, returned in the host's transaction. */
export type AgentLifecycleFact = Readonly<{
  kind: "created" | "deleted";
  agentId: string;
  provenance: AgentGrant["provenance"];
}>;
/** Refusal of supplied authority; no host policy lookup is performed. */
export class AgentGrantError extends Error {
  constructor(
    readonly reason:
      "agent" | "action" | "expired" | "delegation_loop" | "delegation_depth",
  ) {
    super(reason);
  }
}
/** Check every operation against its explicit authority. */
export function assertAgentGrant(
  grant: AgentGrant,
  action: AgentAction,
  agentId?: string,
): void {
  const admitted = agentGrantSchema.parse(grant);
  if (admitted.expiresAt <= Date.now()) throw new AgentGrantError("expired");
  if (!admitted.actions.includes(action)) throw new AgentGrantError("action");
  if (
    admitted.agents !== "all" &&
    (agentId === undefined
      ? action === "create"
      : !admitted.agents.includes(agentId))
  )
    throw new AgentGrantError("agent");
}

/** Stable Agent identities, retained across host compositions. */
export const agentIds = defineIdFamily({
  family: "agent",
  version: 1,
  kinds: { agent: { prefix: "agent" } },
});

/** An ordinary Agent directory identity. */
export type AgentId = CairnId<"agent", "agent">;

const agentRowSchema = z.strictObject({
  agent_id: z.string(),
  name: z.string(),
  actor_user_id: z.string(),
  created_at: z.number().int().nonnegative(),
  updated_at: z.number().int().nonnegative(),
});

const receiptRowSchema = z.strictObject({
  request_id: z.string(),
  actor_user_id: z.string(),
  name: z.string(),
  agent_id: z.string(),
  computer_mode: z.string().min(1).max(4096),
});

/** Stable replay input; configuration is an opaque host creation choice. */
export const agentCreationSchema = z.strictObject({
  name: z.string().min(1).max(256),
  requestId: z.string().min(1).max(256),
  configuration: z.string().min(1).max(4096),
});
/** One idempotent directory reservation. */
export type AgentCreation = z.infer<typeof agentCreationSchema>;

/** Owner-only rename input: an Agent's new display name. */
export const agentRenameSchema = z.strictObject({
  name: z.string().min(1).max(256),
});
/** Input to {@link createAgentDirectory}'s `rename`. */
export type AgentRename = z.infer<typeof agentRenameSchema>;

/** One durable Agent directory record. */
export type DirectoryAgent = Readonly<{
  agentId: AgentId;
  name: string;
  createdAt: number;
  updatedAt: number;
}>;

/** Directory input, replay or storage refusal. */
export class DirectoryError extends Error {
  constructor(
    readonly status: 400 | 403 | 404 | 409 | 503,
    readonly code:
      | "invalid_input"
      | "not_found"
      | "name_taken"
      | "request_conflict"
      | "computer_team_conflict"
      | "partial_init",
  ) {
    super(code);
  }
}

function asDirectoryAgent(row: unknown): DirectoryAgent {
  const parsed = agentRowSchema.safeParse(row);
  if (!parsed.success) throw new DirectoryError(503, "partial_init");
  const agent = agentIds.agent.parse(parsed.data.agent_id);
  if (!agent.ok) throw new DirectoryError(503, "partial_init");
  return Object.freeze({
    agentId: agent.value,
    name: parsed.data.name,
    createdAt: parsed.data.created_at,
    updatedAt: parsed.data.updated_at,
  });
}

/**
 * Agent identities, replay receipts and erasure roster. Mutations belong inside
 * the caller's storage transaction, so lifecycle facts and host projections
 * commit together. Physical table names retain the existing Hub store layout.
 */
export function createAgentDirectory(sql: WatchdogSqliteOwner["sql"]) {
  const store = createFleetDirectoryStore(sql);
  function readStored(agentId: AgentId): DirectoryAgent | null {
    const row = store.read(agentId);
    return row === undefined ? null : asDirectoryAgent(row);
  }

  function findCreation(
    actorUserId: string,
    command: AgentCreation,
  ): DirectoryAgent | null {
    const receipt = store.readReceipt(command.requestId);
    if (receipt !== undefined) {
      const parsedReceipt = receiptRowSchema.safeParse(receipt);
      if (!parsedReceipt.success) throw new DirectoryError(503, "partial_init");
      if (
        parsedReceipt.data.actor_user_id !== actorUserId ||
        parsedReceipt.data.name !== command.name ||
        parsedReceipt.data.computer_mode !== command.configuration
      )
        throw new DirectoryError(409, "request_conflict");
      const agentId = agentIds.agent.parse(parsedReceipt.data.agent_id);
      if (!agentId.ok) throw new DirectoryError(503, "partial_init");
      const existing = readStored(agentId.value);
      if (existing === null) throw new DirectoryError(503, "partial_init");
      return existing;
    }

    return null;
  }

  function readCreation(
    grant: AgentGrant,
    raw: AgentCreation,
  ): DirectoryAgent | null {
    assertAgentGrant(grant, "read");
    const existing = findCreation(
      grant.provenance.actor,
      agentCreationSchema.parse(raw),
    );
    if (existing !== null) assertAgentGrant(grant, "read", existing.agentId);
    return existing;
  }

  function create(grant: AgentGrant, raw: AgentCreation) {
    assertAgentGrant(grant, "create");
    const command = agentCreationSchema.parse(raw);
    const actorUserId = grant.provenance.actor;
    const existing = findCreation(actorUserId, command);
    if (existing !== null) return { agent: existing, fact: null };

    const duplicate = store.findByName(command.name);
    if (duplicate !== undefined) throw new DirectoryError(409, "name_taken");

    const agentId = agentIds.agent.generate();
    const now = Date.now();
    store.reserve(
      agentId,
      command.name,
      actorUserId,
      now,
      command.requestId,
      command.configuration,
    );
    const created = readStored(agentId);
    if (created === null) throw new DirectoryError(503, "partial_init");
    return {
      agent: created,
      fact: {
        kind: "created",
        agentId,
        provenance: grant.provenance,
      } satisfies AgentLifecycleFact,
    };
  }

  function list(grant: AgentGrant): readonly DirectoryAgent[] {
    assertAgentGrant(grant, "read");
    return store
      .list()
      .map(asDirectoryAgent)
      .filter(
        ({ agentId }) =>
          grant.agents === "all" || grant.agents.includes(agentId),
      );
  }

  function read(grant: AgentGrant, agentId: AgentId): DirectoryAgent | null {
    assertAgentGrant(grant, "read", agentId);
    return readStored(agentId);
  }
  /** The user who created an Agent; that user's own credentials follow it. */
  function readCreator(grant: AgentGrant, agentId: string): string | null {
    assertAgentGrant(grant, "read", agentId);
    const raw = store.readCreator(agentId);
    if (raw === undefined) return null;
    const creator = z.string().min(1).max(256).safeParse(raw.actor_user_id);
    if (!creator.success) throw new DirectoryError(503, "partial_init");
    return creator.data;
  }

  function remove(
    grant: AgentGrant,
    agentId: AgentId,
  ): AgentLifecycleFact | null {
    assertAgentGrant(grant, "manage", agentId);
    if (readStored(agentId) === null) return null;
    store.remove(agentId);
    return { kind: "deleted", agentId, provenance: grant.provenance };
  }
  /** Owner-only rename; refuses a name already held by another Agent. */
  function rename(
    grant: AgentGrant,
    agentId: AgentId,
    raw: AgentRename,
  ): DirectoryAgent {
    assertAgentGrant(grant, "manage", agentId);
    const command = agentRenameSchema.parse(raw);
    const existing = readStored(agentId);
    if (existing === null) throw new DirectoryError(404, "not_found");
    if (command.name !== existing.name) {
      const duplicate = store.findByName(command.name);
      if (duplicate !== undefined) throw new DirectoryError(409, "name_taken");
      store.rename(agentId, command.name, Date.now());
    }
    const updated = readStored(agentId);
    if (updated === null) throw new DirectoryError(503, "partial_init");
    return updated;
  }
  return {
    /** Resolve a matching creation receipt before host admission of a new reservation. */
    readCreation,
    /** Reserve an Agent once and return its committed lifecycle fact. */
    create,
    /** Read one admitted Agent identity. */
    read,
    /** List only identities covered by the supplied grant. */
    list,
    /** Remove an identity while retaining its erasure address. */
    remove,
    /** Rename an existing Agent; refuses a name already held by another one. */
    rename,
    /** Read the host actor recorded at creation. */
    readCreator,
    /** Enumerate retained addresses for host erasure. */
    listErasureIds: store.listErasureIds,
    /** Whether the retained erasure roster covers this store. */
    erasureRosterComplete: store.erasureRosterComplete,
  };
}

/** Replay identity and work, with the parent supplied exclusively by the grant. */
export const agentDelegationTaskSchema = z.strictObject({
  requestId: identity,
  agentId: identity,
  sessionId: identity.optional(),
  text: z
    .string()
    .min(1)
    .max(256 * 1024),
});
/** A caller reuses this task unchanged while polling Pending. */
export type AgentDelegationTask = z.infer<typeof agentDelegationTaskSchema>;
/** Exact host facts encoded over internal transports, without transcript content. */
export const commandSettlementSchema: Schema.Codec<CommandSettlement> =
  Schema.Struct({
    terminal: SessionTerminalSchema,
    content: Schema.String,
    usage: Schema.Array(
      Schema.Struct({ provider: Schema.String, model: Schema.String, usage }),
    ),
    toolCalls: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  });
/** Exact settled Session facts, including Pi usage and tool-call records. */
export type AgentDelegationOutcome = Readonly<{
  kind: "settled";
  agentId: string;
  sessionId: string;
}> &
  CommandSettlement;
/** Ordinary runtime and mail transports; these ports do not decide host policy. */
export type FleetDelegationOptions = Readonly<{
  storage: WatchdogSqliteOwner;
  /** Admit the frozen addressed mail through the destination owner. */
  deliver: (mail: BirdDogMail) => Promise<Accepted>;
  /** Read exact work facts at the target owner, independent of transcript selection. */
  readSettlement: (
    agentId: string,
    sessionId: string,
    commandId: string,
  ) => Promise<CommandSettlement | undefined>;
  /** Acquire a Session client only when creating the delegated conversation. */
  open: (agentId: string) => Promise<
    Readonly<{
      agent: ShiitakeAgent;
      /** Release the borrowed transport without stopping its run. */
      release: () => Promise<void>;
    }>
  >;
}>;

/**
 * The delegation meta the host attaches to an authenticated reply: the
 * caller's own path plus the verified reply, whose `agentId` is the reply's
 * author.
 */
export type AuthenticatedDelegationReply = Readonly<{
  ancestors: readonly string[];
  maxDepth: number;
  reply: VerifiedAgentReply;
}>;

/** The host metadata a delegated send carries, as JSON. */
export type DelegationSendMeta = {
  author: FungiAuthor;
  delegation?: { ancestors: string[]; maxDepth: number };
};

/** The host metadata on one authenticated reply line, as JSON. */
export type DelegationReplyMeta = Readonly<{
  author: FungiAuthor;
  delegation: Readonly<{
    ancestors: readonly string[];
    maxDepth: number;
    reply: VerifiedAgentReply;
  }>;
}>;

/**
 * Provenance for one delegated send from the invoking Agent's Session. The
 * model program's digest is frozen with the replay input, so a retried tool
 * call that changed its code conflicts instead of replaying.
 */
export async function delegationProvenance(
  session: Readonly<{ agentId: string; sessionId: string }>,
  toolCallCode?: string,
): Promise<AgentGrant["provenance"]> {
  const provenance: AgentGrant["provenance"] = {
    actor: session.agentId,
    session: { agentId: session.agentId, sessionId: session.sessionId },
  };
  if (toolCallCode === undefined) return provenance;
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(toolCallCode),
  );
  return {
    ...provenance,
    toolCallInputDigest: Array.from(new Uint8Array(digest), (byte) =>
      byte.toString(16).padStart(2, "0"),
    ).join(""),
  };
}

/**
 * The host metadata one delegated send carries: the sending Agent as author
 * and only the path inherited from the invoking line. A reply's verified facts
 * stay on that reply's own line and never travel onward.
 */
export function delegationSendMeta(
  sender: FungiAuthor,
  invocationMeta?: Json,
): DelegationSendMeta {
  const author = authorMeta(sender);
  const inherited =
    invocationMeta === undefined
      ? undefined
      : FungiMessageMetaSchema.parse(invocationMeta).delegation;
  return inherited === undefined
    ? author
    : {
        ...author,
        delegation: {
          ancestors: [...inherited.ancestors],
          maxDepth: inherited.maxDepth,
        },
      };
}

/**
 * The host metadata for one authenticated reply line: the replying Agent is
 * its author, and the verified reply rides beside the caller's own path.
 */
export function delegationReplyMeta(
  delegation: AuthenticatedDelegationReply,
  replierName: string,
): DelegationReplyMeta {
  return {
    ...authorMeta({ id: delegation.reply.agentId, name: replierName }),
    delegation: {
      ancestors: [...delegation.ancestors],
      maxDepth: delegation.maxDepth,
      reply: delegation.reply,
    },
  };
}

const delegationMetadata = z.object({
  delegation: FungiMessageMetaSchema.shape.delegation,
});

const delegationRowSchema = z.strictObject({
  input: z.string(),
  mail: z.string(),
  receipt: z.string().nullable(),
});
const decodeMail = Schema.decodeUnknownSync(
  Schema.fromJsonString(BirdDogMailSchema),
);
const recipientRowSchema = z.strictObject({
  agent_id: identity,
  session_id: identity,
  sent_at: z.number().int().nonnegative(),
  total: z.number().int().nonnegative(),
});

/** One Agent a Session sent work to: its latest addressed conversation. */
export type FleetRecipient = Readonly<{
  agentId: string;
  sessionId: string;
  sentAt: number;
}>;
/** A bounded page of recipients and how many more exist past it. */
export type FleetRecipients = Readonly<{
  recipients: readonly FleetRecipient[];
  more: number;
}>;

/**
 * Hosted BirdDog submission and Pending observation. The durable row freezes
 * correspondence before transport; an ambiguous delivery reuses that exact mail.
 * Shiitake owns work deduplication, execution and settlement; Post owns replies.
 */
export function createFleetDelegation(options: FleetDelegationOptions) {
  const store = createFleetDelegationStore(options.storage);
  function read(requestId: string) {
    const row = store.read(requestId);
    return row === undefined ? undefined : delegationRowSchema.parse(row);
  }
  async function start(
    grant: AgentGrant,
    raw: AgentDelegationTask,
  ): Promise<Accepted> {
    const task = agentDelegationTaskSchema.parse(raw);
    assertAgentGrant(grant, "delegate", task.agentId);
    const parent = grant.provenance.session;
    if (parent === undefined) throw new AgentGrantError("agent");
    assertAgentGrant(grant, "delegate", parent.agentId);
    const meta =
      grant.meta === undefined
        ? undefined
        : checkHostMeta(undefined, grant.meta);
    const objectMeta = z.record(z.string(), z.json()).safeParse(meta);
    const inherited = objectMeta.success
      ? delegationMetadata.parse(objectMeta.data).delegation
      : undefined;
    const ancestors = inherited?.ancestors ?? [];
    const maxDepth = Math.min(grant.maxDepth ?? 8, inherited?.maxDepth ?? 8);
    if (ancestors.includes(task.agentId) || parent.agentId === task.agentId)
      throw new AgentGrantError("delegation_loop");
    if (ancestors.length >= maxDepth)
      throw new AgentGrantError("delegation_depth");
    const delegation = { ancestors: [...ancestors, parent.agentId], maxDepth };
    const outgoingMeta = {
      ...(objectMeta.success
        ? objectMeta.data
        : meta === undefined
          ? {}
          : { hostMeta: meta }),
      delegation,
    };
    checkHostMeta(undefined, outgoingMeta);
    const input = JSON.stringify({
      task,
      provenance: grant.provenance,
      meta,
      maxDepth,
    });
    let row = read(task.requestId);
    if (row === undefined) {
      let sessionId = task.sessionId;
      if (sessionId === undefined) {
        const opened = await options.open(task.agentId);
        try {
          sessionId = await opened.agent.createSession(task.requestId);
        } finally {
          await opened.release();
        }
      }
      const from = { kind: "agent" as const, ...parent };
      let mail: BirdDogMail = {
        key: task.requestId,
        from,
        to: { kind: "agent", agentId: task.agentId, sessionId },
        replyTo: [from],
        content: task.text,
        createdAt: Date.now(),
      };
      mail = { ...mail, meta: outgoingMeta };
      store.reserve(task.requestId, input, JSON.stringify(mail));
      row = read(task.requestId);
    }
    if (row === undefined) throw new DirectoryError(503, "partial_init");
    if (row.input !== input) throw new DirectoryError(409, "request_conflict");
    if (row.receipt !== null) return parseAccepted(row.receipt);
    assertAgentGrant(grant, "delegate", task.agentId);
    const receipt = parseAccepted(
      JSON.stringify(await options.deliver(decodeMail(row.mail))),
    );
    store.recordReceipt(task.requestId, JSON.stringify(receipt));
    return receipt;
  }
  async function delegate(
    grant: AgentGrant,
    task: AgentDelegationTask,
  ): Promise<Pending | AgentDelegationOutcome> {
    const accepted = await start(grant, task);
    const row = read(task.requestId);
    if (row === undefined) throw new DirectoryError(503, "partial_init");
    const mail = decodeMail(row.mail);
    const settlement = await options.readSettlement(
      mail.to.agentId,
      mail.to.sessionId,
      accepted.commandId,
    );
    return settlement === undefined
      ? pending(1000)
      : {
          kind: "settled",
          agentId: mail.to.agentId,
          sessionId: mail.to.sessionId,
          ...settlement,
        };
  }
  /**
   * Authenticate one arriving reply against the frozen mail this owner sent.
   * It must be BirdDog's single reply key for that mail, mirror its sender and
   * recipient exactly, carry BirdDog's settlement facts and, once the send's
   * receipt is recorded, name that receipt's `commandId`. The result restores
   * the caller's own delegation path and the verified reply; it never reads
   * the reply's text or any settlement metadata.
   */
  function authenticateReply(
    mail: BirdDogMail,
  ): AuthenticatedDelegationReply | undefined {
    // This owner sends to one correspondent, so BirdDog's reply index is zero.
    const suffix = ":reply:0";
    if (
      mail.replyTo.length !== 0 ||
      mail.reply === undefined ||
      !mail.key.endsWith(suffix)
    )
      return undefined;
    const row = read(mail.key.slice(0, -suffix.length));
    if (row === undefined) return undefined;
    const sent = decodeMail(row.mail);
    if (
      mail.from.agentId !== sent.to.agentId ||
      mail.from.sessionId !== sent.to.sessionId ||
      mail.to.agentId !== sent.from.agentId ||
      mail.to.sessionId !== sent.from.sessionId
    )
      return undefined;
    if (
      row.receipt !== null &&
      parseAccepted(row.receipt).commandId !== mail.reply.commandId
    )
      return undefined;
    const path = delegationMetadata.parse(sent.meta).delegation;
    if (path === undefined) return undefined;
    const reply: VerifiedAgentReply = {
      agentId: mail.from.agentId,
      sessionId: mail.from.sessionId,
      ...mail.reply,
    };
    return {
      ancestors: path.ancestors.slice(0, -1),
      maxDepth: path.maxDepth,
      reply,
    };
  }
  /**
   * The distinct Agents one parent Session sent work to, newest send first,
   * with how many more exist past `limit`. Read-only over the frozen mail.
   */
  function recipients(
    parent: Readonly<{ agentId: string; sessionId: string }>,
    limit: number,
  ): FleetRecipients {
    const rows = store
      .recipients(parent.agentId, parent.sessionId, limit)
      .map((row) => recipientRowSchema.parse(row));
    return {
      recipients: rows.map((row) => ({
        agentId: row.agent_id,
        sessionId: row.session_id,
        sentAt: row.sent_at,
      })),
      more: Math.max(0, (rows[0]?.total ?? 0) - rows.length),
    };
  }
  return {
    /** Authenticate a reply against its frozen mail and restore the caller's path. */
    authenticateReply,
    /** One grouped read of the Agents a parent Session sent work to. */
    recipients,
    /** Submit once and return the existing Shiitake admission receipt. */
    start,
    /** Submit or poll the exact turn, yielding Agar Pending until settled. */
    delegate,
  };
}

/** Acquire ordinary Session access; an in-process host applies meta with withMeta. */
export type FleetAgentAccess = (
  agentId: string,
  meta?: Json,
) => Promise<
  Readonly<{
    agent: ShiitakeAgent;
    /** Release the borrowed transport without stopping its run. */
    release: () => Promise<void>;
  }>
>;
/** One idempotent prompt, using the Session owner's existing delivery grammar. */
export type FleetMessage = Readonly<{
  requestId: string;
  text: string;
  delivery?: "steer" | "followUp";
}>;
/**
 * Addressed Session operations over the existing Shiitake client. Each operation
 * checks a fresh supplied grant and releases its transport without stopping work.
 */
export function createFleetSessions(open: FleetAgentAccess) {
  async function withAgent<T>(
    grant: AgentGrant,
    action: AgentAction,
    agentId: string,
    run: (agent: ShiitakeAgent) => Promise<T>,
  ): Promise<T> {
    assertAgentGrant(grant, action, agentId);
    const meta =
      grant.meta === undefined
        ? undefined
        : checkHostMeta(undefined, grant.meta);
    const access = await open(agentId, meta);
    try {
      assertAgentGrant(grant, action, agentId);
      return await run(access.agent);
    } finally {
      await access.release();
    }
  }
  return {
    /** Enumerate existing Sessions through the owner's bounded cursor. */
    list: (grant: AgentGrant, agentId: string, after?: string) =>
      withAgent(grant, "read", agentId, (agent) => agent.listSessions(after)),
    /** Open the exact saved conversation without creating work. */
    read: (grant: AgentGrant, agentId: string, sessionId: string) =>
      withAgent(grant, "read", agentId, (agent) =>
        agent.session(sessionId).read(),
      ),
    /** Create a Session once under the caller's replay identity. */
    create: (grant: AgentGrant, agentId: string, requestId: string) =>
      withAgent(grant, "manage", agentId, (agent) =>
        agent.createSession(requestId),
      ),
    /** Remove a Session through its runtime owner. */
    remove: (grant: AgentGrant, agentId: string, sessionId: string) =>
      withAgent(grant, "manage", agentId, (agent) =>
        agent.deleteSession(sessionId),
      ),
    /** Send once; an omitted Session is created under the same replay identity. */
    send: (
      grant: AgentGrant,
      agentId: string,
      sessionId: string | undefined,
      message: FleetMessage,
    ) =>
      withAgent(grant, "send", agentId, async (agent) => {
        const target =
          sessionId ?? (await agent.createSession(message.requestId));
        const accepted = await agent
          .session(target)
          .send(
            message.text,
            message.delivery === undefined
              ? { requestId: message.requestId }
              : { requestId: message.requestId, delivery: message.delivery },
          );
        return { ...accepted, sessionId: target };
      }),
  };
}
