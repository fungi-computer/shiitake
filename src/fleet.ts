export { commandSettlementSchema } from "./client-wire.js";
import {
  createFleetDirectoryStore,
  createFleetDelegationStore,
  readSessionDelegator,
  type VerifiedAgentReply,
} from "./internal/woodstock/index.js";
import type { CommandSettlement } from "./internal/contracts.js";
import { pending, type Pending } from "@fungi.computer/agar";
import type { BirdDogMail } from "@fungi.computer/bird-dog";
import { BirdDogMailSchema } from "@fungi.computer/bird-dog/effect";
import * as Schema from "effect/Schema";
import {
  parseAccepted,
  parseDelegationReceipt,
  type Accepted,
  type DelegationReceipt,
  type Held,
  type ShiitakeAgent,
} from "./client.js";
import type { Json } from "./extensions.js";
import { checkHostMeta } from "./internal/host-meta.js";
import type { WatchdogSqliteOwner } from "@fungi.computer/watchdog";
import { defineIdFamily, type CairnId } from "@fungi.computer/cairn/id";
import { deterministicReplayKey } from "@fungi.computer/cairn";
import { z } from "zod";
import {
  authorMeta,
  type AuthorMeta,
  FungiMessageMetaSchema,
} from "@fungi.computer/agar/agents";

/** Inputs shared by Local and hosted delegation request identity. */
export type AgentDelegationRequestKeyInput = Readonly<{
  owner:
    | Readonly<{ kind: "local"; agentId: string }>
    | Readonly<{ kind: "hosted"; scopeId: string; agentId: string }>;
  sourceSessionId: string;
  targetAgentId: string;
  requestId: string;
}>;

/**
 * One idempotency identity for a caller-owned delegation request.
 * The target Session is deliberately excluded: a sessionless send learns it
 * only after acceptance, when recall must still resolve the original receipt.
 */
export async function agentDelegationRequestKey(
  input: AgentDelegationRequestKeyInput,
): Promise<string> {
  const ownerIdentity =
    input.owner.kind === "hosted"
      ? [input.owner.scopeId, input.owner.agentId]
      : [input.owner.agentId];
  return deterministicReplayKey("agent-delegation-request", [
    input.owner.kind,
    ...ownerIdentity,
    input.sourceSessionId,
    input.targetAgentId,
    input.requestId,
  ]);
}

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
export const agentProvenanceSchema = z.lazy(() =>
  z.strictObject({
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
  }),
);
/** Fleet authority is supplied, never looked up in host policy. */
export const agentGrantSchema = z.lazy(() =>
  z.strictObject({
    agents: z.union([z.literal("all"), z.array(identity)]),
    actions: z.array(agentActionSchema).max(5),
    expiresAt: z.number().int().nonnegative(),
    provenance: agentProvenanceSchema,
    meta: z.json().optional(),
    /** Delegation abuse bound; inherited paths may only narrow it. */
    maxDepth: z.number().int().min(1).max(8).optional(),
  }),
);
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
      | "agent"
      | "action"
      | "expired"
      | "delegation_loop"
      | "delegation_depth"
      | "delegator_missing",
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

const agentRowSchema = z.lazy(() =>
  z.strictObject({
    agent_id: z.string(),
    name: z.string(),
    actor_user_id: z.string(),
    created_by: z.string().min(1).max(256).nullable(),
    created_at: z.number().int().nonnegative(),
    updated_at: z.number().int().nonnegative(),
  }),
);

const receiptRowSchema = z.lazy(() =>
  z.strictObject({
    request_id: z.string(),
    actor_user_id: z.string(),
    name: z.string(),
    agent_id: z.string(),
    computer_mode: z.string().min(1).max(4096),
  }),
);

/** Stable replay input; configuration is an opaque host creation choice. */
export const agentCreationSchema = z.lazy(() =>
  z.strictObject({
    name: z.string().min(1).max(256),
    requestId: z.string().min(1).max(256),
    configuration: z.string().min(1).max(4096),
    createdBy: z.string().min(1).max(256).nullable().optional(),
  }),
);
/** One idempotent directory reservation. */
export type AgentCreation = z.infer<typeof agentCreationSchema>;

/** Owner-only rename input: an Agent's new display name. */
export const agentRenameSchema = z.lazy(() =>
  z.strictObject({
    name: z.string().min(1).max(256),
  }),
);
/** Input to {@link createAgentDirectory}'s `rename`. */
export type AgentRename = z.infer<typeof agentRenameSchema>;

/** One durable Agent directory record. */
export type DirectoryAgent = Readonly<{
  agentId: AgentId;
  /** The ordinary Agent that created this Agent; never the credential owner. */
  createdBy: string | null;
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
    createdBy: parsed.data.created_by,
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
      if (existing.createdBy !== (command.createdBy ?? null))
        throw new DirectoryError(409, "request_conflict");
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
      command.createdBy ?? null,
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

/**
 * Whether an Agent may read one command's settlement: the Agent whose mail the
 * command answered, or the Session's delegator of record for work no Agent's
 * mail admitted (goal, loop and person turns, the work its reports carry).
 * Another Agent's correspondence stays with that Agent.
 */
export function settlementReadableBy(
  facts: Pick<CommandSettlement, "acceptedSender" | "delegator">,
  agentId: string,
): boolean {
  return (
    facts.acceptedSender === agentId ||
    (facts.acceptedSender === null && facts.delegator === agentId)
  );
}

/** Replay identity and work, with the parent supplied exclusively by the grant. */
export const agentDelegationTaskSchema = z.lazy(() =>
  z
    .strictObject({
      requestId: identity,
      agentId: identity,
      sessionId: identity.optional(),
      /** A live steer into the recipient's running turn; queued follow-up when omitted. */
      delivery: z.enum(["followUp", "steer"]).optional(),
      /** Only a new-Session follow-up carries goal intent. */
      goal: z
        .union([z.string().min(1).max(4_000), z.literal(false)])
        .optional(),
      text: z
        .string()
        .min(1)
        .max(256 * 1024),
    })
    .refine(
      ({ goal, sessionId, delivery }) =>
        goal === undefined || (sessionId === undefined && delivery !== "steer"),
      { path: ["goal"], error: "goal requires a new-Session follow-up" },
    ),
);
/** A caller reuses this task unchanged while polling Pending. */
export type AgentDelegationTask = z.infer<typeof agentDelegationTaskSchema>;
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
  deliver: (mail: BirdDogMail) => Promise<DelegationReceipt>;
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
  author: AuthorMeta["author"];
  delegation?: { ancestors: string[]; maxDepth: number };
};

/** The host metadata on one authenticated reply line, as JSON. */
export type DelegationReplyMeta = Readonly<{
  author: AuthorMeta["author"];
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
  sender: Readonly<{ id: string; name: string }>,
  invocationMeta?: Json,
): DelegationSendMeta {
  const author = authorMeta({ ...sender, kind: "agent" });
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
 * The host metadata for one authenticated reply line. An ordinary reply is
 * authored by its replying Agent; a delivery failure is a Hub/system fact,
 * while the verified reply retains the failed recipient's identity.
 */
export function delegationReplyMeta(
  delegation: AuthenticatedDelegationReply,
  replierName: string,
): DelegationReplyMeta {
  const author =
    delegation.reply.outcome === "undeliverable" &&
    delegation.reply.reason === "recipient_offline"
      ? authorMeta({ id: "hub/system", name: "Hub/system" })
      : authorMeta({
          id: delegation.reply.agentId,
          name: replierName,
          kind: "agent",
        });
  return {
    ...author,
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

/**
 * Shape check for a one-way ancestor notice, not sender provenance. The Local
 * mail receiver checks mesh membership before calling it. This addressed mail
 * is still work and may steer the parent's running turn when admitted.
 */
export function isOneWayAncestorNotice(mail: BirdDogMail): boolean {
  if (
    mail.reply !== undefined ||
    mail.replyTo.length !== 0 ||
    mail.from.kind !== "agent" ||
    mail.to.sessionId === undefined
  )
    return false;
  const parsed = delegationMetadata.safeParse(mail.meta);
  return (
    parsed.success &&
    parsed.data.delegation !== undefined &&
    parsed.data.delegation.ancestors.includes(mail.to.agentId) &&
    parsed.data.delegation.ancestors.at(-1) === mail.from.agentId
  );
}

const delegationRowSchema = z.lazy(() =>
  z.strictObject({
    input: z.string(),
    mail: z.string(),
    state: z.enum(["pending", "held", "accepted", "undeliverable"]),
    receipt: z.string().nullable(),
  }),
);
const heldRecipientOffline: Held = {
  status: "held",
  reason: "recipient_offline",
  message: "waiting: recipient offline",
};
const decodeMail = Schema.decodeUnknownSync(
  Schema.fromJsonString(BirdDogMailSchema),
);

function bindRecipientSession(mail: BirdDogMail, sessionId: string) {
  if (mail.from.kind !== "agent") throw new DirectoryError(503, "partial_init");
  return {
    ...mail,
    to: { ...mail.to, sessionId },
  } satisfies BirdDogMail;
}

/**
 * Delegated work an owner still waits on; `unreadable` says why its state
 * could not be read, when it could not.
 */
export type OutstandingDelegation = Readonly<{
  agentId: string;
  sessionId: string;
  commandId: string;
  unreadable?: string;
}>;

/** Read confirmed sender receipts that have no locally observed settlement.
 * Call inside the Agent owner's transaction that also changes archived state.
 * Remote reads happen separately; an unobserved settlement remains busy.
 */
export function readOutstandingDelegationSync(
  storage: WatchdogSqliteOwner,
): OutstandingDelegation | undefined {
  const raw = createFleetDelegationStore(storage).unsettled()[0];
  if (raw === undefined) return undefined;
  const row = delegationRowSchema.parse(raw);
  const mail = decodeMail(row.mail);
  const sessionId = mail.to.sessionId;
  if (sessionId === undefined) throw new DirectoryError(503, "partial_init");
  return {
    agentId: mail.to.agentId,
    sessionId,
    commandId: parseAccepted(z.string().parse(row.receipt)).commandId,
  };
}

/**
 * BirdDog submission and reply authentication. The durable row freezes
 * correspondence before transport; an ambiguous delivery reuses that exact mail.
 * Shiitake owns work deduplication, execution and settlement; Post owns replies.
 */
export function createFleetCorrespondence(
  options: Omit<FleetDelegationOptions, "readSettlement">,
) {
  const store = createFleetDelegationStore(options.storage);
  function read(requestId: string) {
    const row = store.read(requestId);
    return row === undefined ? undefined : delegationRowSchema.parse(row);
  }
  async function start(
    grant: AgentGrant,
    raw: AgentDelegationTask,
  ): Promise<DelegationReceipt> {
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
    // A report to an ancestor is one-way: the parent handles the work but
    // must not send its settlement back as a new prompt to the worker.
    if (parent.agentId === task.agentId)
      throw new AgentGrantError("delegation_loop");
    if (ancestors.length >= maxDepth)
      throw new AgentGrantError("delegation_depth");
    const path = { ancestors: [...ancestors, parent.agentId], maxDepth };
    const delegation = {
      ...path,
      ...(task.delivery === "steer" && { steer: true }),
      ...(task.goal !== undefined && { goal: task.goal }),
    } satisfies Json;
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
      const from = { kind: "agent" as const, ...parent };
      const ancestor = ancestors.includes(task.agentId);
      // A sessionless report to a parent goes to the creator's recorded
      // Session. Never create a fresh Session merely to carry a notice.
      const targetSessionId =
        task.sessionId ??
        (ancestor
          ? readSessionDelegator(
              options.storage,
              parent.sessionId,
            )?.replyTo.find(({ agentId }) => agentId === task.agentId)
              ?.sessionId
          : undefined);
      if (ancestor && targetSessionId === undefined)
        throw new AgentGrantError("delegator_missing");
      let mail: BirdDogMail = {
        key: task.requestId,
        from,
        to:
          targetSessionId === undefined
            ? {
                kind: "agent",
                agentId: task.agentId,
              }
            : {
                kind: "agent",
                agentId: task.agentId,
                sessionId: targetSessionId,
              },
        replyTo: ancestor ? [] : [from],
        content: task.text,
        createdAt: Date.now(),
      };
      mail = { ...mail, meta: outgoingMeta };
      store.reserve(task.requestId, input, JSON.stringify(mail));
      row = read(task.requestId);
    }
    if (row === undefined) throw new DirectoryError(503, "partial_init");
    if (row.input !== input) throw new DirectoryError(409, "request_conflict");
    if (row.state === "held" || row.state === "undeliverable")
      return heldRecipientOffline;
    if (row.state === "accepted") {
      if (row.receipt === null) throw new DirectoryError(503, "partial_init");
      return parseAccepted(row.receipt);
    }
    assertAgentGrant(grant, "delegate", task.agentId);
    const receipt = parseDelegationReceipt(
      JSON.stringify(await options.deliver(decodeMail(row.mail))),
    );
    if (receipt.status === "held") store.recordHeld(task.requestId);
    else {
      const sent = decodeMail(row.mail);
      const sessionId = receipt.sessionId ?? sent.to.sessionId;
      if (sessionId === undefined)
        throw new DirectoryError(503, "partial_init");
      const accepted: Accepted = { ...receipt, sessionId };
      store.recordReceipt(
        task.requestId,
        JSON.stringify(accepted),
        row.mail,
        JSON.stringify(bindRecipientSession(sent, sessionId)),
      );
      return accepted;
    }
    return receipt;
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
    // A report names the idle Session's delegator mail and the settled command.
    if (
      mail.replyTo.length !== 0 ||
      mail.reply === undefined ||
      mail.reply.outcome === "accepted"
    )
      return undefined;
    const reportAt = mail.key.lastIndexOf(":report:");
    // A report's key names the command it settles.
    const isReport =
      reportAt >= 0 &&
      "commandId" in mail.reply &&
      mail.key.slice(reportAt) === `:report:${mail.reply.commandId}:0`;
    const mailKey = isReport
      ? mail.key.slice(0, reportAt)
      : mail.key.endsWith(":reply:0")
        ? mail.key.slice(0, -":reply:0".length)
        : undefined;
    if (mailKey === undefined) return undefined;
    const row = read(mailKey);
    if (row === undefined || row.state === "undeliverable") return undefined;
    const sent = decodeMail(row.mail);
    if (sent.key !== mailKey) return undefined;
    const systemUndeliverable =
      mail.reply.outcome === "undeliverable" &&
      mail.reply.reason === "recipient_offline" &&
      mail.from.kind === "system";
    if (systemUndeliverable) {
      if (
        row.state !== "held" ||
        sent.from.kind !== "agent" ||
        mail.key !== `${sent.key}:reply:0` ||
        mail.to.agentId !== sent.from.agentId ||
        mail.to.sessionId !== sent.from.sessionId
      )
        return undefined;
    } else {
      if (
        sent.from.kind !== "agent" ||
        mail.from.kind !== "agent" ||
        mail.from.agentId !== sent.to.agentId ||
        mail.from.sessionId !== sent.to.sessionId ||
        mail.to.agentId !== sent.from.agentId ||
        mail.to.sessionId !== sent.from.sessionId
      )
        return undefined;
    }
    if (
      mail.reply.outcome === "undeliverable" &&
      mail.reply.reason === "recipient_offline" &&
      !systemUndeliverable
    )
      return undefined;
    if (
      !isReport &&
      mail.reply.outcome !== "undeliverable" &&
      row.state === "accepted" &&
      row.receipt !== null
    ) {
      const sentResult = parseAccepted(row.receipt);
      if (sentResult.commandId !== mail.reply.commandId) return undefined;
    }
    const path = delegationMetadata.parse(sent.meta).delegation;
    if (path === undefined) return undefined;
    let reply: VerifiedAgentReply;
    if (mail.reply.outcome === "undeliverable") {
      const agentId = systemUndeliverable
        ? sent.to.agentId
        : mail.from.kind === "agent"
          ? mail.from.agentId
          : undefined;
      if (agentId === undefined) return undefined;
      const undeliverable = { agentId, ...mail.reply };
      const sessionId = systemUndeliverable
        ? sent.to.sessionId
        : mail.from.kind === "agent"
          ? mail.from.sessionId
          : undefined;
      reply =
        sessionId === undefined
          ? undeliverable
          : { ...undeliverable, sessionId };
    } else {
      if (mail.from.kind !== "agent") return undefined;
      const sessionId = mail.from.sessionId;
      reply = { agentId: mail.from.agentId, sessionId, ...mail.reply };
    }
    return {
      ancestors: path.ancestors.slice(0, -1),
      maxDepth: path.maxDepth,
      reply,
    };
  }
  /** Bind an exhausted local Carrier row to its frozen sender correspondence. */
  function recordLocalUndeliverable(
    original: BirdDogMail,
    reply: BirdDogMail,
  ): boolean {
    const row = read(original.key);
    if (
      row === undefined ||
      (row.state !== "pending" && row.state !== "held") ||
      original.from.kind !== "agent" ||
      original.reply !== undefined ||
      JSON.stringify(decodeMail(row.mail)) !== JSON.stringify(original) ||
      reply.key !== `${original.key}:reply:0` ||
      reply.from.kind !== "system" ||
      reply.reply?.outcome !== "undeliverable" ||
      reply.reply.reason !== "recipient_offline" ||
      reply.to.agentId !== original.from.agentId ||
      reply.to.sessionId !== original.from.sessionId ||
      reply.replyTo.length !== 0
    )
      return false;
    store.recordHeld(original.key);
    return read(original.key)?.state === "held";
  }
  /** Make a successfully admitted system disposition replay-ineligible. */
  function recordUndeliverableReply(mailKey: string): boolean {
    return store.recordUndeliverable(mailKey);
  }
  /** Read only the existing acceptance receipt; do not reserve or deliver mail. */
  function receipt(requestId: string): Accepted | undefined {
    const row = read(requestId);
    return row?.state !== "accepted" || row.receipt === null
      ? undefined
      : parseAccepted(row.receipt);
  }
  /** Atomically bind the recipient Session carried by a late acceptance notice. */
  function recordAcceptedNotice(mail: BirdDogMail): Accepted | undefined {
    const suffix = ":accepted";
    if (mail.from.kind !== "agent") return undefined;
    const sender = mail.from;
    if (
      mail.replyTo.length !== 0 ||
      mail.reply?.outcome !== "accepted" ||
      !mail.key.endsWith(suffix)
    )
      return undefined;
    const requestId = mail.key.slice(0, -suffix.length);
    const row = read(requestId);
    if (row === undefined) return undefined;
    const sent = decodeMail(row.mail);
    if (sent.from.kind !== "agent") return undefined;
    const sentSender = sent.from;
    const path = delegationMetadata.safeParse(sent.meta);
    if (
      !path.success ||
      path.data.delegation === undefined ||
      mail.key !== `${sent.key}:accepted` ||
      sender.agentId !== sent.to.agentId ||
      (sent.to.sessionId !== undefined &&
        sent.to.sessionId !== sender.sessionId) ||
      mail.to.agentId !== sentSender.agentId ||
      mail.to.sessionId !== sentSender.sessionId
    )
      return undefined;
    if (row.state === "accepted" && row.receipt !== null) {
      const current = parseAccepted(row.receipt);
      if (
        current.commandId !== mail.reply.commandId ||
        current.sessionId !== sender.sessionId
      )
        return undefined;
    }
    const accepted: Accepted = {
      status: "accepted",
      commandId: mail.reply.commandId,
      sessionId: sender.sessionId,
    };
    store.recordReceipt(
      requestId,
      JSON.stringify(accepted),
      row.mail,
      JSON.stringify(bindRecipientSession(sent, sender.sessionId)),
    );
    return accepted;
  }
  return {
    /** Submit frozen mail or reuse its recorded Session admission receipt. */
    start,
    /** Match a reply to frozen mail and restore the caller's delegation path. */
    authenticateReply,
    /** Bind exhausted local delivery to its frozen sender correspondence. */
    recordLocalUndeliverable,
    /** Prevent replay after a system undeliverable reply is admitted. */
    recordUndeliverableReply,
    /** Read an existing acceptance receipt without reserving or delivering mail. */
    receipt,
    /** Consume a recipient's stable `<mailKey>:accepted` notice. */
    recordAcceptedNotice,
  };
}

/** Local hosts may observe exact work directly; remote correspondence never pulls it. */
export function createFleetDelegation(options: FleetDelegationOptions) {
  const correspondence = createFleetCorrespondence(options);
  const store = createFleetDelegationStore(options.storage);
  async function delegate(
    grant: AgentGrant,
    task: AgentDelegationTask,
  ): Promise<Pending | AgentDelegationOutcome> {
    const accepted = await correspondence.start(grant, task);
    const raw = store.read(task.requestId);
    if (raw === undefined) throw new DirectoryError(503, "partial_init");
    const mail = decodeMail(delegationRowSchema.parse(raw).mail);
    if (accepted.status === "held") return pending(1000);
    const sessionId = mail.to.sessionId ?? accepted.sessionId;
    if (sessionId === undefined) throw new DirectoryError(503, "partial_init");
    const settlement = await options.readSettlement(
      mail.to.agentId,
      sessionId,
      accepted.commandId,
    );
    if (settlement !== undefined) store.recordSettled(task.requestId);
    return settlement === undefined
      ? pending(1000)
      : {
          kind: "settled",
          agentId: mail.to.agentId,
          sessionId,
          ...settlement,
        };
  }
  /**
   * The newest delegation this owner sent whose work has not settled at its
   * target, so the owner is still waiting for a reply; undefined when none.
   */
  async function outstanding(): Promise<OutstandingDelegation | undefined> {
    let waiting: OutstandingDelegation | undefined;
    for (const raw of store.unsettled()) {
      const row = delegationRowSchema.parse(raw);
      if (row.receipt === null) continue;
      if (row.state !== "accepted") continue;
      const { to } = decodeMail(row.mail);
      const { commandId } = parseAccepted(row.receipt);
      if (to.sessionId === undefined)
        throw new DirectoryError(503, "partial_init");
      const target = {
        agentId: to.agentId,
        sessionId: to.sessionId,
        commandId,
      };
      const result = await options
        .readSettlement(to.agentId, to.sessionId, commandId)
        .then(
          (settlement) => ({ kind: "read" as const, settlement }),
          (cause: unknown) => ({ kind: "unreadable" as const, cause }),
        );
      if (result.kind === "unreadable") {
        // Work whose state cannot be read is still owed; say why.
        waiting ??= {
          ...target,
          unreadable:
            result.cause instanceof Error ? result.cause.message : "unreadable",
        };
        continue;
      }
      if (result.settlement === undefined) waiting ??= target;
      else store.recordSettled(decodeMail(row.mail).key);
    }
    return waiting;
  }
  return {
    /** The delegated work this owner still waits on, if any. */
    outstanding,
    /** Authenticate a reply against its frozen mail and restore the caller's path. */
    ...correspondence,
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
