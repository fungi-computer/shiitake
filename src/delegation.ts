/**
 * Shiitake's Agent delegation composition for hosts: fleet correspondence,
 * addressed Agent mail over Carrier Post, and the Agent grammar a host hands
 * to Oyster's machine link as ports. Oyster core never imports this engine.
 */
import {
  commandSettlementSchema,
  createAgentDirectory,
  createFleetCorrespondence,
  createFleetDelegation,
  agentDelegationRequestKey,
  delegationProvenance,
  delegationReplyMeta,
  delegationSendMeta,
  settlementReadableBy,
  type AgentDelegationTask,
  type AgentGrant,
} from "./fleet.js";
import type { WatchdogSqliteOwner } from "@fungi.computer/watchdog";
import * as Schema from "effect/Schema";
import { agentMailAddressSchema } from "@fungi.computer/seam/machine-link-protocol";
import {
  BirdDogMailSchema,
  BirdDogError,
  settlementReplies,
  type BirdDogEffect,
} from "@fungi.computer/bird-dog/effect";
import * as Effect from "effect/Effect";
import { messageAuthor } from "@fungi.computer/agar/agents";
import { undeliverableReply, type BirdDogMail } from "@fungi.computer/bird-dog";
import {
  PostEnvelopeSchema,
  definePostRoute,
  type PostRoute,
  type PostRuntime,
  type PostEnvelope,
  type PostDeliveryResult,
} from "@fungi.computer/carrier/post";
import {
  type Accepted,
  parseDelegationReceipt,
  type DelegationReceipt,
  type CancelResult,
} from "./client.js";
import { decodeClientSessionCreationRequest } from "./internal/client-boundary.js";
import type {
  CommandSettlement,
  ShiitakeHost,
  VerifiedAgentReply,
} from "./index.js";
import type {
  AgentControlDirectory,
  AgentDelegationInput,
  AgentDelegationInvocation,
  AgentRecallInput,
} from "./agent-control.js";

/** Shiitake host-pack identity; core consumes OysterAgents, never this engine. */
export type DelegationIdentity = Readonly<{ scopeId: string; agentId: string }>;

type AdmittedAgentMail = Readonly<{
  mail: BirdDogMail;
  reply: VerifiedAgentReply | undefined;
}>;

type AddressedAgentMail = Readonly<{
  envelope: PostEnvelope;
  address: Readonly<{ scopeId: string; agentId: string }>;
  mail: BirdDogMail;
}>;

/** Host-owned identity and admission around BirdDog's ordinary envelope. */
export class AgentDelegation {
  constructor(
    private readonly deps: Readonly<{
      storage: WatchdogSqliteOwner;
      expiresAt: () => number;
      identity: () =>
        DelegationIdentity | null | Promise<DelegationIdentity | null>;
      directory: () => AgentControlDirectory;
      deliver: (mail: BirdDogMail) => Promise<DelegationReceipt>;
      prepare: (mail: BirdDogMail) => Promise<void>;
      /**
       * Synchronously stage the accepted notice in this recipient's durable
       * outbox, in the same transaction as acceptance.
       */
      stageAcceptedNotice: (mail: BirdDogMail, scopeId: string) => void;
      /** Admit mail; `reply` is present only for an authenticated reply. */
      accept: (
        mail: BirdDogMail,
        reply: VerifiedAgentReply | undefined,
        stageAcceptedNotice: (mail: BirdDogMail) => void,
      ) => Promise<Accepted>;
    }>,
  ) {}

  private fleetOwner: ReturnType<typeof createFleetCorrespondence> | undefined;
  private fleet() {
    return (this.fleetOwner ??= createFleetCorrespondence({
      storage: this.deps.storage,
      open: (agentId) =>
        this.deps.directory().open(agentId, new AbortController().signal),
      deliver: this.deps.deliver,
    }));
  }

  /** Best-effort remote refresh before an owner-local archive transaction.
   * Confirmed settlements are recorded by Shiitake; unreadable targets leave
   * their local accepted receipt outstanding for busySync to refuse.
   */
  async refreshOutstanding(
    readSettlement: (
      agentId: string,
      sessionId: string,
      commandId: string,
    ) => Promise<CommandSettlement | undefined>,
  ): Promise<void> {
    const fleet = createFleetDelegation({
      storage: this.deps.storage,
      deliver: this.deps.deliver,
      open: (agentId) =>
        this.deps.directory().open(agentId, new AbortController().signal),
      readSettlement,
    });
    await fleet.outstanding();
  }

  private async scope(signal: AbortSignal) {
    const agents = await this.deps.directory().list(signal);
    const identity = await this.deps.identity();
    if (identity === null) throw new Error("Agent scope is unavailable");
    const source = agents.find(({ agentId }) => agentId === identity.agentId);
    if (source === undefined) throw new Error("Agent is outside this scope");
    return { ...identity, agents, source };
  }

  /** Recipient-local disclosure through the public host, never another Agent's RPC. */
  async settlement(
    sender: DelegationIdentity,
    sessionId: string,
    commandId: string,
    host: Pick<ShiitakeHost, "readSettlement">,
  ): Promise<CommandSettlement | undefined> {
    const scope = await this.scope(new AbortController().signal);
    if (
      sender.scopeId !== scope.scopeId ||
      !scope.agents.some(({ agentId }) => agentId === sender.agentId)
    )
      throw new AgentDisclosureRefused();
    const facts = await host.readSettlement(sessionId, commandId);
    if (facts !== undefined && !settlementReadableBy(facts, sender.agentId))
      throw new AgentDisclosureRefused();
    return facts;
  }

  /** Delegate work to an in-scope Agent through fleet correspondence; a steer rides the mail. */
  async send(
    input: AgentDelegationInput,
    sourceSessionId: string,
    signal: AbortSignal,
    invocation?: AgentDelegationInvocation,
  ): Promise<DelegationReceipt> {
    if (invocation === undefined)
      throw new Error("Delegation requires a tool invocation");
    const scope = await this.scope(signal);
    if (!scope.agents.some(({ agentId }) => agentId === input.agentId))
      throw new Error("Agent is outside this scope");
    const requestId = await agentDelegationRequestKey({
      owner: {
        kind: "hosted",
        scopeId: scope.scopeId,
        agentId: scope.agentId,
      },
      sourceSessionId,
      targetAgentId: input.agentId,
      requestId: input.requestId,
    });
    signal.throwIfAborted();
    const provenance = await delegationProvenance({
      agentId: scope.agentId,
      sessionId: sourceSessionId,
    });
    const meta = delegationSendMeta(
      { id: scope.agentId, name: scope.source.name },
      invocation.meta,
    );
    signal.throwIfAborted();
    let task: AgentDelegationTask = {
      agentId: input.agentId,
      sessionId: input.sessionId,
      delivery: input.delivery,
      text: input.text,
      requestId,
    };
    if (input.delivery === "followUp" && input.goal !== undefined)
      task = { ...task, goal: input.goal };
    const accepted = await this.fleet().start(
      {
        agents: scope.agents.map(({ agentId }) => agentId),
        actions: ["delegate"],
        expiresAt: this.deps.expiresAt(),
        provenance,
        meta,
      },
      task,
    );
    return accepted;
  }

  /** Resolve this sender's exact request, then let the recipient guard pending work. */
  async recall(
    input: AgentRecallInput,
    sourceSessionId: string,
    signal: AbortSignal,
  ): Promise<CancelResult> {
    const scope = await this.scope(signal);
    if (!scope.agents.some(({ agentId }) => agentId === input.agentId))
      throw new Error("Agent is outside this scope");
    const requestId = await agentDelegationRequestKey({
      owner: {
        kind: "hosted",
        scopeId: scope.scopeId,
        agentId: scope.agentId,
      },
      sourceSessionId,
      targetAgentId: input.agentId,
      requestId: input.requestId,
    });
    signal.throwIfAborted();
    const opened = await this.deps.directory().open(input.agentId, signal);
    try {
      const receipt = this.fleet().receipt(requestId);
      if (
        receipt?.sessionId !== undefined &&
        receipt.sessionId !== input.sessionId
      )
        return { status: "unknown" };
      const session = opened.agent.session(
        receipt?.sessionId ?? input.sessionId,
      );
      const admitted = receipt ?? (await session.withdraw(requestId));
      return admitted.status === "accepted"
        ? await session.cancel({
            commandId: admitted.commandId,
            onlyIfPending: true,
          })
        : admitted;
    } finally {
      await opened.release();
    }
  }

  /**
   * A reply-less envelope is only ever BirdDog's reply to work this Agent
   * sent. Admit it only after the fleet authenticates it against that frozen
   * mail. A recipient-left reply remains Agent-authored; a recipient-offline
   * reply is Hub-authored. Its meta carries only the authenticated reply.
   */
  private async authenticatedReply(mail: BirdDogMail) {
    const delegation = this.fleet().authenticateReply(mail);
    if (delegation === undefined) return undefined;
    const scope = await this.scope(new AbortController().signal);
    const replier = scope.agents.find(
      ({ agentId }) => agentId === delegation.reply.agentId,
    );
    const undeliverable = delegation.reply.outcome === "undeliverable";
    if (replier === undefined && !undeliverable) return undefined;
    const meta = delegationReplyMeta(
      delegation,
      replier?.name ?? delegation.reply.agentId,
    );
    return { mail: { ...mail, meta }, reply: delegation.reply };
  }

  private parseAddressedMail(
    raw: PostEnvelope,
  ): AddressedAgentMail | undefined {
    const envelope = Schema.decodeUnknownOption(PostEnvelopeSchema)(raw);
    if (envelope._tag === "None" || envelope.value.kind !== "agentMail")
      return undefined;
    const address = Schema.decodeUnknownOption(agentMailAddressSchema)(
      envelope.value.address,
    );
    const mail = Schema.decodeUnknownOption(BirdDogMailSchema)(
      envelope.value.event,
    );
    if (address._tag === "None" || mail._tag === "None") return undefined;
    return {
      envelope: envelope.value,
      address: address.value,
      mail: mail.value,
    };
  }

  /** Admit one addressed Agent mail for this Agent, or refuse it. */
  async receive(raw: PostEnvelope): Promise<PostDeliveryResult> {
    const addressed = this.parseAddressedMail(raw);
    if (addressed === undefined) return "refused";
    const { envelope, address, mail } = addressed;
    let authenticated: Awaited<ReturnType<typeof this.authenticatedReply>>;
    const valid = async () => {
      const scope = await this.scope(new AbortController().signal);
      const sender = mail.from;
      const authenticatedUndeliverable =
        authenticated?.reply.outcome === "undeliverable";
      return (
        address.scopeId === scope.scopeId &&
        address.agentId === scope.agentId &&
        mail.to.agentId === scope.agentId &&
        envelope.key === mail.key &&
        (authenticatedUndeliverable ||
          (sender.kind === "agent" &&
            scope.agents.some(({ agentId }) => agentId === sender.agentId))) &&
        mail.replyTo.every(({ agentId }) =>
          scope.agents.some((agent) => agent.agentId === agentId),
        ) &&
        (mail.replyTo.length === 0 ||
          (mail.replyTo.length === 1 &&
            sender.kind === "agent" &&
            mail.replyTo[0]?.agentId === sender.agentId &&
            mail.replyTo[0].sessionId === sender.sessionId))
      );
    };
    if (mail.replyTo.length === 0)
      authenticated = await this.authenticatedReply(mail);
    if (!(await valid())) return "refused";
    if (mail.reply?.outcome === "accepted")
      return this.recordAcceptedNotice(mail, valid);
    const admitted =
      mail.replyTo.length > 0 ? { mail, reply: undefined } : authenticated;
    if (admitted === undefined) return "refused";
    return this.acceptAdmitted(admitted, address.scopeId, valid);
  }

  private async recordAcceptedNotice(
    mail: BirdDogMail,
    valid: () => Promise<boolean>,
  ): Promise<PostDeliveryResult> {
    if (mail.to.sessionId === undefined) return "refused";
    await this.deps.prepare(mail);
    if (!(await valid())) return "refused";
    const receipt = this.fleet().recordAcceptedNotice(mail);
    return receipt === undefined ? "refused" : { status: "accepted", receipt };
  }

  private async acceptAdmitted(
    admitted: AdmittedAgentMail,
    scopeId: string,
    valid: () => Promise<boolean>,
  ): Promise<PostDeliveryResult> {
    await this.deps.prepare(admitted.mail);
    if (!(await valid())) return "refused";
    const stageAcceptedNotice = (notice: BirdDogMail) => {
      this.deps.stageAcceptedNotice(notice, scopeId);
    };
    const receipt = await this.deps.accept(
      admitted.mail,
      admitted.reply,
      stageAcceptedNotice,
    );
    if (
      admitted.reply?.outcome === "undeliverable" &&
      admitted.mail.from.kind === "system"
    ) {
      this.fleet().recordUndeliverableReply(
        admitted.mail.key.slice(0, -":reply:0".length),
      );
    }
    const sessionId = admitted.mail.to.sessionId ?? receipt.sessionId;
    return {
      status: "accepted",
      receipt: sessionId === undefined ? receipt : { ...receipt, sessionId },
    };
  }

  /** Admit local outbox exhaustion through the same Fleet/system reply gate. */
  async localUndeliverable(original: BirdDogMail): Promise<PostDeliveryResult> {
    const identity = await this.deps.identity();
    if (identity === null) return "refused";
    let reply: BirdDogMail;
    try {
      reply = undeliverableReply(original, Date.now());
    } catch {
      return "refused";
    }
    if (!this.fleet().recordLocalUndeliverable(original, reply))
      return "refused";
    return await this.receive({
      key: reply.key,
      kind: "agentMail",
      address: { scopeId: identity.scopeId, agentId: identity.agentId },
      event: reply,
    });
  }
}

/** A current correspondent no longer has disclosure authority. */
export class AgentDisclosureRefused extends Error {
  constructor() {
    super("Agent is not an admitted original correspondent");
  }
}

/** Undefined is an explicit disclosure refusal; missing facts and read errors must fail. */
export type AgentReplySettlement = (
  mail: BirdDogMail,
) => Promise<CommandSettlement | undefined>;

/** Current disclosure admission runs at every transport attempt, including frozen retries. */
export function agentMailRoute(
  namespace: PostRoute<never, never>["namespace"],
  replySettlement: AgentReplySettlement,
) {
  return definePostRoute({
    addressKind: "agent",
    address: agentMailAddressSchema,
    event: BirdDogMailSchema,
    name: ({ agentId }) => agentId,
    namespace: {
      getByName: (name) => ({
        deliver: async (envelope) => {
          const mail = Schema.decodeUnknownSync(BirdDogMailSchema)(
            envelope.event,
          );
          if (
            mail.reply !== undefined &&
            mail.reply.outcome !== "accepted" &&
            mail.reply.outcome !== "undeliverable" &&
            mail.to.sessionId === undefined
          )
            return "refused";
          if (
            mail.reply !== undefined &&
            mail.reply.outcome !== "undeliverable" &&
            mail.reply.outcome !== "accepted" &&
            (await replySettlement(mail)) === undefined
          )
            return "refused";
          return namespace.getByName(name).deliver(envelope);
        },
      }),
    },
  });
}

/** The host's one Post lane, as Agent mail borrows it. */
export type AgentMailPost = Pick<
  PostRuntime<{ agentMail: ReturnType<typeof agentMailRoute> }>,
  "send" | "read" | "nextWake" | "deadLetters"
>;

/** Recipient-owned replies and receipt-bearing submissions over the host's one Post lane. */
export class AgentMail {
  constructor(
    private readonly deps: Readonly<{
      storage: WatchdogSqliteOwner;
      post: () => AgentMailPost;
      identity: () =>
        DelegationIdentity | null | Promise<DelegationIdentity | null>;
      /** Only a typed disclosure refusal becomes undefined; missing settled facts must fail. */
      replySettlement: AgentReplySettlement;
      /** Route a terminal reply for this sender through its Fleet admission. */
      onUndeliverable: (
        original: BirdDogMail,
        reply: BirdDogMail,
      ) => Promise<PostDeliveryResult>;
      drain: () => Promise<void>;
      wake: () => Promise<void>;
    }>,
  ) {}

  private async envelope(mail: BirdDogMail) {
    const identity = await this.deps.identity();
    if (identity === null) throw new Error("Agent scope is unavailable");
    return {
      key: mail.key,
      kind: "agentMail" as const,
      address: { scopeId: identity.scopeId, agentId: mail.to.agentId },
      event: mail,
    };
  }

  /** Submit one mail and return recipient acceptance or relay-held custody. */
  async send(mail: BirdDogMail): Promise<DelegationReceipt> {
    this.deps.post().send(await this.envelope(mail));
    // Borrow the existing Post lane once, not a new waiter, loop or supervisor.
    await this.deps.drain();
    const row = this.deps.post().read(mail.key);
    if (row?.state === "delivered" && row.receipt !== undefined)
      return parseDelegationReceipt(JSON.stringify(row.receipt));
    if (row?.state === "dead") throw new Error(`Agent delivery ${row.reason}`);
    throw new Error("Agent did not return a delegated work receipt");
  }

  /** Reconcile exhausted original outbox mail into one local system reply. */
  async stageUndeliverableReplies(): Promise<void> {
    const identity = await this.deps.identity();
    if (identity === null) return;
    const post = this.deps.post();
    let cursor: string | undefined;
    do {
      const page = post.deadLetters(64, cursor);
      for (const letter of page.letters) {
        if (
          letter.reason !== "exhausted" ||
          letter.envelope.kind !== "agentMail"
        )
          continue;
        const envelope = Schema.decodeUnknownOption(PostEnvelopeSchema)(
          letter.envelope,
        );
        if (envelope._tag === "None" || envelope.value.kind !== "agentMail")
          continue;
        const original = Schema.decodeUnknownOption(BirdDogMailSchema)(
          envelope.value.event,
        );
        const address = Schema.decodeUnknownOption(agentMailAddressSchema)(
          envelope.value.address,
        );
        if (
          original._tag === "None" ||
          address._tag === "None" ||
          original.value.key !== envelope.value.key ||
          original.value.reply !== undefined ||
          original.value.from.kind !== "agent" ||
          original.value.from.agentId !== identity.agentId ||
          address.value.scopeId !== identity.scopeId ||
          address.value.agentId !== original.value.to.agentId
        )
          continue;
        const reply = undeliverableReply(original.value, Date.now());
        await this.deps.onUndeliverable(original.value, reply);
      }
      cursor = page.nextCursor;
    } while (cursor !== undefined);
  }

  /** Stage the acceptance reply through this Post owner without leaving the transaction. */
  stageAcceptedNotice(mail: BirdDogMail, scopeId: string): void {
    this.deps.post().send({
      key: mail.key,
      kind: "agentMail",
      address: { scopeId, agentId: mail.to.agentId },
      event: mail,
    });
  }

  /** BirdDog's mail room: disclosed settlement replies fan out over Post. */
  mailRoom(): BirdDogEffect & {
    /** Stage acceptance through this Post owner within the caller's transaction. */
    stageAcceptedNotice: (mail: BirdDogMail, scopeId: string) => void;
  } {
    return {
      fanOut: (settlement) =>
        Effect.tryPromise({
          try: async () => {
            const author = messageAuthor(settlement.meta);
            const replies = settlementReplies(
              settlement.job.outcome === "cancelled"
                ? {
                    ...settlement,
                    content:
                      author === undefined
                        ? "Work cancelled."
                        : `Work cancelled by ${author.name}.`,
                  }
                : settlement,
              Date.now(),
            );
            const admitted: Parameters<AgentMailPost["send"]>[0][] = [];
            for (const mail of replies) {
              const facts = await this.deps.replySettlement(mail);
              if (facts === undefined) continue;
              admitted.push(
                await this.envelope({
                  ...mail,
                  content:
                    facts.terminal.outcome === "cancelled"
                      ? mail.content
                      : facts.content || mail.content,
                }),
              );
            }
            this.deps.storage.transactionSync(() => {
              for (const envelope of admitted) {
                if (this.deps.post().read(envelope.key) === undefined)
                  this.deps.post().send(envelope);
              }
            });
            if (admitted.length > 0) await this.deps.wake();
          },
          catch: (cause) =>
            new BirdDogError("Agent reply fan-out is unavailable", { cause }),
        }),
      postMail: (mail) =>
        Effect.tryPromise({
          try: async () => {
            this.deps.post().send(await this.envelope(mail));
            await this.deps.wake();
          },
          catch: (cause) =>
            new BirdDogError("Agent mail staging is unavailable", { cause }),
        }),
      stageAcceptedNotice: (mail, scopeId) => {
        this.stageAcceptedNotice(mail, scopeId);
      },
      drainOutbox: () => Effect.void,
      nextWakeAt: () =>
        Effect.try({
          try: () => this.deps.post().nextWake() ?? undefined,
          catch: (cause) =>
            new BirdDogError("Agent reply wake is unavailable", { cause }),
        }),
    };
  }
}

/**
 * The Agent wire decoders a host lends Oyster's machine-link client, so the
 * device link routes Agent work without owning Shiitake's grammar.
 */
export const machineAgentCodecs = {
  /** The Session-creation intent, or null when the body is not one. */
  sessionCreation(body: Schema.Json): Readonly<{ requestId: string }> | null {
    const decoded = decodeClientSessionCreationRequest(body);
    return decoded._tag === "Failure" ? null : decoded.success;
  },
  /** One serialized settlement from a hosted or machine-homed Agent. */
  settlement(encoded: string): CommandSettlement {
    return Schema.decodeUnknownSync(
      Schema.fromJsonString(commandSettlementSchema),
    )(encoded);
  },
  /** Throws unless an acknowledgment carries a valid acceptance or Held result. */
  accepted(receipt: Schema.Json): void {
    parseDelegationReceipt(JSON.stringify(receipt));
  },
} as const;

/**
 * Machine-homed Agent identities in the ordinary directory over the
 * enrollment's own store: one Team identity per enrollment receipt, reserved
 * without opening a hosted Agent. The enrolling member is the grant actor.
 */
export function machineAgentReservations(sql: WatchdogSqliteOwner["sql"]) {
  const directory = createAgentDirectory(sql);
  return (
    reservation: Readonly<{
      memberId: string;
      requestId: string;
      name: string;
    }>,
  ) => {
    const grant: AgentGrant = {
      agents: "all",
      actions: ["create", "read"],
      expiresAt: Number.MAX_SAFE_INTEGER,
      provenance: { actor: reservation.memberId },
    };
    const { agent } = directory.create(grant, {
      requestId: reservation.requestId,
      name: reservation.name,
      configuration: "machine",
    });
    return { agentId: agent.agentId };
  };
}
