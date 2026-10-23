/**
 * Shiitake's Agent delegation composition for hosts: fleet correspondence,
 * addressed Agent mail over Carrier Post, and the Agent grammar a host hands
 * to Oyster's machine link as ports. Oyster core never imports this engine.
 */
import {
  commandSettlementSchema,
  createAgentDirectory,
  createFleetCorrespondence,
  delegationProvenance,
  delegationReplyMeta,
  delegationSendMeta,
  type AgentGrant,
} from "./fleet.js";
import type { WatchdogSqliteOwner } from "@fungi.computer/watchdog";
import { deterministicReplayKey } from "@fungi.computer/cairn";
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
import type { BirdDogMail } from "@fungi.computer/bird-dog";
import {
  PostEnvelopeSchema,
  definePostRoute,
  type PostRoute,
  type PostRuntime,
  type PostEnvelope,
  type PostDeliveryResult,
} from "@fungi.computer/carrier/post";
import { parseAccepted, type Accepted, type CancelResult } from "./client.js";
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

/** Host-owned identity and admission around BirdDog's ordinary envelope. */
export class AgentDelegation {
  constructor(
    private readonly deps: Readonly<{
      storage: WatchdogSqliteOwner;
      expiresAt: () => number;
      identity: () =>
        DelegationIdentity | null | Promise<DelegationIdentity | null>;
      directory: () => AgentControlDirectory;
      deliver: (mail: BirdDogMail) => Promise<Accepted>;
      prepare: (mail: BirdDogMail) => Promise<void>;
      /** Admit mail; `reply` is present only for an authenticated reply. */
      accept: (
        mail: BirdDogMail,
        reply?: VerifiedAgentReply,
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

  private async scope(signal: AbortSignal) {
    const agents = await this.deps.directory().list(signal);
    const identity = await this.deps.identity();
    if (identity === null) throw new Error("Agent scope is unavailable");
    const source = agents.find(({ agentId }) => agentId === identity.agentId);
    if (source === undefined) throw new Error("Agent is outside this scope");
    return { ...identity, agents, source };
  }

  private requestKey(
    scope: DelegationIdentity,
    input: AgentRecallInput,
    sourceSessionId: string,
  ) {
    return deterministicReplayKey("oyster-agent-delegation", [
      scope.scopeId,
      scope.agentId,
      sourceSessionId,
      input.agentId,
      input.sessionId,
      input.requestId,
    ]);
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
    if (facts !== undefined && facts.acceptedSender !== sender.agentId)
      throw new AgentDisclosureRefused();
    return facts;
  }

  /** Delegate work to an in-scope Agent: steer its Session, or start fleet correspondence. */
  async send(
    input: AgentDelegationInput,
    sourceSessionId: string,
    signal: AbortSignal,
    invocation?: AgentDelegationInvocation,
  ): Promise<Accepted> {
    if (invocation === undefined)
      throw new Error("Delegation requires a tool invocation");
    const scope = await this.scope(signal);
    if (!scope.agents.some(({ agentId }) => agentId === input.agentId))
      throw new Error("Agent is outside this scope");
    const requestId = await this.requestKey(scope, input, sourceSessionId);
    signal.throwIfAborted();
    if (input.delivery === "steer") {
      const opened = await this.deps.directory().open(input.agentId, signal);
      try {
        return await opened.agent.session(input.sessionId).send(input.text, {
          requestId,
          delivery: "steer",
        });
      } finally {
        await opened.release();
      }
    }
    const provenance = await delegationProvenance({
      agentId: scope.agentId,
      sessionId: sourceSessionId,
    });
    const meta = delegationSendMeta(
      { id: scope.agentId, name: scope.source.name },
      invocation.meta,
    );
    signal.throwIfAborted();
    const accepted = await this.fleet().start(
      {
        agents: scope.agents.map(({ agentId }) => agentId),
        actions: ["delegate"],
        expiresAt: this.deps.expiresAt(),
        provenance,
        meta,
      },
      {
        agentId: input.agentId,
        sessionId: input.sessionId,
        text: input.text,
        requestId,
      },
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
    const requestId = await this.requestKey(scope, input, sourceSessionId);
    signal.throwIfAborted();
    const opened = await this.deps.directory().open(input.agentId, signal);
    try {
      const session = opened.agent.session(input.sessionId);
      const admitted =
        this.fleet().receipt(requestId) ?? (await session.withdraw(requestId));
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
   * mail; the line is then authored by the replying Agent, never by whoever
   * settled or cancelled the work, and its meta carries the verified reply.
   */
  private async authenticatedReply(mail: BirdDogMail) {
    const delegation = this.fleet().authenticateReply(mail);
    if (delegation === undefined) return undefined;
    const scope = await this.scope(new AbortController().signal);
    const replier = scope.agents.find(
      ({ agentId }) => agentId === delegation.reply.agentId,
    );
    if (replier === undefined) return undefined;
    const meta = delegationReplyMeta(delegation, replier.name);
    return { mail: { ...mail, meta }, reply: delegation.reply };
  }

  /** Admit one addressed Agent mail for this Agent, or refuse it. */
  async receive(raw: PostEnvelope): Promise<PostDeliveryResult> {
    const envelope = Schema.decodeUnknownOption(PostEnvelopeSchema)(raw);
    if (envelope._tag === "None" || envelope.value.kind !== "agentMail")
      return "refused";
    const address = Schema.decodeUnknownOption(agentMailAddressSchema)(
      envelope.value.address,
    );
    const parsed = Schema.decodeUnknownOption(BirdDogMailSchema)(
      envelope.value.event,
    );
    if (address._tag === "None" || parsed._tag === "None") return "refused";
    const mail = parsed.value;
    const valid = async () => {
      const scope = await this.scope(new AbortController().signal);
      return (
        address.value.scopeId === scope.scopeId &&
        address.value.agentId === scope.agentId &&
        mail.to.agentId === scope.agentId &&
        envelope.value.key === mail.key &&
        [mail.from, ...mail.replyTo].every((ref) =>
          scope.agents.some(({ agentId }) => agentId === ref.agentId),
        ) &&
        (mail.replyTo.length === 0 ||
          (mail.replyTo.length === 1 &&
            mail.replyTo[0]?.agentId === mail.from.agentId &&
            mail.replyTo[0].sessionId === mail.from.sessionId))
      );
    };
    if (!(await valid())) return "refused";
    const admitted =
      mail.replyTo.length > 0
        ? { mail, reply: undefined }
        : await this.authenticatedReply(mail);
    if (admitted === undefined) return "refused";
    await this.deps.prepare(admitted.mail);
    if (!(await valid())) return "refused";
    return {
      status: "accepted",
      receipt: await this.deps.accept(admitted.mail, admitted.reply),
    };
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
  "send" | "read" | "nextWake"
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

  /** Submit one mail and return the recipient's Accepted receipt. */
  async send(mail: BirdDogMail): Promise<Accepted> {
    this.deps.post().send(await this.envelope(mail));
    // Borrow the existing Post lane once, not a new waiter, loop or supervisor.
    await this.deps.drain();
    const row = this.deps.post().read(mail.key);
    if (row?.state === "delivered" && row.receipt !== undefined)
      return parseAccepted(JSON.stringify(row.receipt));
    if (row?.state === "dead") throw new Error(`Agent delivery ${row.reason}`);
    throw new Error("Agent did not return a delegated work receipt");
  }

  /** BirdDog's mail room: disclosed settlement replies fan out over Post. */
  mailRoom(): BirdDogEffect {
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
  /** Throws unless an acknowledgment carries a well-formed Accepted receipt. */
  accepted(receipt: Schema.Json): void {
    parseAccepted(JSON.stringify(receipt));
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
