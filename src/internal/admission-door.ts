import { messageAuthor } from "@fungi.computer/agar/agents";
import type { BirdDogMail } from "@fungi.computer/bird-dog";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import type { AcceptedWork } from "./client-boundary.js";
import type { WithdrawResult } from "../client.js";
import type { Json } from "../extensions.js";
import { checkHostMeta, type HostMetaOptions } from "./host-meta.js";
import { ShiitakeError } from "../errors.js";
import {
  decodeClientAcceptedCommand,
  decodeClientSessionId,
  decodeClientSessionCreationRequestId,
  requireDecoded,
} from "./client-boundary.js";
import type { SqliteOwner } from "./contracts.js";
import { publishOwnerObservation } from "./observation-kernel.js";
import type { AttentionRecipient } from "../session-attention.js";
import {
  AdmissionOriginSchema,
  acceptSessionCommandOwned,
  readSessionCommandAdmissionByRequest,
  withdrawSessionRequestOwned,
  assertSessionRequestNotWithdrawn,
  hasStoredSession,
  hasRecordedRunTerminal,
  readWorkSender,
  recordWorkSender,
  type AdmissionOrigin,
  type EnqueueAcceptedWork,
  type RunContract,
  type SessionQueueHead,
  VerifiedAgentReplySchema,
  type VerifiedAgentReply,
} from "./woodstock/index.js";

export type { AdmissionOrigin } from "./woodstock/index.js";

/**
 * `now` admits immediately (a steer targets the running job). `idle` admits
 * only while the Session queue head is idle and otherwise reports `not_ready`
 * without writing anything; a retried request id still replays.
 */
export type AdmitWhen = "now" | "idle";

/** One prompt or compaction request submitted to the door, still encoded. */
export type AdmitRequest = Readonly<{
  sessionId: string;
  /** Checked atomically before admission; an absent target fails closed. */
  requireExistingSession?: boolean;
  /** Encoded accepted work, decoded once by the public command schema. */
  command: AcceptedWork;
  when: AdmitWhen;
  origin: AdmissionOrigin;
  /** Addressed BirdDog mail whose reply settles with this work. */
  mail?: BirdDogMail;
  /** Host metadata stored beside the prompt; only the in-process host sets it. */
  meta?: Json;
  /**
   * The host's authentication of `mail` as a delegated Agent's reply. Only the
   * host passes it, after checking the reply against the mail it sent.
   */
  reply?: VerifiedAgentReply;
  /** Synchronous owner-local side effect on first admission, in its transaction. */
  onAccepted?: (sessionId: string, commandId: string) => void;
  /**
   * New work that continues earlier work in this Session (for example, the
   * follow-up that delivers an answer) keeps that work's sender.
   */
  continuesWorkId?: string | undefined;
}>;

/** Outcome of one door admission. */
export type AdmitResult =
  | Readonly<{
      status: "accepted";
      commandId: string;
      delivery: "new" | "replay";
    }>
  | Readonly<{ status: "not_ready" }>;

/** Admit inside the door's open owner transaction. Throws `ShiitakeError`. */
export type AdmitWithin = (request: AdmitRequest) => AdmitResult;

/**
 * Where a door transaction runs. `request` commits through the host so the
 * wake guarantee is durable with the mutation; `reconcile` is already inside
 * the host's reconciliation and commits on the owner alone.
 */
export type AdmissionBoundary = "request" | "reconcile";

/** The one accepted-command door. Nothing else writes accepted commands. */
export type AdmissionDoor = Readonly<{
  /** In the same owner transaction as admission, keep the receipt or fence the key. */
  withdraw: (
    sessionId: string,
    requestId: string,
  ) => Effect.Effect<WithdrawResult, ShiitakeError>;
  /** Admit one prompt in its own transaction. */
  admit: (
    request: AdmitRequest,
    boundary?: AdmissionBoundary,
  ) => Effect.Effect<AdmitResult, ShiitakeError>;
  /**
   * Run one owner transaction that may admit through `admit`. After commit the
   * door publishes each Session with new work and requests one wake.
   */
  transact: <A>(
    body: (admit: AdmitWithin) => A,
    boundary?: AdmissionBoundary,
  ) => Effect.Effect<A, ShiitakeError>;
}>;

type DecodedRequest = Readonly<{
  sessionId: string;
  requireExistingSession: boolean;
  command: AcceptedWork;
  origin: AdmissionOrigin;
  when: AdmitWhen;
  mail: BirdDogMail | undefined;
  meta: Json | undefined;
  reply: VerifiedAgentReply | undefined;
  onAccepted: ((sessionId: string, commandId: string) => void) | undefined;
  continuesWorkId: string | undefined;
}>;

const decodeOrigin = Schema.decodeUnknownResult(AdmissionOriginSchema, {
  onExcessProperty: "error",
});
const decodeExistingSessionCondition = Schema.decodeUnknownResult(
  Schema.UndefinedOr(Schema.Boolean),
);

const decodeVerifiedReply = Schema.decodeUnknownResult(
  VerifiedAgentReplySchema,
  { onExcessProperty: "error" },
);

/** A verified reply names the mail it arrived on and that mail's sender. */
function decodeReply(request: AdmitRequest): VerifiedAgentReply | undefined {
  if (request.reply === undefined) return undefined;
  const reply = requireDecoded(
    decodeVerifiedReply(request.reply),
    "Verified Agent reply is invalid",
  );
  const mail = request.mail;
  const agentAuthored =
    mail?.from.kind === "agent" &&
    mail.from.agentId === reply.agentId &&
    mail.from.sessionId === reply.sessionId;
  const systemUndeliverable =
    mail?.from.kind === "system" &&
    reply.outcome === "undeliverable" &&
    reply.reason === "recipient_offline";
  if (
    mail?.replyTo.length !== 0 ||
    (!agentAuthored && !systemUndeliverable) ||
    request.command.type !== "prompt" ||
    request.command.delivery !== "steer"
  )
    throw new ShiitakeError(
      "contract",
      "invalid",
      "Verified Agent reply does not match its mail",
    );
  return reply;
}

/**
 * The Agent Session whose work this admission is: the sender of delegated
 * mail that expects a reply, or the sender of the earlier work this one
 * continues. Replies and every other admission have none.
 */
function workSender(
  owner: SqliteOwner,
  request: DecodedRequest,
): AttentionRecipient | undefined {
  const mail = request.mail;
  if (mail?.from.kind === "agent" && mail.replyTo.length > 0)
    return {
      agentId: mail.from.agentId,
      sessionId: mail.from.sessionId,
      // The host-authenticated author names the sender; mail.meta is the sender's own claim.
      name: messageAuthor(request.meta)?.name ?? mail.from.agentId,
    };
  return request.continuesWorkId === undefined
    ? undefined
    : readWorkSender(owner, request.sessionId, request.continuesWorkId);
}

/** Bind the door to one owner, run contract, Watchdog queue and host wake. */
export function openAdmissionDoor(
  input: Readonly<{
    owner: SqliteOwner;
    /** The host's metadata schema, checked before any write. */
    hostMeta?: HostMetaOptions | undefined;
    /** Refuses a new prompt under storage pressure; replays skip it. */
    admitNew: () => void;
    /** The run contract captured at this instant (participants may change). */
    contract: () => RunContract;
    enqueue: (
      work: Parameters<EnqueueAcceptedWork>[0],
      mail?: BirdDogMail,
    ) => void;
    /** Transactional queue-head projection for the steer and idle decisions. */
    readQueueHead: (sessionId: string) => SessionQueueHead;
    commit: <A>(
      mutation: Effect.Effect<A, ShiitakeError>,
    ) => Effect.Effect<A, ShiitakeError>;
    requestWake: Effect.Effect<void, ShiitakeError>;
  }>,
): AdmissionDoor {
  /** The door's one decode, done before any owner operation when possible. */
  const decode = (request: AdmitRequest): DecodedRequest => ({
    sessionId: decodeClientSessionId(request.sessionId),
    requireExistingSession:
      requireDecoded(
        decodeExistingSessionCondition(request.requireExistingSession),
        "Session admission condition is invalid",
      ) ?? false,
    command: requireDecoded(
      decodeClientAcceptedCommand(request.command),
      "Session command is invalid",
    ),
    origin: requireDecoded(
      decodeOrigin(request.origin),
      "Session command origin is invalid",
    ),
    when: request.when,
    mail: request.mail,
    reply: decodeReply(request),
    onAccepted: request.onAccepted,
    continuesWorkId: request.continuesWorkId,
    meta:
      request.meta === undefined
        ? undefined
        : checkHostMeta(input.hostMeta, request.meta),
  });

  const admitDecoded = (
    request: DecodedRequest,
    touched: Map<string, boolean>,
  ): AdmitResult => {
    const { sessionId, origin } = request;
    const command = request.command;
    assertSessionRequestNotWithdrawn(input.owner, sessionId, command.requestId);
    if (
      request.requireExistingSession &&
      !hasStoredSession(input.owner, sessionId)
    )
      throw new ShiitakeError(
        "contract",
        "closed",
        "Session target is unavailable",
      );
    const head = input.readQueueHead(sessionId);
    if (
      request.when === "idle" &&
      head.state !== "idle" &&
      readSessionCommandAdmissionByRequest(
        input.owner,
        sessionId,
        command.requestId,
      ) === undefined
    )
      return Object.freeze({ status: "not_ready" });
    // Storage pressure refuses new prompts. A compaction request adds no
    // prompt and frees model context, so it is admitted.
    if (
      command.type === "prompt" &&
      readSessionCommandAdmissionByRequest(
        input.owner,
        sessionId,
        command.requestId,
      ) === undefined
    )
      input.admitNew();
    // Only a steering prompt targets the running job; a compaction request
    // always queues behind running work.
    const targetWorkId =
      command.type === "prompt" &&
      command.delivery === "steer" &&
      head.state === "running" &&
      !head.cancellationRequested &&
      !hasRecordedRunTerminal(input.owner, sessionId, head.jobId)
        ? head.jobId
        : undefined;
    const accepted = acceptSessionCommandOwned(
      input.owner,
      sessionId,
      command,
      request.meta,
      origin,
      input.contract(),
      targetWorkId === undefined
        ? (work) => {
            input.enqueue(work, request.mail);
          }
        : undefined,
      targetWorkId,
      request.reply,
    );
    if (accepted.delivery === "new" && targetWorkId === undefined) {
      const sender = workSender(input.owner, request);
      if (sender !== undefined)
        recordWorkSender(
          input.owner,
          sessionId,
          accepted.accepted.commandId,
          sender,
        );
    }
    // The mail notice is staged in the same transaction as the first command.
    // Replaying that admission must not stage a new notice with a different
    // createdAt under the original Post key.
    if (accepted.delivery === "new")
      request.onAccepted?.(sessionId, accepted.accepted.commandId);
    touched.set(
      sessionId,
      (touched.get(sessionId) ?? false) || accepted.delivery === "new",
    );
    return Object.freeze({
      status: "accepted",
      commandId: accepted.accepted.commandId,
      delivery: accepted.delivery,
    });
  };

  const run = <A>(
    body: (admit: (request: DecodedRequest) => AdmitResult) => A,
    boundary: AdmissionBoundary,
  ): Effect.Effect<A, ShiitakeError> =>
    Effect.suspend(() => {
      const touched = new Map<string, boolean>();
      const mutation = Effect.try({
        try: () => {
          touched.clear();
          return input.owner.transactionSync(() =>
            body((request) => admitDecoded(request, touched)),
          );
        },
        catch: (error) =>
          error instanceof ShiitakeError
            ? error
            : new ShiitakeError(
                "store",
                "unavailable",
                "Session command admission is unavailable",
              ),
      });
      return (boundary === "request" ? input.commit(mutation) : mutation).pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            for (const [sessionId, isNew] of touched)
              if (isNew) publishOwnerObservation(input.owner, sessionId);
          }),
        ),
        Effect.tap(() => (touched.size > 0 ? input.requestWake : Effect.void)),
      );
    });

  const transact = <A>(
    body: (admit: AdmitWithin) => A,
    boundary: AdmissionBoundary = "request",
  ): Effect.Effect<A, ShiitakeError> =>
    run((admit) => body((request) => admit(decode(request))), boundary);

  return Object.freeze({
    transact,
    withdraw: (sessionId, requestId) =>
      Effect.try({
        try: () => ({
          sessionId: decodeClientSessionId(sessionId),
          requestId: decodeClientSessionCreationRequestId({ requestId }),
        }),
        catch: (error) =>
          error instanceof ShiitakeError
            ? error
            : new ShiitakeError(
                "contract",
                "invalid",
                "Withdrawal request is invalid",
              ),
      }).pipe(
        Effect.flatMap((decoded) =>
          run(
            () =>
              withdrawSessionRequestOwned(
                input.owner,
                decoded.sessionId,
                decoded.requestId,
              ),
            "request",
          ),
        ),
      ),
    admit: (request, boundary) =>
      Effect.try({
        try: () => decode(request),
        catch: (error) =>
          error instanceof ShiitakeError
            ? error
            : new ShiitakeError(
                "contract",
                "invalid",
                "Session command is invalid",
              ),
      }).pipe(
        Effect.flatMap((decoded) =>
          run((admit) => admit(decoded), boundary ?? "request"),
        ),
      ),
  });
}
