import type { CommandSettlement } from "./contracts.js";
import type { Json } from "../extensions.js";
import type { BirdDogMail } from "@fungi.computer/bird-dog";
import type { VerifiedAgentReply } from "./woodstock/index.js";
import type { BirdDogEffect } from "@fungi.computer/bird-dog/effect";
import * as Effect from "effect/Effect";

import type { Accepted } from "../client.js";
import type { GrantsDelivery } from "./contracts.js";
import { ShiitakeError } from "../errors.js";
import { composeAgentClose } from "./agent-close.js";
import { acceptAddressedMail, openCoordinator } from "./coordinator.js";
import type {
  ShiitakeEffectAgent,
  ShiitakeEffectOptions,
  ShiitakeEffectOwner,
} from "./effect-contracts.js";
import {
  admitExtensionRosterV0,
  coreProtocolDigest,
} from "./extension-roster.js";
import { openRuntimeCoordinator } from "./runtime-coordinator.js";
import type { OwnerBusy } from "./runtime-coordinator.js";
import {
  readCommandSettlement,
  prepareWoodstockSchema,
} from "./woodstock/index.js";

/** Private host composition with the addressed-mail ingress kept off Session. */
export type RuntimeCore = Readonly<{
  agent: ShiitakeEffectOwner;
  readSettlement: (
    sessionId: string,
    commandId: string,
  ) => Effect.Effect<CommandSettlement | undefined, ShiitakeError>;
  /** Owner-local execution projection; never served by the remote client. */
  activeSessions: Effect.Effect<readonly string[]>;
  /** Synchronous archive blocker; the host supplies the enclosing transaction. */
  busySync: () => OwnerBusy | undefined;
  acceptMail: (
    mail: BirdDogMail,
    meta?: Json,
    reply?: VerifiedAgentReply,
    stageAcceptedNotice?: (mail: BirdDogMail) => void,
    sessionGrantDelivery?: (sessionId: string) => GrantsDelivery,
  ) => Effect.Effect<Accepted, ShiitakeError>;
}>;

/** Open Runtime Core, optionally joining its owner-local BirdDog mail room. */
export function openRuntimeCore(
  options: ShiitakeEffectOptions,
  mailRoom?: BirdDogEffect,
): Effect.Effect<RuntimeCore, ShiitakeError> {
  return Effect.gen(function* () {
    const admitted = yield* admitExtensionRosterV0(options.extensions);
    const coreDigest = yield* coreProtocolDigest([]);
    yield* prepareWoodstockSchema(options.store);
    const runtime = yield* openRuntimeCoordinator(
      options,
      coreDigest,
      admitted,
      mailRoom,
    );
    const coordinator = openCoordinator(options, runtime);
    const agent: ShiitakeEffectAgent = Object.freeze({
      session: coordinator.session,
      createSession: coordinator.createSession,
      deleteSession: coordinator.deleteSession,
      listSessions: coordinator.listSessions,
      extensions: coordinator.extensions,
      close: composeAgentClose(coordinator.close, runtime.close),
    });
    const owner: ShiitakeEffectOwner = Object.freeze({
      ...agent,
      prepareConfiguration: runtime.prepareConfiguration,
      grants: coordinator.grants,
      withMeta: (meta) =>
        Object.freeze({
          ...agent,
          session: coordinator.withMeta(meta).session,
        }),
    });
    return Object.freeze({
      agent: owner,
      readSettlement: (sessionId, commandId) =>
        runtime.assertOpen.pipe(
          Effect.andThen(
            readCommandSettlement(options.store, sessionId, commandId),
          ),
        ),
      activeSessions: runtime.activeSessions,
      busySync: runtime.busySync,
      acceptMail: (
        mail,
        meta,
        reply,
        stageAcceptedNotice,
        sessionGrantDelivery,
      ) =>
        acceptAddressedMail(
          runtime,
          mail,
          meta,
          reply,
          stageAcceptedNotice ??
            ((notice) => {
              if (mailRoom?.postMail === undefined)
                throw new Error("Recipient Agent mail outbox is unavailable");
              Effect.runSync(mailRoom.postMail(notice));
            }),
          sessionGrantDelivery,
        ),
    });
  });
}
