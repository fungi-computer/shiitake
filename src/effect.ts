import type { CommandSettlement } from "./internal/contracts.js";
import type { Json as HostJson } from "./extensions.js";
import * as Effect from "effect/Effect";
import type { BirdDogMail } from "@fungi.computer/bird-dog";
import type { VerifiedAgentReply } from "./internal/woodstock/index.js";
import type { BirdDogEffect } from "@fungi.computer/bird-dog/effect";

import type { Accepted } from "./client.js";
import type { ShiitakeError } from "./errors.js";
import type {
  ShiitakeEffectOptions,
  ShiitakeEffectOwner,
} from "./internal/effect-contracts.js";
import type { SqliteOwner } from "./internal/contracts.js";
import { readSessionFactPage } from "./internal/woodstock/index.js";

export type {
  AbortCommand,
  BranchCommand,
  CompactCommand,
  CancelCommand,
  CancelResult,
  Accepted,
  ForkCommand,
  ForkedSession,
  PromptCommand,
  PromptDelivery,
  ReadOptions,
  SessionCommand,
  SessionCommandHandler,
  SessionFrame,
  SessionMessage,
  SessionProgress,
  SessionToolOutput,
  SessionSnapshot,
  SessionActivity,
  AgentRunningWork,
  SubscribeOptions,
  TooLate,
  WatchOptions,
} from "./client.js";
export type {
  InstructionSnapshot,
  InstructionSource,
  ModelAcquisition,
  ModelDescription,
  ModelSource,
  RuntimeHost,
  RuntimePendingWork,
  SqliteCursor,
  SqliteOwner,
  SqliteValue,
  SqliteRow,
  GrantsDelivery,
  GrantsDeliveryResult,
  ShiitakeOptions,
  ToolAccess,
  HostMetaOptions,
  StandardHostMetaResult,
  StandardHostMetaSchema,
  ToolSource,
} from "./internal/contracts.js";
export type {
  InstructionSourceEffect,
  ModelAcquisitionEffect,
  ModelSourceEffect,
  RuntimeHostEffect,
  ShiitakeEffectAgent,
  ShiitakeEffectAgentExtensions,
  ShiitakeEffectCommandHandler,
  ShiitakeEffectOptions,
  ShiitakeEffectOwner,
  ShiitakeEffectSession,
  ShiitakeEffectSessionExtension,
  ShiitakeEffectSendOptions,
  ShiitakeEffectWatch,
  ToolSourceEffect,
} from "./internal/effect-contracts.js";
export type { SessionSummary } from "./client.js";
export type {
  BorrowedCompactionViewV0,
  CompactionHandoffErrorV0,
  CompactionHandoffResultV0,
  CompactionHandoffV0,
  ExtensionCapabilityV0,
  ExtensionGrantV0,
  ExtensionRosterV0,
  ExtensionSlotV0,
  ExtensionStatusV0,
  RosteredExtensionV0,
} from "./extensions.js";
export { ShiitakeError } from "./errors.js";

/** Construct the canonical Effect-native Shiitake scaffold. */
export function make(
  options: ShiitakeEffectOptions,
): Effect.Effect<ShiitakeEffectOwner, ShiitakeError> {
  return Effect.promise(() => import("./internal/runtime-core.js")).pipe(
    Effect.flatMap(({ openRuntimeCore }) => openRuntimeCore(options)),
    Effect.map(({ agent }) => agent),
  );
}

/** Effect-native host composition with addressed mail kept off Session. */
export type ShiitakeEffectHost = Readonly<{
  /** Exact settled command facts; available only to the composing host. */
  readSettlement: (
    sessionId: string,
    commandId: string,
  ) => Effect.Effect<CommandSettlement | undefined, ShiitakeError>;
  agent: ShiitakeEffectOwner;
  /** Durably admit one addressed mail item through the host owner. */
  acceptMail: (
    mail: BirdDogMail,
    meta?: HostJson,
    reply?: VerifiedAgentReply,
    stageAcceptedNotice?: (mail: BirdDogMail) => void,
  ) => Effect.Effect<Accepted, ShiitakeError>;
}>;

/** Open the Effect-native host composition with addressed mail. */
export function openHost(
  options: ShiitakeEffectOptions,
  mailRoom: BirdDogEffect,
): Effect.Effect<ShiitakeEffectHost, ShiitakeError> {
  return Effect.promise(() => import("./internal/runtime-core.js")).pipe(
    Effect.flatMap(({ openRuntimeCore }) => openRuntimeCore(options, mailRoom)),
  );
}

export type { ModelPreference } from "./internal/model-preference.js";

export type { SessionSummaryPage, SessionSummaryRow } from "./client.js";
export type {
  StoredSessionSummary,
  StoredSessionSummaryPage,
  StoredSessionSummaryRow,
} from "./internal/woodstock/index.js";

/**
 * Enumerate saved conversations' stored facts in bounded pages, for readers
 * that hold only the store. Run status is not stored; use the Agent's
 * `listSessions` for it. An unreadable Session is its own `unavailable` row.
 */
export function readSessionSummaries(owner: SqliteOwner, after?: string) {
  return readSessionFactPage(owner, after);
}

export { sqliteRowSchema } from "./internal/contracts.js";

export type { CommandSettlement } from "./internal/contracts.js";
