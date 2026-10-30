import type { CommandSettlement } from "./internal/contracts.js";
import type { Json as HostJson } from "./extensions.js";
import type { BirdDogMail } from "@fungi.computer/bird-dog";
import type { VerifiedAgentReply } from "./internal/woodstock/index.js";
import type { BirdDogEffect } from "@fungi.computer/bird-dog/effect";
import type { Accepted } from "./client.js";
import { make as makeEffect, openHost as openEffectHost } from "./effect.js";
import type {
  ShiitakeOptions,
  ShiitakeOwnerAgent,
} from "./internal/contracts.js";
import {
  openPromiseAgent,
  openPromiseHost,
} from "./internal/promise-facade.js";

export type {
  AbortCommand,
  AgentExtensionControls,
  BranchCommand,
  CompactCommand,
  CancelCommand,
  CancelResult,
  Accepted,
  DelegationReceipt,
  Held,
  ForkCommand,
  ForkedSession,
  OperatorCommand,
  TitleCommand,
  PromptCommand,
  PromptDelivery,
  SendOptions,
  ReadOptions,
  SessionCommand,
  SessionCommandHandler,
  SessionExtensionControls,
  SessionExtensionSlots,
  SessionFrame,
  SessionMessage,
  SessionProgress,
  SessionProgressDelta,
  SessionToolOutput,
  SessionContext,
  SessionSnapshot,
  SessionInboxState,
  SessionSummary,
  SessionSummaryPage,
  SessionSummaryRow,
  SessionActivity,
  AgentRunningWork,
  SessionTerminal,
  SessionCompactionMarker,
  SessionCompactionNotice,
  SessionCompactionTrigger,
  ShiitakeEventSourceConstructor,
  SessionWatch,
  ShiitakeAgent,
  ShiitakeSession,
  SubscribeOptions,
  TooLate,
  UnknownRequest,
  WatchOptions,
} from "./client.js";
export type {
  ColdSessionPage,
  ColdSessionPageRequest,
  ColdSessionRow,
} from "./cold-sessions.js";
export {
  parseForkedSession,
  parseSessionFrame,
  parseSessionSnapshot,
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
  StoragePressureCheck,
  CompactionBudget,
  CompactionOptions,
  GrantsDelivery,
  GrantsDeliveryResult,
  ShiitakeOptions,
  ShiitakeOwnerAgent,
  ToolAccess,
  HostMetaOptions,
  StandardHostMetaResult,
  StandardHostMetaSchema,
  ToolSource,
} from "./internal/contracts.js";
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
export {
  ShiitakeError,
  type ShiitakeErrorBoundary,
  type ShiitakeErrorCause,
} from "./errors.js";

/** Public construction namespace for the Promise-facing durable harness. */
export type ShiitakeApi = Readonly<{
  /** Construct a Promise-facing durable harness. */
  make: (options: ShiitakeOptions) => Promise<ShiitakeOwnerAgent>;
}>;

/** Promise-facing host composition with addressed mail kept off Session. */
export type ShiitakeHost = Readonly<{
  /** Exact settled command facts; never a selected-lineage transcript query. */
  readSettlement: (
    sessionId: string,
    commandId: string,
  ) => Promise<CommandSettlement | undefined>;
  agent: ShiitakeOwnerAgent;
  /** Durably admit one addressed mail item through the host owner. */
  acceptMail: (
    mail: BirdDogMail,
    meta?: HostJson,
    reply?: VerifiedAgentReply,
    stageAcceptedNotice?: (mail: BirdDogMail) => void,
  ) => Promise<Accepted>;
}>;

/** Construct the host-only Promise facade over the canonical Effect program. */
export function makeHost(
  options: ShiitakeOptions,
  mailRoom: BirdDogEffect,
): Promise<ShiitakeHost> {
  return openPromiseHost(options, mailRoom, openEffectHost);
}

/** Construct the Promise facade over the canonical Effect program. */
export const Shiitake: ShiitakeApi = Object.freeze({
  make: (options) => openPromiseAgent(options, makeEffect),
});

export type { ModelPreference } from "./internal/model-preference.js";

export type {
  DocumentSpecV0,
  DocumentsV0,
  DocumentForkPolicyV0,
  DocumentHandleV0,
  DocumentHandlesV0,
} from "./session-documents.js";

export { sqliteRowSchema } from "./internal/contracts.js";

export type { CommandSettlement } from "./internal/contracts.js";
/** A delegated Agent reply after the admitting host authenticated it. */
export type { VerifiedAgentReply } from "./internal/woodstock/index.js";
