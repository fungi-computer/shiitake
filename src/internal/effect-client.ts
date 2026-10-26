import { nanoid } from "@fungi.computer/cairn";
import { fromTypedSandbox } from "@fungi.computer/mycelium/effect";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import type {
  AbortCommand,
  Accepted,
  BranchCommand,
  OperatorCommand,
  TitleCommand,
  CancelCommand,
  CancelResult,
  ForkCommand,
  ForkedSession,
  CompactCommand,
  PromptCommand,
  SessionCommand,
  SendOptions,
  SessionWatch,
  ShiitakeAgent,
  ShiitakeSession,
} from "../client.js";
import { ShiitakeError } from "../errors.js";
import type { Json } from "../extensions.js";
import type { ModelPreference } from "./model-preference.js";
import type {
  ShiitakeEffectAgent,
  ShiitakeEffectOwner,
  ShiitakeEffectSession,
  ShiitakeEffectOptions,
} from "./effect-contracts.js";
import type {
  GrantsDelivery,
  ShiitakeOwnerAgent,
  ShiitakeOptions,
} from "./contracts.js";

/** Adapt a Promise tool configuration for both initial open and reload. */
export function liftTools(
  toolSource: NonNullable<ShiitakeOptions["tools"]>,
): NonNullable<ShiitakeEffectOptions["tools"]> {
  const tools = {
    modules: toolSource.modules ?? [],
    sources: toolSource.sources ?? [],
    sandbox: fromTypedSandbox(toolSource.sandbox),
    access: toolSource.access ?? {},
  };
  return toolSource.execution === undefined
    ? tools
    : { ...tools, execution: toolSource.execution };
}

/** Run one Shiitake Effect through its sole Promise error adaptation. */
export function runShiitakePromise<A, E>(
  effect: Effect.Effect<A, E>,
  options?: Readonly<{ signal?: AbortSignal }>,
): Promise<A> {
  return Effect.runPromiseExit(effect, options).then((exit) => {
    if (Exit.isSuccess(exit)) return exit.value;

    const failure = Option.getOrUndefined(Cause.findErrorOption(exit.cause));
    const error = failure ?? Cause.squash(exit.cause);
    return Promise.reject(
      error instanceof Error ? error : new Error(String(error)),
    );
  });
}

/**
 * Run one synchronous owner Effect and return or throw its result. Only
 * store operations that never suspend use it.
 */
function runShiitakeSync<A, E>(effect: Effect.Effect<A, E>): A {
  const exit = Effect.runSyncExit(effect);
  if (Exit.isSuccess(exit)) return exit.value;
  const failure = Option.getOrUndefined(Cause.findErrorOption(exit.cause));
  const error = failure ?? Cause.squash(exit.cause);
  throw error instanceof Error ? error : new Error(String(error));
}

function observationAsyncIterable<A>(
  stream: Stream.Stream<A, ShiitakeError>,
): AsyncIterable<A> {
  const iterable = Stream.toAsyncIterable(stream);
  return Object.freeze({
    [Symbol.asyncIterator]: (): AsyncIterator<A> => {
      const iterator = iterable[Symbol.asyncIterator]();
      return {
        next: () => iterator.next(),
        return: () =>
          iterator.return?.() ??
          Promise.resolve({ done: true, value: undefined }),
      };
    },
  });
}

function sessionFacade(
  address: string,
  session: ShiitakeEffectSession,
): ShiitakeSession {
  function command(input: PromptCommand | CompactCommand): Promise<Accepted>;
  function command(input: CancelCommand): Promise<CancelResult>;
  function command(
    input: AbortCommand | BranchCommand | OperatorCommand | TitleCommand,
  ): Promise<undefined>;
  function command(input: ForkCommand): Promise<ForkedSession>;
  function command(
    input: SessionCommand,
  ): Promise<Accepted | CancelResult | ForkedSession | undefined>;
  function command(input: SessionCommand) {
    return runShiitakePromise(session.command(input));
  }
  return Object.freeze({
    address,
    command,
    withdraw: (requestId: string) =>
      runShiitakePromise(session.withdraw(requestId)),
    abort: () => runShiitakePromise(session.abort),
    send: (text: string, options?: SendOptions) =>
      runShiitakePromise(session.send(text, options)),
    cancel: (input: { commandId: string; onlyIfPending?: boolean }) =>
      runShiitakePromise(session.cancel(input)),
    latestPending: () => runShiitakePromise(session.latestPending),
    modelPreference: Object.freeze({
      read: () => runShiitakePromise(session.modelPreference.read),
      set: (preference: ModelPreference | null) =>
        runShiitakePromise(session.modelPreference.set(preference)),
    }),
    extension: (extensionId: string) => {
      const extension = session.extension(extensionId);
      return Object.freeze({
        call: (command: string, input: Json) =>
          runShiitakePromise(extension.call(command, input)),
      });
    },
    read: (input) => runShiitakePromise(session.read(input)),
    readSettlement: (commandId, options) =>
      runShiitakePromise(
        session.readSettlement(commandId, options?.timeoutMs),
        options,
      ),
    subscribe: (input) => observationAsyncIterable(session.subscribe(input)),
    watch: (input) =>
      runShiitakePromise(session.watch(input)).then((watch): SessionWatch =>
        Object.freeze({
          snapshot: watch.snapshot,
          changes: observationAsyncIterable(watch.changes),
          close: () => runShiitakePromise(watch.close),
        }),
      ),
  });
}

/** Adapt one canonical Effect agent to Shiitake's ordinary web interface. */
export function toPromiseAgent(
  agent: ShiitakeEffectAgent,
  suppliedAddress?: string,
): ShiitakeAgent {
  const agentAddress = suppliedAddress ?? `local:${nanoid()}`;
  return Object.freeze({
    createSession: (requestId) =>
      runShiitakePromise(agent.createSession(requestId)),
    deleteSession: (sessionId) =>
      runShiitakePromise(agent.deleteSession(sessionId)),
    listSessions: (after) => runShiitakePromise(agent.listSessions(after)),
    extensions: Object.freeze({
      list: () => runShiitakePromise(agent.extensions.list),
      resume: (extensionId: string) =>
        runShiitakePromise(agent.extensions.resume(extensionId)),
      call: (extensionId: string, command: string, input: Json) =>
        runShiitakePromise(agent.extensions.call(extensionId, command, input)),
    }),
    session: (sessionId) =>
      sessionFacade(
        `${agentAddress}/sessions/${encodeURIComponent(sessionId)}`,
        agent.session(sessionId),
      ),
    close: () => runShiitakePromise(agent.close),
  });
}

/**
 * Adapt the in-process owner, keeping its host-only grants and metadata
 * surfaces; its metadata views share one address.
 */
export function toPromiseOwner(owner: ShiitakeEffectOwner): ShiitakeOwnerAgent {
  const address = `local:${nanoid()}`;
  return Object.freeze({
    ...toPromiseAgent(owner, address),
    prepareConfiguration: async (configuration) => {
      const next: Parameters<typeof owner.prepareConfiguration>[0] = {
        extensions: configuration.extensions,
      };
      if (configuration.tools !== undefined)
        Object.assign(next, { tools: liftTools(configuration.tools) });
      const prepared = await runShiitakePromise(
        owner.prepareConfiguration(next),
      );
      return {
        commit: () => {
          runShiitakeSync(prepared.commit);
        },
        discard: () => runShiitakePromise(prepared.discard),
      };
    },
    grants: Object.freeze({
      deliver: (sessionId: string, delivery: GrantsDelivery) =>
        runShiitakeSync(owner.grants.deliver(sessionId, delivery)),
      read: (sessionId: string) =>
        runShiitakeSync(owner.grants.read(sessionId)),
    }),
    withMeta: (meta: Json) => toPromiseAgent(owner.withMeta(meta), address),
  });
}
