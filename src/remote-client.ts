import { nanoid } from "@fungi.computer/cairn";
import { parseModelPreferenceJson } from "./internal/model-preference.js";
import type { ModelPreference } from "./internal/model-preference.js";
import * as Effect from "effect/Effect";
import * as Cause from "effect/Cause";
import * as Latch from "effect/Latch";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";

import {
  parseAccepted,
  parseWithdrawResult,
  parseCancelResult,
  parseLatestPendingPrompt,
  parseForkedSession,
  parseCreatedSession,
  parseDeletedSession,
  parseExtensionOutput,
  parseExtensionStatus,
  parseExtensionStatuses,
  parseSessionFrame,
  parseSessionSnapshot,
  parseSessionSummaryPage,
  parseShiitakeError,
} from "./client-wire.js";
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
  SessionFrame,
  SessionSnapshot,
  ShiitakeEventSourceConstructor,
  ShiitakeAgent,
} from "./client.js";
import { ShiitakeError } from "./errors.js";
import type { Json } from "./extensions.js";
import {
  decodeClientReadOptions,
  decodeClientSessionCreationRequestId,
  decodeClientPageCursor,
  decodeClientSessionId,
  decodeClientSubscribeOptions,
} from "./internal/client-boundary.js";
import { toPromiseAgent } from "./internal/effect-client.js";
import type {
  ShiitakeEffectAgent,
  ShiitakeEffectSession,
} from "./internal/effect-contracts.js";

/** Network location of one remotely hosted Shiitake agent. */
export type ShiitakeClientOptions = Readonly<{
  baseUrl: string;
  /** Host-owned HTTP transport, including authenticated internal bindings. */
  fetch?: typeof globalThis.fetch;
  /** Optional runtime EventSource constructor; browsers use the global by default. */
  eventSource?: ShiitakeEventSourceConstructor;
}>;

function unavailable(message = "Remote Shiitake operation is unavailable") {
  return new ShiitakeError("runtime", "unavailable", message);
}

function invalid(message: string) {
  return new ShiitakeError("runtime", "invalid", message);
}

function clientClosedError() {
  return new ShiitakeError("runtime", "closed", "Shiitake client is closed");
}

type RemoteSessionEvent =
  | Readonly<{ kind: "snapshot"; value: SessionSnapshot }>
  | Readonly<{ kind: "frame"; value: SessionFrame }>;

type RawRemoteSessionEvent = Readonly<{
  kind: "snapshot" | "frame" | "runtime-error" | "source-error";
  event: Event;
}>;

type ActiveRemoteSource = {
  readonly source: InstanceType<ShiitakeEventSourceConstructor>;
  closed: boolean;
};

const closedEventSourceReadyState = 2;
const decodeEventData = Schema.decodeUnknownSync(Schema.String);

function eventData(event: Event): string {
  if (!(event instanceof MessageEvent))
    throw invalid("Session stream event is invalid");
  return decodeEventData(event.data);
}

function decodeRemoteEvent(
  input: RawRemoteSessionEvent,
): Effect.Effect<RemoteSessionEvent, ShiitakeError> {
  if (input.kind === "source-error")
    return Effect.fail(unavailable("Session stream is unavailable"));
  if (input.kind === "snapshot")
    return Effect.try({
      try: () => ({
        kind: "snapshot" as const,
        value: parseSessionSnapshot(eventData(input.event)),
      }),
      catch: () => invalid("Session snapshot is invalid"),
    });
  if (input.kind === "frame")
    return Effect.try({
      try: () => ({
        kind: "frame" as const,
        value: parseSessionFrame(eventData(input.event)),
      }),
      catch: () => invalid("Session frame is invalid"),
    });
  return Effect.try({
    try: () => parseShiitakeError(eventData(input.event)),
    catch: () => invalid("Session runtime error is invalid"),
  }).pipe(Effect.flatMap(Effect.fail));
}

function closeRemoteSource(
  active: ActiveRemoteSource,
  sources: Set<ActiveRemoteSource>,
): Effect.Effect<void> {
  return Effect.sync(() => {
    if (active.closed) return;
    active.closed = true;
    sources.delete(active);
    active.source.close();
  });
}

function remoteEvents(
  url: string,
  clientClosed: Latch.Latch,
  sources: Set<ActiveRemoteSource>,
  eventSource: ShiitakeEventSourceConstructor | undefined,
): Stream.Stream<RemoteSessionEvent, ShiitakeError> {
  return Stream.callback<RawRemoteSessionEvent, ShiitakeError>((queue) =>
    Effect.acquireRelease(
      Effect.try({
        try: () => {
          const constructor = eventSource ?? EventSource;
          const active: ActiveRemoteSource = {
            source: new constructor(url),
            closed: false,
          };
          const emit = (
            kind: RawRemoteSessionEvent["kind"],
            event: Event,
          ): void => {
            Queue.offerUnsafe(queue, { kind, event });
          };
          active.source.addEventListener("snapshot", (event) => {
            emit("snapshot", event);
          });
          active.source.addEventListener("frame", (event) => {
            emit("frame", event);
          });
          active.source.addEventListener("runtime-error", (event) => {
            emit("runtime-error", event);
          });
          active.source.addEventListener("error", (event) => {
            if (active.source.readyState === closedEventSourceReadyState)
              emit("source-error", event);
          });
          sources.add(active);
          return active;
        },
        catch: () => unavailable("Session stream could not open"),
      }),
      (active) => closeRemoteSource(active, sources),
    ),
  ).pipe(
    Stream.mapEffect(decodeRemoteEvent),
    Stream.interruptWhen(clientClosed.await),
  );
}

function sessionUrl(baseUrl: string, sessionId: string): string {
  return `${baseUrl}/sessions/${encodeURIComponent(sessionId)}`;
}

function subscriptionEventsPath(
  eventsUrl: string,
  after: string | undefined,
  includeSandboxFailures = false,
): string {
  const query = new URLSearchParams({ mode: "subscribe" });
  if (after !== undefined) query.set("after", after);
  if (includeSandboxFailures) query.set("includeSandboxFailures", "true");
  return `${eventsUrl}?${query.toString()}`;
}

function remoteFrames(
  url: string,
  clientClosed: Latch.Latch,
  sources: Set<ActiveRemoteSource>,
  eventSource: ShiitakeEventSourceConstructor | undefined,
): Stream.Stream<SessionFrame, ShiitakeError> {
  return remoteEvents(url, clientClosed, sources, eventSource).pipe(
    Stream.mapEffect((event) =>
      event.kind === "frame"
        ? Effect.succeed(event.value)
        : Effect.fail(invalid("Session subscription received a snapshot")),
    ),
  );
}

function openRemoteWatch(
  url: string,
  scopes: Set<Scope.Closeable>,
  ensureOpen: Effect.Effect<void, ShiitakeError>,
  clientClosed: Latch.Latch,
  sources: Set<ActiveRemoteSource>,
  eventSource: ShiitakeEventSourceConstructor | undefined,
) {
  return Effect.gen(function* () {
    yield* ensureOpen;
    const watchClosed = yield* Latch.make(false);
    const scope = yield* Scope.make();
    yield* Effect.sync(() => scopes.add(scope));
    yield* Scope.addFinalizer(
      scope,
      Effect.sync(() => {
        scopes.delete(scope);
      }),
    );

    const opened = yield* Effect.exit(
      remoteEvents(url, clientClosed, sources, eventSource).pipe(
        Stream.peel(Sink.head()),
        Scope.provide(scope),
      ),
    );
    if (Exit.isFailure(opened)) {
      yield* Scope.close(scope, opened);
      return yield* Effect.failCause(opened.cause);
    }

    const [first, remaining] = opened.value;
    if (Option.isNone(first) || first.value.kind !== "snapshot") {
      const error = invalid("Session stream did not begin with a snapshot");
      yield* Scope.close(scope, Exit.fail(error));
      return yield* Effect.fail(error);
    }

    const changes = remaining.pipe(
      Stream.mapEffect((event) =>
        event.kind === "frame"
          ? Effect.succeed(event.value)
          : Effect.succeed<SessionFrame>({
              kind: "reset",
              reason: "history_unavailable",
              cursor: event.value.cursor,
              baseline: event.value,
            }),
      ),
      Stream.interruptWhen(watchClosed.await),
      Stream.catchCause((cause) =>
        watchClosed.isOpen() && Cause.hasInterruptsOnly(cause)
          ? Stream.empty
          : Stream.failCause(cause),
      ),
    );
    return Object.freeze({
      snapshot: first.value.value,
      changes,
      close: watchClosed.open.pipe(
        Effect.andThen(Scope.close(scope, Exit.void)),
      ),
    });
  });
}

function parseResponse<A>(
  send: (
    signal: AbortSignal,
  ) => Promise<Readonly<{ ok: boolean; text: () => Promise<string> }>>,
  parse: (body: string) => A,
  description: string,
): Effect.Effect<A, ShiitakeError> {
  return Effect.tryPromise({
    try: async (signal) => {
      const response = await send(signal);
      return { response, body: await response.text() };
    },
    catch: () => unavailable(`${description} request failed`),
  }).pipe(
    Effect.flatMap(({ response, body }) => {
      if (!response.ok)
        return Effect.try({
          try: () => parseShiitakeError(body),
          catch: () => invalid(`${description} error response is invalid`),
        }).pipe(Effect.flatMap(Effect.fail));
      return Effect.try({
        try: () => parse(body),
        catch: () => invalid(`${description} response is invalid`),
      });
    }),
  );
}

function makeRemoteEffectAgent(
  options: ShiitakeClientOptions,
): ShiitakeEffectAgent {
  const baseUrl = options.baseUrl.replace(/\/+$/u, "");
  const fetch = options.fetch ?? globalThis.fetch;
  const scopes = new Set<Scope.Closeable>();
  const sources = new Set<ActiveRemoteSource>();
  const clientClosed = Latch.makeUnsafe(false);
  let closed = false;

  const ensureOpen = Effect.suspend(() =>
    closed ? Effect.fail(clientClosedError()) : Effect.void,
  );

  const request = <A>(
    send: (
      signal: AbortSignal,
    ) => Promise<Readonly<{ ok: boolean; text: () => Promise<string> }>>,
    parse: (body: string) => A,
    description: string,
  ): Effect.Effect<A, ShiitakeError> =>
    Effect.raceFirst(
      parseResponse(send, parse, description),
      clientClosed.await.pipe(Effect.andThen(Effect.fail(clientClosedError()))),
    );

  /** The one generic extension command fetcher (session or agent scope). */
  const callExtension = (url: string, input: Json) =>
    ensureOpen.pipe(
      Effect.andThen(
        request(
          (signal) =>
            fetch(url, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(input),
              signal,
            }),
          parseExtensionOutput,
          "Extension command",
        ),
      ),
    );

  const session = (sessionIdInput: string): ShiitakeEffectSession => {
    const sessionId = decodeClientSessionId(sessionIdInput);
    const addressedUrl = sessionUrl(baseUrl, sessionId);
    const eventsUrl = `${addressedUrl}/events`;
    function command(
      input: PromptCommand | CompactCommand,
    ): Effect.Effect<Accepted, ShiitakeError>;
    function command(
      input: CancelCommand,
    ): Effect.Effect<CancelResult, ShiitakeError>;
    function command(
      input: AbortCommand | BranchCommand | OperatorCommand | TitleCommand,
    ): Effect.Effect<undefined, ShiitakeError>;
    function command(
      input: ForkCommand,
    ): Effect.Effect<ForkedSession, ShiitakeError>;
    function command(
      input: SessionCommand,
    ): Effect.Effect<
      Accepted | CancelResult | ForkedSession | undefined,
      ShiitakeError
    >;
    function command(
      input: SessionCommand,
    ): Effect.Effect<
      Accepted | CancelResult | ForkedSession | undefined,
      ShiitakeError
    > {
      return ensureOpen.pipe(
        Effect.andThen(
          request<Accepted | CancelResult | ForkedSession | undefined>(
            (signal) =>
              fetch(`${addressedUrl}/command`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ command: input }),
                signal,
              }),
            input.type === "prompt" || input.type === "compact"
              ? parseAccepted
              : input.type === "cancel"
                ? parseCancelResult
                : input.type === "fork"
                  ? parseForkedSession
                  : () => undefined,
            "Command",
          ),
        ),
      );
    }
    return Object.freeze({
      command,
      withdraw: (requestId: string) =>
        ensureOpen.pipe(
          Effect.andThen(
            request(
              (signal) =>
                fetch(`${addressedUrl}/withdraw`, {
                  method: "POST",
                  headers: { "Content-Type": "application/json" },
                  body: JSON.stringify({ requestId }),
                  signal,
                }),
              parseWithdrawResult,
              "Request withdrawal",
            ),
          ),
        ),
      abort: command({ type: "abort" }),
      send: (text: string, sendOptions?: SendOptions) =>
        Effect.sync(() => sendOptions?.requestId ?? nanoid()).pipe(
          Effect.flatMap((requestId) => {
            const prompt: PromptCommand = {
              type: "prompt",
              delivery: sendOptions?.delivery ?? "steer",
              requestId,
              text,
            };
            return command(
              sendOptions?.images === undefined
                ? prompt
                : { ...prompt, images: sendOptions.images },
            ).pipe(
              Effect.mapError((error) =>
                error.requestId === requestId
                  ? error
                  : new ShiitakeError(
                      error.boundary,
                      error.cause,
                      error.message,
                      requestId,
                    ),
              ),
            );
          }),
        ),
      cancel: (input: { commandId: string; onlyIfPending?: boolean }) =>
        command({ type: "cancel", ...input }),
      latestPending: ensureOpen.pipe(
        Effect.andThen(
          request(
            (signal) => fetch(`${addressedUrl}/pending/latest`, { signal }),
            parseLatestPendingPrompt,
            "Newest pending prompt",
          ),
        ),
      ),
      modelPreference: Object.freeze({
        read: ensureOpen.pipe(
          Effect.andThen(
            request(
              (signal) => fetch(`${addressedUrl}/model-preference`, { signal }),
              parseModelPreferenceJson,
              "Model preference read",
            ),
          ),
        ),
        set: (preference: ModelPreference | null) =>
          ensureOpen.pipe(
            Effect.andThen(
              request(
                (signal) =>
                  fetch(`${addressedUrl}/model-preference`, {
                    signal,
                    method: "PUT",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(preference),
                  }),
                () => undefined,
                "Model preference set",
              ),
            ),
          ),
      }),
      extension: (extensionId: string) =>
        Object.freeze({
          call: (command: string, input: Json) =>
            callExtension(
              `${addressedUrl}/extensions/${encodeURIComponent(extensionId)}/commands/${encodeURIComponent(command)}`,
              input,
            ),
        }),
      read: (input) =>
        ensureOpen.pipe(
          Effect.andThen(
            Effect.try({
              try: () => decodeClientReadOptions(input),
              catch: (error) =>
                error instanceof ShiitakeError
                  ? error
                  : invalid("Read options are invalid"),
            }),
          ),
          Effect.flatMap(({ limit, before, includeSandboxFailures }) => {
            const params = new URLSearchParams({ limit: String(limit) });
            if (before !== undefined) params.set("before", before);
            if (includeSandboxFailures === true)
              params.set("includeSandboxFailures", "true");
            return request(
              (signal) =>
                fetch(`${addressedUrl}?${params.toString()}`, { signal }),
              parseSessionSnapshot,
              "Session read",
            );
          }),
        ),
      subscribe: (input) =>
        Stream.unwrap(
          ensureOpen.pipe(
            Effect.andThen(
              Effect.try({
                try: () => decodeClientSubscribeOptions(input),
                catch: (error) =>
                  error instanceof ShiitakeError
                    ? error
                    : invalid("Subscribe options are invalid"),
              }),
            ),
            Effect.map(({ after, includeSandboxFailures }) =>
              remoteFrames(
                subscriptionEventsPath(
                  eventsUrl,
                  after,
                  includeSandboxFailures === true,
                ),
                clientClosed,
                sources,
                options.eventSource,
              ),
            ),
          ),
        ),
      watch: (input) =>
        openRemoteWatch(
          input?.includeSandboxFailures === true
            ? `${eventsUrl}?includeSandboxFailures=true`
            : eventsUrl,
          scopes,
          ensureOpen,
          clientClosed,
          sources,
          options.eventSource,
        ),
    });
  };

  return Object.freeze({
    createSession: (requestIdInput) =>
      ensureOpen.pipe(
        Effect.flatMap(() => {
          const requestId = decodeClientSessionCreationRequestId({
            requestId: requestIdInput,
          });
          return request(
            (signal) =>
              fetch(`${baseUrl}/sessions`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ requestId }),
                signal,
              }),
            parseCreatedSession,
            "Session creation",
          );
        }),
      ),
    deleteSession: (sessionIdInput: string) =>
      ensureOpen.pipe(
        Effect.flatMap(() => {
          const sessionId = decodeClientSessionId(sessionIdInput);
          return request(
            (signal) =>
              fetch(`${baseUrl}/sessions/${encodeURIComponent(sessionId)}`, {
                method: "DELETE",
                signal,
              }),
            parseDeletedSession,
            "Session deletion",
          );
        }),
      ),
    listSessions: (after?: string) =>
      ensureOpen.pipe(
        Effect.flatMap(() => {
          const query = new URLSearchParams();
          if (after !== undefined)
            query.set("after", decodeClientPageCursor(after));
          const suffix = query.size === 0 ? "" : `?${query.toString()}`;
          return request(
            (signal) => fetch(`${baseUrl}/sessions${suffix}`, { signal }),
            parseSessionSummaryPage,
            "Session discovery",
          );
        }),
      ),
    extensions: Object.freeze({
      list: ensureOpen.pipe(
        Effect.andThen(
          request(
            (signal) => fetch(`${baseUrl}/extensions`, { signal }),
            parseExtensionStatuses,
            "Extension list",
          ),
        ),
      ),
      resume: (extensionId: string) =>
        ensureOpen.pipe(
          Effect.andThen(
            request(
              (signal) =>
                fetch(
                  `${baseUrl}/extensions/${encodeURIComponent(extensionId)}/resume`,
                  { method: "POST", signal },
                ),
              parseExtensionStatus,
              "Extension resume",
            ),
          ),
        ),
      call: (extensionId: string, command: string, input: Json) =>
        callExtension(
          `${baseUrl}/extensions/${encodeURIComponent(extensionId)}/commands/${encodeURIComponent(command)}`,
          input,
        ),
    }),
    session,
    close: Effect.suspend(() => {
      if (closed) return Effect.void;
      closed = true;
      return clientClosed.open.pipe(
        Effect.andThen(
          Effect.forEach(
            [...scopes],
            (scope) => Scope.close(scope, Exit.void),
            { discard: true },
          ),
        ),
        Effect.andThen(
          Effect.forEach(
            [...sources],
            (active) => closeRemoteSource(active, sources),
            { discard: true },
          ),
        ),
      );
    }),
  });
}

/** Connect to a remotely hosted Shiitake agent. */
export function createShiitakeClient(
  options: ShiitakeClientOptions,
): ShiitakeAgent {
  return toPromiseAgent(
    makeRemoteEffectAgent(options),
    options.baseUrl.replace(/\/+$/u, ""),
  );
}
