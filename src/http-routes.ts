import { reportCatchFailure } from "./internal/failure.js";
import { parseModelPreference } from "./internal/model-preference.js";
import { Hono } from "hono";
import {
  type SSEMessage,
  type SSEStreamingApi,
  streamSSE,
} from "hono/streaming";
import { validator } from "hono/validator";

import type {
  ReadOptions,
  SubscribeOptions,
  SessionFrame,
  SessionSnapshot,
  AgentExtensionControls,
  ShiitakeAgent,
  ShiitakeSession,
} from "./client.js";
import * as Schema from "effect/Schema";

import { ShiitakeError, toShiitakeErrorWire } from "./errors.js";
import type { Json } from "./extensions.js";
import { JsonValueSchema } from "./internal/json-value.js";
import {
  decodeClientIdentity,
  decodeClientEventsQuery,
  decodeClientReadQuery,
  decodeClientSessionCommandRequest,
  decodeClientSessionCreationRequest,
  decodeClientPageCursor,
  decodeClientSessionId,
} from "./internal/client-boundary.js";

const decodeJsonValue = Schema.decodeUnknownResult(
  Schema.fromJsonString(JsonValueSchema),
);

/** One encoded extension output; its contract, not Hono, types the value. */
function jsonResponse(value: Json): Response {
  return new Response(JSON.stringify(value), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
}

/** The raw request body text; JSON decoding belongs to `decodeJsonValue`. */
function readJsonBody(request: Request): Promise<string> {
  return request.text();
}

/**
 * Hono variables Shiitake's routes read. Only the host's own middleware sets
 * `shiitakeMeta`; no request body can, because every body schema is closed.
 */
export type ShiitakeHttpEnv = {
  Variables: {
    /** Opaque host metadata for prompts and operator lines on this request. */
    shiitakeMeta: Json | undefined;
  };
};

/**
 * Resolve one addressed Session for Shiitake's HTTP transport. `meta` is what
 * the host's middleware attached to this request; a host that attaches none
 * may ignore it.
 */
export type ShiitakeSessionResolver = (
  sessionId: string,
  meta: Json | undefined,
) => Promise<ShiitakeSession> | ShiitakeSession;

function invalidRequest(message: string, cause?: unknown): ShiitakeError {
  return new ShiitakeError("contract", "invalid", message, undefined, cause);
}

/** Build the exact public read options named by a decoded read query. */
function readOptionsFromQuery(query: {
  readonly limit?: number | undefined;
  readonly before?: string | undefined;
  readonly includeSandboxFailures?: "true" | undefined;
}): ReadOptions {
  let options: ReadOptions = {};
  if (query.limit !== undefined) options = { ...options, limit: query.limit };
  if (query.before !== undefined)
    options = { ...options, before: query.before };
  if (query.includeSandboxFailures === "true")
    options = { ...options, includeSandboxFailures: true };
  return options;
}

type SessionEventRequest = Readonly<{
  after: string | undefined;
  limit: number | undefined;
  subscribe: boolean;
  includeSandboxFailures?: boolean;
}>;

type OpenedSessionEvents = Readonly<{
  snapshot: SessionSnapshot | undefined;
  iterator: AsyncIterator<SessionFrame>;
  close: () => Promise<void>;
}>;

function closeOnce(close: () => Promise<void>): () => Promise<void> {
  let closed = false;
  return async () => {
    if (closed) return;
    closed = true;
    await close();
  };
}

async function openSessionEvents(
  session: ShiitakeSession,
  request: SessionEventRequest,
): Promise<OpenedSessionEvents> {
  if (request.subscribe) {
    let options: SubscribeOptions = {};
    if (request.after !== undefined)
      options = { ...options, after: request.after };
    if (request.includeSandboxFailures === true)
      options = { ...options, includeSandboxFailures: true };
    const frames = session.subscribe(options);
    const iterator = frames[Symbol.asyncIterator]();
    return Object.freeze({
      snapshot: undefined,
      iterator,
      close: async () => {
        await iterator.return?.();
      },
    });
  }

  let options: ReadOptions = {};
  if (request.limit !== undefined)
    options = { ...options, limit: request.limit };
  if (request.includeSandboxFailures === true)
    options = { ...options, includeSandboxFailures: true };
  const watch = await session.watch(options);
  return Object.freeze({
    snapshot: watch.snapshot,
    iterator: watch.changes[Symbol.asyncIterator](),
    close: watch.close,
  });
}

function snapshotEvent(snapshot: SessionSnapshot): SSEMessage {
  const event = {
    event: "snapshot",
    data: JSON.stringify(snapshot),
  };
  return snapshot.cursor === null ? event : { ...event, id: snapshot.cursor };
}

function frameEvent(frame: SessionFrame): SSEMessage {
  const id =
    frame.kind === "update"
      ? (frame.cursor ?? "")
      : frame.kind === "reset"
        ? (frame.cursor ?? "")
        : undefined;
  const event = { event: "frame", data: JSON.stringify(frame) };
  return id === undefined ? event : { ...event, id };
}

async function writeSessionEvents(
  stream: SSEStreamingApi,
  session: ShiitakeSession,
  request: SessionEventRequest,
): Promise<void> {
  let close = (): Promise<void> => Promise.resolve();

  try {
    const opened = await openSessionEvents(session, request);
    close = closeOnce(opened.close);
    stream.onAbort(close);

    if (!stream.aborted && opened.snapshot !== undefined)
      await stream.writeSSE(snapshotEvent(opened.snapshot));

    while (!stream.aborted) {
      const next = await opened.iterator.next();
      if (next.done === true) break;
      await stream.writeSSE(frameEvent(next.value));
    }
  } catch (error) {
    reportCatchFailure("session.events", error);
    if (!stream.aborted)
      await stream.writeSSE({
        event: "runtime-error",
        data: JSON.stringify(toShiitakeErrorWire(error)),
      });
  } finally {
    await close();
  }
}

/** Build Shiitake's typed HTTP routes around an existing Session owner. */
export function createShiitakeHttpRoutes(
  resolveSession: ShiitakeSessionResolver,
  listSessions: ShiitakeAgent["listSessions"],
  createSession: ShiitakeAgent["createSession"],
  deleteSession: ShiitakeAgent["deleteSession"],
  resolveExtensions: () =>
    Promise<AgentExtensionControls> | AgentExtensionControls,
) {
  return new Hono<ShiitakeHttpEnv>()
    .get("/extensions", async (context) =>
      context.json(await (await resolveExtensions()).list(), 200),
    )
    .post("/extensions/:extensionId/resume", async (context) => {
      const id = decodeClientIdentity(context.req.param("extensionId"));
      if (id._tag === "Failure")
        return context.json(
          toShiitakeErrorWire(invalidRequest("Extension id is invalid")),
          400,
        );
      return context.json(
        await (await resolveExtensions()).resume(id.success),
        200,
      );
    })
    .post("/extensions/:extensionId/commands/:command", async (context) => {
      const id = decodeClientIdentity(context.req.param("extensionId"));
      const command = decodeClientIdentity(context.req.param("command"));
      const input = decodeJsonValue(await readJsonBody(context.req.raw));
      if (
        id._tag === "Failure" ||
        command._tag === "Failure" ||
        input._tag === "Failure"
      )
        return context.json(
          toShiitakeErrorWire(invalidRequest("Extension command is invalid")),
          400,
        );
      return jsonResponse(
        await (
          await resolveExtensions()
        ).call(id.success, command.success, input.success),
      );
    })
    .post(
      "/sessions/:sessionId/extensions/:extensionId/commands/:command",
      async (context) => {
        const sessionId = decodeClientSessionId(context.req.param("sessionId"));
        const id = decodeClientIdentity(context.req.param("extensionId"));
        const command = decodeClientIdentity(context.req.param("command"));
        const input = decodeJsonValue(await readJsonBody(context.req.raw));
        if (
          id._tag === "Failure" ||
          command._tag === "Failure" ||
          input._tag === "Failure"
        )
          return context.json(
            toShiitakeErrorWire(invalidRequest("Extension command is invalid")),
            400,
          );
        const session = await resolveSession(
          sessionId,
          context.get("shiitakeMeta"),
        );
        return jsonResponse(
          await session
            .extension(id.success)
            .call(command.success, input.success),
        );
      },
    )
    .post(
      "/sessions",
      validator("json", (value, context) => {
        const decoded = decodeClientSessionCreationRequest(value);
        if (decoded._tag === "Failure")
          return context.json(
            toShiitakeErrorWire(
              invalidRequest("Session creation request is invalid"),
            ),
            400,
          );
        return decoded.success;
      }),
      async (context) =>
        context.json(
          {
            sessionId: await createSession(context.req.valid("json").requestId),
          },
          201,
        ),
    )
    .delete("/sessions/:sessionId", async (context) =>
      context.json(
        {
          deleted: await deleteSession(
            decodeClientSessionId(context.req.param("sessionId")),
          ),
        },
        200,
      ),
    )
    .get("/sessions", async (context) => {
      const after = context.req.query("after");
      let cursor: string | undefined;
      try {
        cursor =
          after === undefined ? undefined : decodeClientPageCursor(after);
      } catch (cause) {
        return context.json(
          toShiitakeErrorWire(
            invalidRequest("Session cursor is invalid", cause),
          ),
          400,
        );
      }
      const page = await listSessions(cursor);
      return context.json(page, 200);
    })
    .get("/sessions/:sessionId/pending/latest", async (context) => {
      const session = await resolveSession(
        decodeClientSessionId(context.req.param("sessionId")),
        context.get("shiitakeMeta"),
      );
      return context.json(await session.latestPending(), 200);
    })
    .get("/sessions/:sessionId/model-preference", async (context) => {
      const session = await resolveSession(
        decodeClientSessionId(context.req.param("sessionId")),
        context.get("shiitakeMeta"),
      );
      return context.json(await session.modelPreference.read(), 200);
    })
    .put(
      "/sessions/:sessionId/model-preference",
      validator("json", (value, context) => {
        try {
          return parseModelPreference(value);
        } catch (cause) {
          return context.json(
            toShiitakeErrorWire(
              invalidRequest("Model preference is invalid", cause),
            ),
            400,
          );
        }
      }),
      async (context) => {
        const session = await resolveSession(
          decodeClientSessionId(context.req.param("sessionId")),
          context.get("shiitakeMeta"),
        );
        await session.modelPreference.set(context.req.valid("json"));
        return context.body(null, 204);
      },
    )
    .post(
      "/sessions/:sessionId/withdraw",
      validator("json", (value, context) => {
        const decoded = decodeClientSessionCreationRequest(value);
        if (decoded._tag === "Failure")
          return context.json(
            toShiitakeErrorWire(
              invalidRequest("Withdrawal request is invalid"),
            ),
            400,
          );
        return decoded.success;
      }),
      async (context) => {
        const session = await resolveSession(
          decodeClientSessionId(context.req.param("sessionId")),
          context.get("shiitakeMeta"),
        );
        return context.json(
          await session.withdraw(context.req.valid("json").requestId),
          200,
        );
      },
    )
    .post(
      "/sessions/:sessionId/command",
      validator("json", (value, context) => {
        const decoded = decodeClientSessionCommandRequest(value);
        if (decoded._tag === "Failure")
          return context.json(
            toShiitakeErrorWire(invalidRequest("Command is invalid")),
            400,
          );
        return decoded.success;
      }),
      async (context) => {
        const sessionId = decodeClientSessionId(context.req.param("sessionId"));
        const session = await resolveSession(
          sessionId,
          context.get("shiitakeMeta"),
        );
        const command = context.req.valid("json").command;
        if (
          command.type === "abort" ||
          command.type === "branch" ||
          command.type === "operator" ||
          command.type === "title"
        ) {
          await session.command(command);
          return context.body(null, 204);
        }
        return context.json(await session.command(command), 200);
      },
    )
    .get(
      "/sessions/:sessionId",
      validator("query", (value, context) => {
        const decoded = decodeClientReadQuery(value);
        if (decoded._tag === "Failure")
          return context.json(
            toShiitakeErrorWire(invalidRequest("Read options are invalid")),
            400,
          );
        return decoded.success;
      }),
      async (context) => {
        const sessionId = decodeClientSessionId(context.req.param("sessionId"));
        const session = await resolveSession(
          sessionId,
          context.get("shiitakeMeta"),
        );
        const query = context.req.valid("query");
        return context.json(
          await session.read(readOptionsFromQuery(query)),
          200,
        );
      },
    )
    .get(
      "/sessions/:sessionId/events",
      validator("query", (value, context) => {
        const decoded = decodeClientEventsQuery(value);
        if (decoded._tag === "Failure")
          return context.json(
            toShiitakeErrorWire(invalidRequest("Event options are invalid")),
            400,
          );
        return decoded.success;
      }),
      async (context) => {
        const sessionId = decodeClientSessionId(context.req.param("sessionId"));
        const session = await resolveSession(
          sessionId,
          context.get("shiitakeMeta"),
        );
        const query = context.req.valid("query");
        const lastEventId = context.req.header("Last-Event-ID");
        const after = lastEventId ?? query.after;
        const shouldSubscribe =
          lastEventId !== undefined || query.mode === "subscribe";
        context.header("Content-Encoding", "Identity");
        context.header("Cache-Control", "no-cache");

        return streamSSE(context, (stream) =>
          writeSessionEvents(stream, session, {
            after,
            limit: query.limit,
            subscribe: shouldSubscribe,
            includeSandboxFailures: query.includeSandboxFailures === "true",
          }),
        );
      },
    )
    .onError((error, context) =>
      context.json(
        toShiitakeErrorWire(error),
        error instanceof ShiitakeError &&
          error.boundary === "runtime" &&
          error.cause === "rejected"
          ? 403
          : 503,
      ),
    );
}
