import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import {
  defineCollection,
  defineExtension,
  ExtensionCommandError,
  ExtensionId,
  ExtensionStop,
  type CollectionV0,
  type ExtensionRead,
  type ExtensionStateError,
  type RosteredExtensionV0,
} from "../../src/extensions.js";

const Seen = Schema.Struct({
  sequence: Schema.Number,
  sessionId: Schema.String,
  workId: Schema.String,
  outcome: Schema.String,
  terminal: Schema.String,
  originExtension: Schema.NullOr(Schema.String),
  correlation: Schema.NullOr(Schema.String),
});
const Armed = Schema.Struct({ key: Schema.String, sessionId: Schema.String });

/** Count every row of one collection, optionally within one index value. */
export function countAll<A>(
  collection: CollectionV0<A>,
  keyOf: (row: A) => string,
  index?: string,
): Effect.Effect<number, ExtensionStateError, ExtensionRead> {
  return Effect.gen(function* () {
    let total = 0;
    let after: string | undefined;
    for (;;) {
      const page = yield* collection.page({ limit: 64, index, after });
      total += page.length;
      const last = page.at(-1);
      if (page.length < 64 || last === undefined) return total;
      after = keyOf(last);
    }
  });
}

const seenKey = (row: Readonly<{ sequence: number }>) =>
  String(row.sequence).padStart(12, "0");

/**
 * Records every settled run exactly once (a duplicate delivery stops it),
 * arms timers that send one prompt, and projects a per-Session count.
 */
export const recorderContract = {
  id: ExtensionId.make("test-recorder"),
  commands: {
    arm: {
      scope: "session",
      input: Schema.Struct({ key: Schema.String, dueAt: Schema.Number }),
      output: Schema.Struct({ armed: Schema.Boolean }),
    },
    refuse: {
      scope: "session",
      input: Schema.Struct({}),
      output: Schema.Struct({}),
    },
    seen: {
      scope: "agent",
      input: Schema.Struct({}),
      output: Schema.Struct({ count: Schema.Number }),
    },
  },
  projection: { schema: Schema.Struct({ seen: Schema.Number }) },
} as const;

/** Build the recorder with a configurable prose value (hashed into its revision). */
export function recorderExtension(
  config: Readonly<{ note: string }> = { note: "v1" },
): RosteredExtensionV0 {
  return defineExtension({ id: recorderContract.id, version: "1.0.0" })
    .config(config)
    .state({
      version: 1,
      migrations: {},
      collections: {
        seen: defineCollection(Seen, {
          key: seenKey,
          index: (row) => row.sessionId,
        }),
        armed: defineCollection(Armed, { key: (row) => row.key }),
      },
    })
    .define({
      hooks: {
        onSettled: (ctx, page) =>
          Effect.forEach(
            page,
            (run) =>
              Effect.gen(function* () {
                const key = String(run.sequence).padStart(12, "0");
                if (Option.isSome(yield* ctx.state.seen.get(key)))
                  return yield* Effect.fail(
                    new ExtensionStop({ reason: `duplicate delivery ${key}` }),
                  );
                yield* ctx.state.seen.put({
                  sequence: run.sequence,
                  sessionId: run.sessionId,
                  workId: run.workId,
                  outcome: run.outcome,
                  terminal: run.terminal,
                  originExtension: run.origin?.extensionId ?? null,
                  correlation: run.origin?.correlation ?? null,
                });
                yield* ctx.observe(run.sessionId);
              }),
            { discard: true },
          ),
        onTimer: (ctx, timer) =>
          Effect.gen(function* () {
            const armed = yield* ctx.state.armed.get(timer.key);
            if (Option.isNone(armed)) return;
            yield* ctx.send({
              sessionId: armed.value.sessionId,
              when: "now",
              correlation: timer.key,
              prompt: {
                text: `timer ${timer.key}`,
                requestId: `timer:${timer.key}`,
                delivery: "followUp",
              },
            });
            yield* ctx.state.armed.remove(timer.key);
          }),
      },
      projection: {
        schema: recorderContract.projection.schema,
        read: (ctx, sessionId) =>
          Option.some({
            seen: (() => {
              let seen = 0;
              let after: string | undefined;
              for (;;) {
                const page = ctx.state.seen.peekPage({
                  index: sessionId,
                  after,
                  limit: 64,
                });
                seen += page.length;
                const last = page.at(-1);
                if (last === undefined || page.length < 64) return seen;
                after = seenKey(last);
              }
            })(),
          }),
      },
      commands: {
        specs: recorderContract.commands,
        handlers: (ctx) => ({
          arm: ({ sessionId, input }) =>
            Effect.gen(function* () {
              yield* ctx.state.armed.put({ key: input.key, sessionId });
              yield* ctx.timers.set(input.key, input.dueAt);
              return { armed: true };
            }),
          refuse: () =>
            Effect.fail(
              new ExtensionCommandError({
                reason: "conflict",
                message: "Refused by the fixture",
              }),
            ),
          seen: () =>
            countAll(ctx.state.seen, seenKey).pipe(
              Effect.map((count) => ({ count })),
            ),
        }),
      },
    });
}

/** Behaviour switch shared by one test and its crashing fixture. */
export type CrashSwitch = { failing: boolean; delivered: number[] };

const crasherContract = {
  id: ExtensionId.make("test-crasher"),
  commands: {
    wait: {
      scope: "agent",
      input: Schema.Struct({}),
      output: Schema.Struct({}),
    },
  },
  projection: { schema: Schema.Struct({ ok: Schema.Boolean }) },
} as const;

/** Dies in onSettled while `failing`; an async command always stops it. */
export function crasherExtension(control: CrashSwitch): RosteredExtensionV0 {
  return defineExtension({ id: crasherContract.id, version: "1.0.0" }).define({
    hooks: {
      onSettled: (_context, page) =>
        Effect.sync(() => {
          if (control.failing) throw new Error("fixture onSettled crashed");
          control.delivered.push(...page.map(({ sequence }) => sequence));
        }),
    },
    projection: {
      schema: crasherContract.projection.schema,
      read: (context) =>
        Option.some({ ok: context.grants.includes("projection") }),
    },
    commands: {
      specs: crasherContract.commands,
      handlers: () => ({
        wait: () =>
          Effect.promise(() => Promise.resolve({})).pipe(Effect.as({})),
      }),
    },
  });
}

const toolContract = {
  id: ExtensionId.make("test-tools"),
  commands: {},
  projection: null,
} as const;

/** A run participant (run_tools) whose revision depends on its config. */
export function toolExtension(note: string): RosteredExtensionV0 {
  return defineExtension({ id: toolContract.id, version: "1.0.0" })
    .config({ note })
    .define({ tools: () => Effect.succeed(Option.none()) });
}

/** Grants exactly what each fixture requests. */
export function grantAll(roster: readonly RosteredExtensionV0[]) {
  return {
    roster,
    grants: roster.map(({ manifest }) => ({
      id: manifest.id,
      capabilities: manifest.requests,
    })),
  };
}

const notesContract = {
  id: ExtensionId.make("test-notes"),
  commands: {
    write: {
      scope: "agent",
      input: Schema.Struct({ key: Schema.String, text: Schema.String }),
      output: Schema.Struct({}),
    },
    read: {
      scope: "agent",
      input: Schema.Struct({ key: Schema.String }),
      output: Schema.NullOr(Schema.String),
    },
  },
  projection: null,
} as const;

const NoteV1 = Schema.Struct({ key: Schema.String, text: Schema.String });
const NoteV2 = Schema.Struct({ key: Schema.String, body: Schema.String });

/** A state-only extension whose v2 renames `text` to `body` by migration. */
export function notesExtension(version: 1 | 2 | 3): RosteredExtensionV0 {
  if (version === 1)
    return defineExtension({
      id: notesContract.id,
      version: `${String(version)}.0.0`,
    })
      .state({
        version: 1,
        migrations: {},
        collections: {
          notes: defineCollection(NoteV1, { key: (note) => note.key }),
        },
      })
      .define({
        commands: {
          specs: notesContract.commands,
          handlers: (ctx) => ({
            write: ({ input }: { input: { key: string; text: string } }) =>
              ctx.state.notes.put(input).pipe(Effect.as({})),
            read: ({ input }) =>
              ctx.state.notes
                .get(input.key)
                .pipe(
                  Effect.map((note) => Option.getOrNull(note)?.text ?? null),
                ),
          }),
        },
      });
  return defineExtension({
    id: notesContract.id,
    version: `${String(version)}.0.0`,
  })
    .state({
      version,
      migrations: {
        1: (row) => [
          {
            ...row,
            value: {
              key: row.key,
              body: `migrated: ${JSON.stringify(row.value)}`,
            },
          },
        ],
        2: (row) => [row],
      },
      collections: {
        notes: defineCollection(NoteV2, { key: (note) => note.key }),
      },
    })
    .define({
      commands: {
        specs: notesContract.commands,
        handlers: (ctx) => ({
          write: ({ input }: { input: { key: string; text: string } }) =>
            ctx.state.notes
              .put({ key: input.key, body: input.text })
              .pipe(Effect.as({})),
          read: ({ input }) =>
            ctx.state.notes
              .get(input.key)
              .pipe(Effect.map((note) => Option.getOrNull(note)?.body ?? null)),
        }),
      },
    });
}
