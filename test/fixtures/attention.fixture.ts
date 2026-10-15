import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  AttentionItemSchema,
  defineExtension,
  ExtensionCommandError,
  ExtensionId,
  type RosteredExtensionV0,
} from "../../src/extensions.js";

/** Generic attention-only extension shared by owner and immediate-consumer proofs. */
export function attentionExtension(
  id = "attention-probe",
): RosteredExtensionV0 {
  return defineExtension({ id: ExtensionId.make(id), version: "1.0.0" }).define(
    {
      attention: true,
      commands: {
        specs: {
          set: {
            scope: "session",
            input: AttentionItemSchema,
            output: Schema.Null,
          },
          clear: {
            scope: "session",
            input: Schema.Struct({ id: Schema.String }),
            output: Schema.Null,
          },
          stop: {
            scope: "session",
            input: Schema.Struct({}),
            output: Schema.Null,
          },
          rollback: {
            scope: "session",
            input: AttentionItemSchema,
            output: Schema.Null,
          },
        },
        handlers: (context) => ({
          stop: () => Effect.die(new Error("attention probe stopped")),
          set: ({ sessionId, input }) =>
            context.attention.set(sessionId, input).pipe(Effect.as(null)),
          clear: ({ sessionId, input }) =>
            context.attention.clear(sessionId, input.id).pipe(Effect.as(null)),
          rollback: ({ sessionId, input }) =>
            context.attention.set(sessionId, input).pipe(
              Effect.andThen(
                Effect.fail(
                  new ExtensionCommandError({
                    reason: "rejected",
                    message: "Rollback probe",
                  }),
                ),
              ),
            ),
        }),
      },
    },
  );
}
