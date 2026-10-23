import * as Effect from "effect/Effect";
import type * as Fiber from "effect/Fiber";
import * as Scope from "effect/Scope";

import type { SqliteOwner } from "../contracts.js";

/**
 * An owner's one compaction slot (background compaction design §5). From
 * claim until release it holds the only extra decoded copy of a
 * conversation the owner may have: the summary fiber while it runs, then the
 * summary it hands back. Every other trigger joins it instead of starting a
 * second one. Process memory only: a crash loses it, unwritten.
 *
 * Deliberate Shiitake difference, not Pi parity: Pi Durable's background
 * compaction is a durable task that survives a Conversation abort and
 * resumes after a crash, and Pi does not join triggers into one. Ours is
 * scoped to the run and single-flight; that is what bounds an owner to one
 * extra copy of one conversation.
 */
export type CompactionSlot<A, E> = {
  fiber: Fiber.Fiber<void, E> | undefined;
  summary: A | undefined;
};

const slots = new WeakMap<SqliteOwner, object>();

/** Whether a compaction already holds this owner's slot. */
export function slotTaken(owner: SqliteOwner): boolean {
  return slots.has(owner);
}

/**
 * Claim the owner's slot and fork `work` into `scope`, which interrupts it
 * when the run ends. The release finalizer is registered before the fiber
 * or the slot entry exist, and the entry is written in the same synchronous
 * step that receives the fiber, so an interrupt anywhere leaves no entry.
 * `work` hands its summary back through the slot, never through the fiber's
 * exit, so releasing the slot makes the summary unreachable.
 */
export function claimSlot<A, E>(
  owner: SqliteOwner,
  scope: Scope.Scope,
  work: (handBack: (summary: A) => void) => Effect.Effect<void, E>,
): Effect.Effect<CompactionSlot<A, E>> {
  const slot: CompactionSlot<A, E> = { fiber: undefined, summary: undefined };
  const task = work((summary) => {
    slot.summary = summary;
  });
  return Scope.addFinalizer(
    scope,
    Effect.sync(() => {
      releaseSlot(owner, slot);
    }),
  ).pipe(
    Effect.andThen(Effect.forkIn(task, scope)),
    Effect.map((fiber) => {
      slot.fiber = fiber;
      slots.set(owner, slot);
      return slot;
    }),
  );
}

/** Take the finished summary out of the slot, which no longer reaches it. */
export function takeSummary<A, E>(slot: CompactionSlot<A, E>): A | undefined {
  const summary = slot.summary;
  slot.summary = undefined;
  return summary;
}

/** Drop every reference the slot holds and free it, however it settled. */
export function releaseSlot<A, E>(
  owner: SqliteOwner,
  slot: CompactionSlot<A, E>,
): void {
  slot.fiber = undefined;
  slot.summary = undefined;
  if (slots.get(owner) === slot) slots.delete(owner);
}
