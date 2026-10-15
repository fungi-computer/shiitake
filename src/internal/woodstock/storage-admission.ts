import * as Schema from "effect/Schema";

import { ShiitakeError } from "../../errors.js";
import type { StorageBudget } from "../contracts.js";

/**
 * Provisional disk thresholds, as fractions of the host's budget. At
 * `refuseAt` new Sessions and new prompts are refused; once refusing, they
 * are admitted again only below `readmitBelow`, so admission does not flap
 * while usage hovers at the threshold. The gap is the room the work already
 * admitted keeps for finishing: its writes never pass through this check.
 */
const storagePressureThresholds = Object.freeze({
  refuseAt: 0.8,
  readmitBelow: 0.75,
});

const decodeBudgetBytes = Schema.decodeUnknownResult(
  Schema.Int.pipe(Schema.check(Schema.isGreaterThan(0))),
);
const decodeUsedBytes = Schema.decodeUnknownResult(
  Schema.Number.pipe(
    Schema.check(Schema.isFinite()),
    Schema.check(Schema.isGreaterThanOrEqualTo(0)),
  ),
);

/** The admission check for writes that start new work. */
export type StorageAdmission = Readonly<{
  /**
   * Throw a typed `storage_pressure` refusal when the owner is near its
   * budget. Called only for a new Session or a new prompt, never for a
   * replayed request or for writes that finish admitted work.
   */
  admitNew: () => void;
}>;

/** The admission of a host that set no budget: nothing is refused. */
export const admitWithoutBudget: StorageAdmission = Object.freeze({
  admitNew: () => {
    // No budget, so there is no storage pressure to refuse for.
  },
});

/** Open the owner's admission check over the host's measured budget. */
export function openStorageAdmission(
  budget: StorageBudget | undefined,
): StorageAdmission {
  if (budget === undefined) return admitWithoutBudget;
  const budgetBytes = decodeBudgetBytes(budget.budgetBytes);
  if (budgetBytes._tag === "Failure")
    throw new ShiitakeError(
      "contract",
      "invalid",
      "Storage budget must be a positive whole number of bytes",
    );
  const limit = budgetBytes.success;
  let refusing = false;
  return Object.freeze({
    admitNew: () => {
      const used = decodeUsedBytes(budget.usedBytes());
      if (used._tag === "Failure")
        throw new ShiitakeError(
          "store",
          "unavailable",
          "Owner storage size is unavailable",
        );
      const fraction = used.success / limit;
      refusing = refusing
        ? fraction >= storagePressureThresholds.readmitBelow
        : fraction >= storagePressureThresholds.refuseAt;
      if (refusing)
        throw new ShiitakeError(
          "store",
          "storage_pressure",
          "Storage is near its budget. New Sessions and prompts are refused until space is freed; running work continues.",
        );
    },
  });
}
