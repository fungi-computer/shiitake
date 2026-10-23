import { ShiitakeError } from "../../errors.js";
import type { StoragePressureCheck } from "../contracts.js";

/** The admission check for writes that start new work. */
export type StorageAdmission = Readonly<{
  /**
   * Throw a typed `storage_pressure` refusal when the host reports pressure.
   * Called only for a new Session or a new prompt, never for a replayed
   * request or for writes that finish admitted work.
   */
  admitNew: () => void;
}>;

/** The admission of a host that set no check: nothing is refused. */
export const admitWithoutCheck: StorageAdmission = Object.freeze({
  admitNew: () => {
    // No check, so there is no storage pressure to refuse for.
  },
});

/** Open the owner's refusal door over the host's storage-pressure check. */
export function openStorageAdmission(
  check: StoragePressureCheck | undefined,
): StorageAdmission {
  if (check === undefined) return admitWithoutCheck;
  return Object.freeze({
    admitNew: () => {
      if (check.underPressure())
        throw new ShiitakeError(
          "store",
          "storage_pressure",
          "Storage is near its budget. New Sessions and prompts are refused until space is freed; running work continues.",
        );
    },
  });
}
