import type { Api, Model } from "@fungi.computer/mule/effect";
import * as Schema from "effect/Schema";

import { ShiitakeError } from "../../errors.js";
import type { CompactionOptions } from "../contracts.js";
import {
  DEFAULT_COMPACTION_SETTINGS,
  type CompactionSettings,
} from "./utils.js";

const tokens = Schema.Int.pipe(
  Schema.check(Schema.isGreaterThanOrEqualTo(0)),
  Schema.check(Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)),
);
const BudgetSchema = Schema.Struct({
  reserveTokens: Schema.optionalKey(tokens),
  keepRecentTokens: Schema.optionalKey(tokens),
  backgroundTokens: Schema.optionalKey(tokens),
});
const CompactionOptionsSchema = Schema.Struct({
  enabled: Schema.optionalKey(Schema.Boolean),
  reserveTokens: Schema.optionalKey(tokens),
  keepRecentTokens: Schema.optionalKey(tokens),
  backgroundTokens: Schema.optionalKey(tokens),
  modelOverrides: Schema.optionalKey(
    Schema.Record(Schema.String, BudgetSchema),
  ),
});
const decodeOptions = Schema.decodeUnknownResult(CompactionOptionsSchema, {
  onExcessProperty: "error",
});

/** Resolve one model's compaction settings from the host's validated options. */
export type CompactionSettingsFor = (model: Model<Api>) => CompactionSettings;

/**
 * Validate the host's compaction options once, when the owner opens, and
 * resolve each budget per model: exact `provider/modelId` override, then the
 * ordinary value, then Pi's default.
 */
export function openCompactionSettings(
  options: CompactionOptions | undefined,
): CompactionSettingsFor {
  const decoded = decodeOptions(options ?? {});
  if (decoded._tag === "Failure")
    throw new ShiitakeError(
      "contract",
      "invalid",
      "Compaction settings must be non-negative whole token counts",
    );
  const settings = decoded.success;
  return (model) => {
    const override = settings.modelOverrides?.[`${model.provider}/${model.id}`];
    return {
      enabled: settings.enabled ?? DEFAULT_COMPACTION_SETTINGS.enabled,
      reserveTokens:
        override?.reserveTokens ??
        settings.reserveTokens ??
        DEFAULT_COMPACTION_SETTINGS.reserveTokens,
      keepRecentTokens:
        override?.keepRecentTokens ??
        settings.keepRecentTokens ??
        DEFAULT_COMPACTION_SETTINGS.keepRecentTokens,
      backgroundTokens:
        override?.backgroundTokens ??
        settings.backgroundTokens ??
        DEFAULT_COMPACTION_SETTINGS.backgroundTokens,
    };
  };
}
