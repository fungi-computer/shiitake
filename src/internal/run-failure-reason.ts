import type { MuleRunOutcome } from "@fungi.computer/mule/effect";

/** Observations, not a classification of arbitrary provider error text. */
export type UnavailableRunReason = Readonly<{
  kind: "reason_unavailable";
  runId: string;
  provider: string | null;
  model: string | null;
}>;

export function unavailableRunReason(
  observations: Omit<UnavailableRunReason, "kind">,
): string {
  const reason: UnavailableRunReason = {
    kind: "reason_unavailable",
    ...observations,
    provider: observations.provider === "" ? null : observations.provider,
    model: observations.model === "" ? null : observations.model,
  };
  // Watchdog bounds reasons to 200 characters. Bound identity metadata only;
  // truncation is not redaction and must never make provider prose trusted.
  return `${reason.kind}: run=${reason.runId}; provider=${reason.provider ?? "unknown"}; model=${reason.model ?? "unknown"}`.slice(
    0,
    200,
  );
}

export function returnedFailureReason(run: MuleRunOutcome): string {
  // Pi errorMessage can contain raw SDK messages and response bodies.
  return unavailableRunReason({
    runId: run.run.value,
    provider: run.message.provider,
    model: run.message.model,
  });
}
