/** Process-local liveness cadence, not a durable Session fact. */
export const sessionHeartbeatIntervalMs = 15_000;

/** Silence loses observation; only the service owner's lease check proves death. */
export const sessionSilenceDeadlineMs = 45_000;
