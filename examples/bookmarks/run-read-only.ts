import { readOnlySave } from "./grants.js";

const [outcome] = await Promise.allSettled([readOnlySave()]);
console.log(
  outcome.status === "rejected"
    ? `Refused: ${String(outcome.reason)}`
    : "Saved, which this host should have refused",
);
