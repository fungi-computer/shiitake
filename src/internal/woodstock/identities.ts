import { defineIdFamily } from "@fungi.computer/cairn";

export const sessionIds = defineIdFamily({
  family: "session",
  version: 1,
  kinds: {
    session: { prefix: "session" },
    work: { prefix: "session_work" },
    fact: { prefix: "session_fact" },
    receipt: { prefix: "session_receipt" },
  },
});
