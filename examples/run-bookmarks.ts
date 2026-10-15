import { bookmarkRoundTrip } from "./bookmarks/run.js";

for (const row of await bookmarkRoundTrip())
  console.log(`${row.title}: ${row.detail ?? ""}`);
