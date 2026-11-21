import { expect, it } from "vitest";

import { createShiitakeClient } from "../src/remote-client.js";

it("a request that never reached the Hub names the network's own error", async () => {
  const client = createShiitakeClient({
    baseUrl: "http://fixture.test",
    fetch: () => Promise.reject(new TypeError("net::ERR_NETWORK_CHANGED")),
  });
  try {
    await expect(client.session("lost").read()).rejects.toThrow(
      /request failed: net::ERR_NETWORK_CHANGED/u,
    );
  } finally {
    await client.close();
  }
});
