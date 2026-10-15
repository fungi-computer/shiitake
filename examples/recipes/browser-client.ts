import { createShiitakeClient } from "@fungi.computer/shiitake/client";

/** Connect to an existing admitted HTTP host; the server owns authentication. */
export async function browserConversation(baseUrl: string, requestId: string) {
  const client = createShiitakeClient({ baseUrl });
  const sessionId = await client.createSession(requestId);
  const session = client.session(sessionId);
  const accepted = await session.send("Hello.", {
    requestId: `${requestId}-hello`,
  });
  return { session, accepted };
}
