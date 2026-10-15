import { nodeAgent } from "./node-agent.js";

const snapshot = await nodeAgent("./agent.sqlite");
console.log(JSON.stringify(snapshot.messages.at(-1)?.value, null, 2));
