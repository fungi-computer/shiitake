import { firstSession } from "./first-session.js";

const { receipt, snapshot } = await firstSession();
console.log(receipt.status);
console.log(JSON.stringify(snapshot.messages.at(-1)?.value, null, 2));
