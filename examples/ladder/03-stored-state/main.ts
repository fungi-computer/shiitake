import { storedStateRun } from "./run.js";

const snapshot = await storedStateRun();
const reply = snapshot.messages.at(-1)?.value;
if (reply?.role === "assistant")
  for (const part of reply.content)
    if (part.type === "text") console.log(part.text);
