import { modelToolRun } from "./run.js";

const snapshot = await modelToolRun();
// The tool result the model received, then its reply.
for (const { value } of snapshot.messages.slice(-2))
  if (value.role === "toolResult" || value.role === "assistant")
    for (const part of value.content)
      if (part.type === "text") console.log(part.text);
