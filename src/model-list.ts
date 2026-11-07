import { z } from "zod";
import { modelReasoningLevels } from "./model-preference.js";

/** Secret-free Agent catalog projection. A host owns availability and selection. */
export const agentModelListSchema = z
  .array(
    z.strictObject({
      providerId: z.string().min(1).max(256),
      providerName: z.string().min(1).max(256),
      id: z.string().min(1).max(256),
      name: z.string().min(1).max(256),
      reasoning: z.array(z.enum(modelReasoningLevels)).max(6).readonly(),
    }),
  )
  .max(1024)
  .readonly();

export type AgentModelList = z.output<typeof agentModelListSchema>;
