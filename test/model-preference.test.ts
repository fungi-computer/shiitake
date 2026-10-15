import { expect, it } from "vitest";

import {
  ModelPreferenceSchema,
  modelPreferenceJsonSchema,
  parseModelPreference,
  resolveModelPreference,
  parseModelAddress,
} from "../src/model-preference.js";

it("publishes the maintained model preference parser and generated JSON Schema", () => {
  const preference = {
    providerId: "anthropic",
    modelId: "claude-sonnet",
    reasoning: "high",
  } as const;

  expect(parseModelPreference(preference)).toEqual(preference);
  expect(modelPreferenceJsonSchema).toMatchObject({
    type: "object",
    properties: {
      providerId: { type: "string" },
      modelId: { type: "string" },
    },
    required: ["providerId", "modelId"],
  });

  expect(modelPreferenceJsonSchema).toHaveProperty(
    "properties.reasoning.anyOf",
    expect.arrayContaining([
      expect.objectContaining({
        type: "string",
        enum: ["minimal", "low", "medium", "high", "xhigh", "max"],
      }),
    ]),
  );

  expect(() =>
    parseModelPreference({ ...preference, reasoning: "extreme" }),
  ).toThrow();
  expect(ModelPreferenceSchema).toBeDefined();
});

it("resolves explicit Session, Agent, manual, Team, then available login in that order", () => {
  const choices = ["session", "agent", "manual", "team", "available"].map(
    (modelId) => ({ providerId: "openai", modelId }),
  );
  const [session, agent, manual, team, available] = choices;
  expect(
    resolveModelPreference({ session, agent, manual, team, available }),
  ).toEqual(session);
  expect(
    resolveModelPreference({ session: null, agent, manual, team, available }),
  ).toEqual(agent);
  expect(resolveModelPreference({ manual, team, available })).toEqual(manual);
  expect(resolveModelPreference({ team, available })).toEqual(team);
  expect(resolveModelPreference({ available })).toEqual(available);
  expect(resolveModelPreference({})).toBeNull();
});

it("parses provider/model without losing slashes within a provider's model id", () => {
  expect(parseModelAddress("openai/gpt-6-astra")).toEqual({
    providerId: "openai",
    modelId: "gpt-6-astra",
  });
  expect(parseModelAddress("fungi/@fungi/luna")).toEqual({
    providerId: "fungi",
    modelId: "@fungi/luna",
  });
  for (const invalid of [
    "openai",
    "/gpt-6-astra",
    "openai/",
    "open ai/model",
    "openai/model\n",
    "openai/" + "x".repeat(257),
  ])
    expect(() => parseModelAddress(invalid)).toThrow();
});
