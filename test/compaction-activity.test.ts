import * as Effect from "effect/Effect";
import { expect, it } from "vitest";

import { openObservationKernel } from "../src/internal/observation-kernel.js";
import {
  beginCompactionActivity,
  endCompactionActivity,
  readProcessCompaction,
} from "../src/internal/woodstock/compaction-activity.js";
import { createNativeSqliteOwnerFixture } from "./fixtures/native-sqlite-owner.fixture.js";

it("publishes a live compaction fact around its run without a durable write", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const sessionId = "compaction-activity-session";
  const kernel = openObservationKernel(fixture.owner);
  const observation = await Effect.runPromise(kernel.register(sessionId));
  try {
    expect(readProcessCompaction(fixture.owner, sessionId)).toBeNull();

    const writesBefore = fixture.ownerWrites();
    beginCompactionActivity(fixture.owner, sessionId, "threshold");

    expect(readProcessCompaction(fixture.owner, sessionId)).toEqual({
      trigger: "threshold",
    });
    expect(await Effect.runPromise(observation.pollSignal)).toBe("progress");
    expect(fixture.ownerWrites()).toBe(writesBefore);

    endCompactionActivity(fixture.owner, sessionId);

    expect(readProcessCompaction(fixture.owner, sessionId)).toBeNull();
    expect(await Effect.runPromise(observation.pollSignal)).toBe("progress");
    expect(fixture.ownerWrites()).toBe(writesBefore);
  } finally {
    observation.close();
    await Effect.runPromise(kernel.close);
    fixture.close();
  }
});

it("clears without publishing when no compaction is running", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const sessionId = "compaction-activity-idle-session";
  const kernel = openObservationKernel(fixture.owner);
  const observation = await Effect.runPromise(kernel.register(sessionId));
  try {
    endCompactionActivity(fixture.owner, sessionId);

    expect(readProcessCompaction(fixture.owner, sessionId)).toBeNull();
    expect(await Effect.runPromise(observation.pollSignal)).toBeUndefined();
  } finally {
    observation.close();
    await Effect.runPromise(kernel.close);
    fixture.close();
  }
});

it("keeps compaction activity scoped to its own Session", () => {
  const fixture = createNativeSqliteOwnerFixture();
  const sessionId = "compaction-activity-scoped-session";
  const otherSessionId = "compaction-activity-other-session";
  try {
    beginCompactionActivity(fixture.owner, sessionId, "manual");

    expect(readProcessCompaction(fixture.owner, sessionId)).toEqual({
      trigger: "manual",
    });
    expect(readProcessCompaction(fixture.owner, otherSessionId)).toBeNull();
  } finally {
    endCompactionActivity(fixture.owner, sessionId);
    fixture.close();
  }
});
