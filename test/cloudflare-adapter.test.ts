import { fauxModel } from "@fungi.computer/mule/testing";
import type { WatchdogTickResult } from "@fungi.computer/watchdog";
import { flow } from "effect/Function";
import { cloudflareStorageFixture } from "./fixtures/cloudflare-storage.fixture.js";
import { writeLargeSession } from "./fixtures/large-session.fixture.js";
import { damageOutsideWoodstock } from "./fixtures/lineage-damage.fixture.js";
import { prepareWoodstockSchema } from "../src/internal/woodstock/index.js";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { expect, it, vi } from "vitest";

import {
  makeCloudflareShiitake,
  ShiitakeOwner,
  type CloudflareShiitakeState,
  type CloudflareShiitakeOptions,
} from "../src/cloudflare.js";
import {
  parseAccepted,
  parseSessionSnapshot,
  type ModelPreference,
} from "../src/client.js";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import { inspect } from "node:util";

import {
  defineExtension,
  ExtensionId,
  type RosteredExtensionV0,
} from "../src/extensions.js";
import {
  createNativeSqliteOwnerFixture,
  noExtensions,
} from "./fixtures/native-sqlite-owner.fixture.js";

/** The one structured shape `ShiitakeDO.alarm()` logs through Effect's logger. */
type AlarmFailureLog = Readonly<{
  event: string;
  cause: string | undefined;
  attempt: number | undefined;
  retryInMs: number | undefined;
}>;

const AlarmLogArguments = Schema.Tuple([
  Schema.String,
  Schema.String,
  Schema.Struct({
    cause: Schema.optional(Schema.String),
    attempt: Schema.optional(Schema.Number),
    retryInMs: Schema.optional(Schema.Number),
  }),
]);
const decodeAlarmLogArguments = Schema.decodeUnknownSync(AlarmLogArguments);

/** Decode Effect's native console prefix, message and annotation arguments. */
const parseAlarmFailureLog = flow(
  decodeAlarmLogArguments,
  ([, event, annotations]): AlarmFailureLog => {
    return {
      event,
      cause: annotations.cause,
      attempt: annotations.attempt,
      retryInMs: annotations.retryInMs,
    };
  },
);

function fixedModelSource(
  faux: ReturnType<typeof fauxModel>,
  observe?: (input: {
    sessionId: string;
    preference: ModelPreference | null;
    signal: AbortSignal;
  }) => void,
  release: () => Promise<void> = () => Promise.resolve(),
) {
  return {
    acquire: (input: {
      sessionId: string;
      preference: ModelPreference | null;
      signal: AbortSignal;
    }) => {
      input.signal.throwIfAborted();
      observe?.(input);
      return Promise.resolve({
        model: faux.model,
        retryable: () => false,
        stream: faux.stream,
        release,
      });
    },
  };
}

it("runs one fixed Pi model through a Cloudflare-hosted Session", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const tasks: Promise<unknown>[] = [];
  const state: CloudflareShiitakeState = {
    storage: cloudflareStorageFixture(fixture.owner),
    waitUntil: (task) => {
      tasks.push(task);
    },
  };
  const faux = fauxModel([]);
  const acquisitions: {
    sessionId: string;
    preference: ModelPreference | null;
    aborted: boolean;
  }[] = [];
  let releases = 0;
  let instructionReleases = 0;
  const instructionSessions: string[] = [];
  faux.setResponses([fauxAssistantMessage("cloudflare adapter completed")]);
  const opening = makeCloudflareShiitake(state, {
    extensions: noExtensions,
    model: fixedModelSource(
      faux,
      ({ sessionId, preference, signal }) => {
        acquisitions.push({ sessionId, preference, aborted: signal.aborted });
      },
      () => {
        releases += 1;
        return Promise.resolve();
      },
    ),
    instructions: {
      load: ({ sessionId }) => {
        instructionSessions.push(sessionId);
        return Promise.resolve({
          revision: "fixed-model-test-v1",
          text: "Answer through the fixed model.",
          release: () => {
            instructionReleases += 1;
          },
        });
      },
    },
    tools: [],
  });

  try {
    const agent = await opening;
    try {
      const session = agent.session("cloudflare-adapter-session");
      await session.command({
        requestId: "cloudflare-adapter-command",
        delivery: "followUp",

        type: "prompt",
        text: "Run through the adapter.",
      });
      for (const task of tasks) await task;

      const assistant = (await session.read()).messages.find(
        ({ value }) => value.role === "assistant",
      );
      expect(assistant?.value).toMatchObject({
        role: "assistant",
        content: [{ type: "text", text: "cloudflare adapter completed" }],
      });
      expect(acquisitions).toEqual([
        {
          sessionId: "cloudflare-adapter-session",
          preference: null,
          aborted: false,
        },
      ]);
      expect(instructionSessions).toEqual(["cloudflare-adapter-session"]);
    } finally {
      await agent.close();
    }
  } finally {
    fixture.close();
  }
  expect(releases).toBe(1);
  expect(instructionReleases).toBe(1);
});

it("joins an extension timer onto the shared Durable Object alarm", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const tasks: Promise<unknown>[] = [];
  const storage = cloudflareStorageFixture(fixture.owner);
  let alarmAt: number | null = null;
  storage.getAlarm = () => Promise.resolve(alarmAt);
  storage.setAlarm = (at) => {
    alarmAt = at instanceof Date ? at.getTime() : at;
    return Promise.resolve();
  };
  storage.deleteAlarm = () => {
    alarmAt = null;
    return Promise.resolve();
  };
  const state: CloudflareShiitakeState = {
    storage,
    waitUntil: (task) => tasks.push(task),
  };
  const faux = fauxModel([]);
  const agent = await makeCloudflareShiitake(state, {
    extensions: grantAll([wakeExtension]),
    model: fixedModelSource(faux),
    instructions: {
      load: () => Promise.resolve({ revision: "timer-wake-v1", text: "Wait." }),
    },
    tools: [],
  });
  try {
    await Promise.all(tasks);
    const dueAt = Date.now() + 3_600_000;
    await agent.extensions.call("test-wake", "arm", { dueAt });
    await Promise.all(tasks);
    expect(alarmAt).toBe(dueAt);

    // Simulate delivery of the current shared alarm, then verify that
    // reconciliation replaces recovery with the owner’s next due candidate.
    alarmAt = null;
    await agent.alarm();
    await Promise.all(tasks);
    expect(alarmAt).toBe(dueAt);
  } finally {
    await agent.close();
    fixture.close();
  }
});

it("shares asynchronous options across cold requests and retries a rejected load", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const tasks: Promise<unknown>[] = [];
  const state: CloudflareShiitakeState = {
    storage: cloudflareStorageFixture(fixture.owner),
    waitUntil: (task) => {
      tasks.push(task);
    },
  };
  let attempts = 0;
  let release: (() => void) | undefined;
  const loaded = new Promise<void>((resolve) => {
    release = resolve;
  });
  class Agent extends ShiitakeOwner<Readonly<Record<string, never>>> {
    protected override async shiitakeOptions(): Promise<CloudflareShiitakeOptions> {
      attempts += 1;
      if (attempts === 1) throw new Error("Options load failed");
      await loaded;
      return {
        extensions: noExtensions,
        model: fixedModelSource(fauxModel([])),
        instructions: {
          load: () => Promise.resolve({ revision: "async-options", text: "" }),
        },
        tools: [],
      };
    }
  }
  const owner = new Agent(state, {});
  try {
    await expect(owner.shiitake()).rejects.toThrow("Options load failed");
    const first = owner.shiitake();
    const second = owner.shiitake();
    await Promise.resolve();
    expect(attempts).toBe(2);
    release?.();
    const [a, b] = await Promise.all([first, second]);
    expect(a).toBe(b);
    expect(attempts).toBe(2);
    await owner.closeShiitake();
  } finally {
    release?.();
    await Promise.all(tasks);
    fixture.close();
  }
});

it("retries failed initialization and serves command acceptance through the raw Durable Object route", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const tasks: Promise<unknown>[] = [];
  const state: CloudflareShiitakeState = {
    storage: cloudflareStorageFixture(fixture.owner),
    waitUntil: (task) => {
      tasks.push(task);
    },
  };
  const faux = fauxModel([]);
  faux.setResponses([fauxAssistantMessage("raw DO route completed")]);
  const options: CloudflareShiitakeOptions = {
    extensions: noExtensions,
    model: fixedModelSource(faux),
    instructions: {
      load: () =>
        Promise.resolve({
          revision: "raw-route-test-v1",
          text: "Answer through the raw Durable Object route.",
        }),
    },
    tools: [],
  };

  class TestShiitakeDO extends ShiitakeOwner<Readonly<Record<string, never>>> {
    protected override shiitakeOptions(): CloudflareShiitakeOptions {
      return options;
    }
  }

  const durableObject = new TestShiitakeDO(state, {});
  const setAlarm = state.storage.setAlarm.bind(state.storage);
  let rejectInitialization = true;
  state.storage.setAlarm = (...args) => {
    if (rejectInitialization) {
      rejectInitialization = false;
      return Promise.reject(new Error("Initial native alarm unavailable"));
    }
    return setAlarm(...args);
  };
  try {
    const failedOpening = await durableObject.fetch(
      new Request("http://shiitake.test/sessions/do-session"),
    );
    expect(failedOpening.status).toBe(503);
    const response = await durableObject.fetch(
      new Request("http://shiitake.test/sessions/do-session/command", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          command: {
            requestId: "do-command",
            delivery: "followUp",

            type: "prompt",
            text: "Run through the DO route.",
          },
        }),
      }),
    );
    const receipt = parseAccepted(await response.text());
    for (const task of tasks) await task;

    expect(response.status).toBe(200);
    expect(receipt.status).toBe("accepted");
    expect(receipt.commandId.length).toBeGreaterThan(0);
    expect(receipt.commandId).not.toBe("do-command");
    const snapshotResponse = await durableObject.fetch(
      new Request("http://shiitake.test/sessions/do-session"),
    );
    const snapshot = parseSessionSnapshot(await snapshotResponse.text());
    expect(snapshot.sessionId).toBe("do-session");
    const assistant = snapshot.messages.find(
      ({ value }) => value.role === "assistant",
    );
    expect(assistant?.value).toMatchObject({
      role: "assistant",
      content: [{ type: "text", text: "raw DO route completed" }],
    });
  } finally {
    fixture.close();
  }
});

it("alarm failure re-arms with backoff", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const tasks: Promise<unknown>[] = [];
  const state: CloudflareShiitakeState = {
    storage: cloudflareStorageFixture(fixture.owner),
    waitUntil: (task) => {
      tasks.push(task);
    },
  };
  const faux = fauxModel([]);
  faux.setResponses([fauxAssistantMessage("alarm recovery completed")]);
  const options: CloudflareShiitakeOptions = {
    extensions: noExtensions,
    model: fixedModelSource(faux),
    instructions: {
      load: () =>
        Promise.resolve({
          revision: "alarm-backoff-test-v1",
          text: "Answer after the native alarm recovers.",
        }),
    },
    tools: [],
  };

  // Each construction attempt (one per failed `alarm()` call) arms exactly
  // one reject for owner construction's own first native setAlarm, leaving
  // ShiitakeDO's own explicit backoff re-arm call untouched.
  let constructionFailuresRemaining = 2;
  let failNextSetAlarm = false;
  class TestShiitakeDO extends ShiitakeOwner<Readonly<Record<string, never>>> {
    protected override shiitakeOptions(): CloudflareShiitakeOptions {
      if (constructionFailuresRemaining > 0) {
        constructionFailuresRemaining -= 1;
        failNextSetAlarm = true;
      }
      return options;
    }
  }

  const durableObject = new TestShiitakeDO(state, {});
  const setAlarm = state.storage.setAlarm.bind(state.storage);
  state.storage.setAlarm = (at) => {
    if (failNextSetAlarm) {
      failNextSetAlarm = false;
      return Promise.reject(new Error("Simulated owner construction failure"));
    }
    return setAlarm(at);
  };
  const errors = vi.spyOn(console, "log").mockImplementation(() => undefined);
  try {
    // Three deliveries of the native alarm with no inbound request at all:
    // the first two hit a construction failure, the third succeeds. None of
    // these awaits may reject: a failed construction is caught, logged, and
    // explicitly re-armed by ShiitakeDO itself rather than rethrown for the
    // platform's own alarm retry to pick up.
    await durableObject.alarm();
    await durableObject.alarm();
    await durableObject.alarm();
    for (const task of tasks) await task;

    const failureLogs = errors.mock.calls
      .map((args) => parseAlarmFailureLog(args))
      .filter((entry) => entry.event === "shiitake_alarm_failed");
    expect(failureLogs).toHaveLength(2);
    expect(failureLogs[0]).toMatchObject({ attempt: 1 });
    expect(failureLogs[1]).toMatchObject({ attempt: 2 });
    // The runtime wraps the storage rejection in its own ShiitakeError; the
    // cause field still carries that message through to the structured log.
    expect(String(failureLogs[0]?.cause)).toContain(
      "Shiitake durable wake is unavailable",
    );

    // Bounded exponential growth in the re-arm delay itself, never past the
    // 30s cap, instead of relying on platform alarm retries.
    const firstRetryInMs = Number(failureLogs[0]?.retryInMs);
    const secondRetryInMs = Number(failureLogs[1]?.retryInMs);
    expect(firstRetryInMs).toBeGreaterThanOrEqual(900);
    expect(firstRetryInMs).toBeLessThan(30_000);
    expect(secondRetryInMs).toBeGreaterThan(firstRetryInMs);
    expect(secondRetryInMs).toBeLessThanOrEqual(30_000);
    // No "rearm itself failed" escalation: the explicit re-arm call it took
    // to reach attempt 2 (and recovery) succeeded both times.
    expect(
      errors.mock.calls.some(
        (args) =>
          parseAlarmFailureLog(args).event === "shiitake_alarm_rearm_failed",
      ),
    ).toBe(false);

    // The third call recovered; no further failure was logged, and the
    // recovered owner is actually usable (proving no stuck construction).
    const response = await durableObject.fetch(
      new Request("http://shiitake.test/sessions/backoff-session/command", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          command: {
            requestId: "backoff-command",
            delivery: "followUp",
            type: "prompt",
            text: "Run after alarm recovery.",
          },
        }),
      }),
    );
    for (const task of tasks) await task;
    expect(response.status).toBe(200);
    const stillFailureLogs = errors.mock.calls
      .map((args) => parseAlarmFailureLog(args))
      .filter((entry) => entry.event === "shiitake_alarm_failed");
    expect(stillFailureLogs).toHaveLength(2);
  } finally {
    errors.mockRestore();
    fixture.close();
  }
});

it("a successful alarm settles quietly and does not re-arm with backoff", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const tasks: Promise<unknown>[] = [];
  const state: CloudflareShiitakeState = {
    storage: cloudflareStorageFixture(fixture.owner),
    waitUntil: (task) => {
      tasks.push(task);
    },
  };
  const faux = fauxModel([]);
  faux.setResponses([fauxAssistantMessage("quiet alarm completed")]);
  const options: CloudflareShiitakeOptions = {
    extensions: noExtensions,
    model: fixedModelSource(faux),
    instructions: {
      load: () =>
        Promise.resolve({
          revision: "quiet-alarm-test-v1",
          text: "Answer without any induced failure.",
        }),
    },
    tools: [],
  };

  class TestShiitakeDO extends ShiitakeOwner<Readonly<Record<string, never>>> {
    protected override shiitakeOptions(): CloudflareShiitakeOptions {
      return options;
    }
  }

  const durableObject = new TestShiitakeDO(state, {});
  const errors = vi.spyOn(console, "log").mockImplementation(() => undefined);
  try {
    await durableObject.alarm();
    for (const task of tasks) await task;

    const failureLogs = errors.mock.calls
      .map((args) => parseAlarmFailureLog(args))
      .filter((entry) => entry.event === "shiitake_alarm_failed");
    expect(failureLogs).toHaveLength(0);
  } finally {
    errors.mockRestore();
    fixture.close();
  }
});

it("keeps a host-owned due time on the shared alarm through reconciliation", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const tasks: Promise<unknown>[] = [];
  const storage = cloudflareStorageFixture(fixture.owner);
  let hostDue: number | null = Date.now() + 60_000;
  const agent = await makeCloudflareShiitake(
    { storage, waitUntil: (task) => tasks.push(task) },
    {
      extensions: grantAll([wakeExtension]),
      model: fixedModelSource(fauxModel([])),
      instructions: {
        load: () =>
          Promise.resolve({ revision: "host-wake-v1", text: "Wait." }),
      },
      tools: [],
      hostWork: [
        {
          pending: () => ({ due: false, nextWakeAt: hostDue }),
          run: () => Promise.resolve(),
        },
      ],
    },
  );
  try {
    for (const task of tasks) await task;
    // Idle Shiitake would delete the alarm; the host's due time survives.
    await agent.alarm();
    for (const task of tasks) await task;
    expect(await storage.getAlarm()).toBe(hostDue);
    // A later Shiitake timer never postpones the earlier host due time.
    await agent.extensions.call("test-wake", "arm", {
      dueAt: Date.now() + 3_600_000,
    });
    for (const task of tasks) await task;
    expect(await storage.getAlarm()).toBe(hostDue);
    // With no host work left, Shiitake's own timer is the wake.
    hostDue = null;
    await agent.alarm();
    for (const task of tasks) await task;
    expect(await storage.getAlarm()).toBeGreaterThan(Date.now() + 3_000_000);
  } finally {
    for (const task of tasks) await task;
    await agent.close();
    fixture.close();
  }
});

it("runs a host lane through Shiitake's one Watchdog", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const tasks: Promise<unknown>[] = [];
  const storage = cloudflareStorageFixture(fixture.owner);
  const executed: string[] = [];
  let enqueue: (() => void) | undefined;
  let tick: (() => Promise<WatchdogTickResult>) | undefined;
  const agent = await makeCloudflareShiitake(
    { storage, waitUntil: (task) => tasks.push(task) },
    {
      extensions: noExtensions,
      model: fixedModelSource(fauxModel([])),
      instructions: {
        load: () => Promise.resolve({ revision: "host-job-v1", text: "Wait." }),
      },
      tools: [],
      hostWork: [
        {
          pending: () => ({ due: false, nextWakeAt: null }),
          run: async () => {
            if (tick === undefined) throw new Error("Host lane did not open");
            await tick();
          },
        },
      ],
      hostJobs: {
        lane: "post",
        open: ({ transactional, tickConcurrent }) => {
          tick = tickConcurrent;
          enqueue = () => {
            storage.transactionSync(() => {
              transactional.enqueueAcceptedJob({
                jobId: "host-job-1",
                queue: "host-queue",
                lane: "post",
                priority: 0,
                payload: {},
                state: "queued",
              });
            });
          };
          return {
            execute: (job) => {
              executed.push(job.jobId);
              return Promise.resolve({ kind: "completed" });
            },
          };
        },
      },
    },
  );
  try {
    for (const task of tasks) await task;
    if (enqueue === undefined) throw new Error("Host lane did not open");
    enqueue();
    await agent.alarm();
    for (const task of tasks) await task;
    expect(executed).toEqual(["host-job-1"]);
    expect(await storage.getAlarm()).toBeNull();
  } finally {
    await agent.close();
    fixture.close();
  }
});

it("does not disarm a command committed after the current reconciliation inspected pending work", async () => {
  const gate = () => {
    let release: () => void = () => undefined;
    const promise = new Promise<void>((resolve) => {
      release = resolve;
    });
    return { promise, release };
  };
  const idleReached = gate();
  const allowIdle = gate();
  const commandCommitted = gate();
  const allowCommand = gate();
  const fixture = createNativeSqliteOwnerFixture();
  const tasks: Promise<unknown>[] = [];
  const storage = cloudflareStorageFixture(fixture.owner);
  const agent = await makeCloudflareShiitake(
    { storage, waitUntil: (task) => tasks.push(task) },
    {
      extensions: grantAll([wakeExtension]),
      model: fixedModelSource(fauxModel([])),
      instructions: {
        load: () =>
          Promise.resolve({ revision: "stale-idle-v1", text: "Wait." }),
      },
      tools: [],
    },
  );
  let touched: Promise<unknown> | undefined;
  try {
    for (const task of tasks) await task;
    expect(await storage.getAlarm()).toBeNull();

    const transaction = storage.transaction.bind(storage);
    let holdIdle = true;
    let holdCommand = false;
    storage.transaction = async (operation) => {
      // Hold before the idle transaction begins: a real command transaction
      // remains free to commit while this older reconciliation is suspended.
      if (holdIdle) {
        holdIdle = false;
        idleReached.release();
        await allowIdle.promise;
      }
      const result = await transaction(operation);
      // Hold after commit, before the command can request its live wake.
      if (holdCommand) {
        holdCommand = false;
        commandCommitted.release();
        await allowCommand.promise;
      }
      return result;
    };
    await agent.alarm();
    await idleReached.promise;
    const staleReconciliation = tasks.at(-1);
    holdCommand = true;
    touched = agent.extensions.call("test-wake", "arm", { dueAt: 0 });
    await commandCommitted.promise;
    allowIdle.release();
    await staleReconciliation;
    expect(await storage.getAlarm()).not.toBeNull();
  } finally {
    allowIdle.release();
    allowCommand.release();
    await touched;
    for (const task of tasks) await task;
    await agent.close();
    fixture.close();
  }
});

/** A test-only extension whose one Agent command arms a durable timer. */
const wakeExtension = defineExtension({
  id: ExtensionId.make("test-wake"),
  version: "1.0.0",
}).define({
  timers: true,
  commands: {
    specs: {
      arm: {
        scope: "agent",
        input: Schema.Struct({ dueAt: Schema.Number }),
        output: Schema.Struct({}),
      },
    },
    handlers: (ctx) => ({
      arm: ({ input }: { input: { dueAt: number } }) =>
        ctx.timers.set("wake", input.dueAt).pipe(Effect.as({})),
    }),
  },
});

function grantAll(roster: readonly RosteredExtensionV0[]) {
  return {
    roster,
    grants: roster.map(({ manifest }) => ({
      id: manifest.id,
      capabilities: manifest.requests,
    })),
  };
}

it("seeds extension state before cold queued work, retaining work when startup fails", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const tasks: Promise<unknown>[] = [];
  const storage = cloudflareStorageFixture(fixture.owner);
  const state = {
    storage,
    waitUntil: (task: Promise<unknown>) => tasks.push(task),
  };
  const order: string[] = [];
  let queued = false;
  let tick: (() => Promise<WatchdogTickResult>) | undefined;
  const options: CloudflareShiitakeOptions = {
    extensions: grantAll([wakeExtension]),
    model: fixedModelSource(fauxModel([])),
    instructions: {
      load: () => Promise.resolve({ revision: "startup", text: "Wait." }),
    },
    tools: [],
    initialize: () => Promise.reject(new Error("seed unavailable")),
    hostWork: [
      {
        pending: () => ({ due: false, nextWakeAt: null }),
        run: async () => {
          if (tick === undefined) throw new Error("Host lane did not open");
          await tick();
        },
      },
    ],
    hostJobs: {
      lane: "post",
      open: ({ transactional, tickConcurrent }) => {
        tick = tickConcurrent;
        if (!queued) {
          storage.transactionSync(() => {
            transactional.enqueueAcceptedJob({
              jobId: "cold-start-job",
              queue: "cold-start-queue",
              lane: "post",
              priority: 0,
              payload: {},
              state: "queued",
            });
          });
          queued = true;
        }
        return {
          execute: () => {
            order.push("job");
            return Promise.resolve({ kind: "completed" });
          },
        };
      },
    },
  };
  try {
    await expect(makeCloudflareShiitake(state, options)).rejects.toThrow(
      "seed unavailable",
    );
    for (const task of tasks) await task;
    expect(order).toEqual([]);
    const agent = await makeCloudflareShiitake(state, {
      ...options,
      initialize: async (owner) => {
        await owner.extensions.call("test-wake", "arm", {
          dueAt: Date.now() + 60_000,
        });
        order.push("seed");
      },
    });
    try {
      for (const task of tasks) await task;
      expect(order).toEqual(["seed", "job"]);
    } finally {
      await agent.close();
    }
  } finally {
    fixture.close();
  }
});

type HostPending = ReturnType<
  NonNullable<CloudflareShiitakeOptions["hostWork"]>[number]["pending"]
>;

it("runs every host module and preserves the earliest wake through a failed pass", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const tasks: Promise<unknown>[] = [];
  const storage = cloudflareStorageFixture(fixture.owner);
  const now = Date.now();
  let inbox: HostPending = { due: false, nextWakeAt: now + 120_000 };
  let channel: HostPending = { due: false, nextWakeAt: now + 60_000 };
  let failInbox = false;
  let channelPasses = 0;
  const secretMarker = "synthetic-provider-credential-never-log";
  const inboxCause = new Error(`Synthetic inbox I/O failure ${secretMarker}`);
  const inboxFailure = new Error(`Synthetic inbox failure ${secretMarker}`, {
    cause: inboxCause,
  });
  const channelCause = new Error(
    `Synthetic channel I/O failure ${secretMarker}`,
  );
  const channelFailure = new Error(
    `Synthetic channel failure ${secretMarker}`,
    {
      cause: channelCause,
    },
  );
  const logs = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const agent = await makeCloudflareShiitake(
    { storage, waitUntil: (task) => tasks.push(task) },
    {
      extensions: noExtensions,
      model: fixedModelSource(fauxModel([])),
      instructions: {
        load: () =>
          Promise.resolve({ revision: "host-list-v1", text: "Idle." }),
      },
      tools: [],
      hostWork: [
        {
          pending: () => inbox,
          run: () =>
            failInbox ? Promise.reject(inboxFailure) : Promise.resolve(),
        },
        {
          pending: () => channel,
          run: () => {
            channelPasses += 1;
            return failInbox
              ? Promise.reject(channelFailure)
              : Promise.resolve();
          },
        },
      ],
    },
  );
  try {
    for (const task of tasks) await task;
    expect(await storage.getAlarm()).toBe(channel.nextWakeAt);

    failInbox = true;
    const before = channelPasses;
    await agent.alarm();
    for (const task of tasks) await task;
    expect(channelPasses).toBe(before + 1);
    expect(logs).toHaveBeenCalled();
    const reportedValues = logs.mock.calls
      .flatMap((args: unknown[]) => args)
      .filter(Redacted.isRedacted);
    const reportedErrors = reportedValues
      .map(Redacted.value)
      .filter((value): value is Error => value instanceof Error);
    expect(reportedErrors.find((error) => error === inboxFailure)).toBe(
      inboxFailure,
    );
    expect(reportedErrors.find((error) => error === channelFailure)).toBe(
      channelFailure,
    );
    expect(inboxFailure.cause).toBe(inboxCause);
    expect(channelFailure.cause).toBe(channelCause);
    const aggregate = reportedErrors.find(
      (error) => error instanceof AggregateError,
    );
    expect(aggregate?.errors).toEqual([inboxFailure, channelFailure]);
    expect(logs).toHaveBeenCalledWith(
      expect.any(String),
      "shiitake_reconciliation_failed",
      reportedValues.find((value) => Redacted.value(value) === inboxFailure),
      expect.objectContaining({ operation: "host-work.run", moduleIndex: 0 }),
    );
    expect(logs).toHaveBeenCalledWith(
      expect.any(String),
      "shiitake_reconciliation_failed",
      reportedValues.find((value) => Redacted.value(value) === aggregate),
      expect.objectContaining({ operation: "durable-wake.reconcile" }),
    );
    expect(JSON.stringify(logs.mock.calls)).not.toContain(secretMarker);
    expect(inspect(logs.mock.calls, { depth: null })).not.toContain(
      secretMarker,
    );
    expect(
      logs.mock.calls
        .flatMap((args: unknown[]) => args)
        .map(String)
        .join(" "),
    ).not.toContain(secretMarker);
    expect(await storage.getAlarm()).toBeLessThan(now + 60_000);

    failInbox = false;
    channel = { due: false, nextWakeAt: null };
    await agent.alarm();
    for (const task of tasks) await task;
    expect(await storage.getAlarm()).toBe(inbox.nextWakeAt);

    inbox = { due: true, nextWakeAt: null };
    await agent.alarm();
    for (const task of tasks) await task;
    expect(await storage.getAlarm()).not.toBeNull();

    inbox = { due: false, nextWakeAt: null };
    await agent.alarm();
    for (const task of tasks) await task;
    expect(await storage.getAlarm()).toBeNull();
  } finally {
    for (const task of tasks) await task;
    await agent.close();
    fixture.close();
    logs.mockRestore();
  }
});

it("keeps host work on the shared alarm until it is done", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const tasks: Promise<unknown>[] = [];
  const storage = cloudflareStorageFixture(fixture.owner);
  let alarmAt: number | null = null;
  storage.getAlarm = () => Promise.resolve(alarmAt);
  storage.setAlarm = (at) => {
    alarmAt = at instanceof Date ? at.getTime() : at;
    return Promise.resolve();
  };
  storage.deleteAlarm = () => {
    alarmAt = null;
    return Promise.resolve();
  };
  const state: CloudflareShiitakeState = {
    storage,
    waitUntil: (task) => tasks.push(task),
  };
  const retryAt = Date.now() + 60_000;
  let host: HostPending = { due: true, nextWakeAt: null };
  let runs = 0;
  const agent = await makeCloudflareShiitake(state, {
    extensions: noExtensions,
    model: fixedModelSource(fauxModel([])),
    instructions: {
      load: () => Promise.resolve({ revision: "host-work-v1", text: "Idle." }),
    },
    tools: [],
    hostWork: [
      {
        pending: () => host,
        run: () => {
          runs += 1;
          return Promise.resolve();
        },
      },
    ],
  });
  try {
    await Promise.all(tasks);
    expect(runs).toBe(1);
    // Due host work keeps the recovery alarm armed.
    expect(alarmAt).not.toBeNull();

    host = { due: false, nextWakeAt: retryAt };
    await agent.alarm();
    await Promise.all(tasks);
    expect(runs).toBe(2);
    expect(alarmAt).toBe(retryAt);

    host = { due: false, nextWakeAt: null };
    await agent.alarm();
    await Promise.all(tasks);
    expect(alarmAt).toBeNull();
  } finally {
    await agent.close();
    fixture.close();
  }
});

it("shares one protected runtime with HTTP and retries only a failed ACP factory", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const tasks: Promise<unknown>[] = [];
  const state: CloudflareShiitakeState = {
    storage: cloudflareStorageFixture(fixture.owner),
    waitUntil: (task) => {
      tasks.push(task);
    },
  };
  const faux = fauxModel([]);
  let openings = 0;
  class TestShiitakeDO extends ShiitakeOwner<Readonly<Record<string, never>>> {
    protected override shiitakeOptions(): CloudflareShiitakeOptions {
      return {
        extensions: noExtensions,
        model: fixedModelSource(faux),
        instructions: {
          load: () =>
            Promise.resolve({ revision: "handle-proof", text: "Wait." }),
        },
        acp: (agent) => {
          openings += 1;
          if (openings === 1)
            return Promise.reject(new Error("ACP factory unavailable"));
          return Promise.resolve({
            handleRequest: async () => {
              const page = await agent.listSessions();
              return Response.json(page);
            },
          });
        },
      };
    }
    owned() {
      return this.shiitake();
    }
  }
  const host = new TestShiitakeDO(state, {});
  const agent = await host.owned();
  try {
    const id = await agent.createSession("direct-create");
    const response = await host.fetch(
      new Request(`https://host.test/sessions/${id}`),
    );
    expect(parseSessionSnapshot(await response.text()).sessionId).toBe(id);
    expect(openings).toBe(0);
    const request = () => new Request("https://host.test/acp");
    await expect(host.fetch(request())).rejects.toThrow(
      "ACP factory unavailable",
    );
    const responses = await Promise.all([
      host.fetch(request()),
      host.fetch(request()),
    ]);
    expect(responses.every(({ status }) => status === 200)).toBe(true);
    expect(await responses[0].json()).toMatchObject({
      sessions: [{ sessionId: id }],
    });
    expect(openings).toBe(2);
    expect(await host.owned()).toBe(agent);
  } finally {
    for (const task of tasks) await task;
    await agent.close();
    fixture.close();
  }
});

it("shutdown sees acquisition-stage runs", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const tasks: Promise<unknown>[] = [];
  const load = vi.fn(
    ({ signal }: { signal: AbortSignal }) =>
      new Promise<{ revision: string; text: string }>(() => {
        // This host source deliberately ignores cancellation; the runtime
        // still stops the run when acquisition is aborted.
        expect(signal.aborted).toBe(false);
      }),
  );
  const model = fixedModelSource(fauxModel([]));
  const acquire = vi.spyOn(model, "acquire");
  const agent = await makeCloudflareShiitake(
    {
      storage: cloudflareStorageFixture(fixture.owner),
      waitUntil: (task) => {
        tasks.push(task);
      },
    },
    {
      extensions: noExtensions,
      instructions: { load },
      model,
      tools: [],
    },
  );
  try {
    const sessionId = await agent.createSession("shutdown");
    await agent.session(sessionId).command({
      type: "prompt",
      requestId: "shutdown-acquisition",
      delivery: "followUp",
      text: "wait for instructions",
    });
    await vi.waitFor(() => {
      expect(load).toHaveBeenCalledOnce();
    });
    expect(await agent.activeSessions()).toEqual([sessionId]);
    await agent.abortActive();
    await vi.waitFor(async () => {
      expect(await agent.activeSessions()).toEqual([]);
    });
    expect(acquire).not.toHaveBeenCalled();
  } finally {
    await agent.close();
    await Promise.all(tasks);
    fixture.close();
  }
});

it("preserves a failed delivery pass's retry when Agent reconciliation finishes successfully", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const tasks: Promise<unknown>[] = [];
  const storage = cloudflareStorageFixture(fixture.owner);
  let failDelivery = true;
  const logs = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const agent = await makeCloudflareShiitake(
    {
      storage,
      waitUntil: (task) => {
        tasks.push(task);
      },
    },
    {
      extensions: noExtensions,
      model: fixedModelSource(fauxModel([])),
      instructions: {
        load: () =>
          Promise.resolve({ revision: "failed-host-pass", text: "Wait." }),
      },
      tools: [],
      hostWork: [
        {
          pending: () => ({ due: false, nextWakeAt: null }),
          run: () =>
            failDelivery
              ? Promise.reject(
                  new Error("Transient staging failure before durable work"),
                )
              : Promise.resolve(),
        },
      ],
    },
  );
  try {
    for (const task of tasks) await task;
    expect(await storage.getAlarm()).not.toBeNull();
    failDelivery = false;
    await agent.alarm();
    for (const task of tasks) await task;
    expect(await storage.getAlarm()).toBeNull();
  } finally {
    await agent.close();
    logs.mockRestore();
    fixture.close();
  }
});

it("close joins both active passes even when one recovery alarm fails", async () => {
  const gate = () => {
    let release: () => void = () => undefined;
    const promise = new Promise<void>((resolve) => {
      release = resolve;
    });
    return { promise, release };
  };
  const agentEntered = gate();
  const releaseAgent = gate();
  const alarmEntered = gate();
  const failAlarm = gate();
  const fixture = createNativeSqliteOwnerFixture();
  const storage = cloudflareStorageFixture(fixture.owner);
  const tasks: Promise<unknown>[] = [];
  let failDelivery = false;
  let rejectAlarm = false;
  const setAlarm = storage.setAlarm;
  storage.setAlarm = async (at) => {
    if (rejectAlarm) {
      rejectAlarm = false;
      alarmEntered.release();
      await failAlarm.promise;
      throw new Error("Proof recovery alarm unavailable");
    }
    await setAlarm(at);
  };
  const logs = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const faux = fauxModel([]);
  faux.setResponses([fauxAssistantMessage("finished after release")]);
  const agent = await makeCloudflareShiitake(
    {
      storage,
      waitUntil: (task) => {
        tasks.push(task);
      },
    },
    {
      extensions: noExtensions,
      model: fixedModelSource(faux),
      instructions: {
        load: async () => {
          agentEntered.release();
          await releaseAgent.promise;
          return { revision: "held-close", text: "Wait." };
        },
      },
      tools: [],
      hostWork: [
        {
          pending: () => ({ due: false, nextWakeAt: null }),
          run: () => {
            if (failDelivery) {
              rejectAlarm = true;
              return Promise.reject(new Error("Proof host pass unavailable"));
            }
            return Promise.resolve();
          },
        },
      ],
    },
  );
  try {
    for (const task of tasks) await task;
    const baseline = tasks.length;
    failDelivery = true;
    await agent.session("held-close").send("hold", { delivery: "followUp" });
    await Promise.all([agentEntered.promise, alarmEntered.promise]);
    let closeDone = false;
    const closing = agent.close().then(
      () => {
        closeDone = true;
        return "closed";
      },
      () => {
        closeDone = true;
        return "failed";
      },
    );
    await expect
      .poll(() =>
        agent.alarm().then(
          () => false,
          () => true,
        ),
      )
      .toBe(true);
    failAlarm.release();
    // Observe the failed pass itself before checking that close still joins the Agent.
    await expect
      .poll(async () => {
        const outcomes = await Promise.race(
          tasks
            .map((task) =>
              task.then(
                () => "success",
                () => "failed",
              ),
            )
            .filter((_, index) => index >= baseline),
        );
        return outcomes;
      })
      .toBe("failed");
    expect(closeDone).toBe(false);
    releaseAgent.release();
    expect(await closing).toBe("failed");
    await Promise.allSettled(tasks);
  } finally {
    failDelivery = false;
    failAlarm.release();
    releaseAgent.release();
    await agent.close();
    logs.mockRestore();
    fixture.close();
  }
});

it("serves a bounded read-only cold-Session page through the Durable Object owner", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const tasks: Promise<unknown>[] = [];
  const state: CloudflareShiitakeState = {
    storage: cloudflareStorageFixture(fixture.owner),
    waitUntil: (task) => {
      tasks.push(task);
    },
  };
  const faux = fauxModel([]);
  faux.setResponses([fauxAssistantMessage("cold read answered")]);
  const options: CloudflareShiitakeOptions = {
    extensions: noExtensions,
    model: fixedModelSource(faux),
    instructions: {
      load: () =>
        Promise.resolve({ revision: "cold-read-v1", text: "Answer briefly." }),
    },
    tools: [],
  };

  class TestShiitakeDO extends ShiitakeOwner<Readonly<Record<string, never>>> {
    protected override shiitakeOptions(): CloudflareShiitakeOptions {
      return options;
    }
  }

  const durableObject = new TestShiitakeDO(state, {});
  try {
    expect(await durableObject.readColdSessions({})).toEqual({ sessions: [] });
    const response = await durableObject.fetch(
      new Request("http://shiitake.test/sessions/cold-session/command", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          command: {
            requestId: "cold-command",
            delivery: "followUp",
            type: "prompt",
            text: "Fill the store a little.",
          },
        }),
      }),
    );
    expect(response.status).toBe(200);
    for (const task of tasks) await task;

    const page = await durableObject.readColdSessions({ limit: 1 });
    expect(page.sessions).toHaveLength(1);
    expect(page.sessions[0]).toMatchObject({
      sessionId: "cold-session",
      idle: true,
    });
    expect(page.sessions[0]?.bytes).toBeGreaterThan(0);
    await expect(
      durableObject.readColdSessions({ limit: 201 }),
    ).rejects.toMatchObject({ boundary: "contract", cause: "invalid" });
  } finally {
    await durableObject.closeShiitake();
    fixture.close();
  }
});

it("names its Durable Object when it refuses a Session with a broken lineage", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  try {
    Effect.runSync(prepareWoodstockSchema(fixture.owner));
    writeLargeSession(fixture.owner, { sessionId: "damaged", turns: 2 });
    damageOutsideWoodstock(
      fixture.owner,
      "DELETE FROM session_entries WHERE session_id = ? AND sequence = 2",
      "damaged",
    );
    const state: CloudflareShiitakeState = {
      storage: cloudflareStorageFixture(fixture.owner),
      waitUntil: () => undefined,
      id: { toString: () => "agent-do-1" },
    };
    class TestShiitakeDO extends ShiitakeOwner<
      Readonly<Record<string, never>>
    > {
      protected override shiitakeOptions(): CloudflareShiitakeOptions {
        return {
          extensions: noExtensions,
          model: fixedModelSource(fauxModel([])),
          instructions: {
            load: () => Promise.resolve({ revision: "v1", text: "Unused." }),
          },
          tools: [],
        };
      }
    }
    const agent = await new TestShiitakeDO(state, {}).shiitake();
    try {
      await expect(agent.session("damaged").read()).rejects.toThrow(
        'Durable Object agent-do-1: Woodstock Session damaged holds a broken lineage (this store has 1 damaged Session, first damaged); a store never repairs itself — delete this Session with deleteSession("damaged"), or reset the owning store:',
      );
    } finally {
      await agent.close();
    }
  } finally {
    fixture.close();
  }
});
