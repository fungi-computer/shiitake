import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { createServer } from "node:net";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { fileURLToPath, URL } from "node:url";
import process from "node:process";
import console from "node:console";
import { setTimeout as delay } from "node:timers/promises";
import { parseAccepted, parseSessionSnapshot } from "../dist/client.js";

const { fetch, AbortSignal, Headers, DOMException } = globalThis;

const root = fileURLToPath(new URL("../../..", import.meta.url));
const evidence = await mkdtemp(
  resolve(root, ".botanical/shiitake-durable-wake-restart-"),
);
const persistence = resolve(evidence, "sqlite");
const temporary = await mkdtemp(resolve(tmpdir(), "shiitake-durable-wake-"));
const configPath = resolve(temporary, "wrangler.json");
const reservation = createServer();
await new Promise((yes, no) => {
  reservation.once("error", no);
  reservation.listen(0, "127.0.0.1", yes);
});
const port = reservation.address().port;
await new Promise((yes, no) =>
  reservation.close((error) => (error ? no(error) : yes())),
);
const endpoint = `http://127.0.0.1:${port}`;
await mkdir(persistence, { recursive: true });
await writeFile(
  configPath,
  JSON.stringify({
    name: "shiitake-durable-wake-witness",
    main: resolve(
      root,
      "packages/shiitake/test/fixtures/cloudflare-durable-wake.worker.ts",
    ),
    compatibility_date: "2026-09-02",
    compatibility_flags: ["nodejs_compat"],
    durable_objects: {
      bindings: [
        { name: "TARGET", class_name: "WakeWitness" },
        { name: "OBSERVER", class_name: "WitnessObserver" },
      ],
    },
    migrations: [
      {
        tag: "wake-witness",
        new_sqlite_classes: ["WakeWitness", "WitnessObserver"],
      },
    ],
  }),
  { mode: 0o600 },
);

const sourcePaths = [
  "packages/shiitake/src/cloudflare.ts",
  "packages/shiitake/src/internal/runtime-coordinator.ts",
  "packages/shiitake/src/internal/coordinator.ts",
  "packages/shiitake/src/internal/effect-contracts.ts",
  "packages/watchdog/src/effect.ts",
  "packages/watchdog/src/internal/sqlite-store.ts",
  "packages/shiitake/test/cloudflare-durable-wake.native.mjs",
  "packages/shiitake/test/fixtures/cloudflare-durable-wake.worker.ts",
];
const sources = Object.fromEntries(
  await Promise.all(
    sourcePaths.map(async (path) => [
      path,
      createHash("sha256")
        .update(await readFile(resolve(root, path)))
        .digest("hex"),
    ]),
  ),
);
const receipt = {
  status: "running",
  revision: execFileSync("git", ["rev-parse", "HEAD"], {
    cwd: root,
    encoding: "utf8",
  }).trim(),
  dirty: execFileSync("git", ["status", "--short"], {
    cwd: root,
    encoding: "utf8",
  }).trim(),
  sources,
  endpoint,
  persistence,
  evidence,
  checks: [],
  failures: [],
  processes: [],
};
let server;
let afterRestart = false;
let recoveryProved = false;
let accepted;
let entered;
let firstIncarnation;
let observationStartedAt;
let observationEndedAt;
let windowRequest;
let windowCommitted;
const latestObserverEvents = {};
const targetCalls = {
  beforeRestart: 0,
  afterRestart: 0,
  afterRecoveryProof: 0,
};

function fetchTarget(url, options) {
  targetCalls[
    recoveryProved
      ? "afterRecoveryProof"
      : afterRestart
        ? "afterRestart"
        : "beforeRestart"
  ] += 1;
  return fetch(url, options);
}

async function start() {
  const environment = Object.fromEntries(
    ["PATH", "HOME", "USER", "TMPDIR", "LD_LIBRARY_PATH"]
      .filter((key) => process.env[key])
      .map((key) => [key, process.env[key]]),
  );
  Object.assign(environment, { CI: "true", WRANGLER_SEND_METRICS: "false" });
  const child = spawn(
    process.execPath,
    [
      resolve(root, "node_modules/wrangler/bin/wrangler.js"),
      "dev",
      "--config",
      configPath,
      "--ip",
      "127.0.0.1",
      "--port",
      String(port),
      "--inspector-port",
      "0",
      "--local",
      "--show-interactive-dev-session=false",
      "--persist-to",
      persistence,
    ],
    {
      cwd: root,
      env: environment,
      detached: true,
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  const entry = { pid: child.pid, log: "", exit: null };
  const exited = new Promise((yes, no) => {
    child.once("error", no);
    child.once("exit", (code, signal) => {
      entry.exit = { code, signal };
      yes();
    });
  });
  child.stdout.on("data", (data) => (entry.log += data));
  child.stderr.on("data", (data) => (entry.log += data));
  server = { child, entry, exited };
  receipt.processes.push(entry);
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    assert.equal(entry.exit, null, "Owned Wrangler exited before readiness");
    const response = await fetch(`${endpoint}/health`, {
      signal: AbortSignal.timeout(1000),
    }).catch((cause) => {
      if (
        !(
          cause instanceof TypeError &&
          cause.cause instanceof Error &&
          "code" in cause.cause &&
          cause.cause.code === "ECONNREFUSED"
        ) &&
        !(cause instanceof DOMException && cause.name === "TimeoutError")
      )
        throw cause;
      entry.lastReadinessFailure = cause.name;
      return null;
    });
    if (response?.ok) {
      assert.deepEqual(await response.json(), {
        service: "shiitake-durable-wake-witness",
      });
      return;
    }
    await delay(100);
  }
  throw new Error("Owned Wrangler readiness timeout");
}

async function stop(abrupt = false) {
  if (!server) return;
  const { child, entry, exited } = server;
  for (const signal of abrupt
    ? ["SIGKILL"]
    : ["SIGINT", "SIGTERM", "SIGKILL"]) {
    if (entry.exit) break;
    process.kill(-child.pid, signal);
    await Promise.race([exited, delay(5000, undefined, { ref: false })]);
  }
  await exited;
  for (let attempt = 0; attempt < 50; attempt++) {
    const response = await fetch(`${endpoint}/health`, {
      signal: AbortSignal.timeout(300),
    }).catch((cause) => {
      const code =
        cause instanceof TypeError &&
        cause.cause instanceof Error &&
        "code" in cause.cause
          ? cause.cause.code
          : undefined;
      if (code === "ECONNREFUSED") return null;
      // A pooled socket can reset during SIGKILL. Retry it; only a refused
      // fresh connection proves that the listener has closed.
      if (
        code === "ECONNRESET" ||
        code === "UND_ERR_SOCKET" ||
        (cause instanceof DOMException && cause.name === "TimeoutError")
      ) {
        entry.lastShutdownRetry = cause.name;
        return undefined;
      }
      throw cause;
    });
    if (response === null) {
      entry.listenerClosed = true;
      break;
    }
    await delay(100);
  }
  assert.equal(entry.listenerClosed, true, "Owned Wrangler listener closed");
  server = undefined;
}

async function readObserver(owner) {
  const response = await fetch(
    `${endpoint}/observer?owner=${encodeURIComponent(owner)}`,
    { signal: AbortSignal.timeout(5000) },
  );
  assert.equal(response.status, 200);
  const events = await response.json();
  latestObserverEvents[owner] = events;
  return events;
}

async function until(predicate, label) {
  const deadline = Date.now() + 20_000;
  while (Date.now() < deadline) {
    const result = await predicate();
    if (result) return result;
    await delay(100);
  }
  throw new Error(`Timed out: ${label}`);
}

async function command(
  owner,
  requestId,
  dropAck = false,
  expectedStatus = 200,
) {
  const headers = new Headers({ "content-type": "application/json" });
  if (dropAck) headers.set("x-witness-drop-ack", "true");
  const response = await fetchTarget(
    `${endpoint}/target/${encodeURIComponent(owner)}/sessions/witness/command`,
    {
      method: "POST",
      headers,
      body: JSON.stringify({
        command: {
          delivery: "followUp",

          type: "prompt",
          requestId,
          text: `Run the native witness ${requestId}.`,
        },
      }),
      signal: AbortSignal.timeout(10_000),
    },
  );
  if (dropAck) {
    assert.equal(response.status, 503);
    return undefined;
  }
  assert.equal(response.status, expectedStatus);
  if (expectedStatus !== 200) return response.json();
  return parseAccepted(await response.text());
}

try {
  await start();
  const control = await command("control", "control-command");
  await until(
    async () =>
      (await readObserver("control")).some(
        (event) => event.type === "model-returned",
      ),
    "control model response",
  );
  // The positive control proves ordinary request/response behavior before loss.
  // It is the only target polling in this witness.
  await until(async () => {
    const response = await fetchTarget(
      `${endpoint}/target/control/sessions/witness`,
      { signal: AbortSignal.timeout(5000) },
    );
    return parseSessionSnapshot(await response.text()).messages.some(
      (message) => message.value.role === "assistant",
    );
  }, "control durable assistant");
  receipt.checks.push("positive control completes through target request");

  // The real native transaction must roll back the accepted SQL and its alarm
  // together if the storage-boundary write fails before commit.
  await fetchTarget(`${endpoint}/target/atomic-failure/sessions/witness`);
  await until(async () => {
    const response = await fetchTarget(
      `${endpoint}/target/atomic-failure/__witness/state`,
    );
    return (await response.json()).alarm === null;
  }, "empty owner disarms");
  await fetchTarget(
    `${endpoint}/target/atomic-failure/__witness/fail-next-alarm`,
  );
  const rejected = await command(
    "atomic-failure",
    "rollback-command",
    false,
    503,
  );
  const rollbackResponse = await fetchTarget(
    `${endpoint}/target/atomic-failure/__witness/state`,
  );
  const rollback = await rollbackResponse.json();
  assert.equal(rollback.admissions, 0);
  assert.deepEqual(rollback.jobs, []);
  assert.equal(rollback.alarm, null);
  receipt.rollback = { rejected, rollback };
  await command("atomic-failure", "rollback-command");
  await until(
    async () =>
      (await readObserver("atomic-failure")).some(
        (event) => event.type === "model-returned",
      ),
    "retry after rejected alarm",
  );
  receipt.checks.push(
    "native transaction rolls back acceptance and alarm; same command retries after failure",
  );

  const lostWake = await command("lost-wake", "lost-wake-command");
  entered = await until(
    async () =>
      (await readObserver("lost-wake")).find(
        (event) => event.type === "model-entered",
      ),
    "gated live model entry",
  );
  assert.deepEqual(entered.jobs, [{ state: "running", count: 1 }]);
  assert.ok(
    Number.isFinite(entered.alarm),
    "Running work retains a native recovery alarm",
  );
  // Accepted turns recover through the maintained bounded model policy. The
  // second command stays queued until the first resumes and settles.
  await command("lost-wake", "queued-command", true);
  const queued = await command("lost-wake", "queued-command");
  const duplicate = await command("lost-wake", "queued-command");
  assert.deepEqual(duplicate, queued);
  receipt.checks.push(
    "lost acceptance acknowledgement replays the exact command",
  );
  await fetchTarget(`${endpoint}/target/lost-wake/__witness/duplicate-alarm`);
  await fetchTarget(
    `${endpoint}/target/lost-wake/__witness/fail-alarm-delivery`,
  );
  const retriedAlarm = await until(async () => {
    const events = await readObserver("lost-wake");
    const failedAt = events.findIndex(
      (event) => event.type === "alarm-write-failed",
    );
    return (
      failedAt >= 0 &&
      events
        .slice(failedAt + 1)
        .find((event) => event.type === "alarm-returned")
    );
  }, "native alarm rejection retries and rearms while execution is live");
  assert.ok(Number.isFinite(retriedAlarm.alarm));
  assert.equal(
    (await readObserver("lost-wake")).filter(
      (event) => event.type === "alarm-write-failed",
    ).length,
    1,
    "the actual native alarm write was rejected once",
  );
  assert.equal(
    (await readObserver("lost-wake")).filter(
      (event) => event.type === "model-entered",
    ).length,
    1,
  );
  receipt.checks.push(
    "duplicate alarm and native alarm retry preserve one live execution and rearm",
  );
  firstIncarnation = entered.incarnation;
  accepted = { control, lostWake, queued, duplicate };
  await writeFile(
    resolve(evidence, "pre-loss.json"),
    JSON.stringify(
      {
        accepted,
        entered,
        observer: latestObserverEvents["lost-wake"] ?? [],
        targetCalls,
      },
      null,
      2,
    ),
  );

  windowRequest = command("commit-window", "window-command").then(
    (value) => ({ value }),
    (error) => ({
      error: error instanceof Error ? error.message : String(error),
    }),
  );
  windowCommitted = await until(
    async () =>
      (await readObserver("commit-window")).find(
        (event) => event.type === "committed-before-wake",
      ),
    "native commit before live wake and acknowledgement",
  );
  assert.ok(Number.isFinite(windowCommitted.alarm));

  // SIGKILL bypasses evictDurableObject's active-reference wait. This is the
  // owned Wrangler process loss used by the maintained Game proof.
  await stop(true);
  receipt.windowRequest = await windowRequest;
  receipt.windowCommitted = windowCommitted;
  receipt.checks.push("owned Wrangler process killed with active executor");
  await start();
  afterRestart = true;
  observationStartedAt = Date.now();

  // /health is Worker-level readiness. From here on only the observer is read;
  // the target is never fetched, inspected, replayed, or warmed.
  const observed = await until(async () => {
    const events = await readObserver("lost-wake");
    const returned = events.filter(
      (event) =>
        event.type === "model-returned" &&
        event.incarnation !== firstIncarnation,
    );
    assert.ok(returned.length <= 2, "exactly two accepted turns recover");
    return returned.length === 2 ? events : false;
  }, "request-free native recovery");
  const windowRecovered = await until(
    async () =>
      (await readObserver("commit-window")).find(
        (event) =>
          event.type === "model-returned" &&
          event.incarnation !== windowCommitted.incarnation,
      ),
    "request-free commit-window recovery",
  );
  assert.equal(
    windowRecovered.requestText,
    "Run the native witness window-command.",
  );
  receipt.windowRecovered = windowRecovered;
  observationEndedAt = Date.now();
  const recovered = observed.some(
    (event) =>
      event.type === "model-returned" && event.incarnation !== firstIncarnation,
  );
  const oldExecutorReturned = observed.some(
    (event) =>
      event.type === "model-returned" && event.incarnation === firstIncarnation,
  );
  const result = {
    scope:
      "native process restart: recovered running turn and completed queued command",
    receipt: accepted,
    entered,
    observed,
    recovered,
    oldExecutorReturned,
    targetCalls,
    observationStartedAt,
    observationEndedAt,
    observationDurationMs: observationEndedAt - observationStartedAt,
    observerReadsAfterRestart: true,
  };
  await writeFile(
    resolve(evidence, "result.json"),
    JSON.stringify(result, null, 2),
  );
  assert.equal(oldExecutorReturned, false);
  assert.equal(recovered, true);
  assert.equal(targetCalls.afterRestart, 0);
  const resumed = observed.filter(
    (event) =>
      event.type === "model-returned" && event.incarnation !== firstIncarnation,
  );
  assert.equal(resumed.length, 2);
  assert.deepEqual(
    resumed.map((event) => event.requestText),
    [
      "Run the native witness lost-wake-command.",
      "Run the native witness queued-command.",
    ],
  );
  recoveryProved = true;
  // Recovery above is already established without target traffic. This read
  // separately checks final owner truth; it is not a wake stimulus for that claim.
  const finalState = await until(async () => {
    const response = await fetchTarget(
      `${endpoint}/target/lost-wake/__witness/state`,
    );
    const state = await response.json();
    return state.jobs.length === 2 &&
      state.jobs.every((job) => job.state === "settled") &&
      state.alarm === null
      ? state
      : false;
  }, "settled owner disarms after request-free recovery");
  assert.deepEqual(
    finalState.jobs.map(({ state, outcome }) => ({ state, outcome })),
    [
      { state: "settled", outcome: "completed" },
      { state: "settled", outcome: "completed" },
    ],
  );
  assert.equal(finalState.cancellations, 0);
  const snapshotResponse = await fetchTarget(
    `${endpoint}/target/lost-wake/sessions/witness`,
  );
  const snapshot = parseSessionSnapshot(await snapshotResponse.text());
  assert.deepEqual(
    snapshot.terminals.map(({ commandId, outcome }) => ({
      commandId,
      outcome,
    })),
    [
      { commandId: lostWake.commandId, outcome: "completed" },
      { commandId: queued.commandId, outcome: "completed" },
    ],
  );
  receipt.finalState = { ...finalState, snapshot };
  receipt.checks.push(
    "running turn resumes once and queued work completes after request-free wake, without invented cancellation",
  );
  receipt.status = "passed";
} catch (error) {
  observationEndedAt ??= Date.now();
  receipt.status = "failed";
  receipt.failures.push({
    stage: "durable-wake-proof",
    errorKind: error instanceof Error ? error.name : "NonErrorThrown",
  });
  receipt.witness = {
    accepted,
    entered,
    observer: latestObserverEvents["lost-wake"] ?? [],
    targetCalls,
    observationStartedAt,
    observationEndedAt,
    observationDurationMs:
      observationStartedAt === undefined
        ? null
        : observationEndedAt - observationStartedAt,
  };
  await writeFile(
    resolve(evidence, "result.json"),
    JSON.stringify(receipt, null, 2),
  );
  process.exitCode = 1;
} finally {
  try {
    await stop();
  } catch (error) {
    receipt.status = "failed";
    receipt.failures.push({
      stage: "durable-wake-cleanup",
      errorKind: error instanceof Error ? error.name : "NonErrorThrown",
    });
    process.exitCode = 1;
  }
  for (const [index, entry] of receipt.processes.entries()) {
    await writeFile(resolve(evidence, `wrangler-${index}.log`), entry.log);
    delete entry.log;
  }
  await rm(temporary, { recursive: true, force: true });
  await writeFile(
    resolve(evidence, "receipt.json"),
    JSON.stringify(receipt, null, 2),
  );
  console.log(JSON.stringify(receipt));
}
