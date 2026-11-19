import { expect, it } from "vitest";
import {
  makeCloudflareShiitake,
  type CloudflareHostJobs,
} from "../src/cloudflare.js";
import { cloudflareStorageFixture } from "./fixtures/cloudflare-storage.fixture.js";
import {
  compatibilityRunDependencies,
  createNativeSqliteOwnerFixture,
} from "./fixtures/native-sqlite-owner.fixture.js";

function gate() {
  let resolve: () => void = () => undefined;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

it("closes while a host job is running: the interrupted job is handed back queued, not refused as undrained", async () => {
  const fixture = createNativeSqliteOwnerFixture();
  const storage = cloudflareStorageFixture(fixture.owner);
  const tasks: Promise<unknown>[] = [];
  const started = gate();
  let hostRuntime: Parameters<CloudflareHostJobs["open"]>[0] | undefined;
  const agent = await makeCloudflareShiitake(
    {
      storage,
      waitUntil: (task) => {
        tasks.push(task);
      },
    },
    {
      ...compatibilityRunDependencies(),
      hostJobs: {
        lane: "post",
        open: (runtime) => {
          hostRuntime = runtime;
          return {
            // A delivery that ends when the host aborts it, as Local's does.
            execute: (_job, signal) =>
              new Promise((_resolve, reject) => {
                started.resolve();
                signal.addEventListener("abort", () => {
                  reject(new Error("aborted"));
                });
              }),
          };
        },
      },
    },
  );
  try {
    for (const task of tasks) await task;
    if (hostRuntime === undefined) throw new Error("Host lane missing");
    const transactional = hostRuntime.transactional;
    storage.transactionSync(() => {
      transactional.enqueueAcceptedJob({
        jobId: "post-1",
        queue: "post-queue",
        lane: "post",
        state: "queued",
        priority: 0,
        payload: {},
      });
    });
    await agent.alarm();
    await started.promise;
    await expect(agent.close()).resolves.toBeUndefined();
    expect(
      fixture.owner.sql
        .exec("SELECT state FROM watchdog_jobs WHERE job_id = ?", "post-1")
        .one(),
    ).toEqual({ state: "queued" });
  } finally {
    fixture.close();
  }
});
