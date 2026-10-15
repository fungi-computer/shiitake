import { fauxModel } from "@fungi.computer/mule/testing";
import { DurableObject } from "cloudflare:workers";
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import {
  ShiitakeDO,
  type CloudflareShiitakeOptions,
} from "../../src/cloudflare.js";

export type CloudflareDurableWakeObservation = {
  type: string;
  owner: string;
  incarnation: string;
  attempt?: number;
  requestText?: string;
  alarm?: number | null;
  jobs?: { state: string; count: number }[];
};
export type CloudflareDurableWakeEnvironment = {
  TARGET: DurableObjectNamespace<WakeWitness>;
  OBSERVER: DurableObjectNamespace<WitnessObserver>;
};

/** Test observation only; this owner never wakes or calls the target. */
export class WitnessObserver extends DurableObject<CloudflareDurableWakeEnvironment> {
  private releaseGate: (() => void) | undefined;

  async record(event: CloudflareDurableWakeObservation): Promise<void> {
    const events =
      (await this.ctx.storage.get<CloudflareDurableWakeObservation[]>(
        "events",
      )) ?? [];
    await this.ctx.storage.put("events", [...events, event]);
  }

  async beginModel(event: CloudflareDurableWakeObservation): Promise<number> {
    const attempt =
      ((await this.ctx.storage.get<number>("model-attempts")) ?? 0) + 1;
    await this.ctx.storage.put("model-attempts", attempt);
    await this.record({ ...event, attempt });
    return attempt;
  }

  async read(): Promise<CloudflareDurableWakeObservation[]> {
    return (
      (await this.ctx.storage.get<CloudflareDurableWakeObservation[]>(
        "events",
      )) ?? []
    );
  }

  waitForRelease(): Promise<void> {
    return new Promise((resolve) => {
      this.releaseGate = resolve;
    });
  }

  release(): void {
    this.releaseGate?.();
    this.releaseGate = undefined;
  }
}

/** Actual production host and SQL; only its model response is deterministic. */
export class WakeWitness extends ShiitakeDO<CloudflareDurableWakeEnvironment> {
  private readonly incarnation = crypto.randomUUID();
  private readonly owner: string;
  private failNextArm = false;
  private failAlarmDelivery = false;
  private holdAfterCommit = true;

  constructor(
    state: DurableObjectState,
    env: CloudflareDurableWakeEnvironment,
  ) {
    super(state, env);
    state.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS witness_identity (name TEXT NOT NULL)",
    );
    const storedName = state.storage.sql
      .exec<{ name: string }>("SELECT name FROM witness_identity LIMIT 1")
      .toArray()[0]?.name;
    this.owner = storedName ?? state.id.name ?? "unnamed";
    if (storedName === undefined)
      state.storage.sql.exec(
        "INSERT INTO witness_identity VALUES (?)",
        this.owner,
      );
    const observer = env.OBSERVER.getByName(this.owner);
    state.waitUntil(
      observer.record({
        type: "constructed",
        owner: this.owner,
        incarnation: this.incarnation,
      }),
    );

    // Native failure injection at the maintained storage boundary: the real
    // alarm write occurs, then its rejection must roll back both alarm and SQL.
    const setAlarm = state.storage.setAlarm.bind(state.storage);
    state.storage.setAlarm = async (...args) => {
      if (this.failAlarmDelivery) {
        this.failAlarmDelivery = false;
        await observer.record({
          type: "alarm-write-failed",
          owner: this.owner,
          incarnation: this.incarnation,
        });
        throw new Error("Injected native alarm delivery write rejection");
      }
      await setAlarm(...args);
      if (
        this.failNextArm &&
        state.storage.sql
          .exec("SELECT 1 FROM watchdog_jobs WHERE state = 'queued' LIMIT 1")
          .toArray().length !== 0
      ) {
        this.failNextArm = false;
        throw new Error("Injected alarm failure after native write");
      }
    };
    // Pause only the return from the real committed transaction. There is no
    // product hook and no new target request can repair the following kill.
    const transaction = state.storage.transaction.bind(state.storage);
    state.storage.transaction = async (operation) => {
      const result = await transaction(operation);
      if (
        this.owner === "commit-window" &&
        this.holdAfterCommit &&
        state.storage.sql
          .exec("SELECT 1 FROM watchdog_jobs WHERE state = 'queued' LIMIT 1")
          .toArray().length !== 0
      ) {
        this.holdAfterCommit = false;
        await observer.record({
          type: "committed-before-wake",
          owner: this.owner,
          incarnation: this.incarnation,
          alarm: await state.storage.getAlarm(),
        });
        await observer.waitForRelease();
      }
      return result;
    };
  }

  public override async alarm(): Promise<void> {
    const observer = this.env.OBSERVER.getByName(this.owner);
    try {
      await super.alarm();
      await observer.record({
        type: "alarm-returned",
        owner: this.owner,
        incarnation: this.incarnation,
        alarm: await this.ctx.storage.getAlarm(),
      });
    } catch (error) {
      await observer.record({
        type: "alarm-failed",
        owner: this.owner,
        incarnation: this.incarnation,
      });
      throw error;
    }
  }

  public override async fetch(request: Request): Promise<Response> {
    if (new URL(request.url).pathname === "/__witness/duplicate-alarm") {
      await Promise.all([super.alarm(), super.alarm()]);
      return new Response("Delivered twice");
    }
    if (new URL(request.url).pathname === "/__witness/fail-alarm-delivery") {
      this.failAlarmDelivery = true;
      return new Response("Configured");
    }
    if (new URL(request.url).pathname === "/__witness/fail-next-alarm") {
      this.failNextArm = true;
      return new Response("Configured");
    }
    if (new URL(request.url).pathname === "/__witness/state") {
      await this.ctx.storage.sync();
      return Response.json({
        jobs: this.ctx.storage.sql
          .exec("SELECT state, outcome FROM watchdog_jobs ORDER BY sequence")
          .toArray(),
        admissions: this.ctx.storage.sql
          .exec<{ count: number }>(
            "SELECT COUNT(*) AS count FROM session_command_admissions",
          )
          .one().count,
        cancellations: this.ctx.storage.sql
          .exec<{ count: number }>(
            "SELECT COUNT(*) AS count FROM watchdog_jobs WHERE cancellation_id IS NOT NULL",
          )
          .one().count,
        alarm: await this.ctx.storage.getAlarm(),
      });
    }
    return super.fetch(request);
  }

  protected override shiitakeOptions(): CloudflareShiitakeOptions {
    const owner = this.owner;
    const observer = this.env.OBSERVER.getByName(owner);
    const faux = fauxModel([]);
    let requestText = "";
    const respond = async () => {
      // Instrumentation runs in the target before process loss. The runner
      // performs no target storage read during post-restart observation.
      await this.ctx.storage.sync();
      const attempt = await observer.beginModel({
        type: "model-entered",
        requestText,
        owner,
        incarnation: this.incarnation,
        alarm: await this.ctx.storage.getAlarm(),
        jobs: this.ctx.storage.sql
          .exec<{ state: string; count: number }>(
            "SELECT state, COUNT(*) AS count FROM watchdog_jobs GROUP BY state",
          )
          .toArray(),
      });
      // The persisted count lets only the first incarnation block. A fresh
      // instance can return when native wake legitimately reconstructs it.
      if (owner === "lost-wake" && attempt === 1)
        await observer.waitForRelease();
      await observer.record({
        type: "model-returned",
        requestText,
        owner,
        incarnation: this.incarnation,
        attempt,
      });
      return fauxAssistantMessage("native wake witness response");
    };
    faux.setResponses([respond, respond]);
    return {
      extensions: { roster: [], grants: [] },
      model: {
        acquire: ({ signal }) => {
          signal.throwIfAborted();
          return Promise.resolve({
            model: faux.model,
            stream: (model, context, options) => {
              const user = [...context.messages]
                .reverse()
                .find((message) => message.role === "user");
              requestText =
                user?.role === "user"
                  ? Array.isArray(user.content)
                    ? user.content
                        .flatMap((part) =>
                          part.type === "text" ? [part.text] : [],
                        )
                        .join("")
                    : user.content
                  : "";
              return faux.stream(model, context, options);
            },
            release: () => Promise.resolve(),
          });
        },
      },
      instructions: {
        load: () =>
          Promise.resolve({
            revision: "durable-wake-test-v1",
            text: "Return the deterministic test response.",
          }),
      },
      tools: [],
    };
  }
}

function ownerFromPath(pathname: string): string | undefined {
  return /^\/target\/([^/]+)(\/.*)?$/.exec(pathname)?.[1];
}

export default {
  async fetch(
    request: Request,
    env: CloudflareDurableWakeEnvironment,
  ): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/health")
      return Response.json({ service: "shiitake-durable-wake-witness" });
    if (url.pathname === "/observer" && request.method === "GET") {
      const owner = url.searchParams.get("owner");
      if (owner === null) return new Response("Missing owner", { status: 400 });
      return Response.json(await env.OBSERVER.getByName(owner).read());
    }
    const owner = ownerFromPath(url.pathname);
    if (owner === undefined) return new Response("Not found", { status: 404 });
    const targetPath = url.pathname.slice(`/target/${owner}`.length) || "/";
    const targetUrl = new URL(targetPath + url.search, "https://wake.test");
    const init: RequestInit = {
      method: request.method,
      headers: request.headers,
    };
    if (request.method !== "GET" && request.method !== "HEAD")
      init.body = request.body;
    const response = await env.TARGET.getByName(owner).fetch(
      new Request(targetUrl, init),
    );
    return request.headers.get("x-witness-drop-ack") === "true"
      ? new Response("Injected lost acknowledgement", { status: 503 })
      : response;
  },
};
