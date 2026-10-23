import { DurableObject } from "cloudflare:workers";

import {
  createWoodstockStorageConformance,
  type WoodstockConformanceCase,
} from "../../src/internal/woodstock/storage-conformance.js";
import type { SqliteOwner } from "../../src/internal/contracts.js";

export type ConformanceEnvironment = {
  HOST: DurableObjectNamespace<ConformanceHost>;
};

export type CaseOutcome =
  | Readonly<{ name: string; ok: true }>
  | Readonly<{ name: string; ok: false; message: string }>;

function conformance(owner: SqliteOwner): readonly WoodstockConformanceCase[] {
  return createWoodstockStorageConformance((use) => {
    use(owner);
    return Promise.resolve();
  });
}

/** One Durable Object per case, so every case starts on empty native storage. */
export class ConformanceHost extends DurableObject<ConformanceEnvironment> {
  names(): readonly string[] {
    return conformance(this.ctx.storage).map(({ name }) => name);
  }

  async runCase(name: string): Promise<CaseOutcome> {
    const found = conformance(this.ctx.storage).find(
      (candidate) => candidate.name === name,
    );
    if (found === undefined)
      return { name, ok: false, message: "Unknown conformance case" };
    try {
      await found.run();
      return { name, ok: true };
    } catch (cause) {
      return {
        name,
        ok: false,
        message: cause instanceof Error ? cause.message : String(cause),
      };
    }
  }
}

export default {
  async fetch(
    request: Request,
    env: ConformanceEnvironment,
  ): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/cases")
      return Response.json(await env.HOST.getByName("names").names());
    const name = url.searchParams.get("case");
    if (url.pathname !== "/run" || name === null)
      return new Response("Not found", { status: 404 });
    return Response.json(await env.HOST.getByName(name).runCase(name));
  },
};
