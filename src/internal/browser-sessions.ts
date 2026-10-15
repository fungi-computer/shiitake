import type { ModuleRegistration } from "@fungi.computer/mycelium";

/** The maintained browser transport and its Session-bound tool descriptors. */
export type BrowserHost = Readonly<{
  /** Mycelium tools bound to this host's Session and call-time gates. */
  modules: readonly ModuleRegistration[];
  /** Forward one browser events, claims or results request to the maintained host. */
  fetch: (request: Request) => Promise<Response>;
}>;
type SessionBrowserHost = {
  host: BrowserHost;
  activeRuns: number;
  browserConnections: number;
  connections: Set<() => Promise<void>>;
};
export type BrowserLease = Readonly<{ host: BrowserHost; release: () => void }>;

/** One ephemeral browser host per Session, borrowed by run and SSE scopes. */
export class BrowserSessions {
  private generation = 0;
  private closed = false;
  private readonly hosts = new Map<string, SessionBrowserHost>();
  constructor(
    private readonly open: (sessionId: string) => BrowserHost,
    private readonly hasSession: (sessionId: string) => Promise<boolean>,
  ) {}
  async clear(): Promise<void> {
    this.generation += 1;
    const old = [...this.hosts.values()];
    this.hosts.clear();
    await Promise.all(
      old.flatMap((entry) => [...entry.connections].map((close) => close())),
    );
  }
  async close(): Promise<void> {
    this.closed = true;
    await this.clear();
  }
  async fetch(request: Request): Promise<Response> {
    if (this.closed) return new Response(null, { status: 410 });
    const generation = this.generation;
    const path = new URL(request.url).pathname;
    const browserRoute =
      /^\/browser\/sessions\/([^/]+)\/browser\/(events|claims|results)$/u.exec(
        path,
      );
    if (browserRoute === null)
      return new Response("Not found", { status: 404 });
    let sessionId: string;
    try {
      sessionId = decodeURIComponent(browserRoute[1] ?? "");
    } catch {
      return new Response("Not found", { status: 404 });
    }
    const route = browserRoute[2];
    if (!sessionId || !route) return new Response("Not found", { status: 404 });
    let entry = this.hosts.get(sessionId);
    if (entry === undefined) {
      if (route !== "events") return new Response("Not found", { status: 404 });
      if (!(await this.hasSession(sessionId)))
        return new Response("Not found", { status: 404 });
      if (generation !== this.generation)
        return new Response(null, { status: 404 });
      entry = this.hosts.get(sessionId) ?? this.createHost(sessionId);
    }
    const target = new URL(request.url);
    target.pathname = `/browser/${route}`;
    if (route !== "events")
      return entry.host.fetch(new Request(target, request));
    return this.openEvents(
      new Request(target, request),
      sessionId,
      entry,
      generation,
    );
  }

  /** Own the async transport opening until its stream joins the connection scope. */
  private async openEvents(
    request: Request,
    sessionId: string,
    entry: SessionBrowserHost,
    generation: number,
  ): Promise<Response> {
    entry.browserConnections += 1;
    try {
      const response = await entry.host.fetch(request);
      if (generation !== this.generation) {
        // The maintained transport may finish opening after teardown. Its
        // late scope belongs to the old generation and must never escape.
        await response.body?.cancel();
        this.releaseBrowserConnection(sessionId, entry);
        return new Response(null, { status: 404 });
      }
      if (!response.ok || response.body === null) {
        this.releaseBrowserConnection(sessionId, entry);
        return response;
      }
      return this.trackBrowserConnection(response, sessionId, entry);
    } catch (error) {
      this.releaseBrowserConnection(sessionId, entry);
      throw error;
    }
  }
  private createHost(sessionId: string): SessionBrowserHost {
    const entry: SessionBrowserHost = {
      host: this.open(sessionId),
      activeRuns: 0,
      browserConnections: 0,
      connections: new Set(),
    };
    this.hosts.set(sessionId, entry);
    return entry;
  }

  acquire(sessionId: string): BrowserLease {
    if (this.closed) throw new Error("Browser host is closed");
    const entry = this.hosts.get(sessionId) ?? this.createHost(sessionId);
    entry.activeRuns += 1;
    let released = false;
    return {
      host: entry.host,
      release: () => {
        if (released) return;
        released = true;
        entry.activeRuns -= 1;
        this.releaseIdleHost(sessionId, entry);
      },
    };
  }

  private releaseBrowserConnection(
    sessionId: string,
    entry: SessionBrowserHost,
  ): void {
    entry.browserConnections -= 1;
    this.releaseIdleHost(sessionId, entry);
  }

  private releaseIdleHost(sessionId: string, entry: SessionBrowserHost): void {
    if (
      entry.activeRuns === 0 &&
      entry.browserConnections === 0 &&
      this.hosts.get(sessionId) === entry
    )
      this.hosts.delete(sessionId);
  }

  private trackBrowserConnection(
    response: Response,
    sessionId: string,
    entry: SessionBrowserHost,
  ): Response {
    const reader: ReadableStreamDefaultReader<Uint8Array> | undefined =
      response.body?.getReader();
    if (reader === undefined) {
      this.releaseBrowserConnection(sessionId, entry);
      return response;
    }
    let released = false;
    let controller: ReadableStreamDefaultController<Uint8Array>;
    const close = async () => {
      if (released) return;
      release();
      controller.close();
      await reader.cancel();
    };
    const release = () => {
      if (released) return;
      released = true;
      entry.connections.delete(close);
      this.releaseBrowserConnection(sessionId, entry);
    };
    entry.connections.add(close);
    const body = new ReadableStream<Uint8Array>({
      start: (streamController) => {
        controller = streamController;
      },
      pull: async (controller) => {
        try {
          const next = await reader.read();
          if (released) return;
          if (next.done) {
            controller.close();
            release();
          } else controller.enqueue(next.value);
        } catch (error) {
          if (released) return;
          controller.error(error);
          release();
        }
      },
      cancel: async (reason) => {
        release();
        await reader.cancel(reason);
      },
    });
    return new Response(body, {
      status: response.status,
      statusText: response.statusText,
      headers: response.headers,
    });
  }
}
