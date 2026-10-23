/** One Session as the store keeper sees it: what it holds and how cold it is. */
export type ColdSessionRow = Readonly<{
  sessionId: string;
  /**
   * Bytes of the Session's stored payloads: conversation entries, accepted
   * commands, open attention and tool grants. Measured from the stored rows
   * on every read, never from a counter.
   */
  bytes: number;
  /** Milliseconds since the epoch of the Session's latest conversation or run activity. */
  lastActivityAt: number;
  /**
   * True only when no run is queued or live and no extension attention waits
   * on a person: the Session is settled. Anything else is not idle.
   */
  idle: boolean;
}>;

/** One bounded page of Sessions, coldest first. */
export type ColdSessionPage = Readonly<{
  sessions: readonly ColdSessionRow[];
  /** Pass as `after` to read the next page; absent on the last page. */
  next?: string;
}>;

/** A page request: `limit` is a whole number from 1 to 200 (default 50). */
export type ColdSessionPageRequest = Readonly<{
  limit?: number;
  after?: string;
}>;
