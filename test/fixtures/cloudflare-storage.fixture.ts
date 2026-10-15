import * as Schema from "effect/Schema";
import type { CloudflareShiitakeState } from "../../src/cloudflare.js";
import type { SqliteOwner } from "../../src/internal/contracts.js";

/** Adapter-call fixture only. Native restart/atomicity proof uses the Worker. */
const decodePages = Schema.decodeUnknownSync(
  Schema.Struct({ bytes: Schema.Number }),
);

export function cloudflareStorageFixture(
  owner: SqliteOwner,
): CloudflareShiitakeState["storage"] & {
  get: (key: string) => Promise<undefined>;
  sql: SqliteOwner["sql"] & { readonly databaseSize: number };
} {
  let alarm: number | null = null;
  let transaction: Promise<unknown> = Promise.resolve();
  return {
    ...owner,
    sql: {
      // Delegate at call time, so a test spying on the owner's `exec` still sees
      // every statement this storage runs.
      exec: (statement, ...bindings) => owner.sql.exec(statement, ...bindings),
      /** SQLite's own page accounting, as the Durable Object reports it. */
      get databaseSize() {
        const pages = decodePages(
          owner.sql
            .exec(
              "SELECT page_count * page_size AS bytes FROM pragma_page_count(), pragma_page_size()",
            )
            .one(),
        );
        return pages.bytes;
      },
    },
    get: () => Promise.resolve(undefined),
    getAlarm: () => Promise.resolve(alarm),
    setAlarm: (at) => {
      alarm = at instanceof Date ? at.getTime() : at;
      return Promise.resolve();
    },
    deleteAlarm: () => {
      alarm = null;
      return Promise.resolve();
    },
    transaction: (operation) => {
      // Native DO transactions serialize the alarm decision with acceptance.
      // Keep a rejected operation visible to its caller without poisoning the queue.
      const current = transaction.then(operation, operation);
      transaction = current;
      return current;
    },
  };
}
