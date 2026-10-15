import { fauxModel } from "@fungi.computer/mule/testing";
import { rejectingSandbox } from "@fungi.computer/mycelium/testing";
import * as Schema from "effect/Schema";
import { sqliteRowSchema } from "@fungi.computer/shiitake";
import { DatabaseSync, type SQLOutputValue } from "node:sqlite";

import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";

import type {
  RuntimeHost,
  SqliteCursor,
  SqliteOwner,
  SqliteValue,
} from "@fungi.computer/shiitake";

const decodeSqliteRow = Schema.decodeUnknownSync(sqliteRowSchema);

type NativeOutput = null | number | bigint | string | Uint8Array;
type NativeInput = null | number | string | Uint8Array;

type CompatibilityRuntimeState = "idle" | "pending" | "running" | "closed";

/** Manual public runtime host for deterministic Runtime Core laws. */
export function createCompatibilityRuntimeHost() {
  let state: CompatibilityRuntimeState = "idle";
  let closed = false;
  let rerunRequested = false;
  let reconcile: (() => Promise<void>) | undefined;
  let runningCallback: Promise<void> | undefined;
  let binds = 0;
  let wakes = 0;
  let closes = 0;
  let reconciles = 0;
  let coalescedWakes = 0;

  const runtime: RuntimeHost = {
    bind: (input) => {
      binds += 1;
      reconcile = input.reconcile;
      return Promise.resolve({
        commit: (mutation) => mutation(),
        requestWake: () => {
          wakes += 1;
          if (closed)
            return Promise.reject(new Error("Compatibility runtime is closed"));
          if (state === "idle") state = "pending";
          else if (state === "pending") coalescedWakes += 1;
          else rerunRequested = true;
          return Promise.resolve();
        },
        close: async () => {
          closes += 1;
          closed = true;
          state = "closed";
          rerunRequested = false;
          await runningCallback;
        },
      });
    },
  };

  return {
    runtime,
    snapshot: () => ({
      state,
      rerunRequested,
      binds,
      wakes,
      closes,
      reconciles,
      coalescedWakes,
    }),
    runNext: async () => {
      if (state !== "pending")
        throw new Error("Compatibility runtime has no pending callback");
      const callback = reconcile;
      if (callback === undefined)
        throw new Error("Compatibility runtime has no reconciler");
      state = "running";
      const operation = (async () => {
        try {
          reconciles += 1;
          await callback();
        } finally {
          if (!closed) {
            if (rerunRequested) {
              rerunRequested = false;
              state = "pending";
            } else state = "idle";
          }
        }
      })();
      runningCallback = operation;
      try {
        await operation;
      } finally {
        if (runningCallback === operation) runningCallback = undefined;
      }
    },
  };
}

/** A host that rosters no extensions. */
export const noExtensions = Object.freeze({
  roster: Object.freeze([]),
  grants: Object.freeze([]),
});

/** Successful deterministic dependencies for extension/runtime laws. */
export function compatibilityRunDependencies() {
  return {
    extensions: noExtensions,
    instructions: {
      load: () =>
        Promise.resolve({
          revision: "compatibility-instructions-v1",
          text: "Complete this deterministic run without direct tools.",
        }),
    },
    model: {
      acquire: () => {
        const faux = fauxModel([]);
        faux.setResponses([fauxAssistantMessage("complete")]);
        return Promise.resolve({
          model: faux.model,
          stream: faux.stream,
          release: () => Promise.resolve(),
        });
      },
    },
    tools: {
      modules: [],
      sandbox: rejectingSandbox("Compatibility run has no direct tools"),
    },
  };
}

function inputValue(value: SqliteValue): NativeInput {
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  return value;
}

function outputValue(value: NativeOutput): SqliteValue {
  if (value === null) return value;
  if (value instanceof Uint8Array) return new Uint8Array(value).slice().buffer;
  if (Object.prototype.toString.call(value) === "[object Number]")
    return Number(value);
  if (Object.prototype.toString.call(value) === "[object String]")
    return String(value);
  return Number(value);
}

function outputRow(row: Record<string, NativeOutput>) {
  return Object.fromEntries(
    Object.entries(row).map(([key, value]) => [key, outputValue(value)]),
  );
}

/** Decode Node SQLite output with the same row contract used by the native owner. */
export function decodeNativeSqliteRow(row: Record<string, SQLOutputValue>) {
  return decodeSqliteRow(outputRow(row));
}

export type NativeSqliteOwnerFixture = Readonly<{
  owner: SqliteOwner;
  ownerOperations: () => number;
  ownerWrites: () => number;
  close: () => void;
}>;

/** Real node:sqlite owner exposing only the Workers-shaped owner capability. */
export function createNativeSqliteOwnerFixture(
  databasePath = ":memory:",
): NativeSqliteOwnerFixture {
  const database = new DatabaseSync(databasePath);
  database.exec("PRAGMA foreign_keys = ON");
  let ownerOperationCount = 0;
  let ownerWriteCount = 0;

  const sql = {
    exec: (statement: string, ...bindings: SqliteValue[]): SqliteCursor => {
      ownerOperationCount += 1;
      const reader = /^\s*(?:SELECT|PRAGMA|WITH)\b/iu.test(statement);
      if (!reader) ownerWriteCount += 1;
      const prepared = database.prepare(statement);
      const normalizedBindings = bindings.map(inputValue);
      const rows = reader
        ? prepared.all(...normalizedBindings).map(outputRow)
        : (prepared.run(...normalizedBindings), []);
      return {
        toArray: () => rows.map((row) => decodeSqliteRow(row)),
        one: () => {
          const row = rows[0];
          if (row === undefined) throw new Error("Expected one SQLite row");
          return decodeSqliteRow(row);
        },
      };
    },
  };

  const owner: SqliteOwner = {
    sql,
    transactionSync: <A>(operation: () => A): A => {
      ownerOperationCount += 1;
      database.exec("BEGIN");
      try {
        const result = operation();
        database.exec("COMMIT");
        return result;
      } catch (error) {
        database.exec("ROLLBACK");
        throw error;
      }
    },
  };

  return {
    owner,
    ownerOperations: () => ownerOperationCount,
    ownerWrites: () => ownerWriteCount,
    close: () => {
      database.close();
    },
  };
}
