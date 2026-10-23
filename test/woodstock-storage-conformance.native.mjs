/** Run Woodstock's storage conformance cases on native Durable Object SQLite. */
import assert from "node:assert/strict";
import { log } from "node:console";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { fileURLToPath, URL } from "node:url";
import { unstable_dev } from "wrangler";
import * as z from "zod";

const outcome = z.discriminatedUnion("ok", [
  z.strictObject({ name: z.string(), ok: z.literal(true) }),
  z.strictObject({
    name: z.string(),
    ok: z.literal(false),
    message: z.string(),
  }),
]);
const temporary = await mkdtemp(resolve(tmpdir(), "woodstock-conformance-"));
const config = resolve(temporary, "wrangler.json");
await writeFile(
  config,
  JSON.stringify({
    name: "woodstock-storage-conformance",
    main: fileURLToPath(
      new URL(
        "./fixtures/woodstock-storage-conformance.worker.ts",
        import.meta.url,
      ),
    ),
    compatibility_date: "2026-09-02",
    compatibility_flags: ["nodejs_compat"],
    durable_objects: {
      bindings: [{ name: "HOST", class_name: "ConformanceHost" }],
    },
    migrations: [
      { tag: "conformance", new_sqlite_classes: ["ConformanceHost"] },
    ],
  }),
);
const worker = await unstable_dev(
  fileURLToPath(
    new URL(
      "./fixtures/woodstock-storage-conformance.worker.ts",
      import.meta.url,
    ),
  ),
  {
    config,
    local: true,
    ip: "127.0.0.1",
    port: 0,
    logLevel: "none",
    persist: false,
    experimental: {
      disableExperimentalWarning: true,
      disableDevRegistry: true,
    },
  },
);
try {
  const names = z
    .array(z.string())
    .parse(await (await worker.fetch("/cases")).json());
  assert.ok(names.length > 0, "the Worker lists its conformance cases");
  const failures = [];
  for (const name of names) {
    const response = await worker.fetch(
      `/run?case=${encodeURIComponent(name)}`,
    );
    assert.equal(response.status, 200);
    const result = outcome.parse(await response.json());
    if (result.ok) log(`PASS: ${name}`);
    else {
      failures.push(result);
      log(`FAIL: ${name}: ${result.message}`);
    }
  }
  log(
    `Durable Object SQLite: ${names.length - failures.length}/${names.length} passed`,
  );
  assert.deepEqual(failures, []);
} finally {
  await worker.stop();
  await rm(temporary, { recursive: true, force: true });
}
