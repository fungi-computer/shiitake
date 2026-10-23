import { defineConfig } from "vitest/config";

const source = (path: string): string =>
  new URL(path, import.meta.url).pathname;

export default defineConfig({
  resolve: {
    alias: {
      "cloudflare:workers": source(
        "./test/fixtures/cloudflare-workers.fixture.ts",
      ),
    },
  },
  test: {
    environment: "node",
    include: ["test/**/*.test.ts"],
    // compaction-background.test.ts collects garbage to prove the summarized
    // prefix is unreachable before placement reads the context again.
    execArgv: ["--expose-gc"],
  },
});
