import { defineConfig } from "vitest/config";

import unit from "./vitest.config.js";

export default defineConfig({
  ...unit,
  test: {
    ...unit.test,
    include: ["test/load/live-updates.load.test.ts"],
    exclude: ["**/node_modules/**"],
    testTimeout: 1_200_000,
  },
});
