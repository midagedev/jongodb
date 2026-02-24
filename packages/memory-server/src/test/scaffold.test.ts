import { test } from "node:test";
import assert from "node:assert/strict";

import { startJongodbMemoryServer } from "../index.js";

test("startJongodbMemoryServer fails fast when classpath is missing", async () => {
  await assert.rejects(async () => {
    await startJongodbMemoryServer();
  }, /classpath/i);
});
