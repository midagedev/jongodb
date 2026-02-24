import { test } from "node:test";
import assert from "node:assert/strict";

import { startJongodbMemoryServer } from "../index.js";

test("startJongodbMemoryServer fails fast when binary and classpath are missing", async () => {
  await assert.rejects(async () => {
    await startJongodbMemoryServer();
  }, /binary|classpath|runtime/i);
});

test("launchMode=java fails fast when classpath is missing", async () => {
  await assert.rejects(async () => {
    await startJongodbMemoryServer({ launchMode: "java" });
  }, /classpath/i);
});
