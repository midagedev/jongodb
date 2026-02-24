import { test } from "node:test";
import assert from "node:assert/strict";

import { startJongodbMemoryServer } from "../index.js";
import { createJongodbEnvRuntime } from "../runtime.js";

test("startJongodbMemoryServer fails fast when binaryPath is empty in binary mode", async () => {
  await assert.rejects(async () => {
    await startJongodbMemoryServer({
      launchMode: "binary",
      binaryPath: "   ",
    });
  }, /binarypath|binary/i);
});

test("launchMode=java fails fast when classpath is missing", async () => {
  await assert.rejects(async () => {
    await startJongodbMemoryServer({ launchMode: "java" });
  }, /classpath/i);
});

test("createJongodbEnvRuntime resolves envVarNames with stable order and deduplication", () => {
  const runtime = createJongodbEnvRuntime({
    envVarName: "MONGODB_URI",
    envVarNames: ["DATABASE_URL", "MONGODB_URI", "DATABASE_URL"],
  });

  assert.equal(runtime.envVarName, "MONGODB_URI");
  assert.deepEqual(runtime.envVarNames, ["MONGODB_URI", "DATABASE_URL"]);
});
