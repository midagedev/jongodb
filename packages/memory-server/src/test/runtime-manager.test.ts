import assert from "node:assert/strict";
import { test } from "node:test";

import { createJongodbEnvRuntime } from "../runtime.js";
import { resolveTestClasspath } from "./support/classpath.js";

const classpathForRuntime = resolveTestClasspath();

test(
  "createJongodbEnvRuntime sets uri env on setup and restores previous value on teardown",
  { concurrency: false },
  async () => {
    const previous = process.env.TEST_MONGODB_URI;
    process.env.TEST_MONGODB_URI = "mongodb://previous-host:27017/previous";

    try {
      const runtime = createJongodbEnvRuntime({
        classpath: classpathForRuntime,
        envVarName: "TEST_MONGODB_URI",
        startupTimeoutMs: 20_000,
      });

      const uri = await runtime.setup();
      assert.match(uri, /^mongodb:\/\//u);
      assert.equal(runtime.running, true);
      assert.equal(process.env.TEST_MONGODB_URI, uri);
      assert.equal(runtime.uri, uri);

      await runtime.teardown();
      assert.equal(runtime.running, false);
      assert.equal(process.env.TEST_MONGODB_URI, "mongodb://previous-host:27017/previous");
      assert.throws(() => runtime.uri, /before setup/i);
    } finally {
      if (previous === undefined) {
        delete process.env.TEST_MONGODB_URI;
      } else {
        process.env.TEST_MONGODB_URI = previous;
      }
    }
  }
);

test(
  "createJongodbEnvRuntime setup and teardown are idempotent",
  { concurrency: false },
  async () => {
    const runtime = createJongodbEnvRuntime({
      classpath: classpathForRuntime,
      envVarName: "TEST_MONGODB_URI_IDEMPOTENT",
      startupTimeoutMs: 20_000,
    });

    try {
      const first = await runtime.setup();
      const second = await runtime.setup();
      assert.equal(first, second);
    } finally {
      await runtime.teardown();
      await runtime.teardown();
    }
  }
);

test(
  "createJongodbEnvRuntime writes and restores multiple env keys",
  { concurrency: false },
  async () => {
    const previousPrimary = process.env.TEST_MULTI_PRIMARY;
    const previousSecondary = process.env.TEST_MULTI_SECONDARY;
    process.env.TEST_MULTI_PRIMARY = "mongodb://previous-host:27017/previous";
    delete process.env.TEST_MULTI_SECONDARY;

    const runtime = createJongodbEnvRuntime({
      classpath: classpathForRuntime,
      envVarNames: ["TEST_MULTI_PRIMARY", "TEST_MULTI_SECONDARY"],
      startupTimeoutMs: 20_000,
    });

    try {
      const uri = await runtime.setup();
      assert.equal(process.env.TEST_MULTI_PRIMARY, uri);
      assert.equal(process.env.TEST_MULTI_SECONDARY, uri);

      await runtime.teardown();
      assert.equal(process.env.TEST_MULTI_PRIMARY, "mongodb://previous-host:27017/previous");
      assert.equal(process.env.TEST_MULTI_SECONDARY, undefined);
    } finally {
      if (previousPrimary === undefined) {
        delete process.env.TEST_MULTI_PRIMARY;
      } else {
        process.env.TEST_MULTI_PRIMARY = previousPrimary;
      }

      if (previousSecondary === undefined) {
        delete process.env.TEST_MULTI_SECONDARY;
      } else {
        process.env.TEST_MULTI_SECONDARY = previousSecondary;
      }
    }
  }
);
