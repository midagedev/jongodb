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

test(
  "createJongodbEnvRuntime propagates single-node replica-set URI contract",
  { concurrency: false },
  async () => {
    const envKey = "TEST_MONGODB_URI_REPLICA";
    const previous = process.env[envKey];

    const runtime = createJongodbEnvRuntime({
      classpath: classpathForRuntime,
      envVarName: envKey,
      databaseName: "runtime_replica",
      topologyProfile: "singleNodeReplicaSet",
      replicaSetName: "rs-runtime",
      startupTimeoutMs: 20_000,
    });

    try {
      const uri = await runtime.setup();
      assert.match(
        uri,
        /^mongodb:\/\/127\.0\.0\.1:\d+\/runtime_replica\?replicaSet=rs-runtime$/u
      );
      assert.equal(process.env[envKey], uri);
    } finally {
      await runtime.teardown();
      if (previous === undefined) {
        delete process.env[envKey];
      } else {
        process.env[envKey] = previous;
      }
    }
  }
);

test(
  "createJongodbEnvRuntime avoids env clobbering across overlapping runtimes",
  { concurrency: false, timeout: 120_000 },
  async () => {
    const envKey = "TEST_MONGODB_URI_OVERLAP";
    const previous = process.env[envKey];
    delete process.env[envKey];

    const first = createJongodbEnvRuntime({
      classpath: classpathForRuntime,
      envVarName: envKey,
      startupTimeoutMs: 20_000,
    });
    const second = createJongodbEnvRuntime({
      classpath: classpathForRuntime,
      envVarName: envKey,
      startupTimeoutMs: 20_000,
    });
    assert.equal(first.useGlobalEnvBindings, true);
    assert.equal(second.useGlobalEnvBindings, true);

    try {
      const firstUri = await first.setup();
      assert.equal(process.env[envKey], firstUri);

      const secondUri = await second.setup();
      assert.equal(process.env[envKey], secondUri);

      await first.teardown();
      assert.equal(process.env[envKey], secondUri);

      await second.teardown();
      assert.equal(process.env[envKey], undefined);
    } finally {
      await first.teardown();
      await second.teardown();
      if (previous === undefined) {
        delete process.env[envKey];
      } else {
        process.env[envKey] = previous;
      }
    }
  }
);

test(
  "createJongodbEnvRuntime supports scoped env targets without mutating process env",
  { concurrency: false, timeout: 120_000 },
  async () => {
    const envKey = "TEST_MONGODB_URI_SCOPED";
    const previousProcess = process.env[envKey];
    process.env[envKey] = "mongodb://process-only:27017/original";

    const scopedEnv: Record<string, string | undefined> = {
      [envKey]: "mongodb://scoped-only:27017/original",
    };
    const runtime = createJongodbEnvRuntime({
      classpath: classpathForRuntime,
      envVarName: envKey,
      envTarget: scopedEnv,
      startupTimeoutMs: 20_000,
    });
    assert.equal(runtime.useGlobalEnvBindings, false);
    assert.equal(runtime.envTarget, scopedEnv);

    try {
      const uri = await runtime.setup();
      assert.equal(scopedEnv[envKey], uri);
      assert.equal(process.env[envKey], "mongodb://process-only:27017/original");

      await runtime.teardown();
      assert.equal(scopedEnv[envKey], "mongodb://scoped-only:27017/original");
      assert.equal(process.env[envKey], "mongodb://process-only:27017/original");
    } finally {
      await runtime.teardown();
      if (previousProcess === undefined) {
        delete process.env[envKey];
      } else {
        process.env[envKey] = previousProcess;
      }
    }
  }
);
