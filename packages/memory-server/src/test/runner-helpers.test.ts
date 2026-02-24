import assert from "node:assert/strict";
import { tmpdir } from "node:os";
import path from "node:path";
import { test } from "node:test";

import {
  createJestGlobalSetup,
  createJestGlobalTeardown,
  readJestGlobalState,
  registerJongodbForJest,
} from "../jest.js";
import { registerJongodbForNestJest } from "../nestjs.js";
import { registerJongodbForVitest } from "../vitest.js";
import { resolveTestClasspath } from "./support/classpath.js";

const classpathForRuntime = resolveTestClasspath();

test(
  "registerJongodbForJest wires lifecycle hooks and exposes URI",
  { concurrency: false },
  async () => {
    const hooks = new HookHarness();
    const registration = registerJongodbForJest(hooks, {
      classpath: classpathForRuntime,
      envVarName: "JONGODB_JEST_URI",
      startupTimeoutMs: 20_000,
    });

    assert.throws(() => registration.uri, /before (setup|all)/i);

    await hooks.runBeforeAll();
    assert.match(registration.uri, /^mongodb:\/\//u);
    assert.equal(process.env.JONGODB_JEST_URI, registration.uri);

    await hooks.runAfterAll();
  }
);

test(
  "registerJongodbForVitest wires lifecycle hooks and exposes URI",
  { concurrency: false },
  async () => {
    const hooks = new HookHarness();
    const registration = registerJongodbForVitest(hooks, {
      classpath: classpathForRuntime,
      envVarName: "JONGODB_VITEST_URI",
      startupTimeoutMs: 20_000,
    });

    assert.throws(() => registration.uri, /before (setup|all)/i);

    await hooks.runBeforeAll();
    assert.match(registration.uri, /^mongodb:\/\//u);
    assert.equal(process.env.JONGODB_VITEST_URI, registration.uri);

    await hooks.runAfterAll();
  }
);

test(
  "registerJongodbForNestJest wires lifecycle hooks and restores previous env value",
  { concurrency: false },
  async () => {
    const previous = process.env.MONGODB_URI;
    process.env.MONGODB_URI = "mongodb://previous-host:27017/previous";

    try {
      const hooks = new HookHarness();
      const registration = registerJongodbForNestJest(hooks, {
        classpath: classpathForRuntime,
        startupTimeoutMs: 20_000,
      });

      await hooks.runBeforeAll();
      assert.match(registration.uri, /^mongodb:\/\//u);
      assert.equal(process.env.MONGODB_URI, registration.uri);

      await hooks.runAfterAll();
      assert.equal(process.env.MONGODB_URI, "mongodb://previous-host:27017/previous");
    } finally {
      if (previous === undefined) {
        delete process.env.MONGODB_URI;
      } else {
        process.env.MONGODB_URI = previous;
      }
    }
  }
);

test(
  "jest global setup and teardown manage detached process through state file",
  { concurrency: false },
  async () => {
    const stateFile = path.join(
      tmpdir(),
      `jongodb-jest-state-${Date.now()}-${Math.random().toString(16).slice(2)}.json`
    );

    const setup = createJestGlobalSetup({
      classpath: classpathForRuntime,
      stateFile,
      envVarName: "JONGODB_GLOBAL_URI",
      startupTimeoutMs: 20_000,
    });
    const teardown = createJestGlobalTeardown({
      stateFile,
      killTimeoutMs: 5_000,
    });

    await setup();

    const state = await readJestGlobalState({ stateFile });
    assert.notEqual(state, null);
    assert.match(state!.uri, /^mongodb:\/\//u);
    assert.equal(process.env.JONGODB_GLOBAL_URI, state!.uri);
    assert.equal(isProcessRunning(state!.pid), true);

    await teardown();
    assert.equal(isProcessRunning(state!.pid), false);
    assert.equal(await readJestGlobalState({ stateFile }), null);
  }
);

class HookHarness {
  private readonly beforeCallbacks: Array<() => unknown | Promise<unknown>> = [];
  private readonly afterCallbacks: Array<() => unknown | Promise<unknown>> = [];

  beforeAll(callback: () => unknown | Promise<unknown>): void {
    this.beforeCallbacks.push(callback);
  }

  afterAll(callback: () => unknown | Promise<unknown>): void {
    this.afterCallbacks.push(callback);
  }

  async runBeforeAll(): Promise<void> {
    for (const callback of this.beforeCallbacks) {
      await callback();
    }
  }

  async runAfterAll(): Promise<void> {
    for (const callback of [...this.afterCallbacks].reverse()) {
      await callback();
    }
  }
}

function isProcessRunning(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error: unknown) {
    if (
      typeof error === "object" &&
      error !== null &&
      "code" in error &&
      (error as { code?: string }).code === "ESRCH"
    ) {
      return false;
    }
    throw error;
  }
}
