import assert from "node:assert/strict";
import { writeFile } from "node:fs/promises";
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
import {
  registerJongodbForVitest,
  registerJongodbForVitestWorkspace,
} from "../vitest.js";
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
  "registerJongodbForVitestWorkspace applies project-level isolation defaults",
  { concurrency: false },
  async () => {
    const projectEnvKey = "MONGODB_URI_CATALOG_API";
    const previousDefault = process.env.MONGODB_URI;
    const previousProject = process.env[projectEnvKey];
    delete process.env[projectEnvKey];

    const hooks = new HookHarness();
    const registration = registerJongodbForVitestWorkspace(hooks, {
      classpath: classpathForRuntime,
      projectName: "catalog-api",
      databaseName: "vitest_workspace",
      startupTimeoutMs: 20_000,
    });

    try {
      await hooks.runBeforeAll();
      assert.equal(registration.isolationMode, "project");
      assert.equal(registration.databaseName, "vitest_workspace_pcatalog_api");
      assert.match(
        registration.uri,
        /^mongodb:\/\/.+\/vitest_workspace_pcatalog_api$/u
      );
      assert.equal(process.env.MONGODB_URI, registration.uri);
      assert.equal(process.env[projectEnvKey], registration.uri);
      assert.ok(registration.envVarNames.includes("MONGODB_URI"));
      assert.ok(registration.envVarNames.includes(projectEnvKey));
    } finally {
      await hooks.runAfterAll();
      if (previousDefault === undefined) {
        delete process.env.MONGODB_URI;
      } else {
        process.env.MONGODB_URI = previousDefault;
      }
      if (previousProject === undefined) {
        delete process.env[projectEnvKey];
      } else {
        process.env[projectEnvKey] = previousProject;
      }
    }
  }
);

test(
  "registerJongodbForVitestWorkspace supports shared mode without project DB suffix",
  { concurrency: false },
  async () => {
    const projectEnvKey = "MONGODB_URI_BILLING";
    const previousProject = process.env[projectEnvKey];
    delete process.env[projectEnvKey];

    const hooks = new HookHarness();
    const registration = registerJongodbForVitestWorkspace(hooks, {
      classpath: classpathForRuntime,
      projectName: "billing",
      isolationMode: "shared",
      databaseName: "vitest_shared",
      startupTimeoutMs: 20_000,
    });

    try {
      await hooks.runBeforeAll();
      assert.equal(registration.isolationMode, "shared");
      assert.equal(registration.databaseName, "vitest_shared");
      assert.match(registration.uri, /^mongodb:\/\/.+\/vitest_shared$/u);
      assert.equal(process.env[projectEnvKey], undefined);
    } finally {
      await hooks.runAfterAll();
      if (previousProject === undefined) {
        delete process.env[projectEnvKey];
      } else {
        process.env[projectEnvKey] = previousProject;
      }
    }
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

test(
  "jest global setup is idempotent when state file already references a running process",
  { concurrency: false },
  async () => {
    const stateFile = path.join(
      tmpdir(),
      `jongodb-jest-state-reuse-${Date.now()}-${Math.random().toString(16).slice(2)}.json`
    );

    const setup = createJestGlobalSetup({
      classpath: classpathForRuntime,
      stateFile,
      envVarName: "JONGODB_GLOBAL_URI_REUSE",
      startupTimeoutMs: 20_000,
    });
    const teardown = createJestGlobalTeardown({
      stateFile,
      killTimeoutMs: 5_000,
    });

    await setup();
    const first = await readJestGlobalState({ stateFile });
    assert.notEqual(first, null);

    await setup();
    const second = await readJestGlobalState({ stateFile });
    assert.notEqual(second, null);
    assert.equal(second!.pid, first!.pid);
    assert.equal(second!.uri, first!.uri);
    assert.equal(process.env.JONGODB_GLOBAL_URI_REUSE, first!.uri);

    await teardown();
    assert.equal(await readJestGlobalState({ stateFile }), null);
  }
);

test(
  "jest global setup replaces stale state file entries with a fresh detached process",
  { concurrency: false },
  async () => {
    const stateFile = path.join(
      tmpdir(),
      `jongodb-jest-state-stale-${Date.now()}-${Math.random().toString(16).slice(2)}.json`
    );
    const stalePid = findUnusedPid();
    await writeFile(
      stateFile,
      JSON.stringify(
        {
          uri: "mongodb://127.0.0.1:27017/stale",
          pid: stalePid,
          envVarName: "JONGODB_GLOBAL_URI_STALE",
        },
        null,
        2
      ),
      "utf8"
    );

    const setup = createJestGlobalSetup({
      classpath: classpathForRuntime,
      stateFile,
      envVarName: "JONGODB_GLOBAL_URI_STALE",
      startupTimeoutMs: 20_000,
    });
    const teardown = createJestGlobalTeardown({
      stateFile,
      killTimeoutMs: 5_000,
    });

    await setup();

    const refreshed = await readJestGlobalState({ stateFile });
    assert.notEqual(refreshed, null);
    assert.notEqual(refreshed!.pid, stalePid);
    assert.equal(isProcessRunning(refreshed!.pid), true);

    await teardown();
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

function findUnusedPid(): number {
  // Choose a high PID range and probe until we hit an unused entry.
  for (let pid = 900_000; pid < 910_000; pid += 1) {
    if (!isProcessRunning(pid)) {
      return pid;
    }
  }
  return 999_999;
}
