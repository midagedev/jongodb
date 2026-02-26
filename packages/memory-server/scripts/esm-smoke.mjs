import assert from "node:assert/strict";

import * as core from "@jongodb/memory-server";
import * as jest from "@jongodb/memory-server/jest";
import * as nestjs from "@jongodb/memory-server/nestjs";
import * as runtime from "@jongodb/memory-server/runtime";
import * as vitest from "@jongodb/memory-server/vitest";

assert.equal(typeof core.startJongodbMemoryServer, "function");
assert.equal(typeof runtime.createJongodbEnvRuntime, "function");
assert.equal(typeof jest.registerJongodbForJest, "function");
assert.equal(typeof jest.createJestGlobalSetup, "function");
assert.equal(typeof jest.createJestGlobalTeardown, "function");
assert.equal(typeof jest.readJestGlobalState, "function");
assert.equal(typeof jest.readJestGlobalUri, "function");
assert.equal(typeof nestjs.registerJongodbForNestJest, "function");
assert.equal(typeof vitest.registerJongodbForVitest, "function");
assert.equal(typeof vitest.registerJongodbForVitestWorkspace, "function");

process.stdout.write("ESM smoke check passed.\n");
