const assert = require("node:assert/strict");

const core = require("@jongodb/memory-server");
const runtime = require("@jongodb/memory-server/runtime");
const jest = require("@jongodb/memory-server/jest");
const nestjs = require("@jongodb/memory-server/nestjs");
const vitest = require("@jongodb/memory-server/vitest");

assert.equal(typeof core.startJongodbMemoryServer, "function");
assert.equal(typeof runtime.createJongodbEnvRuntime, "function");
assert.equal(typeof jest.registerJongodbForJest, "function");
assert.equal(typeof nestjs.registerJongodbForNestJest, "function");
assert.equal(typeof vitest.registerJongodbForVitest, "function");

process.stdout.write("CJS smoke check passed.\n");
