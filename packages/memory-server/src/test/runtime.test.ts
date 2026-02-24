import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createServer } from "node:net";
import path from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import { MongoClient } from "mongodb";

import { startJongodbMemoryServer } from "../index.js";

const classpathForRuntime = resolveTestClasspath();

test(
  "runtime manager starts server and serves mongodb driver requests",
  { concurrency: false },
  async () => {
    const server = await startJongodbMemoryServer({
      classpath: classpathForRuntime,
      databaseName: "node_runtime",
      startupTimeoutMs: 20_000,
    });

    try {
      const client = new MongoClient(server.uri);
      await client.connect();
      try {
        const db = client.db("node_runtime");
        const ping = await db.command({ ping: 1 });
        assert.equal(ping.ok, 1);

        await db.collection("users").insertOne({ name: "alice", age: 30 });
        const found = await db.collection("users").findOne({ name: "alice" });
        assert.equal(found?.name, "alice");
        assert.equal(found?.age, 30);
      } finally {
        await client.close();
      }
    } finally {
      await server.stop();
    }
  }
);

test(
  "runtime manager supports repeated start and stop cycles",
  { concurrency: false, timeout: 120_000 },
  async () => {
    for (let index = 0; index < 20; index += 1) {
      const server = await startJongodbMemoryServer({
        classpath: classpathForRuntime,
        startupTimeoutMs: 20_000,
      });
      await server.stop();
    }
  }
);

test(
  "runtime manager reports actionable error for missing java binary",
  { concurrency: false },
  async () => {
    await assert.rejects(
      async () => {
        await startJongodbMemoryServer({
          classpath: classpathForRuntime,
          javaPath: "java-command-not-found",
          startupTimeoutMs: 3_000,
        });
      },
      /spawn|java|classpath/i
    );
  }
);

test(
  "runtime manager fails clearly when requested port is already in use",
  { concurrency: false },
  async () => {
    const blocker = createServer();
    await new Promise<void>((resolve, reject) => {
      blocker.once("error", reject);
      blocker.listen(0, "127.0.0.1", () => resolve());
    });

    const address = blocker.address();
    if (address === null || typeof address === "string") {
      throw new Error("Unable to allocate blocker TCP port for test.");
    }

    try {
      await assert.rejects(
        async () => {
          await startJongodbMemoryServer({
            classpath: classpathForRuntime,
            host: "127.0.0.1",
            port: address.port,
            startupTimeoutMs: 5_000,
          });
        },
        /port|bind|in use|failed/i
      );
    } finally {
      await new Promise<void>((resolve, reject) => {
        blocker.close((error) => {
          if (error) {
            reject(error);
            return;
          }
          resolve();
        });
      });
    }
  }
);

function resolveTestClasspath(): string {
  const fromEnv = process.env.JONGODB_TEST_CLASSPATH?.trim();
  if (fromEnv !== undefined && fromEnv.length > 0) {
    return fromEnv;
  }

  const currentFile = fileURLToPath(import.meta.url);
  const testDir = path.dirname(currentFile);
  const packageDir = path.resolve(testDir, "..", "..");
  const repoRoot = path.resolve(packageDir, "..", "..");
  const gradle = path.resolve(repoRoot, ".tooling", "gradle-8.10.2", "bin", "gradle");

  const output = execFileSync(
    gradle,
    ["--no-daemon", "-q", "printLauncherClasspath"],
    {
      cwd: repoRoot,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    }
  );

  const lines = output
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  const classpath = lines.at(-1);
  if (classpath === undefined || classpath.length === 0) {
    throw new Error("Failed to resolve launcher classpath from Gradle output.");
  }
  return classpath;
}
