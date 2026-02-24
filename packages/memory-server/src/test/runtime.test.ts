import assert from "node:assert/strict";
import { createServer } from "node:net";
import { test } from "node:test";

import { MongoClient } from "mongodb";

import { startJongodbMemoryServer } from "../index.js";
import { resolveTestClasspath } from "./support/classpath.js";

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
