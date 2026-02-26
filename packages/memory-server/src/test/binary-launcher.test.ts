import assert from "node:assert/strict";
import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { test } from "node:test";

import { startJongodbMemoryServer } from "../index.js";

test(
  "startJongodbMemoryServer supports explicit binaryPath without classpath",
  { concurrency: false },
  async () => {
    await withFakeBinary(async (binaryPath) => {
      const server = await startJongodbMemoryServer({
        launchMode: "binary",
        binaryPath,
        host: "127.0.0.1",
        port: 0,
        databaseName: "binary_path",
      });

      try {
        assert.match(server.uri, /^mongodb:\/\/127\.0\.0\.1:\d+\/binary_path$/u);
      } finally {
        await server.stop();
      }
    });
  }
);

test(
  "startJongodbMemoryServer supports singleNodeReplicaSet topology profile",
  { concurrency: false },
  async () => {
    await withFakeBinary(async (binaryPath) => {
      const server = await startJongodbMemoryServer({
        launchMode: "binary",
        binaryPath,
        host: "127.0.0.1",
        port: 0,
        databaseName: "replica_profile",
        topologyProfile: "singleNodeReplicaSet",
        replicaSetName: "rs-test",
      });

      try {
        assert.match(
          server.uri,
          /^mongodb:\/\/127\.0\.0\.1:\d+\/replica_profile\?replicaSet=rs-test$/u
        );
      } finally {
        await server.stop();
      }
    });
  }
);

test(
  "startJongodbMemoryServer supports JONGODB_BINARY_PATH in binary mode",
  { concurrency: false },
  async () => {
    await withFakeBinary(async (binaryPath) => {
      const previous = process.env.JONGODB_BINARY_PATH;
      process.env.JONGODB_BINARY_PATH = binaryPath;

      try {
        const server = await startJongodbMemoryServer({
          launchMode: "binary",
          host: "127.0.0.1",
          port: 0,
          databaseName: "binary_env",
        });
        try {
          assert.match(server.uri, /^mongodb:\/\/127\.0\.0\.1:\d+\/binary_env$/u);
        } finally {
          await server.stop();
        }
      } finally {
        if (previous === undefined) {
          delete process.env.JONGODB_BINARY_PATH;
        } else {
          process.env.JONGODB_BINARY_PATH = previous;
        }
      }
    });
  }
);

test(
  "startJongodbMemoryServer appends explicit databaseNameSuffix",
  { concurrency: false },
  async () => {
    await withFakeBinary(async (binaryPath) => {
      const server = await startJongodbMemoryServer({
        launchMode: "binary",
        binaryPath,
        host: "127.0.0.1",
        port: 0,
        databaseName: "suffix_base",
        databaseNameSuffix: "_ci",
      });

      try {
        assert.match(server.uri, /^mongodb:\/\/127\.0\.0\.1:\d+\/suffix_base_ci$/u);
      } finally {
        await server.stop();
      }
    });
  }
);

test(
  "startJongodbMemoryServer supports worker databaseNameStrategy for isolation",
  { concurrency: false },
  async () => {
    await withFakeBinary(async (binaryPath) => {
      const previous = process.env.JONGODB_WORKER_ID;
      process.env.JONGODB_WORKER_ID = "worker 2/A";

      try {
        const server = await startJongodbMemoryServer({
          launchMode: "binary",
          binaryPath,
          host: "127.0.0.1",
          port: 0,
          databaseName: "worker_db",
          databaseNameStrategy: "worker",
        });

        try {
          assert.match(server.uri, /^mongodb:\/\/127\.0\.0\.1:\d+\/worker_db_wworker_2_A$/u);
        } finally {
          await server.stop();
        }
      } finally {
        if (previous === undefined) {
          delete process.env.JONGODB_WORKER_ID;
        } else {
          process.env.JONGODB_WORKER_ID = previous;
        }
      }
    });
  }
);

test(
  "startJongodbMemoryServer falls back to java in auto mode when binary launcher exits",
  { concurrency: false },
  async () => {
    await withFakeLaunchers(async ({ brokenBinaryPath, fakeJavaPath }) => {
      const server = await startJongodbMemoryServer({
        launchMode: "auto",
        binaryPath: brokenBinaryPath,
        javaPath: fakeJavaPath,
        classpath: "ignored-classpath-for-fake-java",
        host: "127.0.0.1",
        port: 0,
        databaseName: "auto_fallback",
      });

      try {
        assert.match(server.uri, /^mongodb:\/\/127\.0\.0\.1:\d+\/auto_fallback$/u);
      } finally {
        await server.stop();
      }
    });
  }
);

test(
  "startJongodbMemoryServer propagates replica-set profile args in java mode",
  { concurrency: false },
  async () => {
    await withFakeLaunchers(async ({ fakeJavaPath }) => {
      const server = await startJongodbMemoryServer({
        launchMode: "java",
        javaPath: fakeJavaPath,
        classpath: "ignored-classpath-for-fake-java",
        host: "127.0.0.1",
        port: 0,
        databaseName: "java_replica",
        topologyProfile: "singleNodeReplicaSet",
        replicaSetName: "rs-java",
      });

      try {
        assert.match(
          server.uri,
          /^mongodb:\/\/127\.0\.0\.1:\d+\/java_replica\?replicaSet=rs-java$/u
        );
      } finally {
        await server.stop();
      }
    });
  }
);

test(
  "startJongodbMemoryServer reports both launch failures when auto mode cannot start",
  { concurrency: false },
  async () => {
    await withBrokenBinary(async (brokenBinaryPath) => {
      await assert.rejects(
        async () => {
          await startJongodbMemoryServer({
            launchMode: "auto",
            binaryPath: brokenBinaryPath,
            javaPath: "java-command-not-found",
            classpath: "placeholder-classpath",
            startupTimeoutMs: 2_000,
          });
        },
        (error: unknown) => {
          if (!(error instanceof Error)) {
            return false;
          }
          assert.match(error.message, /\[binary:options\.binaryPath\]/u);
          assert.match(error.message, /\[java:options\.classpath\]/u);
          return true;
        }
      );
    });
  }
);

test(
  "startJongodbMemoryServer fails when replica-set profile URI omits replicaSet query",
  { concurrency: false },
  async () => {
    await withTopologyMismatchBinary(async (binaryPath) => {
      await assert.rejects(
        async () => {
          await startJongodbMemoryServer({
            launchMode: "binary",
            binaryPath,
            host: "127.0.0.1",
            port: 0,
            databaseName: "replica_mismatch",
            topologyProfile: "singleNodeReplicaSet",
            replicaSetName: "rs-expected",
            startupTimeoutMs: 5_000,
          });
        },
        /topology options are out of sync|missing replicaSet/i
      );
    });
  }
);

test(
  "startJongodbMemoryServer fails when standalone profile URI includes replicaSet query",
  { concurrency: false },
  async () => {
    await withTopologyMismatchBinary(async (binaryPath) => {
      await assert.rejects(
        async () => {
          await startJongodbMemoryServer({
            launchMode: "binary",
            binaryPath,
            host: "127.0.0.1",
            port: 0,
            databaseName: "standalone_mismatch",
            topologyProfile: "standalone",
            startupTimeoutMs: 5_000,
          });
        },
        /topology options are out of sync|standalone/i
      );
    });
  }
);

test(
  "startJongodbMemoryServer redacts secrets from launcher failure logs",
  { concurrency: false },
  async () => {
    await withSecretFailureBinary(async (binaryPath) => {
      await assert.rejects(
        async () => {
          await startJongodbMemoryServer({
            launchMode: "binary",
            binaryPath,
            host: "127.0.0.1",
            port: 0,
            databaseName: "redaction_case",
            startupTimeoutMs: 3_000,
          });
        },
        (error: unknown) => {
          if (!(error instanceof Error)) {
            return false;
          }
          assert.match(error.message, /<redacted>/u);
          assert.doesNotMatch(error.message, /superSecretPwd/u);
          assert.doesNotMatch(error.message, /token-123456/u);
          assert.doesNotMatch(error.message, /plainpass/u);
          assert.doesNotMatch(error.message, /querySecret/u);
          return true;
        }
      );
    });
  }
);

async function withFakeBinary(
  block: (binaryPath: string) => Promise<void>
): Promise<void> {
  const tempDir = await mkdtemp(path.join(tmpdir(), "jongodb-bin-"));
  const binaryPath = path.join(tempDir, "fake-jongodb-binary");

  try {
    await writeFile(binaryPath, fakeBinaryLauncherScript(), "utf8");
    await chmod(binaryPath, 0o755);
    await block(binaryPath);
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
}

async function withTopologyMismatchBinary(
  block: (binaryPath: string) => Promise<void>
): Promise<void> {
  const tempDir = await mkdtemp(path.join(tmpdir(), "jongodb-bin-topology-mismatch-"));
  const binaryPath = path.join(tempDir, "fake-jongodb-binary-topology-mismatch");

  try {
    await writeFile(binaryPath, topologyMismatchBinaryLauncherScript(), "utf8");
    await chmod(binaryPath, 0o755);
    await block(binaryPath);
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
}

async function withBrokenBinary(
  block: (binaryPath: string) => Promise<void>
): Promise<void> {
  const tempDir = await mkdtemp(path.join(tmpdir(), "jongodb-bin-fail-"));
  const binaryPath = path.join(tempDir, "broken-jongodb-binary");

  try {
    await writeFile(binaryPath, brokenBinaryScript(), "utf8");
    await chmod(binaryPath, 0o755);
    await block(binaryPath);
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
}

async function withSecretFailureBinary(
  block: (binaryPath: string) => Promise<void>
): Promise<void> {
  const tempDir = await mkdtemp(path.join(tmpdir(), "jongodb-bin-secret-fail-"));
  const binaryPath = path.join(tempDir, "secret-failing-jongodb-binary");

  try {
    await writeFile(binaryPath, secretFailureBinaryScript(), "utf8");
    await chmod(binaryPath, 0o755);
    await block(binaryPath);
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
}

async function withFakeLaunchers(
  block: (paths: { brokenBinaryPath: string; fakeJavaPath: string }) => Promise<void>
): Promise<void> {
  const tempDir = await mkdtemp(path.join(tmpdir(), "jongodb-launchers-"));
  const brokenBinaryPath = path.join(tempDir, "broken-jongodb-binary");
  const fakeJavaPath = path.join(tempDir, "fake-java");

  try {
    await writeFile(brokenBinaryPath, brokenBinaryScript(), "utf8");
    await writeFile(fakeJavaPath, fakeJavaLauncherScript(), "utf8");
    await chmod(brokenBinaryPath, 0o755);
    await chmod(fakeJavaPath, 0o755);
    await block({ brokenBinaryPath, fakeJavaPath });
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
}

function brokenBinaryScript(): string {
  return `#!/usr/bin/env sh
echo "JONGODB_START_FAILURE=simulated binary startup failure" 1>&2
exit 1
`;
}

function secretFailureBinaryScript(): string {
  return `#!/usr/bin/env sh
echo "JONGODB_START_FAILURE=auth failed password=superSecretPwd token=token-123456 uri=mongodb://alice:plainpass@127.0.0.1:27017/test?authSource=admin&password=querySecret" 1>&2
exit 1
`;
}

function fakeBinaryLauncherScript(): string {
  return `#!/usr/bin/env node
const valueOf = (name, fallback) => {
  const prefixed = name + "=";
  const found = process.argv.find((arg) => arg.startsWith(prefixed));
  return found ? found.slice(prefixed.length) : fallback;
};
const host = valueOf("--host", "127.0.0.1");
const portRaw = Number(valueOf("--port", "0"));
const db = valueOf("--database", "test");
const topologyProfile = valueOf("--topology-profile", "standalone");
const replicaSetName = valueOf("--replica-set-name", "jongodb-rs0");
const port = Number.isInteger(portRaw) && portRaw > 0 ? portRaw : 27017;
const query =
  topologyProfile === "singleNodeReplicaSet"
    ? "?replicaSet=" + replicaSetName
    : "";
console.log("JONGODB_URI=" + "mongodb://" + host + ":" + port + "/" + db + query);
const keepAlive = setInterval(() => {}, 1000);
const shutdown = () => {
  clearInterval(keepAlive);
  process.exit(0);
};
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
`;
}

function topologyMismatchBinaryLauncherScript(): string {
  return `#!/usr/bin/env node
const valueOf = (name, fallback) => {
  const prefixed = name + "=";
  const found = process.argv.find((arg) => arg.startsWith(prefixed));
  return found ? found.slice(prefixed.length) : fallback;
};
const host = valueOf("--host", "127.0.0.1");
const portRaw = Number(valueOf("--port", "0"));
const db = valueOf("--database", "test");
const topologyProfile = valueOf("--topology-profile", "standalone");
const port = Number.isInteger(portRaw) && portRaw > 0 ? portRaw : 27017;
const query =
  topologyProfile === "singleNodeReplicaSet"
    ? ""
    : "?replicaSet=unexpected-rs";
console.log("JONGODB_URI=" + "mongodb://" + host + ":" + port + "/" + db + query);
const keepAlive = setInterval(() => {}, 1000);
const shutdown = () => {
  clearInterval(keepAlive);
  process.exit(0);
};
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
`;
}

function fakeJavaLauncherScript(): string {
  return `#!/usr/bin/env node
const valueOf = (name, fallback) => {
  const prefixed = name + "=";
  const found = process.argv.find((arg) => arg.startsWith(prefixed));
  return found ? found.slice(prefixed.length) : fallback;
};
const host = valueOf("--host", "127.0.0.1");
const portRaw = Number(valueOf("--port", "0"));
const db = valueOf("--database", "test");
const topologyProfile = valueOf("--topology-profile", "standalone");
const replicaSetName = valueOf("--replica-set-name", "jongodb-rs0");
const port = Number.isInteger(portRaw) && portRaw > 0 ? portRaw : 27017;
const query =
  topologyProfile === "singleNodeReplicaSet"
    ? "?replicaSet=" + replicaSetName
    : "";
console.log("JONGODB_URI=" + "mongodb://" + host + ":" + port + "/" + db + query);
const keepAlive = setInterval(() => {}, 1000);
const shutdown = () => {
  clearInterval(keepAlive);
  process.exit(0);
};
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
`;
}
