import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import {
  chmod,
  mkdtemp,
  readdir,
  readFile,
  rm,
  utimes,
  writeFile,
} from "node:fs/promises";
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
  "startJongodbMemoryServer verifies explicit binary checksum when configured",
  { concurrency: false },
  async () => {
    await withFakeBinary(async (binaryPath) => {
      const checksum = await sha256(binaryPath);
      const server = await startJongodbMemoryServer({
        launchMode: "binary",
        binaryPath,
        binaryChecksum: checksum,
        host: "127.0.0.1",
        port: 0,
        databaseName: "binary_checksum_ok",
      });

      try {
        assert.match(server.uri, /^mongodb:\/\/127\.0\.0\.1:\d+\/binary_checksum_ok$/u);
      } finally {
        await server.stop();
      }
    });
  }
);

test(
  "startJongodbMemoryServer rejects explicit binary when checksum does not match",
  { concurrency: false },
  async () => {
    await withFakeBinary(async (binaryPath) => {
      await assert.rejects(
        async () => {
          await startJongodbMemoryServer({
            launchMode: "binary",
            binaryPath,
            binaryChecksum:
              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            host: "127.0.0.1",
            port: 0,
            databaseName: "binary_checksum_mismatch",
          });
        },
        /Binary checksum verification failed/i
      );
    });
  }
);

test(
  "startJongodbMemoryServer supports JONGODB_BINARY_CHECKSUM for explicit binary verification",
  { concurrency: false },
  async () => {
    await withFakeBinary(async (binaryPath) => {
      const checksum = await sha256(binaryPath);
      const previousPath = process.env.JONGODB_BINARY_PATH;
      const previousChecksum = process.env.JONGODB_BINARY_CHECKSUM;
      process.env.JONGODB_BINARY_PATH = binaryPath;
      process.env.JONGODB_BINARY_CHECKSUM = checksum;

      try {
        const server = await startJongodbMemoryServer({
          launchMode: "binary",
          host: "127.0.0.1",
          port: 0,
          databaseName: "binary_checksum_env",
        });

        try {
          assert.match(server.uri, /^mongodb:\/\/127\.0\.0\.1:\d+\/binary_checksum_env$/u);
        } finally {
          await server.stop();
        }
      } finally {
        if (previousPath === undefined) {
          delete process.env.JONGODB_BINARY_PATH;
        } else {
          process.env.JONGODB_BINARY_PATH = previousPath;
        }
        if (previousChecksum === undefined) {
          delete process.env.JONGODB_BINARY_CHECKSUM;
        } else {
          process.env.JONGODB_BINARY_CHECKSUM = previousChecksum;
        }
      }
    });
  }
);

test(
  "startJongodbMemoryServer cleans up managed launcher process on parent exit",
  { concurrency: false },
  async () => {
    await withFakeBinary(async (binaryPath) => {
      const tempDir = await mkdtemp(path.join(tmpdir(), "jongodb-exit-cleanup-"));
      const pidFile = path.join(tempDir, "launcher.pid");
      const scriptPath = path.join(tempDir, "parent-exit-cleanup.mjs");
      const memoryServerEntry = path.resolve(process.cwd(), "packages/memory-server/dist/esm/index.js");

      try {
        await writeFile(
          scriptPath,
          `import { startJongodbMemoryServer } from ${JSON.stringify(memoryServerEntry)};
await startJongodbMemoryServer({
  launchMode: "binary",
  binaryPath: ${JSON.stringify(binaryPath)},
  host: "127.0.0.1",
  port: 0,
  databaseName: "exit_cleanup_probe",
  env: { JONGODB_PID_FILE: ${JSON.stringify(pidFile)} },
});
setTimeout(() => process.exit(0), 50);
`,
          "utf8"
        );

        const parent = spawn(process.execPath, [scriptPath], {
          stdio: "ignore",
        });
        const exitCode = await waitForExitCode(parent);
        assert.equal(exitCode, 0);

        const launcherPidRaw = await readFile(pidFile, "utf8");
        const launcherPid = Number.parseInt(launcherPidRaw.trim(), 10);
        assert.ok(Number.isInteger(launcherPid) && launcherPid > 0);

        const cleaned = await waitForPidExit(launcherPid, 5_000);
        assert.equal(cleaned, true);
      } finally {
        await rm(tempDir, { recursive: true, force: true });
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
  "startJongodbMemoryServer auto mode falls back to classpath auto-discovery after binary failure",
  { concurrency: false },
  async () => {
    await withFakeLaunchersWithClasspathProbe(
      async ({ brokenBinaryPath, fakeJavaPath, fakeGradlePath, workingDirectory }) => {
        const events: Array<{
          attempt: number;
          mode: "binary" | "java";
          source: string;
          startupDurationMs: number;
          success: boolean;
          errorMessage?: string;
        }> = [];

        const server = await startJongodbMemoryServer({
          launchMode: "auto",
          binaryPath: brokenBinaryPath,
          javaPath: fakeJavaPath,
          classpathDiscoveryCommand: fakeGradlePath,
          classpathDiscoveryWorkingDirectory: workingDirectory,
          host: "127.0.0.1",
          port: 0,
          databaseName: "auto_probe_fallback",
          onStartupTelemetry(event) {
            events.push(event);
          },
        });

        try {
          assert.match(server.uri, /^mongodb:\/\/127\.0\.0\.1:\d+\/auto_probe_fallback$/u);
          assert.equal(events.length, 2);
          assert.equal(events[0].mode, "binary");
          assert.equal(events[0].success, false);
          assert.equal(events[1].mode, "java");
          assert.equal(events[1].success, true);
          assert.equal(events[1].source, "classpath-auto-discovery");
        } finally {
          await server.stop();
        }
      }
    );
  }
);

test(
  "startJongodbMemoryServer retries with next port after collision",
  { concurrency: false },
  async () => {
    await withPortCollisionBinary(async (binaryPath) => {
      const server = await startJongodbMemoryServer({
        launchMode: "binary",
        binaryPath,
        host: "127.0.0.1",
        port: 28017,
        portRetryAttempts: 2,
        portRetryBackoffMs: 1,
        databaseName: "port_retry_success",
        env: {
          JONGODB_COLLISION_PORT: "28017",
        },
      });

      try {
        assert.match(server.uri, /^mongodb:\/\/127\.0\.0\.1:28018\/port_retry_success$/u);
      } finally {
        await server.stop();
      }
    });
  }
);

test(
  "startJongodbMemoryServer fails on collision when port retry is disabled",
  { concurrency: false },
  async () => {
    await withPortCollisionBinary(async (binaryPath) => {
      await assert.rejects(
        async () => {
          await startJongodbMemoryServer({
            launchMode: "binary",
            binaryPath,
            host: "127.0.0.1",
            port: 28117,
            portRetryAttempts: 0,
            databaseName: "port_retry_disabled",
            env: {
              JONGODB_COLLISION_PORT: "28117",
            },
          });
        },
        /EADDRINUSE|address already in use/i
      );
    });
  }
);

test(
  "startJongodbMemoryServer java mode resolves classpath via auto-discovery",
  { concurrency: false },
  async () => {
    await withFakeJavaAndClasspathProbe(async ({ fakeJavaPath, fakeGradlePath, workingDirectory }) => {
      const server = await startJongodbMemoryServer({
        launchMode: "java",
        javaPath: fakeJavaPath,
        classpathDiscoveryCommand: fakeGradlePath,
        classpathDiscoveryWorkingDirectory: workingDirectory,
        host: "127.0.0.1",
        port: 0,
        databaseName: "java_auto_probe",
      });

      try {
        assert.match(server.uri, /^mongodb:\/\/127\.0\.0\.1:\d+\/java_auto_probe$/u);
      } finally {
        await server.stop();
      }
    });
  }
);

test(
  "startJongodbMemoryServer reuses classpath auto-discovery cache entries",
  { concurrency: false },
  async () => {
    await withFakeJavaAndClasspathProbe(async ({ fakeJavaPath, fakeGradlePath, workingDirectory }) => {
      const cacheDir = path.join(workingDirectory, "artifact-cache");

      const firstServer = await startJongodbMemoryServer({
        launchMode: "java",
        javaPath: fakeJavaPath,
        classpathDiscoveryCommand: fakeGradlePath,
        classpathDiscoveryWorkingDirectory: workingDirectory,
        artifactCacheDir: cacheDir,
        host: "127.0.0.1",
        port: 0,
        databaseName: "java_cache_hit_1",
      });
      await firstServer.stop();

      await writeFile(fakeGradlePath, brokenGradleClasspathProbeScript(), "utf8");
      await chmod(fakeGradlePath, 0o755);

      const secondServer = await startJongodbMemoryServer({
        launchMode: "java",
        javaPath: fakeJavaPath,
        classpathDiscoveryCommand: fakeGradlePath,
        classpathDiscoveryWorkingDirectory: workingDirectory,
        artifactCacheDir: cacheDir,
        host: "127.0.0.1",
        port: 0,
        databaseName: "java_cache_hit_2",
      });

      try {
        assert.match(secondServer.uri, /^mongodb:\/\/127\.0\.0\.1:\d+\/java_cache_hit_2$/u);
      } finally {
        await secondServer.stop();
      }
    });
  }
);

test(
  "startJongodbMemoryServer enforces artifact cache maxEntries pruning",
  { concurrency: false },
  async () => {
    await withFakeJavaAndClasspathProbe(async ({ fakeJavaPath, fakeGradlePath, workingDirectory }) => {
      const cacheDir = path.join(workingDirectory, "artifact-cache-prune");
      const secondProbePath = path.join(workingDirectory, "fake-gradle-second");
      await writeFile(secondProbePath, fakeGradleClasspathProbeScript(), "utf8");
      await chmod(secondProbePath, 0o755);

      const firstServer = await startJongodbMemoryServer({
        launchMode: "java",
        javaPath: fakeJavaPath,
        classpathDiscoveryCommand: fakeGradlePath,
        classpathDiscoveryWorkingDirectory: workingDirectory,
        artifactCacheDir: cacheDir,
        artifactCacheMaxEntries: 1,
        host: "127.0.0.1",
        port: 0,
        databaseName: "java_cache_prune_1",
      });
      await firstServer.stop();

      const secondServer = await startJongodbMemoryServer({
        launchMode: "java",
        javaPath: fakeJavaPath,
        classpathDiscoveryCommand: secondProbePath,
        classpathDiscoveryWorkingDirectory: workingDirectory,
        artifactCacheDir: cacheDir,
        artifactCacheMaxEntries: 1,
        host: "127.0.0.1",
        port: 0,
        databaseName: "java_cache_prune_2",
      });
      await secondServer.stop();

      const cacheFiles = (await readdir(cacheDir)).filter((fileName) =>
        /^classpath-[a-f0-9]{64}\.json$/u.test(fileName)
      );
      assert.ok(
        cacheFiles.length <= 1,
        `expected <=1 cache entry after prune, got ${cacheFiles.length}`
      );
    });
  }
);

test(
  "startJongodbMemoryServer expires stale classpath cache entries by TTL",
  { concurrency: false },
  async () => {
    await withFakeJavaAndClasspathProbe(async ({ fakeJavaPath, fakeGradlePath, workingDirectory }) => {
      const cacheDir = path.join(workingDirectory, "artifact-cache-ttl");

      const firstServer = await startJongodbMemoryServer({
        launchMode: "java",
        javaPath: fakeJavaPath,
        classpathDiscoveryCommand: fakeGradlePath,
        classpathDiscoveryWorkingDirectory: workingDirectory,
        artifactCacheDir: cacheDir,
        artifactCacheTtlMs: 86_400_000,
        host: "127.0.0.1",
        port: 0,
        databaseName: "java_cache_ttl_1",
      });
      await firstServer.stop();

      const cacheFile = (await readdir(cacheDir)).find((fileName) =>
        /^classpath-[a-f0-9]{64}\.json$/u.test(fileName)
      );
      assert.ok(cacheFile, "expected classpath cache entry to exist");
      const staleTime = new Date(Date.now() - 3 * 86_400_000);
      await utimes(path.join(cacheDir, cacheFile!), staleTime, staleTime);

      await assert.rejects(
        async () => {
          await startJongodbMemoryServer({
            launchMode: "java",
            javaPath: fakeJavaPath,
            classpathDiscoveryCommand: "command-that-does-not-exist",
            classpathDiscoveryWorkingDirectory: workingDirectory,
            artifactCacheDir: cacheDir,
            artifactCacheTtlMs: 1,
            host: "127.0.0.1",
            port: 0,
            databaseName: "java_cache_ttl_2",
          });
        },
        /Classpath auto-discovery probe failed/i
      );
    });
  }
);

test(
  "startJongodbMemoryServer java mode reports auto-discovery diagnostics when probe fails",
  { concurrency: false },
  async () => {
    await withFakeLaunchers(async ({ fakeJavaPath }) => {
      await assert.rejects(
        async () => {
          await startJongodbMemoryServer({
            launchMode: "java",
            javaPath: fakeJavaPath,
            classpathDiscoveryCommand: "command-that-does-not-exist",
            classpathDiscoveryWorkingDirectory: "/tmp",
            host: "127.0.0.1",
            port: 0,
            databaseName: "java_probe_fail",
          });
        },
        (error: unknown) => {
          if (!(error instanceof Error)) {
            return false;
          }
          assert.match(error.message, /Classpath auto-discovery probe failed/u);
          assert.match(error.message, /JONGODB_CLASSPATH/u);
          return true;
        }
      );
    });
  }
);

test(
  "startJongodbMemoryServer java mode fails fast when classpath discovery is off",
  { concurrency: false },
  async () => {
    await withFakeLaunchers(async ({ fakeJavaPath }) => {
      await assert.rejects(
        async () => {
          await startJongodbMemoryServer({
            launchMode: "java",
            javaPath: fakeJavaPath,
            classpathDiscovery: "off",
            host: "127.0.0.1",
            port: 0,
            databaseName: "java_probe_off",
          });
        },
        /Java launch mode requested but Java classpath is not configured/i
      );
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
  "startJongodbMemoryServer emits startup telemetry for successful binary launch",
  { concurrency: false },
  async () => {
    await withFakeBinary(async (binaryPath) => {
      const events: Array<{
        attempt: number;
        mode: "binary" | "java";
        source: string;
        startupDurationMs: number;
        success: boolean;
        errorMessage?: string;
      }> = [];
      const server = await startJongodbMemoryServer({
        launchMode: "binary",
        binaryPath,
        host: "127.0.0.1",
        port: 0,
        databaseName: "telemetry_success",
        onStartupTelemetry(event) {
          events.push(event);
        },
      });

      try {
        assert.equal(events.length, 1);
        assert.equal(events[0].attempt, 1);
        assert.equal(events[0].mode, "binary");
        assert.equal(events[0].source, "options.binaryPath");
        assert.equal(events[0].success, true);
        assert.equal(events[0].errorMessage, undefined);
        assert.ok(events[0].startupDurationMs >= 0);
      } finally {
        await server.stop();
      }
    });
  }
);

test(
  "startJongodbMemoryServer emits telemetry for auto fallback failure and recovery",
  { concurrency: false },
  async () => {
    await withFakeLaunchers(async ({ brokenBinaryPath, fakeJavaPath }) => {
      const events: Array<{
        attempt: number;
        mode: "binary" | "java";
        source: string;
        startupDurationMs: number;
        success: boolean;
        errorMessage?: string;
      }> = [];

      const server = await startJongodbMemoryServer({
        launchMode: "auto",
        binaryPath: brokenBinaryPath,
        javaPath: fakeJavaPath,
        classpath: "ignored-classpath-for-fake-java",
        host: "127.0.0.1",
        port: 0,
        databaseName: "telemetry_auto_fallback",
        onStartupTelemetry(event) {
          events.push(event);
        },
      });

      try {
        assert.equal(events.length, 2);
        assert.equal(events[0].attempt, 1);
        assert.equal(events[0].mode, "binary");
        assert.equal(events[0].success, false);
        assert.match(events[0].errorMessage ?? "", /simulated binary startup failure/u);

        assert.equal(events[1].attempt, 2);
        assert.equal(events[1].mode, "java");
        assert.equal(events[1].success, true);
        assert.ok(events[1].startupDurationMs >= 0);
      } finally {
        await server.stop();
      }
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

async function withPortCollisionBinary(
  block: (binaryPath: string) => Promise<void>
): Promise<void> {
  const tempDir = await mkdtemp(path.join(tmpdir(), "jongodb-bin-port-collision-"));
  const binaryPath = path.join(tempDir, "fake-jongodb-binary-port-collision");

  try {
    await writeFile(binaryPath, portCollisionBinaryLauncherScript(), "utf8");
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

async function withFakeJavaAndClasspathProbe(
  block: (paths: {
    fakeJavaPath: string;
    fakeGradlePath: string;
    workingDirectory: string;
  }) => Promise<void>
): Promise<void> {
  const tempDir = await mkdtemp(path.join(tmpdir(), "jongodb-java-probe-"));
  const fakeJavaPath = path.join(tempDir, "fake-java");
  const fakeGradlePath = path.join(tempDir, "fake-gradle");

  try {
    await writeFile(fakeJavaPath, fakeJavaLauncherScript(), "utf8");
    await writeFile(fakeGradlePath, fakeGradleClasspathProbeScript(), "utf8");
    await chmod(fakeJavaPath, 0o755);
    await chmod(fakeGradlePath, 0o755);
    await block({
      fakeJavaPath,
      fakeGradlePath,
      workingDirectory: tempDir,
    });
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
}

async function withFakeLaunchersWithClasspathProbe(
  block: (paths: {
    brokenBinaryPath: string;
    fakeJavaPath: string;
    fakeGradlePath: string;
    workingDirectory: string;
  }) => Promise<void>
): Promise<void> {
  const tempDir = await mkdtemp(path.join(tmpdir(), "jongodb-launchers-probe-"));
  const brokenBinaryPath = path.join(tempDir, "broken-jongodb-binary");
  const fakeJavaPath = path.join(tempDir, "fake-java");
  const fakeGradlePath = path.join(tempDir, "fake-gradle");

  try {
    await writeFile(brokenBinaryPath, brokenBinaryScript(), "utf8");
    await writeFile(fakeJavaPath, fakeJavaLauncherScript(), "utf8");
    await writeFile(fakeGradlePath, fakeGradleClasspathProbeScript(), "utf8");
    await chmod(brokenBinaryPath, 0o755);
    await chmod(fakeJavaPath, 0o755);
    await chmod(fakeGradlePath, 0o755);
    await block({ brokenBinaryPath, fakeJavaPath, fakeGradlePath, workingDirectory: tempDir });
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
}

async function sha256(filePath: string): Promise<string> {
  const contents = await readFile(filePath);
  return createHash("sha256").update(contents).digest("hex");
}

function waitForExitCode(child: ReturnType<typeof spawn>): Promise<number | null> {
  return new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", (code) => {
      resolve(code);
    });
  });
}

async function waitForPidExit(pid: number, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (!isPidRunning(pid)) {
      return true;
    }
    await delay(50);
  }
  return !isPidRunning(pid);
}

function isPidRunning(pid: number): boolean {
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

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
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
const fs = require("node:fs");
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
const pidFile = process.env.JONGODB_PID_FILE;
if (typeof pidFile === "string" && pidFile.length > 0) {
  fs.writeFileSync(pidFile, String(process.pid));
}
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

function portCollisionBinaryLauncherScript(): string {
  return `#!/usr/bin/env node
const valueOf = (name, fallback) => {
  const prefixed = name + "=";
  const found = process.argv.find((arg) => arg.startsWith(prefixed));
  return found ? found.slice(prefixed.length) : fallback;
};
const host = valueOf("--host", "127.0.0.1");
const portRaw = Number(valueOf("--port", "0"));
const db = valueOf("--database", "test");
const port = Number.isInteger(portRaw) && portRaw > 0 ? portRaw : 27017;
const collisionPort = Number(process.env.JONGODB_COLLISION_PORT ?? "27017");
if (port === collisionPort) {
  console.error(
    "JONGODB_START_FAILURE=listen EADDRINUSE: address already in use " + host + ":" + port
  );
  process.exit(1);
}
console.log("JONGODB_URI=" + "mongodb://" + host + ":" + port + "/" + db);
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

function fakeGradleClasspathProbeScript(): string {
  return `#!/usr/bin/env sh
echo "> Task :printLauncherClasspath"
echo "/tmp/jongodb/fake-launcher-classpath.jar"
`;
}

function brokenGradleClasspathProbeScript(): string {
  return `#!/usr/bin/env sh
echo "simulated classpath probe failure" 1>&2
exit 1
`;
}
