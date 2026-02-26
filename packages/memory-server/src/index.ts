import { execFileSync, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import {
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { createRequire } from "node:module";
import { delimiter, dirname, join, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { createInterface } from "node:readline";

const moduleRequire = createRequire(
  resolve(process.cwd(), "__jongodb_module_resolver__.js")
);

const READY_PREFIX = "JONGODB_URI=";
const FAILURE_PREFIX = "JONGODB_START_FAILURE=";
const DEFAULT_HOST = "127.0.0.1";
const DEFAULT_DATABASE = "test";
const DEFAULT_STARTUP_TIMEOUT_MS = 15_000;
const DEFAULT_STOP_TIMEOUT_MS = 5_000;
const DEFAULT_LAUNCHER_CLASS = "org.jongodb.server.TcpMongoServerLauncher";
const DEFAULT_CLASSPATH_DISCOVERY_TASK_ARGS = [
  "--no-daemon",
  "-q",
  "printLauncherClasspath",
] as const;
const DEFAULT_ARTIFACT_CACHE_RELATIVE_DIR = ".jongodb/cache";
const DEFAULT_ARTIFACT_CACHE_MAX_ENTRIES = 32;
const DEFAULT_ARTIFACT_CACHE_MAX_BYTES = 5 * 1024 * 1024;
const DEFAULT_ARTIFACT_CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const MAX_LOG_LINES = 50;
const REDACTED_PLACEHOLDER = "<redacted>";
const URI_CREDENTIAL_PATTERN =
  /(mongodb(?:\+srv)?:\/\/)([^\/\s:@]+):([^@\s\/]+)@/giu;
const SECRET_ASSIGNMENT_PATTERN =
  /((?:password|passwd|pwd|token|secret|api[_-]?key|access[_-]?token)\s*[=:]\s*)([^,\s;]+)/giu;
const SECRET_QUERY_PATTERN =
  /([?&](?:password|passwd|pwd|token|secret|api[_-]?key|access[_-]?token)=)([^&\s]+)/giu;
const SECRET_JSON_PATTERN =
  /("(?:password|passwd|pwd|token|secret|api[_-]?key|access[_-]?token)"\s*:\s*")([^"]*)(")/giu;

const BUNDLED_BINARY_PACKAGE_PREFIX = "@jongodb/memory-server-bin";

type LogLevel = "silent" | "info" | "debug";
type LaunchMode = "auto" | "binary" | "java";
type DatabaseNameStrategy = "static" | "worker";
type TopologyProfile = "standalone" | "singleNodeReplicaSet";
type ClasspathDiscoveryMode = "auto" | "off";

export interface JongodbMemoryServerOptions {
  host?: string;
  port?: number;
  databaseName?: string;
  databaseNameSuffix?: string;
  databaseNameStrategy?: DatabaseNameStrategy;
  startupTimeoutMs?: number;
  stopTimeoutMs?: number;
  javaPath?: string;
  launcherClass?: string;
  classpath?: string | string[];
  classpathDiscovery?: ClasspathDiscoveryMode;
  classpathDiscoveryCommand?: string;
  classpathDiscoveryWorkingDirectory?: string;
  artifactCacheDir?: string;
  artifactCacheMaxEntries?: number;
  artifactCacheMaxBytes?: number;
  artifactCacheTtlMs?: number;
  binaryPath?: string;
  binaryChecksum?: string;
  launchMode?: LaunchMode;
  topologyProfile?: TopologyProfile;
  replicaSetName?: string;
  env?: Record<string, string>;
  logLevel?: LogLevel;
  onStartupTelemetry?: (event: JongodbStartupTelemetry) => void;
}

export interface JongodbMemoryServer {
  readonly uri: string;
  readonly pid: number;
  detach(): void;
  stop(): Promise<void>;
}

export interface JongodbStartupTelemetry {
  attempt: number;
  mode: "binary" | "java";
  source: string;
  startupDurationMs: number;
  success: boolean;
  errorMessage?: string;
}

interface ExitResult {
  code: number | null;
  signal: NodeJS.Signals | null;
}

interface SpawnLaunchConfig {
  mode: "binary" | "java";
  command: string;
  args: string[];
  source: string;
  topologyProfile: TopologyProfile;
  replicaSetName: string;
  binaryChecksum?: string;
  binaryChecksumSource?: string;
  binaryChecksumTargetPath?: string;
}

interface LaunchResolution {
  launchConfigs: SpawnLaunchConfig[];
  deferredJavaFallback?: () => DeferredJavaFallbackResolution;
}

interface DeferredJavaFallbackResolution {
  launchConfig: SpawnLaunchConfig | null;
  diagnostics?: string;
}

interface JavaCandidateResolution {
  candidate: {
    javaPath: string;
    classpath: string;
    launcherClass: string;
    source: string;
  } | null;
  diagnostics?: string;
}

interface BinaryCandidate {
  path: string;
  source: string;
  checksum?: string;
  checksumSource?: string;
  checksumTargetPath?: string;
}

interface ArtifactCachePolicy {
  dir: string;
  maxEntries: number;
  maxBytes: number;
  ttlMs: number;
}

export async function startJongodbMemoryServer(
  options: JongodbMemoryServerOptions = {}
): Promise<JongodbMemoryServer> {
  const startupTimeoutMs = normalizeTimeout(
    options.startupTimeoutMs,
    DEFAULT_STARTUP_TIMEOUT_MS,
    "startupTimeoutMs"
  );
  const stopTimeoutMs = normalizeTimeout(
    options.stopTimeoutMs,
    DEFAULT_STOP_TIMEOUT_MS,
    "stopTimeoutMs"
  );
  const host = normalizeHost(options.host);
  const port = normalizePort(options.port);
  const databaseName = resolveDatabaseName(options);
  const topologyProfile = normalizeTopologyProfile(options.topologyProfile);
  const replicaSetName = normalizeReplicaSetName(options.replicaSetName);
  const logLevel = options.logLevel ?? "silent";
  const onStartupTelemetry = options.onStartupTelemetry;
  const launchResolution = resolveLaunchConfigs(options, {
    host,
    port,
    databaseName,
    topologyProfile,
    replicaSetName,
  });
  const launchConfigs = launchResolution.launchConfigs;
  const launchErrors: string[] = [];

  let attempt = 0;
  for (const launchConfig of launchConfigs) {
    const startupStartedAt = performance.now();
    attempt += 1;
    try {
      const server = await startWithLaunchConfig(launchConfig, {
        startupTimeoutMs,
        stopTimeoutMs,
        logLevel,
        env: options.env,
      });
      emitStartupTelemetry(onStartupTelemetry, {
        attempt,
        mode: launchConfig.mode,
        source: launchConfig.source,
        startupDurationMs: roundDurationMs(performance.now() - startupStartedAt),
        success: true,
      });
      return server;
    } catch (error: unknown) {
      const normalized = wrapError(error);
      emitStartupTelemetry(onStartupTelemetry, {
        attempt,
        mode: launchConfig.mode,
        source: launchConfig.source,
        startupDurationMs: roundDurationMs(performance.now() - startupStartedAt),
        success: false,
        errorMessage: normalized.message,
      });
      launchErrors.push(
        `[${launchConfig.mode}:${launchConfig.source}] ${normalized.message}`
      );
    }
  }

  if (launchResolution.deferredJavaFallback !== undefined) {
    const deferredAttempt = launchResolution.deferredJavaFallback();
    if (deferredAttempt.launchConfig !== null) {
      const launchConfig = deferredAttempt.launchConfig;
      const startupStartedAt = performance.now();
      attempt += 1;
      try {
        const server = await startWithLaunchConfig(launchConfig, {
          startupTimeoutMs,
          stopTimeoutMs,
          logLevel,
          env: options.env,
        });
        emitStartupTelemetry(onStartupTelemetry, {
          attempt,
          mode: launchConfig.mode,
          source: launchConfig.source,
          startupDurationMs: roundDurationMs(performance.now() - startupStartedAt),
          success: true,
        });
        return server;
      } catch (error: unknown) {
        const normalized = wrapError(error);
        emitStartupTelemetry(onStartupTelemetry, {
          attempt,
          mode: launchConfig.mode,
          source: launchConfig.source,
          startupDurationMs: roundDurationMs(performance.now() - startupStartedAt),
          success: false,
          errorMessage: normalized.message,
        });
        launchErrors.push(
          `[${launchConfig.mode}:${launchConfig.source}] ${normalized.message}`
        );
      }
    } else if (deferredAttempt.diagnostics !== undefined) {
      launchErrors.push(`[java:classpath-auto-discovery] ${deferredAttempt.diagnostics}`);
    }
  }

  throw new Error(
    [
      "Failed to start jongodb with available launch configurations.",
      ...launchErrors,
    ].join(" ")
  );
}

async function startWithLaunchConfig(
  launchConfig: SpawnLaunchConfig,
  context: {
    startupTimeoutMs: number;
    stopTimeoutMs: number;
    logLevel: LogLevel;
    env?: Record<string, string>;
  }
): Promise<JongodbMemoryServer> {
  if (launchConfig.mode === "binary") {
    verifyBinaryChecksum(launchConfig);
  }

  const child = spawn(launchConfig.command, launchConfig.args, {
    stdio: ["ignore", "pipe", "pipe"],
    windowsHide: true,
    env: {
      ...process.env,
      ...context.env,
    },
  });

  const stdoutLines: string[] = [];
  const stderrLines: string[] = [];
  let exitResult: ExitResult | null = null;
  let stopped = false;

  child.on("exit", (code, signal) => {
    exitResult = { code, signal };
  });

  const stdoutReader = createInterface({ input: child.stdout });
  const stderrReader = createInterface({ input: child.stderr });

  const startupResult = await waitForStartup({
    child,
    stdoutReader,
    stderrReader,
    stdoutLines,
    stderrLines,
    startupTimeoutMs: context.startupTimeoutMs,
    logLevel: context.logLevel,
    launchDescription: `${launchConfig.mode}:${launchConfig.source}`,
  }).catch(async (error: unknown) => {
    await forceStopIfAlive(child, context.stopTimeoutMs);
    throw wrapError(error);
  });

  try {
    ensureUriTopologyProfileSync(
      startupResult.uri,
      launchConfig.topologyProfile,
      launchConfig.replicaSetName
    );
  } catch (error: unknown) {
    await forceStopIfAlive(child, context.stopTimeoutMs);
    throw wrapError(error);
  }

  const stop = async (): Promise<void> => {
    if (stopped) {
      return;
    }
    stopped = true;

    stdoutReader.close();
    stderrReader.close();

    if (exitResult !== null || child.exitCode !== null) {
      return;
    }

    const terminated = child.kill("SIGTERM");
    if (!terminated) {
      if (exitResult !== null || child.exitCode !== null) {
        return;
      }
      throw new Error(
        "Failed to stop jongodb server process: unable to send SIGTERM."
      );
    }

    const gracefulExit = await waitForExit(child, context.stopTimeoutMs);
    if (gracefulExit !== null) {
      return;
    }

    const killed = child.kill("SIGKILL");
    if (!killed) {
      throw new Error(
        "Failed to stop jongodb server process: SIGTERM timeout and SIGKILL failed."
      );
    }

    const forcedExit = await waitForExit(child, context.stopTimeoutMs);
    if (forcedExit === null) {
      throw new Error(
        "Failed to stop jongodb server process: process did not exit after SIGKILL."
      );
    }
  };

  if (child.pid === undefined) {
    throw new Error("Jongodb process started without a PID.");
  }

  const detach = (): void => {
    stdoutReader.close();
    stderrReader.close();
    child.stdout.destroy();
    child.stderr.destroy();
    child.unref();
  };

  return {
    uri: startupResult.uri,
    pid: child.pid,
    detach,
    stop,
  };
}

function resolveLaunchConfigs(
  options: JongodbMemoryServerOptions,
  context: {
    host: string;
    port: number;
    databaseName: string;
    topologyProfile: TopologyProfile;
    replicaSetName: string;
  }
): LaunchResolution {
  const mode = options.launchMode ?? "auto";
  if (mode !== "auto" && mode !== "binary" && mode !== "java") {
    throw new Error(`launchMode must be one of: auto, binary, java (got: ${mode}).`);
  }

  const binary = resolveBinaryCandidate(options);
  const javaWithoutDiscovery = resolveJavaCandidate(options, {
    allowClasspathDiscovery: false,
  });

  if (mode === "binary") {
    if (binary === null) {
      throw new Error(
        [
          "Binary launch mode requested but no binary was found.",
          "Provide options.binaryPath or JONGODB_BINARY_PATH.",
          bundledBinaryHint(),
        ].join(" ")
      );
    }
    return {
      launchConfigs: [toBinaryLaunchConfig(binary, context)],
    };
  }

  if (mode === "java") {
    const javaWithDiscovery = resolveJavaCandidate(options, {
      allowClasspathDiscovery: true,
    });
    if (javaWithDiscovery.candidate === null) {
      throw new Error(
        [
          "Java launch mode requested but Java classpath is not configured.",
          "Pass options.classpath or set JONGODB_CLASSPATH.",
          "Classpath auto-discovery uses Gradle task `printLauncherClasspath` by default.",
          javaWithDiscovery.diagnostics ??
            "Set options.classpathDiscovery='off' (or JONGODB_CLASSPATH_DISCOVERY=off) to skip probing.",
        ].join(" ")
      );
    }
    return {
      launchConfigs: [toJavaLaunchConfig(javaWithDiscovery.candidate, context)],
    };
  }

  if (binary !== null && javaWithoutDiscovery.candidate !== null) {
    return {
      launchConfigs: [
        toBinaryLaunchConfig(binary, context),
        toJavaLaunchConfig(javaWithoutDiscovery.candidate, context),
      ],
    };
  }

  if (binary !== null) {
    return {
      launchConfigs: [toBinaryLaunchConfig(binary, context)],
      deferredJavaFallback: () => {
        const discoveredJava = resolveJavaCandidate(options, {
          allowClasspathDiscovery: true,
        });
        if (discoveredJava.candidate === null) {
          return {
            launchConfig: null,
            diagnostics: discoveredJava.diagnostics,
          };
        }
        return {
          launchConfig: toJavaLaunchConfig(discoveredJava.candidate, context),
        };
      },
    };
  }

  if (javaWithoutDiscovery.candidate !== null) {
    return {
      launchConfigs: [toJavaLaunchConfig(javaWithoutDiscovery.candidate, context)],
    };
  }

  const javaWithDiscovery = resolveJavaCandidate(options, {
    allowClasspathDiscovery: true,
  });
  if (javaWithDiscovery.candidate !== null) {
    return {
      launchConfigs: [toJavaLaunchConfig(javaWithDiscovery.candidate, context)],
    };
  }

  throw new Error(
    [
      "No launcher runtime configured.",
      "Provide one of:",
      "1) options.binaryPath or JONGODB_BINARY_PATH",
      "2) options.classpath or JONGODB_CLASSPATH",
      javaWithDiscovery.diagnostics ?? "",
      bundledBinaryHint(),
    ].join(" ")
  );
}

function toBinaryLaunchConfig(
  binary: BinaryCandidate,
  context: {
    host: string;
    port: number;
    databaseName: string;
    topologyProfile: TopologyProfile;
    replicaSetName: string;
  }
): SpawnLaunchConfig {
  const args = [
    `--host=${context.host}`,
    `--port=${context.port}`,
    `--database=${context.databaseName}`,
    `--topology-profile=${context.topologyProfile}`,
  ];
  if (context.topologyProfile === "singleNodeReplicaSet") {
    args.push(`--replica-set-name=${context.replicaSetName}`);
  }
  return {
    mode: "binary",
    command: binary.path,
    args,
    source: binary.source,
    topologyProfile: context.topologyProfile,
    replicaSetName: context.replicaSetName,
    binaryChecksum: binary.checksum,
    binaryChecksumSource: binary.checksumSource,
    binaryChecksumTargetPath: binary.checksumTargetPath,
  };
}

function toJavaLaunchConfig(
  java: { javaPath: string; classpath: string; launcherClass: string; source: string },
  context: {
    host: string;
    port: number;
    databaseName: string;
    topologyProfile: TopologyProfile;
    replicaSetName: string;
  }
): SpawnLaunchConfig {
  const launcherArgs = [
    `--host=${context.host}`,
    `--port=${context.port}`,
    `--database=${context.databaseName}`,
    `--topology-profile=${context.topologyProfile}`,
  ];
  if (context.topologyProfile === "singleNodeReplicaSet") {
    launcherArgs.push(`--replica-set-name=${context.replicaSetName}`);
  }
  return {
    mode: "java",
    command: java.javaPath,
    args: [
      "-cp",
      java.classpath,
      java.launcherClass,
      ...launcherArgs,
    ],
    source: java.source,
    topologyProfile: context.topologyProfile,
    replicaSetName: context.replicaSetName,
  };
}

function ensureUriTopologyProfileSync(
  uri: string,
  topologyProfile: TopologyProfile,
  replicaSetName: string
): void {
  let parsedUri: URL;
  try {
    parsedUri = new URL(uri);
  } catch {
    throw new Error(`Launcher emitted invalid URI: ${uri}`);
  }

  const replicaSetFromUri = parsedUri.searchParams.get("replicaSet");
  if (topologyProfile === "standalone") {
    if (replicaSetFromUri !== null && replicaSetFromUri.trim().length > 0) {
      throw new Error(
        [
          "Launcher URI topology options are out of sync.",
          "Requested topologyProfile=standalone but URI includes replicaSet query.",
          `uri=${uri}`,
        ].join(" ")
      );
    }
    return;
  }

  if (replicaSetFromUri === null || replicaSetFromUri.trim().length === 0) {
    throw new Error(
      [
        "Launcher URI topology options are out of sync.",
        "Requested topologyProfile=singleNodeReplicaSet but URI is missing replicaSet query.",
        `expectedReplicaSet=${replicaSetName}`,
        `uri=${uri}`,
      ].join(" ")
    );
  }

  const normalizedReplicaSet = replicaSetFromUri.trim();
  if (normalizedReplicaSet !== replicaSetName) {
    throw new Error(
      [
        "Launcher URI topology options are out of sync.",
        "Requested replicaSetName does not match URI replicaSet query.",
        `expectedReplicaSet=${replicaSetName}`,
        `actualReplicaSet=${normalizedReplicaSet}`,
        `uri=${uri}`,
      ].join(" ")
    );
  }
}

function resolveBinaryCandidate(options: JongodbMemoryServerOptions): BinaryCandidate | null {
  const fromOption = normalizeBinaryPath(options.binaryPath, "options.binaryPath");
  if (fromOption !== null) {
    return {
      path: fromOption,
      source: "options.binaryPath",
      ...resolveExplicitBinaryChecksum(options.binaryChecksum),
    };
  }

  const fromEnv = normalizeBinaryPath(process.env.JONGODB_BINARY_PATH, "JONGODB_BINARY_PATH");
  if (fromEnv !== null) {
    return {
      path: fromEnv,
      source: "JONGODB_BINARY_PATH",
      ...resolveExplicitBinaryChecksum(options.binaryChecksum),
    };
  }

  const fromBundled = resolveBundledBinaryPath();
  if (fromBundled !== null) {
    return fromBundled;
  }

  return null;
}

function normalizeBinaryPath(value: string | undefined, fieldName: string): string | null {
  if (value === undefined) {
    return null;
  }
  const normalized = value.trim();
  if (normalized.length === 0) {
    throw new Error(`${fieldName} must not be empty when provided.`);
  }
  return normalized;
}

function resolveBundledBinaryPath(): BinaryCandidate | null {
  const candidates = bundledBinaryPackageCandidates();
  for (const packageName of candidates) {
    const candidate = resolveBinaryPathFromPackage(packageName);
    if (candidate !== null) {
      return {
        path: candidate.path,
        source: `bundled-package:${packageName}`,
        checksum: candidate.checksum,
        checksumSource: `bundled-package:${packageName}:jongodb.sha256`,
        checksumTargetPath: candidate.checksumTargetPath,
      };
    }
  }
  return null;
}

function bundledBinaryPackageCandidates(): string[] {
  const platform = process.platform;
  const arch = process.arch;

  if (platform === "darwin") {
    return [`${BUNDLED_BINARY_PACKAGE_PREFIX}-darwin-${arch}`];
  }

  if (platform === "win32") {
    return [`${BUNDLED_BINARY_PACKAGE_PREFIX}-win32-${arch}`];
  }

  if (platform === "linux") {
    const libcVariant = detectLinuxLibcVariant();
    return [
      `${BUNDLED_BINARY_PACKAGE_PREFIX}-linux-${arch}-${libcVariant}`,
      `${BUNDLED_BINARY_PACKAGE_PREFIX}-linux-${arch}`,
    ];
  }

  return [];
}

function detectLinuxLibcVariant(): "gnu" | "musl" {
  const report = process.report?.getReport?.() as
    | { header?: { glibcVersionRuntime?: string } }
    | undefined;
  const glibcVersionRuntime = report?.header?.glibcVersionRuntime;
  if (typeof glibcVersionRuntime === "string" && glibcVersionRuntime.length > 0) {
    return "gnu";
  }
  return "musl";
}

function resolveBinaryPathFromPackage(
  packageName: string
): { path: string; checksum: string; checksumTargetPath: string } | null {
  try {
    const packageJsonPath = moduleRequire.resolve(`${packageName}/package.json`);
    const packageDir = dirname(packageJsonPath);
    const packageJson = JSON.parse(readFileSync(packageJsonPath, "utf8")) as {
      bin?: string | Record<string, string>;
      jongodb?: { binary?: string; sha256?: string; sha256Target?: string };
    };

    const binaryRelativePath =
      readJongodbBinaryPath(packageJson) ??
      readBinEntryPath(packageJson.bin);

    if (binaryRelativePath === null) {
      return null;
    }

    const checksum = normalizeRequiredChecksum(
      packageJson.jongodb?.sha256,
      `${packageName} package.json#jongodb.sha256`
    );
    const checksumTargetRelativePath = readJongodbChecksumTargetPath(
      packageJson,
      binaryRelativePath
    );

    return {
      path: resolve(packageDir, binaryRelativePath),
      checksum,
      checksumTargetPath: resolve(packageDir, checksumTargetRelativePath),
    };
  } catch {
    return null;
  }
}

function readJongodbBinaryPath(packageJson: {
  jongodb?: { binary?: string };
}): string | null {
  const binary = packageJson.jongodb?.binary;
  if (typeof binary !== "string") {
    return null;
  }
  const normalized = binary.trim();
  return normalized.length === 0 ? null : normalized;
}

function readJongodbChecksumTargetPath(
  packageJson: {
    jongodb?: { sha256Target?: string };
  },
  fallbackRelativePath: string
): string {
  const target = packageJson.jongodb?.sha256Target;
  if (typeof target !== "string") {
    return fallbackRelativePath;
  }
  const normalized = target.trim();
  if (normalized.length === 0) {
    return fallbackRelativePath;
  }
  return normalized;
}

function resolveExplicitBinaryChecksum(
  explicitChecksum: string | undefined
): { checksum?: string; checksumSource?: string } {
  const fromOption = normalizeOptionalChecksum(
    explicitChecksum,
    "options.binaryChecksum"
  );
  if (fromOption !== undefined) {
    return {
      checksum: fromOption,
      checksumSource: "options.binaryChecksum",
    };
  }

  const fromEnv = normalizeOptionalChecksum(
    process.env.JONGODB_BINARY_CHECKSUM,
    "JONGODB_BINARY_CHECKSUM"
  );
  if (fromEnv !== undefined) {
    return {
      checksum: fromEnv,
      checksumSource: "JONGODB_BINARY_CHECKSUM",
    };
  }

  return {};
}

function normalizeRequiredChecksum(
  value: string | undefined,
  fieldName: string
): string {
  if (value === undefined) {
    throw new Error(`${fieldName} is required.`);
  }

  const normalized = value.trim().toLowerCase();
  if (normalized.length === 0) {
    throw new Error(`${fieldName} must not be empty.`);
  }
  assertSha256Hex(normalized, fieldName);
  return normalized;
}

function normalizeOptionalChecksum(
  value: string | undefined,
  fieldName: string
): string | undefined {
  if (value === undefined) {
    return undefined;
  }

  const normalized = value.trim().toLowerCase();
  if (normalized.length === 0) {
    return undefined;
  }
  assertSha256Hex(normalized, fieldName);
  return normalized;
}

function assertSha256Hex(value: string, fieldName: string): void {
  if (!/^[a-f0-9]{64}$/u.test(value)) {
    throw new Error(
      `${fieldName} must be a 64-character lowercase hex SHA-256 value.`
    );
  }
}

function readBinEntryPath(
  value: string | Record<string, string> | undefined
): string | null {
  if (typeof value === "string") {
    const normalized = value.trim();
    return normalized.length === 0 ? null : normalized;
  }

  if (value !== undefined && typeof value === "object") {
    const preferred = value["jongodb-memory-server"];
    if (typeof preferred === "string" && preferred.trim().length > 0) {
      return preferred.trim();
    }

    for (const candidate of Object.values(value)) {
      if (typeof candidate === "string" && candidate.trim().length > 0) {
        return candidate.trim();
      }
    }
  }

  return null;
}

function bundledBinaryHint(): string {
  const candidates = bundledBinaryPackageCandidates();
  if (candidates.length === 0) {
    return "Bundled platform binary package is not defined for this OS/architecture.";
  }
  return `Bundled platform binary package candidates: ${candidates.join(", ")}.`;
}

function resolveJavaCandidate(
  options: JongodbMemoryServerOptions,
  context: { allowClasspathDiscovery: boolean }
): JavaCandidateResolution {
  const explicitClasspath = resolveExplicitClasspath(options.classpath);
  if (explicitClasspath !== null) {
    return {
      candidate: {
        javaPath: options.javaPath?.trim() || process.env.JONGODB_JAVA_PATH || "java",
        classpath: explicitClasspath,
        launcherClass: options.launcherClass?.trim() || DEFAULT_LAUNCHER_CLASS,
        source: "options.classpath",
      },
    };
  }

  const fromEnv = process.env.JONGODB_CLASSPATH?.trim();
  if (fromEnv !== undefined && fromEnv.length > 0) {
    return {
      candidate: {
        javaPath: options.javaPath?.trim() || process.env.JONGODB_JAVA_PATH || "java",
        classpath: fromEnv,
        launcherClass: options.launcherClass?.trim() || DEFAULT_LAUNCHER_CLASS,
        source: "JONGODB_CLASSPATH",
      },
    };
  }

  if (!context.allowClasspathDiscovery) {
    return {
      candidate: null,
    };
  }

  const discovered = resolveClasspathViaAutoDiscovery(options);
  if (discovered.classpath === null) {
    return {
      candidate: null,
      diagnostics: discovered.diagnostics,
    };
  }

  return {
    candidate: {
      javaPath: options.javaPath?.trim() || process.env.JONGODB_JAVA_PATH || "java",
      classpath: discovered.classpath,
      launcherClass: options.launcherClass?.trim() || DEFAULT_LAUNCHER_CLASS,
      source: "classpath-auto-discovery",
    },
    diagnostics: discovered.diagnostics,
  };
}

function resolveClasspathViaAutoDiscovery(options: JongodbMemoryServerOptions): {
  classpath: string | null;
  diagnostics: string;
} {
  if (!isClasspathDiscoveryEnabled(options.classpathDiscovery)) {
    return {
      classpath: null,
      diagnostics:
        "Classpath auto-discovery disabled (set classpathDiscovery='auto' or unset JONGODB_CLASSPATH_DISCOVERY=off).",
    };
  }

  const command = resolveClasspathDiscoveryCommand(options);
  const workingDirectory = resolveClasspathDiscoveryWorkingDirectory(options);
  const commandDescription = `${command} ${DEFAULT_CLASSPATH_DISCOVERY_TASK_ARGS.join(" ")}`;
  const artifactCachePolicy = resolveArtifactCachePolicy(options);
  const cacheKey = buildClasspathDiscoveryCacheKey(commandDescription, workingDirectory);

  pruneArtifactCache(artifactCachePolicy);
  const cachedClasspath = readClasspathDiscoveryCache(
    artifactCachePolicy,
    cacheKey
  );
  if (cachedClasspath !== null) {
    return {
      classpath: cachedClasspath,
      diagnostics: [
        "Classpath auto-discovery cache hit.",
        `Cache key: ${cacheKey}`,
        `Cache dir: ${artifactCachePolicy.dir}`,
      ].join(" "),
    };
  }

  let output: string;
  try {
    output = execFileSync(command, [...DEFAULT_CLASSPATH_DISCOVERY_TASK_ARGS], {
      cwd: workingDirectory,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    return {
      classpath: null,
      diagnostics: [
        "Classpath auto-discovery probe failed.",
        `Attempted command: ${commandDescription}`,
        `Working directory: ${workingDirectory}`,
        `Cause: ${message}`,
      ].join(" "),
    };
  }

  const lines = output
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  const classpath = lines.at(-1);
  if (classpath === undefined || classpath.length === 0) {
    return {
      classpath: null,
      diagnostics: [
        "Classpath auto-discovery probe returned empty output.",
        `Attempted command: ${commandDescription}`,
        `Working directory: ${workingDirectory}`,
      ].join(" "),
    };
  }

  writeClasspathDiscoveryCache(
    artifactCachePolicy,
    cacheKey,
    commandDescription,
    workingDirectory,
    classpath
  );
  pruneArtifactCache(artifactCachePolicy);

  return {
    classpath,
    diagnostics: [
      "Classpath auto-discovery succeeded.",
      `Attempted command: ${commandDescription}`,
      `Working directory: ${workingDirectory}`,
      `Cache key: ${cacheKey}`,
      `Cache dir: ${artifactCachePolicy.dir}`,
    ].join(" "),
  };
}

function isClasspathDiscoveryEnabled(
  mode: JongodbMemoryServerOptions["classpathDiscovery"]
): boolean {
  const candidate = mode ?? process.env.JONGODB_CLASSPATH_DISCOVERY ?? "auto";
  const normalized = candidate.trim().toLowerCase();
  if (normalized === "auto" || normalized === "on" || normalized === "enabled") {
    return true;
  }
  if (
    normalized === "off" ||
    normalized === "disabled" ||
    normalized === "false" ||
    normalized === "0"
  ) {
    return false;
  }
  throw new Error(
    `classpathDiscovery must be 'auto' or 'off' (received: ${String(candidate)}).`
  );
}

function resolveClasspathDiscoveryCommand(options: JongodbMemoryServerOptions): string {
  const explicit = options.classpathDiscoveryCommand?.trim();
  if (explicit !== undefined && explicit.length > 0) {
    return explicit;
  }

  const fromEnv = process.env.JONGODB_CLASSPATH_DISCOVERY_CMD?.trim();
  if (fromEnv !== undefined && fromEnv.length > 0) {
    return fromEnv;
  }

  const cwd = resolveClasspathDiscoveryWorkingDirectory(options);
  const repoLocalGradle = resolve(cwd, ".tooling", "gradle-8.10.2", "bin", "gradle");
  if (existsSync(repoLocalGradle)) {
    return repoLocalGradle;
  }

  return "gradle";
}

function resolveClasspathDiscoveryWorkingDirectory(
  options: JongodbMemoryServerOptions
): string {
  const fromOption = options.classpathDiscoveryWorkingDirectory?.trim();
  if (fromOption !== undefined && fromOption.length > 0) {
    return fromOption;
  }

  const fromEnv = process.env.JONGODB_CLASSPATH_DISCOVERY_CWD?.trim();
  if (fromEnv !== undefined && fromEnv.length > 0) {
    return fromEnv;
  }

  return process.cwd();
}

function resolveArtifactCachePolicy(
  options: JongodbMemoryServerOptions
): ArtifactCachePolicy {
  const fromOption = options.artifactCacheDir?.trim();
  const fromEnv = process.env.JONGODB_ARTIFACT_CACHE_DIR?.trim();
  const dir =
    (fromOption !== undefined && fromOption.length > 0
      ? fromOption
      : fromEnv !== undefined && fromEnv.length > 0
      ? fromEnv
      : undefined) ?? resolve(process.cwd(), DEFAULT_ARTIFACT_CACHE_RELATIVE_DIR);

  return {
    dir,
    maxEntries: resolvePositiveIntegerOption(
      options.artifactCacheMaxEntries,
      "JONGODB_ARTIFACT_CACHE_MAX_ENTRIES",
      DEFAULT_ARTIFACT_CACHE_MAX_ENTRIES,
      "artifactCacheMaxEntries"
    ),
    maxBytes: resolvePositiveIntegerOption(
      options.artifactCacheMaxBytes,
      "JONGODB_ARTIFACT_CACHE_MAX_BYTES",
      DEFAULT_ARTIFACT_CACHE_MAX_BYTES,
      "artifactCacheMaxBytes"
    ),
    ttlMs: resolvePositiveIntegerOption(
      options.artifactCacheTtlMs,
      "JONGODB_ARTIFACT_CACHE_TTL_MS",
      DEFAULT_ARTIFACT_CACHE_TTL_MS,
      "artifactCacheTtlMs"
    ),
  };
}

function resolvePositiveIntegerOption(
  fromOption: number | undefined,
  envVarName: string,
  fallback: number,
  fieldName: string
): number {
  if (fromOption !== undefined) {
    if (!Number.isFinite(fromOption) || fromOption <= 0) {
      throw new Error(`${fieldName} must be a positive number.`);
    }
    return Math.floor(fromOption);
  }

  const rawFromEnv = process.env[envVarName]?.trim();
  if (rawFromEnv !== undefined && rawFromEnv.length > 0) {
    const parsed = Number(rawFromEnv);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      throw new Error(`${envVarName} must be a positive number.`);
    }
    return Math.floor(parsed);
  }

  return fallback;
}

function buildClasspathDiscoveryCacheKey(
  commandDescription: string,
  workingDirectory: string
): string {
  return createHash("sha256")
    .update(`${commandDescription}\n${workingDirectory}`)
    .digest("hex");
}

function pruneArtifactCache(policy: ArtifactCachePolicy): void {
  if (!existsSync(policy.dir)) {
    return;
  }

  let entries = readClasspathCacheFiles(policy.dir);
  if (entries.length === 0) {
    return;
  }

  const now = Date.now();
  const staleEntries = entries.filter(
    (entry) => now - entry.mtimeMs > policy.ttlMs
  );
  for (const stale of staleEntries) {
    safeRemoveFile(stale.fullPath);
  }
  entries = entries.filter((entry) => now - entry.mtimeMs <= policy.ttlMs);

  entries.sort((left, right) => left.mtimeMs - right.mtimeMs);
  while (entries.length > policy.maxEntries) {
    const oldest = entries.shift();
    if (oldest === undefined) {
      break;
    }
    safeRemoveFile(oldest.fullPath);
  }

  let totalBytes = entries.reduce((sum, entry) => sum + entry.sizeBytes, 0);
  while (totalBytes > policy.maxBytes && entries.length > 0) {
    const oldest = entries.shift();
    if (oldest === undefined) {
      break;
    }
    safeRemoveFile(oldest.fullPath);
    totalBytes -= oldest.sizeBytes;
  }
}

function readClasspathDiscoveryCache(
  policy: ArtifactCachePolicy,
  cacheKey: string
): string | null {
  const filePath = join(policy.dir, `classpath-${cacheKey}.json`);
  if (!existsSync(filePath)) {
    return null;
  }

  try {
    const raw = JSON.parse(readFileSync(filePath, "utf8")) as {
      classpath?: string;
      updatedAt?: number;
    };

    if (typeof raw.classpath !== "string" || raw.classpath.trim().length === 0) {
      safeRemoveFile(filePath);
      return null;
    }
    if (typeof raw.updatedAt !== "number" || !Number.isFinite(raw.updatedAt)) {
      safeRemoveFile(filePath);
      return null;
    }
    if (Date.now() - raw.updatedAt > policy.ttlMs) {
      safeRemoveFile(filePath);
      return null;
    }
    return raw.classpath.trim();
  } catch {
    safeRemoveFile(filePath);
    return null;
  }
}

function writeClasspathDiscoveryCache(
  policy: ArtifactCachePolicy,
  cacheKey: string,
  commandDescription: string,
  workingDirectory: string,
  classpath: string
): void {
  mkdirSync(policy.dir, { recursive: true });
  const filePath = join(policy.dir, `classpath-${cacheKey}.json`);
  const payload = {
    schema: "classpath-discovery-cache-v1",
    classpath,
    commandDescription,
    workingDirectory,
    updatedAt: Date.now(),
  };
  writeFileSync(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
}

function readClasspathCacheFiles(cacheDir: string): Array<{
  fullPath: string;
  mtimeMs: number;
  sizeBytes: number;
}> {
  let fileNames: string[];
  try {
    fileNames = readdirSync(cacheDir);
  } catch {
    return [];
  }

  const results: Array<{ fullPath: string; mtimeMs: number; sizeBytes: number }> = [];
  for (const fileName of fileNames) {
    if (!/^classpath-[a-f0-9]{64}\.json$/u.test(fileName)) {
      continue;
    }
    const fullPath = join(cacheDir, fileName);
    try {
      const stats = statSync(fullPath);
      if (!stats.isFile()) {
        continue;
      }
      results.push({
        fullPath,
        mtimeMs: stats.mtimeMs,
        sizeBytes: stats.size,
      });
    } catch {
      // skip transiently unavailable files
    }
  }
  return results;
}

function safeRemoveFile(filePath: string): void {
  try {
    rmSync(filePath, { force: true });
  } catch {
    // best-effort cleanup
  }
}

function normalizeTimeout(
  value: number | undefined,
  fallback: number,
  fieldName: string
): number {
  const resolved = value ?? fallback;
  if (!Number.isFinite(resolved) || resolved <= 0) {
    throw new Error(`${fieldName} must be a positive number.`);
  }
  return Math.floor(resolved);
}

function normalizeHost(host: string | undefined): string {
  const normalized = host?.trim() || DEFAULT_HOST;
  if (normalized.length === 0) {
    throw new Error("host must not be empty.");
  }
  return normalized;
}

function normalizePort(port: number | undefined): number {
  const normalized = port ?? 0;
  if (!Number.isInteger(normalized) || normalized < 0 || normalized > 65535) {
    throw new Error("port must be an integer between 0 and 65535.");
  }
  return normalized;
}

function normalizeTopologyProfile(
  profile: TopologyProfile | undefined
): TopologyProfile {
  const normalized = profile ?? "standalone";
  if (normalized !== "standalone" && normalized !== "singleNodeReplicaSet") {
    throw new Error(
      `topologyProfile must be one of: standalone, singleNodeReplicaSet (got: ${String(profile)}).`
    );
  }
  return normalized;
}

function normalizeReplicaSetName(replicaSetName: string | undefined): string {
  const normalized = replicaSetName?.trim();
  if (normalized === undefined || normalized.length === 0) {
    return "jongodb-rs0";
  }
  return normalized;
}

function resolveDatabaseName(options: JongodbMemoryServerOptions): string {
  const base = normalizeDatabaseNameBase(options.databaseName);
  const explicitSuffix = normalizeDatabaseNameSuffix(options.databaseNameSuffix);
  const workerSuffix = resolveWorkerDatabaseNameSuffix(options.databaseNameStrategy);
  return `${base}${explicitSuffix}${workerSuffix}`;
}

function normalizeDatabaseNameBase(databaseName: string | undefined): string {
  return databaseName?.trim() || DEFAULT_DATABASE;
}

function normalizeDatabaseNameSuffix(databaseNameSuffix: string | undefined): string {
  if (databaseNameSuffix === undefined) {
    return "";
  }

  const normalized = databaseNameSuffix.trim();
  if (normalized.length === 0) {
    throw new Error("databaseNameSuffix must not be empty when provided.");
  }
  return normalized;
}

function resolveWorkerDatabaseNameSuffix(
  strategy: DatabaseNameStrategy | undefined
): string {
  const resolvedStrategy = strategy ?? "static";
  if (resolvedStrategy !== "static" && resolvedStrategy !== "worker") {
    throw new Error(
      `databaseNameStrategy must be one of: static, worker (got: ${String(strategy)}).`
    );
  }

  if (resolvedStrategy === "static") {
    return "";
  }

  const workerToken = resolveWorkerToken();
  return `_w${sanitizeDatabaseNameToken(workerToken)}`;
}

function resolveWorkerToken(): string {
  const envCandidates = [
    process.env.JONGODB_WORKER_ID,
    process.env.JEST_WORKER_ID,
    process.env.VITEST_WORKER_ID,
    process.env.VITEST_POOL_ID,
    process.env.NODE_UNIQUE_ID,
  ];

  for (const candidate of envCandidates) {
    const normalized = candidate?.trim();
    if (normalized !== undefined && normalized.length > 0) {
      return normalized;
    }
  }

  return String(process.pid);
}

function sanitizeDatabaseNameToken(token: string): string {
  const sanitized = token.replace(/[^A-Za-z0-9_-]/g, "_");
  if (sanitized.length > 0) {
    return sanitized;
  }
  return "unknown";
}

function resolveExplicitClasspath(
  classpath: string | string[] | undefined
): string | null {
  if (typeof classpath === "string") {
    const normalized = classpath.trim();
    if (normalized.length === 0) {
      throw new Error("classpath string is empty.");
    }
    return normalized;
  }

  if (Array.isArray(classpath)) {
    if (classpath.length === 0) {
      throw new Error("classpath array is empty.");
    }

    const normalizedParts = classpath
      .map((part) => part.trim())
      .filter((part) => part.length > 0);

    if (normalizedParts.length === 0) {
      throw new Error("classpath array has no valid entries.");
    }

    return normalizedParts.join(delimiter);
  }

  return null;
}

function verifyBinaryChecksum(launchConfig: SpawnLaunchConfig): void {
  if (launchConfig.mode !== "binary") {
    return;
  }

  const expectedChecksum = launchConfig.binaryChecksum;
  if (expectedChecksum === undefined) {
    return;
  }

  const checksumTargetPath = launchConfig.binaryChecksumTargetPath ?? launchConfig.command;
  const binaryContents = readFileSync(checksumTargetPath);
  const actualChecksum = createHash("sha256").update(binaryContents).digest("hex");
  if (actualChecksum !== expectedChecksum) {
    throw new Error(
      [
        "Binary checksum verification failed.",
        `source=${launchConfig.source}`,
        `checksumSource=${launchConfig.binaryChecksumSource ?? "unknown"}`,
        `expected=${expectedChecksum}`,
        `actual=${actualChecksum}`,
        `path=${checksumTargetPath}`,
      ].join(" ")
    );
  }
}

async function waitForStartup(params: {
  child: ReturnType<typeof spawn>;
  stdoutReader: ReturnType<typeof createInterface>;
  stderrReader: ReturnType<typeof createInterface>;
  stdoutLines: string[];
  stderrLines: string[];
  startupTimeoutMs: number;
  logLevel: LogLevel;
  launchDescription: string;
}): Promise<{ uri: string }> {
  const {
    child,
    stdoutReader,
    stderrReader,
    stdoutLines,
    stderrLines,
    startupTimeoutMs,
    logLevel,
    launchDescription,
  } = params;

  return new Promise((resolve, reject) => {
    let settled = false;
    let resolvedUri: string | null = null;

    const timeout = setTimeout(() => {
      if (settled) {
        return;
      }
      settled = true;
      reject(
        new Error(
          [
            `Timed out waiting for jongodb startup after ${startupTimeoutMs}ms (${launchDescription}).`,
            formatLogTail("stdout", stdoutLines),
            formatLogTail("stderr", stderrLines),
          ].join(" ")
        )
      );
    }, startupTimeoutMs);

    const cleanupListeners = () => {
      stdoutReader.off("line", onStdout);
      stderrReader.off("line", onStderr);
      child.off("error", onError);
      child.off("exit", onExit);
    };

    const finish = (fn: () => void) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timeout);
      cleanupListeners();
      fn();
    };

    const onStdout = (line: string) => {
      appendLine(stdoutLines, line);
      maybeLog("stdout", line, logLevel);

      if (!line.startsWith(READY_PREFIX)) {
        return;
      }
      const uri = line.slice(READY_PREFIX.length).trim();
      if (uri.length === 0) {
        finish(() => reject(new Error("Launcher emitted empty JONGODB_URI line.")));
        return;
      }
      resolvedUri = uri;
      finish(() => resolve({ uri }));
    };

    const onStderr = (line: string) => {
      appendLine(stderrLines, line);
      maybeLog("stderr", line, logLevel);
    };

    const onError = (error: Error) => {
      finish(() => {
        reject(
          new Error(
            [
              `Failed to spawn launcher '${child.spawnfile}': ${error.message}`,
              `Launch source: ${launchDescription}.`,
              "Check binary/classpath configuration.",
            ].join(" ")
          )
        );
      });
    };

    const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
      if (resolvedUri !== null) {
        return;
      }
      finish(() => {
        reject(
          new Error(
            [
              `Jongodb process exited before readiness (code=${code}, signal=${signal}).`,
              `Launch source: ${launchDescription}.`,
              formatFailureLine(stderrLines),
              formatLogTail("stdout", stdoutLines),
              formatLogTail("stderr", stderrLines),
            ].join(" ")
          )
        );
      });
    };

    stdoutReader.on("line", onStdout);
    stderrReader.on("line", onStderr);
    child.once("error", onError);
    child.once("exit", onExit);
  });
}

function maybeLog(stream: "stdout" | "stderr", line: string, logLevel: LogLevel) {
  if (logLevel === "silent") {
    return;
  }
  if (logLevel === "info" && stream === "stdout") {
    return;
  }
  const redactedLine = redactSensitiveData(line);
  if (stream === "stdout") {
    // eslint-disable-next-line no-console
    console.log(`[jongodb:${stream}] ${redactedLine}`);
    return;
  }
  // eslint-disable-next-line no-console
  console.error(`[jongodb:${stream}] ${redactedLine}`);
}

function appendLine(lines: string[], line: string): void {
  lines.push(redactSensitiveData(line));
  if (lines.length > MAX_LOG_LINES) {
    lines.shift();
  }
}

function formatLogTail(name: string, lines: string[]): string {
  if (lines.length === 0) {
    return `${name}:<empty>`;
  }
  return `${name}:` + lines.join(" | ");
}

function formatFailureLine(stderrLines: string[]): string {
  const failureLine = stderrLines.find((line) => line.startsWith(FAILURE_PREFIX));
  if (failureLine === undefined) {
    return "";
  }
  return failureLine;
}

function redactSensitiveData(input: string): string {
  return input
    .replace(URI_CREDENTIAL_PATTERN, `$1$2:${REDACTED_PLACEHOLDER}@`)
    .replace(SECRET_JSON_PATTERN, `$1${REDACTED_PLACEHOLDER}$3`)
    .replace(SECRET_QUERY_PATTERN, `$1${REDACTED_PLACEHOLDER}`)
    .replace(SECRET_ASSIGNMENT_PATTERN, `$1${REDACTED_PLACEHOLDER}`);
}

function emitStartupTelemetry(
  hook: ((event: JongodbStartupTelemetry) => void) | undefined,
  event: JongodbStartupTelemetry
): void {
  if (hook === undefined) {
    return;
  }
  try {
    hook(event);
  } catch {
    // telemetry hooks must not break runtime startup flow
  }
}

function roundDurationMs(value: number): number {
  return Math.round(value * 100) / 100;
}

async function forceStopIfAlive(
  child: ReturnType<typeof spawn>,
  stopTimeoutMs: number
): Promise<void> {
  if (child.exitCode !== null) {
    return;
  }
  child.kill("SIGTERM");
  const graceful = await waitForExit(child, stopTimeoutMs);
  if (graceful !== null) {
    return;
  }
  child.kill("SIGKILL");
  await waitForExit(child, stopTimeoutMs);
}

function waitForExit(
  child: ReturnType<typeof spawn>,
  timeoutMs: number
): Promise<ExitResult | null> {
  if (child.exitCode !== null) {
    return Promise.resolve({
      code: child.exitCode,
      signal: child.signalCode,
    });
  }

  return new Promise((resolve) => {
    const timeout = setTimeout(() => {
      cleanup();
      resolve(null);
    }, timeoutMs);

    const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
      cleanup();
      resolve({ code, signal });
    };

    const cleanup = () => {
      clearTimeout(timeout);
      child.off("exit", onExit);
    };

    child.on("exit", onExit);
  });
}

function wrapError(error: unknown): Error {
  if (error instanceof Error) {
    return error;
  }
  return new Error(String(error));
}
