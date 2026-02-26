import { spawn } from "node:child_process";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { delimiter, dirname, resolve } from "node:path";
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
const MAX_LOG_LINES = 50;

const BUNDLED_BINARY_PACKAGE_PREFIX = "@jongodb/memory-server-bin";

type LogLevel = "silent" | "info" | "debug";
type LaunchMode = "auto" | "binary" | "java";
type DatabaseNameStrategy = "static" | "worker";
type TopologyProfile = "standalone" | "singleNodeReplicaSet";

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
  binaryPath?: string;
  launchMode?: LaunchMode;
  topologyProfile?: TopologyProfile;
  replicaSetName?: string;
  env?: Record<string, string>;
  logLevel?: LogLevel;
}

export interface JongodbMemoryServer {
  readonly uri: string;
  readonly pid: number;
  detach(): void;
  stop(): Promise<void>;
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
  const launchConfigs = resolveLaunchConfigs(options, {
    host,
    port,
    databaseName,
    topologyProfile,
    replicaSetName,
  });
  const launchErrors: string[] = [];

  for (let index = 0; index < launchConfigs.length; index += 1) {
    const launchConfig = launchConfigs[index];
    try {
      return await startWithLaunchConfig(launchConfig, {
        startupTimeoutMs,
        stopTimeoutMs,
        logLevel,
        env: options.env,
      });
    } catch (error: unknown) {
      const normalized = wrapError(error);
      launchErrors.push(
        `[${launchConfig.mode}:${launchConfig.source}] ${normalized.message}`
      );
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
): SpawnLaunchConfig[] {
  const mode = options.launchMode ?? "auto";
  if (mode !== "auto" && mode !== "binary" && mode !== "java") {
    throw new Error(`launchMode must be one of: auto, binary, java (got: ${mode}).`);
  }

  const binary = resolveBinaryCandidate(options.binaryPath);
  const java = resolveJavaCandidate(options);

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
    return [toBinaryLaunchConfig(binary.path, binary.source, context)];
  }

  if (mode === "java") {
    if (java === null) {
      throw new Error(
        [
          "Java launch mode requested but Java classpath is not configured.",
          "Pass options.classpath or set JONGODB_CLASSPATH.",
          "Example (repo-local): ./.tooling/gradle-8.10.2/bin/gradle -q printLauncherClasspath",
        ].join(" ")
      );
    }
    return [toJavaLaunchConfig(java, context)];
  }

  if (binary !== null && java !== null) {
    return [
      toBinaryLaunchConfig(binary.path, binary.source, context),
      toJavaLaunchConfig(java, context),
    ];
  }
  if (binary !== null) {
    return [toBinaryLaunchConfig(binary.path, binary.source, context)];
  }
  if (java !== null) {
    return [toJavaLaunchConfig(java, context)];
  }

  throw new Error(
    [
      "No launcher runtime configured.",
      "Provide one of:",
      "1) options.binaryPath or JONGODB_BINARY_PATH",
      "2) options.classpath or JONGODB_CLASSPATH",
      bundledBinaryHint(),
    ].join(" ")
  );
}

function toBinaryLaunchConfig(
  binaryPath: string,
  source: string,
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
    command: binaryPath,
    args,
    source,
    topologyProfile: context.topologyProfile,
    replicaSetName: context.replicaSetName,
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

function resolveBinaryCandidate(
  explicitBinaryPath: string | undefined
): { path: string; source: string } | null {
  const fromOption = normalizeBinaryPath(explicitBinaryPath, "options.binaryPath");
  if (fromOption !== null) {
    return { path: fromOption, source: "options.binaryPath" };
  }

  const fromEnv = normalizeBinaryPath(process.env.JONGODB_BINARY_PATH, "JONGODB_BINARY_PATH");
  if (fromEnv !== null) {
    return { path: fromEnv, source: "JONGODB_BINARY_PATH" };
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

function resolveBundledBinaryPath(): { path: string; source: string } | null {
  const candidates = bundledBinaryPackageCandidates();
  for (const packageName of candidates) {
    const pathFromPackage = resolveBinaryPathFromPackage(packageName);
    if (pathFromPackage !== null) {
      return {
        path: pathFromPackage,
        source: `bundled-package:${packageName}`,
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

function resolveBinaryPathFromPackage(packageName: string): string | null {
  try {
    const packageJsonPath = moduleRequire.resolve(`${packageName}/package.json`);
    const packageDir = dirname(packageJsonPath);
    const packageJson = JSON.parse(readFileSync(packageJsonPath, "utf8")) as {
      bin?: string | Record<string, string>;
      jongodb?: { binary?: string };
    };

    const binaryRelativePath =
      readJongodbBinaryPath(packageJson) ??
      readBinEntryPath(packageJson.bin);

    if (binaryRelativePath === null) {
      return null;
    }

    return resolve(packageDir, binaryRelativePath);
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
  options: JongodbMemoryServerOptions
): { javaPath: string; classpath: string; launcherClass: string; source: string } | null {
  const classpath = resolveClasspathOrNull(options.classpath);
  if (classpath === null) {
    return null;
  }
  return {
    javaPath: options.javaPath?.trim() || process.env.JONGODB_JAVA_PATH || "java",
    classpath,
    launcherClass: options.launcherClass?.trim() || DEFAULT_LAUNCHER_CLASS,
    source: options.classpath !== undefined ? "options.classpath" : "JONGODB_CLASSPATH",
  };
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

function resolveClasspathOrNull(classpath: string | string[] | undefined): string | null {
  const explicit = resolveExplicitClasspath(classpath);
  if (explicit !== null) {
    return explicit;
  }

  const fromEnv = process.env.JONGODB_CLASSPATH?.trim();
  if (fromEnv !== undefined && fromEnv.length > 0) {
    return fromEnv;
  }
  return null;
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
  if (stream === "stdout") {
    // eslint-disable-next-line no-console
    console.log(`[jongodb:${stream}] ${line}`);
    return;
  }
  // eslint-disable-next-line no-console
  console.error(`[jongodb:${stream}] ${line}`);
}

function appendLine(lines: string[], line: string): void {
  lines.push(line);
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
