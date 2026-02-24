import { spawn } from "node:child_process";
import { delimiter } from "node:path";
import { createInterface } from "node:readline";

const READY_PREFIX = "JONGODB_URI=";
const FAILURE_PREFIX = "JONGODB_START_FAILURE=";
const DEFAULT_HOST = "127.0.0.1";
const DEFAULT_DATABASE = "test";
const DEFAULT_STARTUP_TIMEOUT_MS = 15_000;
const DEFAULT_STOP_TIMEOUT_MS = 5_000;
const DEFAULT_LAUNCHER_CLASS = "org.jongodb.server.TcpMongoServerLauncher";
const MAX_LOG_LINES = 50;

type LogLevel = "silent" | "info" | "debug";

export interface JongodbMemoryServerOptions {
  host?: string;
  port?: number;
  databaseName?: string;
  startupTimeoutMs?: number;
  stopTimeoutMs?: number;
  javaPath?: string;
  launcherClass?: string;
  classpath?: string | string[];
  env?: Record<string, string>;
  logLevel?: LogLevel;
}

export interface JongodbMemoryServer {
  readonly uri: string;
  stop(): Promise<void>;
}

interface ExitResult {
  code: number | null;
  signal: NodeJS.Signals | null;
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
  const databaseName = normalizeDatabaseName(options.databaseName);
  const launcherClass = options.launcherClass?.trim() || DEFAULT_LAUNCHER_CLASS;
  const javaPath =
    options.javaPath?.trim() || process.env.JONGODB_JAVA_PATH || "java";
  const classpath = resolveClasspath(options.classpath);
  const logLevel = options.logLevel ?? "silent";

  const args = [
    "-cp",
    classpath,
    launcherClass,
    `--host=${host}`,
    `--port=${port}`,
    `--database=${databaseName}`,
  ];

  const child = spawn(javaPath, args, {
    stdio: ["ignore", "pipe", "pipe"],
    windowsHide: true,
    env: {
      ...process.env,
      ...options.env,
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
    startupTimeoutMs,
    logLevel,
  }).catch(async (error: unknown) => {
    await forceStopIfAlive(child, stopTimeoutMs);
    throw wrapError(error);
  });

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

    const gracefulExit = await waitForExit(child, stopTimeoutMs);
    if (gracefulExit !== null) {
      return;
    }

    const killed = child.kill("SIGKILL");
    if (!killed) {
      throw new Error(
        "Failed to stop jongodb server process: SIGTERM timeout and SIGKILL failed."
      );
    }

    const forcedExit = await waitForExit(child, stopTimeoutMs);
    if (forcedExit === null) {
      throw new Error(
        "Failed to stop jongodb server process: process did not exit after SIGKILL."
      );
    }
  };

  return {
    uri: startupResult.uri,
    stop,
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

function normalizeDatabaseName(databaseName: string | undefined): string {
  const normalized = databaseName?.trim() || DEFAULT_DATABASE;
  if (normalized.length === 0) {
    throw new Error("databaseName must not be empty.");
  }
  return normalized;
}

function resolveClasspath(
  classpath: string | string[] | undefined
): string {
  const explicit = resolveExplicitClasspath(classpath);
  if (explicit !== null) {
    return explicit;
  }

  const fromEnv = process.env.JONGODB_CLASSPATH?.trim();
  if (fromEnv !== undefined && fromEnv.length > 0) {
    return fromEnv;
  }

  throw new Error(
    [
      "Jongodb Java classpath is not configured.",
      "Pass options.classpath or set JONGODB_CLASSPATH.",
      "Example (repo-local): ./.tooling/gradle-8.10.2/bin/gradle -q printLauncherClasspath",
    ].join(" ")
  );
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
}): Promise<{ uri: string }> {
  const {
    child,
    stdoutReader,
    stderrReader,
    stdoutLines,
    stderrLines,
    startupTimeoutMs,
    logLevel,
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
            `Timed out waiting for jongodb startup after ${startupTimeoutMs}ms.`,
            formatLogTail("stdout", stdoutLines),
            formatLogTail("stderr", stderrLines),
          ].join(" ")
        )
      );
    }, startupTimeoutMs);

    const finish = (fn: () => void) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timeout);
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
              `Failed to spawn Java process '${child.spawnfile}': ${error.message}`,
              "Check javaPath and classpath configuration.",
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
