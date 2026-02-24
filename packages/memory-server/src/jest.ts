import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";

import {
  type JongodbMemoryServerOptions,
  startJongodbMemoryServer,
} from "./index.js";

const DEFAULT_ENV_VAR_NAME = "MONGODB_URI";
const DEFAULT_STATE_FILE = path.join(
  process.cwd(),
  ".jongodb",
  "jest-memory-server.json"
);
const DEFAULT_STOP_TIMEOUT_MS = 5_000;

export interface JestHookRegistrar {
  beforeAll(callback: () => unknown | Promise<unknown>): void;
  afterAll(callback: () => unknown | Promise<unknown>): void;
}

export interface JestHookOptions extends JongodbMemoryServerOptions {
  envVarName?: string;
}

export interface RegisteredJongodbTestServer {
  readonly uri: string;
}

export interface JestGlobalLifecycleOptions extends JongodbMemoryServerOptions {
  envVarName?: string;
  stateFile?: string;
  killTimeoutMs?: number;
}

export interface JestGlobalState {
  uri: string;
  pid: number;
  envVarName: string;
}

export function registerJongodbForJest(
  hooks: JestHookRegistrar,
  options: JestHookOptions = {}
): RegisteredJongodbTestServer {
  const envVarName = normalizeEnvVarName(options.envVarName);
  let runtimeServer:
    | Awaited<ReturnType<typeof startJongodbMemoryServer>>
    | null = null;
  let uri: string | null = null;

  hooks.beforeAll(async () => {
    runtimeServer = await startJongodbMemoryServer(options);
    uri = runtimeServer.uri;
    process.env[envVarName] = uri;
  });

  hooks.afterAll(async () => {
    if (runtimeServer !== null) {
      await runtimeServer.stop();
      runtimeServer = null;
    }
  });

  return {
    get uri(): string {
      if (uri === null) {
        throw new Error(
          "Jongodb URI is not available before beforeAll completes."
        );
      }
      return uri;
    },
  };
}

export function createJestGlobalSetup(
  options: JestGlobalLifecycleOptions = {}
): () => Promise<void> {
  return async () => {
    const { envVarName, stateFile, runtimeOptions } =
      splitLifecycleOptions(options);
    const server = await startJongodbMemoryServer(runtimeOptions);

    process.env[envVarName] = server.uri;

    await mkdir(path.dirname(stateFile), { recursive: true });
    const state: JestGlobalState = {
      uri: server.uri,
      pid: server.pid,
      envVarName,
    };
    await writeFile(stateFile, JSON.stringify(state, null, 2), "utf8");
    server.detach();
  };
}

export function createJestGlobalTeardown(
  options: JestGlobalLifecycleOptions = {}
): () => Promise<void> {
  return async () => {
    const { stateFile } = splitLifecycleOptions(options);
    const state = await readJestGlobalState({ stateFile });
    if (state === null) {
      return;
    }

    await terminatePid(state.pid, options.killTimeoutMs ?? DEFAULT_STOP_TIMEOUT_MS);
    await rm(stateFile, { force: true });
  };
}

export async function readJestGlobalState(options: {
  stateFile?: string;
} = {}): Promise<JestGlobalState | null> {
  const stateFile = normalizeStateFile(options.stateFile);
  try {
    const raw = await readFile(stateFile, "utf8");
    const parsed = JSON.parse(raw) as Partial<JestGlobalState>;
    if (
      typeof parsed.uri !== "string" ||
      typeof parsed.pid !== "number" ||
      typeof parsed.envVarName !== "string"
    ) {
      throw new Error("Jest global state file has invalid schema.");
    }
    return parsed as JestGlobalState;
  } catch (error: unknown) {
    if (isMissingFileError(error)) {
      return null;
    }
    throw error;
  }
}

export async function readJestGlobalUri(options: {
  stateFile?: string;
} = {}): Promise<string | null> {
  const state = await readJestGlobalState(options);
  if (state === null) {
    return null;
  }
  return state.uri;
}

function splitLifecycleOptions(options: JestGlobalLifecycleOptions): {
  envVarName: string;
  stateFile: string;
  runtimeOptions: JongodbMemoryServerOptions;
} {
  const { envVarName, stateFile, killTimeoutMs: _killTimeoutMs, ...runtimeOptions } =
    options;
  return {
    envVarName: normalizeEnvVarName(envVarName),
    stateFile: normalizeStateFile(stateFile),
    runtimeOptions,
  };
}

function normalizeEnvVarName(name: string | undefined): string {
  const normalized = name?.trim() || DEFAULT_ENV_VAR_NAME;
  if (normalized.length === 0) {
    throw new Error("envVarName must not be empty.");
  }
  return normalized;
}

function normalizeStateFile(stateFile: string | undefined): string {
  const normalized = stateFile?.trim() || DEFAULT_STATE_FILE;
  if (normalized.length === 0) {
    throw new Error("stateFile must not be empty.");
  }
  return normalized;
}

async function terminatePid(pid: number, timeoutMs: number): Promise<void> {
  if (!Number.isInteger(pid) || pid <= 0) {
    throw new Error(`Invalid PID in Jest global state: ${pid}`);
  }

  if (!isProcessRunning(pid)) {
    return;
  }

  process.kill(pid, "SIGTERM");

  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (!isProcessRunning(pid)) {
      return;
    }
    await sleep(50);
  }

  if (!isProcessRunning(pid)) {
    return;
  }

  process.kill(pid, "SIGKILL");
}

function isProcessRunning(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error: unknown) {
    if (isNoSuchProcessError(error)) {
      return false;
    }
    throw error;
  }
}

function isNoSuchProcessError(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    (error as { code?: string }).code === "ESRCH"
  );
}

function isMissingFileError(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    (error as { code?: string }).code === "ENOENT"
  );
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}
