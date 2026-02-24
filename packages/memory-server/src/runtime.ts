import {
  type JongodbMemoryServer,
  type JongodbMemoryServerOptions,
  startJongodbMemoryServer,
} from "./index.js";

const DEFAULT_ENV_VAR_NAME = "MONGODB_URI";

export interface JongodbEnvRuntimeOptions extends JongodbMemoryServerOptions {
  envVarName?: string;
  envVarNames?: string[];
}

export interface JongodbEnvRuntime {
  readonly envVarName: string;
  readonly envVarNames: readonly string[];
  readonly running: boolean;
  readonly uri: string;
  setup(): Promise<string>;
  teardown(): Promise<void>;
}

export function createJongodbEnvRuntime(
  options: JongodbEnvRuntimeOptions = {}
): JongodbEnvRuntime {
  const envVarNames = resolveEnvVarNames(options);
  const envVarName = envVarNames[0];
  const {
    envVarName: _envVarName,
    envVarNames: _envVarNames,
    ...serverOptions
  } = options;

  let runtimeServer: JongodbMemoryServer | null = null;
  let uri: string | null = null;
  let previousEnvState = captureEnvState(envVarNames);

  return {
    envVarName,
    envVarNames,
    get running(): boolean {
      return runtimeServer !== null;
    },
    get uri(): string {
      if (uri === null) {
        throw new Error("Jongodb URI is not available before setup completes.");
      }
      return uri;
    },
    async setup(): Promise<string> {
      if (runtimeServer !== null && uri !== null) {
        return uri;
      }

      previousEnvState = captureEnvState(envVarNames);

      runtimeServer = await startJongodbMemoryServer(serverOptions);
      uri = runtimeServer.uri;
      for (const candidate of envVarNames) {
        process.env[candidate] = uri;
      }
      return uri;
    },
    async teardown(): Promise<void> {
      if (runtimeServer !== null) {
        await runtimeServer.stop();
        runtimeServer = null;
      }

      restoreEnvState(previousEnvState);

      uri = null;
      previousEnvState = captureEnvState(envVarNames);
    },
  };
}

function resolveEnvVarNames(options: JongodbEnvRuntimeOptions): string[] {
  const explicitCandidates: string[] = [];
  if (options.envVarName !== undefined) {
    explicitCandidates.push(options.envVarName);
  }
  if (options.envVarNames !== undefined) {
    explicitCandidates.push(...options.envVarNames);
  }

  if (explicitCandidates.length === 0) {
    return [DEFAULT_ENV_VAR_NAME];
  }

  const resolved: string[] = [];
  const seen = new Set<string>();

  for (const candidate of explicitCandidates) {
    const normalized = normalizeEnvVarName(candidate);
    if (seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    resolved.push(normalized);
  }

  if (resolved.length === 0) {
    return [DEFAULT_ENV_VAR_NAME];
  }
  return resolved;
}

function normalizeEnvVarName(name: string): string {
  const normalized = name.trim();
  if (normalized.length === 0) {
    throw new Error("envVarName/envVarNames entries must not be empty.");
  }
  return normalized;
}

function captureEnvState(
  envVarNames: readonly string[]
): Map<string, { hadPrevious: boolean; previousValue: string | undefined }> {
  const state = new Map<string, { hadPrevious: boolean; previousValue: string | undefined }>();
  for (const envVarName of envVarNames) {
    state.set(envVarName, {
      hadPrevious: Object.prototype.hasOwnProperty.call(process.env, envVarName),
      previousValue: process.env[envVarName],
    });
  }
  return state;
}

function restoreEnvState(
  state: Map<string, { hadPrevious: boolean; previousValue: string | undefined }>
): void {
  for (const [envVarName, previous] of state.entries()) {
    if (previous.hadPrevious) {
      process.env[envVarName] = previous.previousValue;
    } else {
      delete process.env[envVarName];
    }
  }
}
