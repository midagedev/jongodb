import {
  type JongodbMemoryServer,
  type JongodbMemoryServerOptions,
  startJongodbMemoryServer,
} from "./index.js";

const DEFAULT_ENV_VAR_NAME = "MONGODB_URI";
const processEnv = process.env;

type EnvTarget = Record<string, string | undefined>;

interface GlobalEnvBinding {
  readonly token: symbol;
  readonly value: string;
}

interface GlobalEnvBindingState {
  readonly hadPrevious: boolean;
  readonly previousValue: string | undefined;
  readonly stack: GlobalEnvBinding[];
}

const globalEnvBindings = new Map<string, GlobalEnvBindingState>();

export interface JongodbEnvRuntimeOptions extends JongodbMemoryServerOptions {
  envVarName?: string;
  envVarNames?: string[];
  envTarget?: EnvTarget;
}

export interface JongodbEnvRuntime {
  readonly envVarName: string;
  readonly envVarNames: readonly string[];
  readonly envTarget: EnvTarget;
  readonly useGlobalEnvBindings: boolean;
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
    envTarget: _envTarget,
    ...serverOptions
  } = options;
  const envTarget = options.envTarget ?? processEnv;
  const useGlobalEnvBindings = envTarget === processEnv;

  let runtimeServer: JongodbMemoryServer | null = null;
  let uri: string | null = null;
  let previousEnvState = captureEnvState(envTarget, envVarNames);
  let bindingToken: symbol | null = null;

  return {
    envVarName,
    envVarNames,
    envTarget,
    useGlobalEnvBindings,
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

      if (!useGlobalEnvBindings) {
        previousEnvState = captureEnvState(envTarget, envVarNames);
      }

      runtimeServer = await startJongodbMemoryServer(serverOptions);
      uri = runtimeServer.uri;
      if (useGlobalEnvBindings) {
        bindingToken = claimGlobalEnvBinding(envVarNames, uri);
      } else {
        for (const candidate of envVarNames) {
          envTarget[candidate] = uri;
        }
      }
      return uri;
    },
    async teardown(): Promise<void> {
      if (runtimeServer !== null) {
        await runtimeServer.stop();
        runtimeServer = null;
      }

      if (uri !== null) {
        if (useGlobalEnvBindings && bindingToken !== null) {
          releaseGlobalEnvBinding(bindingToken, envVarNames);
          bindingToken = null;
        } else if (!useGlobalEnvBindings) {
          restoreEnvState(envTarget, previousEnvState);
        }
      }

      uri = null;
      if (!useGlobalEnvBindings) {
        previousEnvState = captureEnvState(envTarget, envVarNames);
      }
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
  envTarget: EnvTarget,
  envVarNames: readonly string[]
): Map<string, { hadPrevious: boolean; previousValue: string | undefined }> {
  const state = new Map<string, { hadPrevious: boolean; previousValue: string | undefined }>();
  for (const envVarName of envVarNames) {
    state.set(envVarName, {
      hadPrevious: Object.prototype.hasOwnProperty.call(envTarget, envVarName),
      previousValue: envTarget[envVarName],
    });
  }
  return state;
}

function restoreEnvState(
  envTarget: EnvTarget,
  state: Map<string, { hadPrevious: boolean; previousValue: string | undefined }>
): void {
  for (const [envVarName, previous] of state.entries()) {
    if (previous.hadPrevious) {
      envTarget[envVarName] = previous.previousValue;
    } else {
      delete envTarget[envVarName];
    }
  }
}

function claimGlobalEnvBinding(envVarNames: readonly string[], value: string): symbol {
  const token = Symbol("jongodb-env-runtime");
  for (const envVarName of envVarNames) {
    let state = globalEnvBindings.get(envVarName);
    if (state === undefined) {
      state = {
        hadPrevious: Object.prototype.hasOwnProperty.call(processEnv, envVarName),
        previousValue: processEnv[envVarName],
        stack: [],
      };
      globalEnvBindings.set(envVarName, state);
    }

    state.stack.push({ token, value });
    processEnv[envVarName] = value;
  }
  return token;
}

function releaseGlobalEnvBinding(token: symbol, envVarNames: readonly string[]): void {
  for (const envVarName of envVarNames) {
    const state = globalEnvBindings.get(envVarName);
    if (state === undefined) {
      continue;
    }

    const index = state.stack.findIndex((entry) => entry.token === token);
    if (index < 0) {
      continue;
    }

    const removedTop = index === state.stack.length - 1;
    state.stack.splice(index, 1);

    if (!removedTop) {
      continue;
    }
    if (state.stack.length > 0) {
      processEnv[envVarName] = state.stack[state.stack.length - 1].value;
      continue;
    }

    if (state.hadPrevious) {
      processEnv[envVarName] = state.previousValue;
    } else {
      delete processEnv[envVarName];
    }
    globalEnvBindings.delete(envVarName);
  }
}
