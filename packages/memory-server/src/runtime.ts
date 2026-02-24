import {
  type JongodbMemoryServer,
  type JongodbMemoryServerOptions,
  startJongodbMemoryServer,
} from "./index.js";

const DEFAULT_ENV_VAR_NAME = "MONGODB_URI";

export interface JongodbEnvRuntimeOptions extends JongodbMemoryServerOptions {
  envVarName?: string;
}

export interface JongodbEnvRuntime {
  readonly envVarName: string;
  readonly running: boolean;
  readonly uri: string;
  setup(): Promise<string>;
  teardown(): Promise<void>;
}

export function createJongodbEnvRuntime(
  options: JongodbEnvRuntimeOptions = {}
): JongodbEnvRuntime {
  const envVarName = normalizeEnvVarName(options.envVarName);
  const { envVarName: _envVarName, ...serverOptions } = options;

  let runtimeServer: JongodbMemoryServer | null = null;
  let uri: string | null = null;
  let previousEnvValue: string | undefined;
  let hadPreviousEnv = false;

  return {
    envVarName,
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

      hadPreviousEnv = Object.prototype.hasOwnProperty.call(process.env, envVarName);
      previousEnvValue = process.env[envVarName];

      runtimeServer = await startJongodbMemoryServer(serverOptions);
      uri = runtimeServer.uri;
      process.env[envVarName] = uri;
      return uri;
    },
    async teardown(): Promise<void> {
      if (runtimeServer !== null) {
        await runtimeServer.stop();
        runtimeServer = null;
      }

      if (hadPreviousEnv) {
        process.env[envVarName] = previousEnvValue;
      } else {
        delete process.env[envVarName];
      }

      uri = null;
      previousEnvValue = undefined;
      hadPreviousEnv = false;
    },
  };
}

function normalizeEnvVarName(name: string | undefined): string {
  const normalized = name?.trim() || DEFAULT_ENV_VAR_NAME;
  if (normalized.length === 0) {
    throw new Error("envVarName must not be empty.");
  }
  return normalized;
}
