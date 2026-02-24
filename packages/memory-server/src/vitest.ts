import {
  type JongodbMemoryServerOptions,
  startJongodbMemoryServer,
} from "./index.js";

const DEFAULT_ENV_VAR_NAME = "MONGODB_URI";

export interface VitestHookRegistrar {
  beforeAll(callback: () => unknown | Promise<unknown>): void;
  afterAll(callback: () => unknown | Promise<unknown>): void;
}

export interface VitestHookOptions extends JongodbMemoryServerOptions {
  envVarName?: string;
}

export interface RegisteredVitestJongodbServer {
  readonly uri: string;
}

export function registerJongodbForVitest(
  hooks: VitestHookRegistrar,
  options: VitestHookOptions = {}
): RegisteredVitestJongodbServer {
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

function normalizeEnvVarName(name: string | undefined): string {
  const normalized = name?.trim() || DEFAULT_ENV_VAR_NAME;
  if (normalized.length === 0) {
    throw new Error("envVarName must not be empty.");
  }
  return normalized;
}
