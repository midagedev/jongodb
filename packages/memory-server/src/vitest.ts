import { type JongodbMemoryServerOptions } from "./index.js";
import { createJongodbEnvRuntime } from "./runtime.js";

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
  const runtime = createJongodbEnvRuntime(options);

  hooks.beforeAll(async () => {
    await runtime.setup();
  });

  hooks.afterAll(async () => {
    await runtime.teardown();
  });

  return {
    get uri(): string {
      return runtime.uri;
    },
  };
}
