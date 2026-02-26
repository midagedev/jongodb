import {
  type JestHookOptions,
  type JestHookRegistrar,
  type RegisteredJongodbTestServer,
  registerJongodbForJest,
} from "./jest.js";

export type NestJestHookRegistrar = JestHookRegistrar;
export type NestJongodbOptions = JestHookOptions;
export type RegisteredNestJongodbServer = RegisteredJongodbTestServer;

/**
 * Convenience wrapper for NestJS E2E tests that run on Jest.
 */
export function registerJongodbForNestJest(
  hooks: NestJestHookRegistrar,
  options: NestJongodbOptions = {}
): RegisteredNestJongodbServer {
  const hasExplicitEnvBinding =
    options.envVarName !== undefined || options.envVarNames !== undefined;

  return registerJongodbForJest(hooks, {
    ...options,
    // Nest projects commonly consume either spring-style MONGODB_URI or DATABASE_URL in test modules.
    ...(hasExplicitEnvBinding
      ? {}
      : { envVarNames: ["MONGODB_URI", "DATABASE_URL"] }),
    // Parallel Nest Jest workers should isolate DBs by default to reduce cross-suite data bleed.
    databaseNameStrategy: options.databaseNameStrategy ?? "worker",
  });
}
