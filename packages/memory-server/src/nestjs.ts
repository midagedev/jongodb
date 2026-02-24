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
  return registerJongodbForJest(hooks, options);
}
