export interface JongodbMemoryServerOptions {
  host?: string;
  port?: number;
  databaseName?: string;
  startupTimeoutMs?: number;
  logLevel?: "silent" | "info" | "debug";
}

export interface JongodbMemoryServer {
  readonly uri: string;
  stop(): Promise<void>;
}

function notImplementedError(): Error {
  return new Error(
    "Runtime process manager is not implemented yet. Track progress in issue #108."
  );
}

/**
 * Starts a local jongodb-backed MongoDB-compatible endpoint for tests.
 * Runtime process lifecycle is implemented in issue #108.
 */
export async function startJongodbMemoryServer(
  _options: JongodbMemoryServerOptions = {}
): Promise<JongodbMemoryServer> {
  throw notImplementedError();
}
