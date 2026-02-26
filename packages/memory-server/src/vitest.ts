import { type JongodbMemoryServerOptions } from "./index.js";
import { createJongodbEnvRuntime } from "./runtime.js";

const DEFAULT_DATABASE = "test";
const DEFAULT_ENV_VAR_NAME = "MONGODB_URI";

export interface VitestHookRegistrar {
  beforeAll(callback: () => unknown | Promise<unknown>): void;
  afterAll(callback: () => unknown | Promise<unknown>): void;
}

export interface VitestHookOptions extends JongodbMemoryServerOptions {
  envVarName?: string;
  envVarNames?: string[];
}

export interface RegisteredVitestJongodbServer {
  readonly uri: string;
}

export type VitestWorkspaceIsolationMode = "project" | "shared";

export interface VitestWorkspaceHookOptions extends VitestHookOptions {
  projectName: string;
  isolationMode?: VitestWorkspaceIsolationMode;
  projectEnvVarName?: string;
}

export interface RegisteredVitestWorkspaceJongodbServer
  extends RegisteredVitestJongodbServer {
  readonly isolationMode: VitestWorkspaceIsolationMode;
  readonly databaseName: string;
  readonly envVarNames: readonly string[];
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

export function registerJongodbForVitestWorkspace(
  hooks: VitestHookRegistrar,
  options: VitestWorkspaceHookOptions
): RegisteredVitestWorkspaceJongodbServer {
  const isolationMode = options.isolationMode ?? "project";
  const projectName = normalizeProjectName(options.projectName);
  const {
    projectName: _projectName,
    isolationMode: _isolationMode,
    projectEnvVarName,
    ...runtimeOptionsBase
  } = options;

  const runtimeOptions: VitestHookOptions = { ...runtimeOptionsBase };
  if (isolationMode === "project") {
    runtimeOptions.databaseName = withProjectDatabaseSuffix(
      runtimeOptions.databaseName,
      projectName
    );
  }

  const envVarNames = resolveWorkspaceEnvVarNames(
    runtimeOptions,
    projectName,
    isolationMode,
    projectEnvVarName
  );
  if (envVarNames !== null) {
    runtimeOptions.envVarNames = envVarNames;
    delete runtimeOptions.envVarName;
  }

  const runtime = createJongodbEnvRuntime(runtimeOptions);

  hooks.beforeAll(async () => {
    await runtime.setup();
  });

  hooks.afterAll(async () => {
    await runtime.teardown();
  });

  return {
    isolationMode,
    databaseName: runtimeOptions.databaseName ?? DEFAULT_DATABASE,
    get envVarNames(): readonly string[] {
      return runtime.envVarNames;
    },
    get uri(): string {
      return runtime.uri;
    },
  };
}

function normalizeProjectName(name: string): string {
  const normalized = name.trim();
  if (normalized.length === 0) {
    throw new Error("projectName must not be empty.");
  }
  return normalized;
}

function withProjectDatabaseSuffix(
  databaseName: string | undefined,
  projectName: string
): string {
  const base = databaseName?.trim() || DEFAULT_DATABASE;
  return `${base}_p${sanitizeProjectToken(projectName)}`;
}

function sanitizeProjectToken(token: string): string {
  const normalized = token
    .trim()
    .replace(/[^A-Za-z0-9_]+/g, "_")
    .replace(/^_+/g, "")
    .replace(/_+$/g, "");
  return normalized.length > 0 ? normalized : "project";
}

function resolveWorkspaceEnvVarNames(
  runtimeOptions: VitestHookOptions,
  projectName: string,
  isolationMode: VitestWorkspaceIsolationMode,
  projectEnvVarName: string | undefined
): string[] | null {
  const candidates: string[] = [];
  if (runtimeOptions.envVarName !== undefined) {
    candidates.push(runtimeOptions.envVarName);
  }
  if (runtimeOptions.envVarNames !== undefined) {
    candidates.push(...runtimeOptions.envVarNames);
  }
  if (projectEnvVarName !== undefined) {
    candidates.push(projectEnvVarName);
  }

  if (isolationMode === "project") {
    if (candidates.length === 0) {
      candidates.push(DEFAULT_ENV_VAR_NAME);
    }
    candidates.push(`MONGODB_URI_${sanitizeProjectToken(projectName).toUpperCase()}`);
  }

  if (candidates.length === 0) {
    return null;
  }

  const resolved: string[] = [];
  const seen = new Set<string>();
  for (const candidate of candidates) {
    const normalized = candidate.trim();
    if (normalized.length === 0) {
      throw new Error("envVarName/envVarNames entries must not be empty.");
    }
    if (seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    resolved.push(normalized);
  }
  return resolved;
}
