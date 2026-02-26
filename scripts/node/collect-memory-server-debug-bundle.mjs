#!/usr/bin/env node

import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const REDACTED_PLACEHOLDER = "<redacted>";
const URI_CREDENTIAL_PATTERN =
  /(mongodb(?:\+srv)?:\/\/)([^\/\s:@]+):([^@\s\/]+)@/giu;
const SECRET_ASSIGNMENT_PATTERN =
  /((?:password|passwd|pwd|token|secret|api[_-]?key|access[_-]?token)\s*[=:]\s*)([^,\s;]+)/giu;
const SECRET_QUERY_PATTERN =
  /([?&](?:password|passwd|pwd|token|secret|api[_-]?key|access[_-]?token)=)([^&\s]+)/giu;
const SECRET_JSON_PATTERN =
  /("(?:password|passwd|pwd|token|secret|api[_-]?key|access[_-]?token)"\s*:\s*")([^"]*)(")/giu;

const DEFAULT_ENV_KEYS = [
  "JONGODB_CLASSPATH",
  "JONGODB_BINARY_PATH",
  "JONGODB_TEST_CLASSPATH",
  "MONGODB_URI",
  "DATABASE_URL",
  "NODE_ENV",
  "CI",
];

const DEFAULT_FILE_CANDIDATES = [
  ".jongodb/jest-memory-server.json",
  "testkit/node-compat/reports/node-compat-smoke.json",
  "build/reports/node-cold-start/report.json",
];

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`[node-debug-bundle] ${message}`);
  process.exit(1);
});

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const repoRoot = resolveRepoRoot();
  const outputDir = path.resolve(
    repoRoot,
    typeof args.outputDir === "string"
      ? args.outputDir
      : "build/reports/node-debug-bundle"
  );

  const envKeys = resolveEnvKeys(args.env);
  const requestedFiles = [
    ...DEFAULT_FILE_CANDIDATES,
    ...toArray(args.file),
    ...toArray(args["log-file"]),
  ];

  const bundle = {
    generatedAt: new Date().toISOString(),
    host: {
      platform: process.platform,
      arch: process.arch,
      node: process.version,
      cwd: process.cwd(),
    },
    tools: {
      npm: safeExec("npm", ["--version"], repoRoot),
      git: {
        commit: safeExec("git", ["rev-parse", "HEAD"], repoRoot),
        branch: safeExec("git", ["rev-parse", "--abbrev-ref", "HEAD"], repoRoot),
        statusShort: safeExec("git", ["status", "--short"], repoRoot),
      },
    },
    packageInfo: {
      root: readPackageJson(path.join(repoRoot, "package.json")),
      memoryServer: readPackageJson(
        path.join(repoRoot, "packages", "memory-server", "package.json")
      ),
    },
    env: collectEnv(envKeys),
    files: collectFiles(repoRoot, dedupeNonEmpty(requestedFiles)),
  };

  mkdirSync(outputDir, { recursive: true });
  const jsonPath = path.join(outputDir, "bundle.json");
  const summaryPath = path.join(outputDir, "SUMMARY.md");
  writeFileSync(jsonPath, `${JSON.stringify(bundle, null, 2)}\n`, "utf8");
  writeFileSync(summaryPath, toSummary(bundle), "utf8");

  console.log(`[node-debug-bundle] bundle=${jsonPath}`);
  console.log(`[node-debug-bundle] summary=${summaryPath}`);
}

function parseArgs(argv) {
  const parsed = {};
  for (let index = 0; index < argv.length; index += 1) {
    const current = argv[index];
    if (!current.startsWith("--")) {
      continue;
    }
    const normalized = current.slice(2);
    const equalIndex = normalized.indexOf("=");
    if (equalIndex >= 0) {
      const key = normalized.slice(0, equalIndex);
      const value = normalized.slice(equalIndex + 1);
      appendArg(parsed, key, value);
      continue;
    }
    const next = argv[index + 1];
    if (next !== undefined && !next.startsWith("--")) {
      appendArg(parsed, normalized, next);
      index += 1;
      continue;
    }
    appendArg(parsed, normalized, "true");
  }
  return parsed;
}

function appendArg(parsed, key, value) {
  const existing = parsed[key];
  if (existing === undefined) {
    parsed[key] = value;
    return;
  }
  if (Array.isArray(existing)) {
    existing.push(value);
    return;
  }
  parsed[key] = [existing, value];
}

function resolveRepoRoot() {
  const scriptFile = fileURLToPath(import.meta.url);
  const scriptDir = path.dirname(scriptFile);
  return path.resolve(scriptDir, "..", "..");
}

function resolveEnvKeys(rawEnv) {
  if (rawEnv === undefined) {
    return DEFAULT_ENV_KEYS;
  }
  const entries = toArray(rawEnv)
    .flatMap((value) => value.split(","))
    .map((value) => value.trim())
    .filter((value) => value.length > 0);
  if (entries.length === 0) {
    return DEFAULT_ENV_KEYS;
  }
  return dedupeNonEmpty(entries);
}

function collectEnv(envKeys) {
  const result = {};
  for (const key of envKeys) {
    const value = process.env[key];
    result[key] = value === undefined ? null : redactSensitiveData(value);
  }
  return result;
}

function collectFiles(repoRoot, relativePaths) {
  return relativePaths.map((candidate) => {
    const fullPath = path.resolve(repoRoot, candidate);
    if (!existsSync(fullPath)) {
      return {
        path: candidate,
        exists: false,
      };
    }

    const stats = statSync(fullPath);
    const raw = readFileSync(fullPath, "utf8");
    const lines = raw.split(/\r?\n/u);
    const tail = lines.slice(Math.max(0, lines.length - 200)).join("\n");
    const redactedTail = redactSensitiveData(tail);

    return {
      path: candidate,
      exists: true,
      sizeBytes: stats.size,
      mtime: stats.mtime.toISOString(),
      tail: redactedTail,
    };
  });
}

function readPackageJson(packageJsonPath) {
  if (!existsSync(packageJsonPath)) {
    return null;
  }
  const parsed = JSON.parse(readFileSync(packageJsonPath, "utf8"));
  return {
    name: parsed.name ?? null,
    version: parsed.version ?? null,
  };
}

function safeExec(command, args, cwd) {
  try {
    return execFileSync(command, args, {
      cwd,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    })
      .trim()
      .slice(0, 20_000);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return `ERROR: ${message}`;
  }
}

function redactSensitiveData(input) {
  return input
    .replace(URI_CREDENTIAL_PATTERN, `$1$2:${REDACTED_PLACEHOLDER}@`)
    .replace(SECRET_JSON_PATTERN, `$1${REDACTED_PLACEHOLDER}$3`)
    .replace(SECRET_QUERY_PATTERN, `$1${REDACTED_PLACEHOLDER}`)
    .replace(SECRET_ASSIGNMENT_PATTERN, `$1${REDACTED_PLACEHOLDER}`);
}

function dedupeNonEmpty(values) {
  const seen = new Set();
  const result = [];
  for (const value of values) {
    const normalized = value.trim();
    if (normalized.length === 0 || seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    result.push(normalized);
  }
  return result;
}

function toArray(value) {
  if (value === undefined) {
    return [];
  }
  return Array.isArray(value) ? value : [value];
}

function toSummary(bundle) {
  return [
    "# Node Debug Bundle Summary",
    "",
    `Generated at: ${bundle.generatedAt}`,
    "",
    "## Host",
    `- platform: ${bundle.host.platform}`,
    `- arch: ${bundle.host.arch}`,
    `- node: ${bundle.host.node}`,
    "",
    "## Packages",
    `- root: ${bundle.packageInfo.root?.name ?? "unknown"}@${
      bundle.packageInfo.root?.version ?? "unknown"
    }`,
    `- memory-server: ${bundle.packageInfo.memoryServer?.name ?? "unknown"}@${
      bundle.packageInfo.memoryServer?.version ?? "unknown"
    }`,
    "",
    "## Collected Files",
    ...bundle.files.map((entry) =>
      entry.exists
        ? `- ${entry.path} (size=${entry.sizeBytes} bytes, mtime=${entry.mtime})`
        : `- ${entry.path} (missing)`
    ),
    "",
    "Attach both files when opening an issue:",
    "- `bundle.json`",
    "- `SUMMARY.md`",
    "",
  ].join("\n");
}
