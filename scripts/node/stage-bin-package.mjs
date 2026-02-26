#!/usr/bin/env node

import { createHash } from "node:crypto";
import { chmodSync, copyFileSync, existsSync, mkdirSync, readFileSync } from "node:fs";
import { writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";

const args = parseArgs(process.argv.slice(2));
const workspace = resolve(requireValue(args, "workspace"));
const binary = resolveBinaryPath(resolve(requireValue(args, "binary")));

const packageJsonPath = join(workspace, "package.json");
const packageJson = JSON.parse(readFileSync(packageJsonPath, "utf8"));
const relativePrimaryPath = packageJson?.jongodb?.binary;

if (typeof relativePrimaryPath !== "string" || relativePrimaryPath.trim().length === 0) {
  throw new Error(
    `package.json at ${packageJsonPath} must define jongodb.binary as a non-empty string`
  );
}

const primaryPath = resolve(workspace, relativePrimaryPath.trim());
mkdirSync(dirname(primaryPath), { recursive: true });

if (primaryPath.endsWith(".cmd")) {
  const exePath = join(dirname(primaryPath), "jongodb.exe");
  copyFileSync(binary, exePath);
  console.log(`[stage-bin] copied executable -> ${exePath}`);
} else {
  copyFileSync(binary, primaryPath);
  chmodSync(primaryPath, 0o755);
  console.log(`[stage-bin] copied executable -> ${primaryPath}`);
}

const checksumTargetRelativePath = resolveChecksumTargetRelativePath(
  packageJson,
  relativePrimaryPath.trim()
);
const checksumTargetPath = resolve(workspace, checksumTargetRelativePath);
if (!existsSync(checksumTargetPath)) {
  throw new Error(
    `[stage-bin] checksum target file does not exist: ${checksumTargetPath}`
  );
}

const checksum = createHash("sha256")
  .update(readFileSync(checksumTargetPath))
  .digest("hex");
if (typeof packageJson.jongodb !== "object" || packageJson.jongodb === null) {
  packageJson.jongodb = {};
}
packageJson.jongodb.sha256 = checksum;
writeFileSync(packageJsonPath, `${JSON.stringify(packageJson, null, 2)}\n`, "utf8");
console.log(
  `[stage-bin] updated checksum -> ${checksum} (${checksumTargetRelativePath})`
);

function parseArgs(argv) {
  const parsed = {};
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith("--")) {
      throw new Error(`Unexpected argument: ${token}`);
    }
    const pair = token.slice(2).split("=");
    const key = pair[0];
    const inlineValue = pair.slice(1).join("=");

    if (inlineValue.length > 0) {
      parsed[key] = inlineValue;
      continue;
    }
    const next = argv[i + 1];
    if (next === undefined || next.startsWith("--")) {
      throw new Error(`Missing value for --${key}`);
    }
    parsed[key] = next;
    i += 1;
  }
  return parsed;
}

function requireValue(parsed, key) {
  const value = parsed[key];
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`--${key} is required`);
  }
  return value.trim();
}

function resolveBinaryPath(requested) {
  if (existsSync(requested)) {
    return requested;
  }
  if (process.platform === "win32" && !requested.toLowerCase().endsWith(".exe")) {
    const withExe = `${requested}.exe`;
    if (existsSync(withExe)) {
      return withExe;
    }
  }
  throw new Error(`Binary file not found: ${requested}`);
}

function resolveChecksumTargetRelativePath(packageJson, defaultRelativePath) {
  const candidate = packageJson?.jongodb?.sha256Target;
  if (typeof candidate !== "string" || candidate.trim().length === 0) {
    return defaultRelativePath;
  }
  return candidate.trim();
}
