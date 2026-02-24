#!/usr/bin/env node

import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

const BIN_PACKAGES = [
  "@jongodb/memory-server-bin-darwin-arm64",
  "@jongodb/memory-server-bin-linux-x64-gnu",
  "@jongodb/memory-server-bin-win32-x64",
];

const args = parseArgs(process.argv.slice(2));
const version = requireValue(args, "version");
const packageJsonPath =
  args.packageJson !== undefined
    ? resolve(args.packageJson)
    : resolve("packages/memory-server/package.json");

const packageJson = JSON.parse(readFileSync(packageJsonPath, "utf8"));
const optionalDependencies =
  typeof packageJson.optionalDependencies === "object" &&
  packageJson.optionalDependencies !== null
    ? { ...packageJson.optionalDependencies }
    : {};

for (const packageName of BIN_PACKAGES) {
  optionalDependencies[packageName] = version;
}

packageJson.optionalDependencies = optionalDependencies;
writeFileSync(packageJsonPath, `${JSON.stringify(packageJson, null, 2)}\n`, "utf8");
console.log(`[sync-optional-bins] updated ${packageJsonPath} -> ${version}`);

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
