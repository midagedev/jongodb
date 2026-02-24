#!/usr/bin/env node

import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { spawnSync } from "node:child_process";

const args = parseArgs(process.argv.slice(2));
const output = requireValue(args, "output");
const mainClass = args.mainClass ?? "org.jongodb.server.TcpMongoServerLauncher";
const gradleCmd = args.gradle ?? "gradle";

const classpath = args.classpath ?? resolveClasspathFromGradle(gradleCmd);

mkdirSync(dirname(output), { recursive: true });

const nativeImageArgs = [
  "--no-fallback",
  "--install-exit-handlers",
  "--report-unsupported-elements-at-runtime",
  "-cp",
  classpath,
  mainClass,
  output,
];

console.log(`[native-image] output=${output}`);
console.log(`[native-image] mainClass=${mainClass}`);

const nativeImage = spawnSync("native-image", nativeImageArgs, {
  stdio: "inherit",
  shell: false,
});

if (nativeImage.status !== 0) {
  process.exit(nativeImage.status ?? 1);
}

function resolveClasspathFromGradle(gradleCmd) {
  const resolved = spawnSync(
    gradleCmd,
    ["--no-daemon", "-q", "printLauncherClasspath"],
    {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
      shell: false,
    }
  );

  if (resolved.status !== 0) {
    process.stderr.write(resolved.stderr ?? "");
    throw new Error("Failed to resolve launcher classpath via Gradle.");
  }

  const lines = (resolved.stdout ?? "")
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);

  const classpath = lines.at(-1);
  if (classpath === undefined || classpath.length === 0) {
    throw new Error("Gradle did not return a launcher classpath.");
  }
  return classpath;
}

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
