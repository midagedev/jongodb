#!/usr/bin/env node

import { existsSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
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
console.log(`[native-image] classpathLength=${classpath.length}`);

const nativeImage = runNativeImage(nativeImageArgs);

if (nativeImage.error !== undefined) {
  console.error(`[native-image] failed to start: ${nativeImage.error.message}`);
  if (nativeImage.error.code === "ENOENT") {
    console.error(
      "[native-image] command not found. Ensure GraalVM native-image is installed and available on PATH."
    );
    console.error(`[native-image] GRAALVM_HOME=${process.env.GRAALVM_HOME ?? "<unset>"}`);
    console.error(`[native-image] JAVA_HOME=${process.env.JAVA_HOME ?? "<unset>"}`);
  }
  process.exit(1);
}

if (nativeImage.status !== 0) {
  process.exit(nativeImage.status ?? 1);
}

function runNativeImage(nativeImageArgs) {
  let lastEnoentResult = null;
  for (const command of resolveNativeImageCommands()) {
    console.log(`[native-image] command=${command}`);
    const result = spawnSync(command, nativeImageArgs, {
      stdio: "inherit",
      shell: false,
    });
    if (result.error?.code === "ENOENT") {
      lastEnoentResult = result;
      continue;
    }
    return result;
  }
  if (lastEnoentResult !== null) {
    return lastEnoentResult;
  }
  return spawnSync("native-image", nativeImageArgs, {
    stdio: "inherit",
    shell: false,
  });
}

function resolveNativeImageCommands() {
  const candidates = [];
  if (process.platform === "win32") {
    pushIfExists(candidates, process.env.GRAALVM_HOME, "native-image.cmd");
    pushIfExists(candidates, process.env.JAVA_HOME, "native-image.cmd");
  } else {
    pushIfExists(candidates, process.env.GRAALVM_HOME, "native-image");
    pushIfExists(candidates, process.env.JAVA_HOME, "native-image");
  }
  candidates.push("native-image");
  return [...new Set(candidates)];
}

function pushIfExists(candidates, home, binaryName) {
  if (typeof home !== "string" || home.trim().length === 0) {
    return;
  }
  const candidate = join(home, "bin", binaryName);
  if (existsSync(candidate)) {
    candidates.push(candidate);
  }
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
