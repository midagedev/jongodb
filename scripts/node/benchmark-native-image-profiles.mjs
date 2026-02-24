#!/usr/bin/env node

import {
  existsSync,
  mkdirSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { spawn, spawnSync } from "node:child_process";
import { createInterface } from "node:readline";
import { join, resolve } from "node:path";
import { performance } from "node:perf_hooks";

const READY_PREFIX = "JONGODB_URI=";
const FAILURE_PREFIX = "JONGODB_START_FAILURE=";
const DEFAULT_MAIN_CLASS = "org.jongodb.server.TcpMongoServerLauncher";

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`[benchmark-native] ${message}`);
  process.exit(1);
});

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const profiles = parseProfiles(args.profiles ?? "size,balanced,speed");
  const startupIterations = parsePositiveInt(
    args.startupIterations ?? "8",
    "startupIterations"
  );
  const startupTimeoutMs = parsePositiveInt(
    args.startupTimeoutMs ?? "15000",
    "startupTimeoutMs"
  );
  const outputDir = resolve(args.outputDir ?? "build/reports/native-image-profiles");
  const gradleCmd = args.gradle ?? resolveDefaultGradle();
  const launcherScript = resolve(
    args.launcherScript ?? "scripts/node/build-native-launcher.mjs"
  );
  const mainClass = args.mainClass ?? DEFAULT_MAIN_CLASS;
  const classpath = args.classpath ?? resolveClasspathFromGradle(gradleCmd);
  const clean = parseBoolean(args.clean ?? "true", "clean");

  if (clean) {
    rmSync(outputDir, { recursive: true, force: true });
  }
  mkdirSync(outputDir, { recursive: true });

  const results = [];
  for (const profile of profiles) {
    const profileDir = join(outputDir, profile);
    mkdirSync(profileDir, { recursive: true });
    const outputBinaryBase = join(profileDir, "jongodb");

    console.log(`[benchmark-native] profile=${profile} build=start`);
    const buildStarted = performance.now();
    runNativeBuild({
      launcherScript,
      outputBinaryBase,
      profile,
      gradleCmd,
      classpath,
      mainClass,
    });
    const buildMs = performance.now() - buildStarted;
    const binaryPath = resolveBuiltBinary(outputBinaryBase);
    const sizeBytes = statSync(binaryPath).size;

    console.log(`[benchmark-native] profile=${profile} startup=measure (${startupIterations} runs)`);
    const startupSamplesMs = [];
    for (let iteration = 0; iteration < startupIterations; iteration += 1) {
      const startupMs = await measureStartup(binaryPath, startupTimeoutMs);
      startupSamplesMs.push(startupMs);
    }

    results.push({
      profile,
      binaryPath,
      sizeBytes,
      buildMs,
      startupSamplesMs,
      startup: summarizeStartup(startupSamplesMs),
    });
  }

  const report = {
    generatedAt: new Date().toISOString(),
    host: {
      platform: process.platform,
      arch: process.arch,
      node: process.version,
    },
    settings: {
      gradleCmd,
      launcherScript,
      mainClass,
      startupIterations,
      startupTimeoutMs,
      profiles,
    },
    results,
  };

  const jsonPath = join(outputDir, "report.json");
  const markdownPath = join(outputDir, "report.md");
  writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  writeFileSync(markdownPath, toMarkdown(report), "utf8");

  console.log(`[benchmark-native] report=${jsonPath}`);
  console.log(`[benchmark-native] report=${markdownPath}`);
}

function runNativeBuild({
  launcherScript,
  outputBinaryBase,
  profile,
  gradleCmd,
  classpath,
  mainClass,
}) {
  const args = [
    launcherScript,
    "--output",
    outputBinaryBase,
    "--optimizationProfile",
    profile,
    "--gradle",
    gradleCmd,
    "--classpath",
    classpath,
    "--mainClass",
    mainClass,
  ];
  const result = spawnSync(process.execPath, args, {
    stdio: "inherit",
    shell: false,
  });
  if (result.status !== 0) {
    throw new Error(`native build failed for profile '${profile}'`);
  }
}

function resolveBuiltBinary(outputBinaryBase) {
  if (existsSync(outputBinaryBase)) {
    return outputBinaryBase;
  }
  const withExe = `${outputBinaryBase}.exe`;
  if (existsSync(withExe)) {
    return withExe;
  }
  throw new Error(`Built binary not found: ${outputBinaryBase}`);
}

async function measureStartup(binaryPath, timeoutMs) {
  const args = ["--host=127.0.0.1", "--port=0", "--database=bench"];
  const started = performance.now();

  const child = spawn(binaryPath, args, {
    stdio: ["ignore", "pipe", "pipe"],
    windowsHide: true,
  });

  const stderrLines = [];
  const stdoutReader = createInterface({ input: child.stdout });
  const stderrReader = createInterface({ input: child.stderr });

  return new Promise((resolvePromise, rejectPromise) => {
    let settled = false;
    const timeout = setTimeout(() => {
      if (settled) {
        return;
      }
      settled = true;
      void stopChild(child);
      rejectPromise(
        new Error(`startup timeout (${timeoutMs}ms). stderr=${stderrLines.join(" | ")}`)
      );
    }, timeoutMs);

    const fail = (reason) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timeout);
      void stopChild(child);
      rejectPromise(new Error(reason));
    };

    stdoutReader.on("line", (line) => {
      if (line.startsWith(READY_PREFIX)) {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timeout);
        const startupMs = performance.now() - started;
        void stopChild(child);
        resolvePromise(startupMs);
        return;
      }
      if (line.startsWith(FAILURE_PREFIX)) {
        fail(`launcher failure: ${line}`);
      }
    });

    stderrReader.on("line", (line) => {
      stderrLines.push(line);
    });

    child.on("error", (error) => {
      fail(`spawn error: ${error.message}`);
    });

    child.on("exit", (code, signal) => {
      if (settled) {
        return;
      }
      fail(
        `launcher exited before ready (code=${code ?? "null"}, signal=${signal ?? "null"}) stderr=${stderrLines.join(" | ")}`
      );
    });
  }).finally(() => {
    stdoutReader.close();
    stderrReader.close();
  });
}

function stopChild(child) {
  return new Promise((resolvePromise) => {
    if (child.exitCode !== null || child.killed) {
      resolvePromise();
      return;
    }

    const forceTimer = setTimeout(() => {
      if (child.exitCode === null) {
        child.kill("SIGKILL");
      }
    }, 1000);

    const doneTimer = setTimeout(() => {
      clearTimeout(forceTimer);
      resolvePromise();
    }, 1500);

    child.once("exit", () => {
      clearTimeout(forceTimer);
      clearTimeout(doneTimer);
      resolvePromise();
    });

    child.kill("SIGTERM");
  });
}

function summarize(samples) {
  const sorted = [...samples].sort((left, right) => left - right);
  return {
    minMs: sorted[0],
    maxMs: sorted[sorted.length - 1],
    meanMs: sum(samples) / samples.length,
    p50Ms: percentile(sorted, 0.5),
    p95Ms: percentile(sorted, 0.95),
  };
}

function summarizeStartup(samples) {
  const overall = summarize(samples);
  const coldStartMs = samples[0];
  const warmSamplesMs = samples.slice(1);
  const warm = warmSamplesMs.length > 0 ? summarize(warmSamplesMs) : null;
  return {
    overall,
    coldStartMs,
    warmSamplesMs,
    warm,
  };
}

function sum(values) {
  let total = 0;
  for (const value of values) {
    total += value;
  }
  return total;
}

function percentile(sorted, rank) {
  const index = Math.max(0, Math.min(sorted.length - 1, Math.ceil(sorted.length * rank) - 1));
  return sorted[index];
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Native Image Profile Benchmark");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Platform: ${report.host.platform}-${report.host.arch}`);
  lines.push(`- Node: ${report.host.node}`);
  lines.push(`- Startup iterations: ${report.settings.startupIterations}`);
  lines.push("");
  lines.push("| Profile | Binary Size (MB) | Build Time (s) | Cold Start (ms) | Warm p50 (ms) | Warm p95 (ms) | Warm mean (ms) |");
  lines.push("| --- | ---: | ---: | ---: | ---: | ---: | ---: |");
  for (const result of report.results) {
    const warm = result.startup.warm;
    lines.push(
      `| ${result.profile} | ${formatMb(result.sizeBytes)} | ${formatSeconds(result.buildMs)} | ${formatMs(result.startup.coldStartMs)} | ${formatMsOrDash(warm?.p50Ms)} | ${formatMsOrDash(warm?.p95Ms)} | ${formatMsOrDash(warm?.meanMs)} |`
    );
  }
  lines.push("");
  lines.push("## Binary Paths");
  for (const result of report.results) {
    lines.push(`- ${result.profile}: \`${result.binaryPath}\``);
  }
  lines.push("");
  lines.push("## Startup Samples (ms)");
  for (const result of report.results) {
    lines.push(`- ${result.profile}: ${result.startupSamplesMs.map((value) => formatMs(value)).join(", ")}`);
  }
  lines.push("");
  return `${lines.join("\n")}\n`;
}

function formatMb(bytes) {
  return (bytes / (1024 * 1024)).toFixed(2);
}

function formatSeconds(ms) {
  return (ms / 1000).toFixed(2);
}

function formatMs(ms) {
  return ms.toFixed(2);
}

function formatMsOrDash(value) {
  if (typeof value !== "number") {
    return "-";
  }
  return formatMs(value);
}

function resolveClasspathFromGradle(gradleCmd) {
  const result = spawnSync(gradleCmd, ["--no-daemon", "-q", "printLauncherClasspath"], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
    shell: false,
  });
  if (result.status !== 0) {
    process.stderr.write(result.stderr ?? "");
    throw new Error("Failed to resolve launcher classpath from Gradle.");
  }
  const lines = (result.stdout ?? "")
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  const classpath = lines.at(-1);
  if (classpath === undefined || classpath.length === 0) {
    throw new Error("Gradle did not return launcher classpath.");
  }
  return classpath;
}

function resolveDefaultGradle() {
  const local = resolve(".tooling/gradle-8.10.2/bin/gradle");
  if (existsSync(local)) {
    return local;
  }
  return "gradle";
}

function parseProfiles(raw) {
  const values = raw
    .split(",")
    .map((value) => value.trim().toLowerCase())
    .filter((value) => value.length > 0);
  if (values.length === 0) {
    throw new Error("profiles must include at least one profile.");
  }
  return values;
}

function parsePositiveInt(raw, name) {
  const value = Number.parseInt(raw, 10);
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer (got: ${raw})`);
  }
  return value;
}

function parseBoolean(raw, name) {
  const normalized = raw.trim().toLowerCase();
  if (normalized === "true") {
    return true;
  }
  if (normalized === "false") {
    return false;
  }
  throw new Error(`${name} must be true or false (got: ${raw})`);
}

function parseArgs(argv) {
  const parsed = {};
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith("--")) {
      throw new Error(`Unexpected argument: ${token}`);
    }
    const [rawKey, ...rest] = token.slice(2).split("=");
    const inlineValue = rest.join("=");
    if (inlineValue.length > 0) {
      parsed[rawKey] = inlineValue;
      continue;
    }
    const next = argv[index + 1];
    if (next === undefined || next.startsWith("--")) {
      throw new Error(`Missing value for --${rawKey}`);
    }
    parsed[rawKey] = next;
    index += 1;
  }
  return parsed;
}
