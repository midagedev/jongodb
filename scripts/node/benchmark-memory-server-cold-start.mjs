#!/usr/bin/env node

import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";

import { startJongodbMemoryServer } from "../../packages/memory-server/dist/esm/index.js";

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`[node-cold-start] ${message}`);
  process.exit(1);
});

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const repoRoot = resolveRepoRoot();
  const iterations = parsePositiveInt(args.iterations ?? "7", "iterations");
  const startupTimeoutMs = parsePositiveInt(
    args.startupTimeoutMs ?? "20000",
    "startupTimeoutMs"
  );
  const budgetMedianMs = parsePositiveNumber(
    args["budget-median-ms"] ?? "3000",
    "budget-median-ms"
  );
  const budgetP95Ms = parsePositiveNumber(
    args["budget-p95-ms"] ?? "5000",
    "budget-p95-ms"
  );
  const outputDir = path.resolve(
    repoRoot,
    args.outputDir ?? "build/reports/node-cold-start"
  );
  const classpath =
    args.classpath?.trim() ||
    process.env.JONGODB_CLASSPATH?.trim() ||
    resolveClasspathFromGradle(repoRoot, args.gradle);

  if (classpath.length === 0) {
    throw new Error("Resolved classpath is empty.");
  }

  const samplesMs = [];
  for (let index = 0; index < iterations; index += 1) {
    const startedAt = performance.now();
    const server = await startJongodbMemoryServer({
      classpath,
      databaseName: `cold_start_${index + 1}`,
      startupTimeoutMs,
    });
    const elapsedMs = performance.now() - startedAt;
    samplesMs.push(roundMs(elapsedMs));
    await server.stop();
  }

  const summary = summarize(samplesMs);
  const budget = {
    medianMs: budgetMedianMs,
    p95Ms: budgetP95Ms,
  };
  const gate = {
    medianWithinBudget: summary.medianMs <= budgetMedianMs,
    p95WithinBudget: summary.p95Ms <= budgetP95Ms,
  };

  const report = {
    generatedAt: new Date().toISOString(),
    host: {
      platform: process.platform,
      arch: process.arch,
      node: process.version,
    },
    settings: {
      iterations,
      startupTimeoutMs,
      budget,
    },
    samplesMs,
    summary,
    gate,
  };

  mkdirSync(outputDir, { recursive: true });
  const jsonPath = path.join(outputDir, "report.json");
  const markdownPath = path.join(outputDir, "report.md");
  writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  writeFileSync(markdownPath, toMarkdown(report), "utf8");

  console.log(`[node-cold-start] report=${jsonPath}`);
  console.log(`[node-cold-start] report=${markdownPath}`);
  console.log(
    `[node-cold-start] summary median=${summary.medianMs}ms p95=${summary.p95Ms}ms`
  );

  if (!gate.medianWithinBudget || !gate.p95WithinBudget) {
    throw new Error(
      [
        "Cold-start budget gate failed.",
        `median ${summary.medianMs}ms (budget ${budgetMedianMs}ms)`,
        `p95 ${summary.p95Ms}ms (budget ${budgetP95Ms}ms)`,
      ].join(" ")
    );
  }
}

function resolveRepoRoot() {
  const scriptFile = fileURLToPath(import.meta.url);
  const scriptDir = path.dirname(scriptFile);
  return path.resolve(scriptDir, "..", "..");
}

function resolveClasspathFromGradle(repoRoot, gradleArg) {
  const repoLocalGradle = path.resolve(
    repoRoot,
    ".tooling",
    "gradle-8.10.2",
    "bin",
    "gradle"
  );
  const gradleCmd =
    gradleArg?.trim() ||
    process.env.JONGODB_TEST_GRADLE_CMD?.trim() ||
    (existsSync(repoLocalGradle) ? repoLocalGradle : "gradle");

  let output;
  try {
    output = execFileSync(gradleCmd, ["--no-daemon", "-q", "printLauncherClasspath"], {
      cwd: repoRoot,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(
      [
        "Failed to resolve classpath via Gradle.",
        `Attempted command: ${gradleCmd} --no-daemon -q printLauncherClasspath`,
        "Set --classpath or JONGODB_CLASSPATH to bypass Gradle resolution.",
        `Cause: ${message}`,
      ].join(" ")
    );
  }

  const lines = output
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  const classpath = lines.at(-1);
  if (classpath === undefined || classpath.length === 0) {
    throw new Error("Failed to resolve classpath from Gradle output.");
  }
  return classpath;
}

function summarize(samplesMs) {
  const sorted = [...samplesMs].sort((left, right) => left - right);
  const total = sorted.reduce((sum, value) => sum + value, 0);
  return {
    count: sorted.length,
    minMs: sorted[0],
    maxMs: sorted[sorted.length - 1],
    meanMs: roundMs(total / sorted.length),
    medianMs: percentile(sorted, 50),
    p95Ms: percentile(sorted, 95),
  };
}

function percentile(sorted, percentileValue) {
  if (sorted.length === 0) {
    return 0;
  }
  const index = Math.max(
    0,
    Math.min(
      sorted.length - 1,
      Math.ceil((percentileValue / 100) * sorted.length) - 1
    )
  );
  return sorted[index];
}

function roundMs(value) {
  return Math.round(value * 100) / 100;
}

function parsePositiveInt(raw, name) {
  const value = Number.parseInt(raw, 10);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer.`);
  }
  return value;
}

function parsePositiveNumber(raw, name) {
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`${name} must be a positive number.`);
  }
  return value;
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
      parsed[key] = value;
      continue;
    }
    const next = argv[index + 1];
    if (next !== undefined && !next.startsWith("--")) {
      parsed[normalized] = next;
      index += 1;
      continue;
    }
    parsed[normalized] = "true";
  }
  return parsed;
}

function toMarkdown(report) {
  return [
    "# Node Cold-Start Benchmark",
    "",
    `Generated at: ${report.generatedAt}`,
    "",
    "## Summary",
    `- iterations: ${report.settings.iterations}`,
    `- median: ${report.summary.medianMs} ms (budget ${report.settings.budget.medianMs} ms)`,
    `- p95: ${report.summary.p95Ms} ms (budget ${report.settings.budget.p95Ms} ms)`,
    `- pass: ${String(
      report.gate.medianWithinBudget && report.gate.p95WithinBudget
    )}`,
    "",
    "## Samples (ms)",
    `- ${report.samplesMs.join(", ")}`,
    "",
  ].join("\n");
}
