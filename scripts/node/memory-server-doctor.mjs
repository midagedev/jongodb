#!/usr/bin/env node

import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import path from "node:path";
import process from "node:process";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

const requireFromRoot = createRequire(path.resolve(process.cwd(), "__doctor_resolver__.cjs"));

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`[node-doctor] ${message}`);
  process.exit(1);
});

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const repoRoot = resolveRepoRoot();
  const outputDir = path.resolve(repoRoot, args.outputDir ?? "build/reports/node-doctor");
  const shouldFailOnIssues = args["no-fail"] !== "true";

  const checks = [];
  checks.push(checkNodeVersion());
  checks.push(checkBuildArtifacts(repoRoot));
  checks.push(checkBinaryEnvPath());

  const bundledBinary = resolveBundledBinary(repoRoot);
  checks.push(bundledBinary.check);

  const classpathEnv = process.env.JONGODB_CLASSPATH?.trim() ?? "";
  if (classpathEnv.length > 0) {
    checks.push({
      id: "classpath-env",
      status: "pass",
      summary: "JONGODB_CLASSPATH is set.",
      details: truncate(classpathEnv, 400),
    });
  } else {
    const classpathProbe = resolveClasspathViaGradle(repoRoot);
    checks.push(classpathProbe.check);
  }

  const hasAnyRuntimePath =
    checkPassed(checks, "binary-env-path") ||
    checkPassed(checks, "bundled-binary") ||
    checkPassed(checks, "classpath-env") ||
    checkPassed(checks, "classpath-gradle");

  checks.push({
    id: "runtime-preflight",
    status: hasAnyRuntimePath ? "pass" : "fail",
    summary: hasAnyRuntimePath
      ? "At least one launcher runtime path is available."
      : "No launcher runtime path detected (binary/classpath missing).",
    details: hasAnyRuntimePath
      ? "binary or classpath is resolvable."
      : "Configure JONGODB_BINARY_PATH, bundled binaries, or JONGODB_CLASSPATH.",
  });

  const report = {
    generatedAt: new Date().toISOString(),
    host: {
      platform: process.platform,
      arch: process.arch,
      node: process.version,
    },
    checks,
  };

  mkdirSync(outputDir, { recursive: true });
  const jsonPath = path.join(outputDir, "doctor.json");
  const markdownPath = path.join(outputDir, "doctor.md");
  writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  writeFileSync(markdownPath, toMarkdown(report), "utf8");

  console.log(`[node-doctor] report=${jsonPath}`);
  console.log(`[node-doctor] report=${markdownPath}`);

  const hasFail = checks.some((entry) => entry.status === "fail");
  if (hasFail && shouldFailOnIssues) {
    throw new Error("Doctor detected failing preflight checks.");
  }
}

function checkNodeVersion() {
  const major = Number.parseInt(process.versions.node.split(".")[0] ?? "0", 10);
  if (major >= 20) {
    return {
      id: "node-version",
      status: "pass",
      summary: `Node version is supported (${process.version}).`,
      details: "Required: >=20",
    };
  }
  return {
    id: "node-version",
    status: "fail",
    summary: `Node version is unsupported (${process.version}).`,
    details: "Required: >=20",
  };
}

function checkBuildArtifacts(repoRoot) {
  const esmEntry = path.join(repoRoot, "packages/memory-server/dist/esm/index.js");
  if (!existsSync(esmEntry)) {
    return {
      id: "build-artifacts",
      status: "warn",
      summary: "memory-server dist artifacts not found.",
      details: "Run: npm run node:build",
    };
  }
  return {
    id: "build-artifacts",
    status: "pass",
    summary: "memory-server build artifacts exist.",
    details: esmEntry,
  };
}

function checkBinaryEnvPath() {
  const candidate = process.env.JONGODB_BINARY_PATH?.trim() ?? "";
  if (candidate.length === 0) {
    return {
      id: "binary-env-path",
      status: "warn",
      summary: "JONGODB_BINARY_PATH is not set.",
      details: "Optional; bundled binary or classpath may still be used.",
    };
  }
  if (!existsSync(candidate)) {
    return {
      id: "binary-env-path",
      status: "fail",
      summary: "JONGODB_BINARY_PATH is set but file does not exist.",
      details: candidate,
    };
  }
  return {
    id: "binary-env-path",
    status: "pass",
    summary: "JONGODB_BINARY_PATH points to an existing file.",
    details: candidate,
  };
}

function resolveBundledBinary(repoRoot) {
  const packageName = detectBundledPackageName();
  if (packageName === null) {
    return {
      check: {
        id: "bundled-binary",
        status: "warn",
        summary: "No bundled binary mapping for current platform/arch.",
        details: `${process.platform}/${process.arch}`,
      },
    };
  }

  try {
    const packageJsonPath = requireFromRoot.resolve(`${packageName}/package.json`);
    const parsed = JSON.parse(readFileSync(packageJsonPath, "utf8"));
    const binaryRelativePath = parsed?.jongodb?.binary;
    if (typeof binaryRelativePath !== "string" || binaryRelativePath.trim().length === 0) {
      return {
        check: {
          id: "bundled-binary",
          status: "warn",
          summary: `Bundled package '${packageName}' found but binary metadata is missing.`,
          details: packageJsonPath,
        },
      };
    }
    const packageDir = path.dirname(packageJsonPath);
    const binaryPath = path.resolve(packageDir, binaryRelativePath);
    if (!existsSync(binaryPath)) {
      return {
        check: {
          id: "bundled-binary",
          status: "fail",
          summary: `Bundled package '${packageName}' found but binary file is missing.`,
          details: binaryPath,
        },
      };
    }
    const sizeBytes = statSync(binaryPath).size;
    return {
      check: {
        id: "bundled-binary",
        status: "pass",
        summary: `Bundled binary package '${packageName}' is available.`,
        details: `${path.relative(repoRoot, binaryPath)} (${sizeBytes} bytes)`,
      },
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return {
      check: {
        id: "bundled-binary",
        status: "warn",
        summary: `Bundled binary package '${packageName}' is not resolvable in current install.`,
        details: message,
      },
    };
  }
}

function resolveClasspathViaGradle(repoRoot) {
  const gradleCmd = resolveGradleCmd(repoRoot);
  try {
    const output = execFileSync(gradleCmd, ["--no-daemon", "-q", "printLauncherClasspath"], {
      cwd: repoRoot,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    });
    const classpath = output
      .split(/\r?\n/u)
      .map((line) => line.trim())
      .filter((line) => line.length > 0)
      .at(-1);
    if (classpath === undefined || classpath.length === 0) {
      return {
        check: {
          id: "classpath-gradle",
          status: "fail",
          summary: "Gradle classpath probe returned empty output.",
          details: `${gradleCmd} --no-daemon -q printLauncherClasspath`,
        },
      };
    }
    return {
      check: {
        id: "classpath-gradle",
        status: "pass",
        summary: "Gradle classpath auto-probe succeeded.",
        details: truncate(classpath, 400),
      },
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return {
      check: {
        id: "classpath-gradle",
        status: "warn",
        summary: "Gradle classpath auto-probe failed.",
        details: `${gradleCmd} --no-daemon -q printLauncherClasspath | ${message}`,
      },
    };
  }
}

function resolveGradleCmd(repoRoot) {
  const repoLocalGradle = path.resolve(repoRoot, ".tooling/gradle-8.10.2/bin/gradle");
  if (existsSync(repoLocalGradle)) {
    return repoLocalGradle;
  }
  return "gradle";
}

function detectBundledPackageName() {
  if (process.platform === "darwin" && process.arch === "arm64") {
    return "@jongodb/memory-server-bin-darwin-arm64";
  }
  if (process.platform === "linux" && process.arch === "x64") {
    return "@jongodb/memory-server-bin-linux-x64-gnu";
  }
  if (process.platform === "win32" && process.arch === "x64") {
    return "@jongodb/memory-server-bin-win32-x64";
  }
  return null;
}

function checkPassed(checks, id) {
  return checks.some((entry) => entry.id === id && entry.status === "pass");
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
      parsed[normalized.slice(0, equalIndex)] = normalized.slice(equalIndex + 1);
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

function resolveRepoRoot() {
  const scriptFile = fileURLToPath(import.meta.url);
  const scriptDir = path.dirname(scriptFile);
  return path.resolve(scriptDir, "..", "..");
}

function truncate(value, maxLength) {
  if (value.length <= maxLength) {
    return value;
  }
  return `${value.slice(0, maxLength)}...`;
}

function toMarkdown(report) {
  const rows = report.checks
    .map(
      (entry) =>
        `| ${entry.id} | ${entry.status.toUpperCase()} | ${entry.summary} | ${entry.details ?? ""} |`
    )
    .join("\n");
  return [
    "# Node Launcher Doctor",
    "",
    `Generated at: ${report.generatedAt}`,
    "",
    "| Check | Status | Summary | Details |",
    "| --- | --- | --- | --- |",
    rows,
    "",
  ].join("\n");
}
