import { execFileSync } from "node:child_process";
import { existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export function resolveTestClasspath(): string {
  const fromEnv = process.env.JONGODB_TEST_CLASSPATH?.trim();
  if (fromEnv !== undefined && fromEnv.length > 0) {
    return fromEnv;
  }

  const currentFile = fileURLToPath(import.meta.url);
  const currentDir = path.dirname(currentFile);
  const packageDir = path.resolve(currentDir, "..", "..", "..");
  const repoRoot = path.resolve(packageDir, "..", "..");
  const repoLocalGradle = path.resolve(
    repoRoot,
    ".tooling",
    "gradle-8.10.2",
    "bin",
    "gradle"
  );
  const gradle =
    process.env.JONGODB_TEST_GRADLE_CMD?.trim() ||
    (existsSync(repoLocalGradle) ? repoLocalGradle : "gradle");

  let output: string;
  try {
    output = execFileSync(gradle, ["--no-daemon", "-q", "printLauncherClasspath"], {
      cwd: repoRoot,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    });
  } catch (error: unknown) {
    if (error instanceof Error) {
      throw new Error(
        [
          "Failed to resolve test classpath via Gradle.",
          `Attempted command: ${gradle} --no-daemon -q printLauncherClasspath`,
          "Set JONGODB_TEST_CLASSPATH directly to bypass Gradle resolution.",
          `Cause: ${error.message}`,
        ].join(" ")
      );
    }
    throw error;
  }

  const lines = output
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  const classpath = lines.at(-1);
  if (classpath === undefined || classpath.length === 0) {
    throw new Error("Failed to resolve launcher classpath from Gradle output.");
  }
  return classpath;
}
