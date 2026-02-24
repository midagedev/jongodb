import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const scriptsDir = path.dirname(fileURLToPath(import.meta.url));
const packageDir = path.resolve(scriptsDir, "..");
const cjsDir = path.join(packageDir, "dist", "cjs");
const cjsPackageJsonPath = path.join(cjsDir, "package.json");

await mkdir(cjsDir, { recursive: true });
await writeFile(cjsPackageJsonPath, '{\n  "type": "commonjs"\n}\n', "utf8");
