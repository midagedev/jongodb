import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import ts from "typescript";

const scriptsDir = path.dirname(fileURLToPath(import.meta.url));
const packageDir = path.resolve(scriptsDir, "..");
const reportPath = path.join(packageDir, "reports", "type-api-coverage.md");

const modules = [
  {
    subpath: ".",
    source: path.join(packageDir, "src", "index.ts"),
    declaration: path.join(packageDir, "dist", "esm", "index.d.ts"),
  },
  {
    subpath: "./runtime",
    source: path.join(packageDir, "src", "runtime.ts"),
    declaration: path.join(packageDir, "dist", "esm", "runtime.d.ts"),
  },
  {
    subpath: "./jest",
    source: path.join(packageDir, "src", "jest.ts"),
    declaration: path.join(packageDir, "dist", "esm", "jest.d.ts"),
  },
  {
    subpath: "./nestjs",
    source: path.join(packageDir, "src", "nestjs.ts"),
    declaration: path.join(packageDir, "dist", "esm", "nestjs.d.ts"),
  },
  {
    subpath: "./vitest",
    source: path.join(packageDir, "src", "vitest.ts"),
    declaration: path.join(packageDir, "dist", "esm", "vitest.d.ts"),
  },
];

const rows = [];
let totalExports = 0;
let totalMissing = 0;

for (const entry of modules) {
  const sourceText = await readFile(entry.source, "utf8");
  const declarationText = await readFile(entry.declaration, "utf8");

  const sourceExports = collectExportedNames(sourceText, entry.source);
  const declarationExports = collectExportedNames(
    declarationText,
    entry.declaration
  );

  const missing = [...sourceExports].filter(
    (name) => !declarationExports.has(name)
  );
  const coverage =
    sourceExports.size === 0
      ? 100
      : Math.round(
          ((sourceExports.size - missing.length) / sourceExports.size) * 10_000
        ) / 100;

  totalExports += sourceExports.size;
  totalMissing += missing.length;

  rows.push({
    subpath: entry.subpath,
    sourceExportCount: sourceExports.size,
    declarationExportCount: declarationExports.size,
    missing,
    coverage,
  });
}

const overallCoverage =
  totalExports === 0
    ? 100
    : Math.round(((totalExports - totalMissing) / totalExports) * 10_000) / 100;

const reportLines = [
  "# Type API Coverage Report",
  "",
  "Strict type gate:",
  "- `npm run typecheck`",
  "- `npm run build:esm`",
  "",
  "| Export subpath | Source exports | Declaration exports | Missing | Coverage |",
  "| --- | ---: | ---: | ---: | ---: |",
  ...rows.map(
    (row) =>
      `| \`${row.subpath}\` | ${row.sourceExportCount} | ${row.declarationExportCount} | ${row.missing.length} | ${row.coverage.toFixed(
        2
      )}% |`
  ),
  "",
  `Overall: ${totalExports - totalMissing}/${totalExports} (${overallCoverage.toFixed(
    2
  )}%)`,
];

for (const row of rows) {
  if (row.missing.length === 0) {
    continue;
  }
  reportLines.push("");
  reportLines.push(`## Missing exports in \`${row.subpath}\``);
  for (const name of row.missing) {
    reportLines.push(`- \`${name}\``);
  }
}

await mkdir(path.dirname(reportPath), { recursive: true });
await writeFile(reportPath, `${reportLines.join("\n")}\n`, "utf8");

console.log(`Type API coverage report written: ${reportPath}`);
console.log(
  `Type API coverage summary: exports=${totalExports} missing=${totalMissing} coverage=${overallCoverage.toFixed(
    2
  )}%`
);

if (totalMissing > 0) {
  throw new Error(
    `Type API declaration coverage check failed (missing ${totalMissing} exports).`
  );
}

function collectExportedNames(sourceText, filePath) {
  const sourceFile = ts.createSourceFile(
    filePath,
    sourceText,
    ts.ScriptTarget.Latest,
    true
  );
  const names = new Set();

  for (const node of sourceFile.statements) {
    if (ts.isExportDeclaration(node)) {
      if (
        node.exportClause !== undefined &&
        ts.isNamedExports(node.exportClause)
      ) {
        for (const element of node.exportClause.elements) {
          names.add(element.name.text);
        }
      }
      continue;
    }

    if (!hasExportModifier(node)) {
      continue;
    }

    if (
      ts.isInterfaceDeclaration(node) ||
      ts.isTypeAliasDeclaration(node) ||
      ts.isFunctionDeclaration(node) ||
      ts.isClassDeclaration(node) ||
      ts.isEnumDeclaration(node)
    ) {
      if (node.name !== undefined) {
        names.add(node.name.text);
      }
      continue;
    }

    if (ts.isVariableStatement(node)) {
      for (const declaration of node.declarationList.declarations) {
        if (ts.isIdentifier(declaration.name)) {
          names.add(declaration.name.text);
        }
      }
    }
  }

  return names;
}

function hasExportModifier(node) {
  return (
    node.modifiers?.some(
      (modifier) => modifier.kind === ts.SyntaxKind.ExportKeyword
    ) ?? false
  );
}
