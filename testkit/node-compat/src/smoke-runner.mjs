import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { performance } from "node:perf_hooks";
import process from "node:process";

import { MongoClient } from "mongodb";
import mongoose from "mongoose";
import { startJongodbMemoryServer } from "../../../packages/memory-server/dist/index.js";

const DEFAULT_REPORT_PATH = path.resolve(
  process.cwd(),
  "reports",
  "node-compat-smoke.json"
);

const reportPath = parseArg("--report") ?? DEFAULT_REPORT_PATH;
const classpath = process.env.JONGODB_CLASSPATH?.trim();

if (classpath === undefined || classpath.length === 0) {
  throw new Error(
    "JONGODB_CLASSPATH is required. Resolve it with: ./.tooling/gradle-8.10.2/bin/gradle -q printLauncherClasspath"
  );
}

const server = await startJongodbMemoryServer({
  classpath,
  databaseName: "node_smoke",
  startupTimeoutMs: 20_000,
});

const scenarioResults = [];

try {
  const uri = server.uri;
  scenarioResults.push(await runScenario("mongodb.crud", () => mongodbCrud(uri)));
  scenarioResults.push(
    await runScenario("mongodb.transaction.commit", () =>
      mongodbTransactionCommit(uri)
    )
  );
  scenarioResults.push(
    await runScenario("mongodb.transaction.rollback", () =>
      mongodbTransactionRollback(uri)
    )
  );
  scenarioResults.push(await runScenario("mongoose.crud", () => mongooseCrud(uri)));
  scenarioResults.push(
    await runScenario("mongoose.transaction.commit", () =>
      mongooseTransactionCommit(uri)
    )
  );
  scenarioResults.push(
    await runScenario("mongoose.transaction.rollback", () =>
      mongooseTransactionRollback(uri)
    )
  );
} finally {
  await safeMongooseDisconnect();
  await server.stop();
}

const passed = scenarioResults.filter((scenario) => scenario.status === "pass").length;
const failed = scenarioResults.length - passed;

const report = {
  generatedAt: new Date().toISOString(),
  compatibilityTarget: "node-driver-and-mongoose-smoke",
  server: {
    uriScheme: "mongodb",
    backend: "jongodb",
  },
  summary: {
    total: scenarioResults.length,
    passed,
    failed,
  },
  scenarios: scenarioResults,
};

await mkdir(path.dirname(reportPath), { recursive: true });
await writeFile(reportPath, JSON.stringify(report, null, 2), "utf8");

console.log(`node-compat report written: ${reportPath}`);
console.log(`node-compat summary: passed=${passed} failed=${failed}`);

if (failed > 0) {
  process.exitCode = 1;
}

async function mongodbCrud(uri) {
  const client = new MongoClient(uri);
  await client.connect();
  try {
    const db = client.db("node_smoke");
    await db.collection("items").deleteMany({});
    await db.collection("items").insertOne({ _id: "item-1", value: 10 });
    const found = await db.collection("items").findOne({ _id: "item-1" });
    if (found?.value !== 10) {
      throw new Error("CRUD read value mismatch for mongodb driver scenario.");
    }
  } finally {
    await client.close();
  }
}

async function mongodbTransactionCommit(uri) {
  const client = new MongoClient(uri);
  await client.connect();
  try {
    const db = client.db("node_smoke");
    await db.collection("tx_commit").deleteMany({});

    const session = client.startSession();
    try {
      session.startTransaction();
      await db
        .collection("tx_commit")
        .insertOne({ _id: "commit-1", value: "committed" }, { session });
      await session.commitTransaction();
    } finally {
      await session.endSession();
    }

    const found = await db.collection("tx_commit").findOne({ _id: "commit-1" });
    if (found?.value !== "committed") {
      throw new Error("Transaction commit did not persist document.");
    }
  } finally {
    await client.close();
  }
}

async function mongodbTransactionRollback(uri) {
  const client = new MongoClient(uri);
  await client.connect();
  try {
    const db = client.db("node_smoke");
    await db.collection("tx_abort").deleteMany({});

    const session = client.startSession();
    try {
      session.startTransaction();
      await db
        .collection("tx_abort")
        .insertOne({ _id: "abort-1", value: "rolled-back" }, { session });
      await session.abortTransaction();
    } finally {
      await session.endSession();
    }

    const found = await db.collection("tx_abort").findOne({ _id: "abort-1" });
    if (found !== null) {
      throw new Error("Transaction rollback did not remove transient document.");
    }
  } finally {
    await client.close();
  }
}

async function mongooseCrud(uri) {
  const connection = await mongoose.createConnection(uri).asPromise();
  try {
    const schema = new mongoose.Schema(
      {
        _id: String,
        value: String,
      },
      { versionKey: false }
    );
    const Model = connection.model("MongooseCrudSmoke", schema, "mongoose_crud");
    await Model.deleteMany({});
    await Model.create({ _id: "mongoose-1", value: "ok" });
    const found = await Model.findById("mongoose-1").lean().exec();
    if (found?.value !== "ok") {
      throw new Error("Mongoose CRUD value mismatch.");
    }
  } finally {
    await connection.close();
  }
}

async function mongooseTransactionRollback(uri) {
  const connection = await mongoose.createConnection(uri).asPromise();
  try {
    const schema = new mongoose.Schema(
      {
        _id: String,
        value: String,
      },
      { versionKey: false }
    );
    const Model = connection.model("MongooseTxSmoke", schema, "mongoose_tx");
    await Model.deleteMany({});

    const session = await connection.startSession();
    try {
      session.startTransaction();
      await Model.create([{ _id: "mongoose-tx-1", value: "rolled-back" }], {
        session,
      });
      await session.abortTransaction();
    } finally {
      await session.endSession();
    }

    const found = await Model.findById("mongoose-tx-1").lean().exec();
    if (found !== null) {
      throw new Error("Mongoose transaction rollback did not rollback.");
    }
  } finally {
    await connection.close();
  }
}

async function mongooseTransactionCommit(uri) {
  const connection = await mongoose.createConnection(uri).asPromise();
  try {
    const schema = new mongoose.Schema(
      {
        _id: String,
        value: String,
      },
      { versionKey: false }
    );
    const Model = connection.model(
      "MongooseTxCommitSmoke",
      schema,
      "mongoose_tx_commit"
    );
    await Model.deleteMany({});

    const session = await connection.startSession();
    try {
      session.startTransaction();
      await Model.create([{ _id: "mongoose-tx-commit-1", value: "committed" }], {
        session,
      });
      await session.commitTransaction();
    } finally {
      await session.endSession();
    }

    const found = await Model.findById("mongoose-tx-commit-1").lean().exec();
    if (found?.value !== "committed") {
      throw new Error("Mongoose transaction commit did not persist document.");
    }
  } finally {
    await connection.close();
  }
}

async function runScenario(name, fn) {
  const startedAtMs = performance.now();
  try {
    await fn();
    return {
      name,
      status: "pass",
      durationMs: durationMs(startedAtMs),
    };
  } catch (error) {
    return {
      name,
      status: "fail",
      durationMs: durationMs(startedAtMs),
      reasonCategory: classifyError(error),
      error: formatError(error),
    };
  }
}

function durationMs(startedAtMs) {
  return Math.round((performance.now() - startedAtMs) * 100) / 100;
}

function classifyError(error) {
  const message = String(error instanceof Error ? error.message : error);
  if (
    message.includes("UnsupportedFeature") ||
    message.includes("NotImplemented")
  ) {
    return "unsupported";
  }
  if (message.includes("Transaction")) {
    return "transaction";
  }
  return "runtime";
}

function formatError(error) {
  if (error instanceof Error) {
    return {
      name: error.name,
      message: error.message,
    };
  }
  return {
    name: "Error",
    message: String(error),
  };
}

function parseArg(name) {
  const argv = process.argv.slice(2);
  for (let index = 0; index < argv.length; index += 1) {
    const current = argv[index];
    if (current === name && index + 1 < argv.length) {
      return argv[index + 1];
    }
    if (current.startsWith(`${name}=`)) {
      return current.slice(name.length + 1);
    }
  }
  return null;
}

async function safeMongooseDisconnect() {
  try {
    await mongoose.disconnect();
  } catch (_) {
    // no-op
  }
}
