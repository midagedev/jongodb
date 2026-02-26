import { createServer } from "node:http";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { performance } from "node:perf_hooks";
import process from "node:process";

import "reflect-metadata";
import express from "express";
import Koa from "koa";
import { MongoClient } from "mongodb";
import mongoose from "mongoose";
import { DataSource, EntitySchema } from "typeorm";
import { startJongodbMemoryServer } from "../../../packages/memory-server/dist/esm/index.js";

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
    await runScenario("express.mongodb.route", () => expressMongoRoute(uri))
  );
  scenarioResults.push(
    await runScenario("koa.mongodb.route", () => koaMongoRoute(uri))
  );
  scenarioResults.push(
    await runScenario("typeorm.mongodb.repository", () => typeormMongoRepository(uri))
  );
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
    await runScenario("mongoose.session.with-transaction", () =>
      mongooseSessionWithTransaction(uri)
    )
  );
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
  compatibilityTarget: "node-driver-express-koa-typeorm-and-mongoose-smoke",
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

async function expressMongoRoute(uri) {
  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db("node_smoke");
  await db.collection("express_users").deleteMany({});

  const app = express();
  app.use(express.json());

  app.post("/users", async (request, response) => {
    const id = String(request.body?.id ?? "").trim();
    const name = String(request.body?.name ?? "").trim();
    if (id.length === 0 || name.length === 0) {
      response.status(400).json({ error: "id and name are required" });
      return;
    }
    await db.collection("express_users").insertOne({ _id: id, name });
    response.status(201).json({ ok: 1, id });
  });

  app.get("/users/:id", async (request, response) => {
    const found = await db.collection("express_users").findOne({ _id: request.params.id });
    if (found === null) {
      response.status(404).json({ error: "not found" });
      return;
    }
    response.json(found);
  });

  const httpServer = createServer(app);
  const port = await listenOnRandomPort(httpServer);
  const baseUrl = `http://127.0.0.1:${port}`;

  try {
    const createResponse = await fetch(`${baseUrl}/users`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ id: "express-1", name: "neo" }),
    });
    if (createResponse.status !== 201) {
      throw new Error(`Express create route failed with status ${createResponse.status}.`);
    }

    const readResponse = await fetch(`${baseUrl}/users/express-1`);
    if (readResponse.status !== 200) {
      throw new Error(`Express read route failed with status ${readResponse.status}.`);
    }
    const payload = await readResponse.json();
    if (payload?.name !== "neo") {
      throw new Error("Express route returned unexpected payload.");
    }
  } finally {
    await closeHttpServer(httpServer);
    await client.close();
  }
}

async function koaMongoRoute(uri) {
  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db("node_smoke");
  await db.collection("koa_users").deleteMany({});

  const app = new Koa();
  app.use(async (ctx) => {
    const match = /^\/users\/([^/]+)$/.exec(ctx.path);
    if (match === null) {
      ctx.status = 404;
      ctx.body = { error: "not found" };
      return;
    }

    const id = decodeURIComponent(match[1]);
    if (ctx.method === "PUT") {
      const name = String(ctx.query.name ?? "").trim();
      if (name.length === 0) {
        ctx.status = 400;
        ctx.body = { error: "name query is required" };
        return;
      }

      await db.collection("koa_users").updateOne(
        { _id: id },
        {
          $set: { name },
        },
        { upsert: true }
      );

      ctx.status = 200;
      ctx.body = { ok: 1, id };
      return;
    }

    if (ctx.method === "GET") {
      const found = await db.collection("koa_users").findOne({ _id: id });
      if (found === null) {
        ctx.status = 404;
        ctx.body = { error: "not found" };
        return;
      }
      ctx.body = found;
      return;
    }

    ctx.status = 405;
    ctx.body = { error: "method not allowed" };
  });

  const httpServer = createServer(app.callback());
  const port = await listenOnRandomPort(httpServer);
  const baseUrl = `http://127.0.0.1:${port}`;

  try {
    const createResponse = await fetch(`${baseUrl}/users/koa-1?name=atlas`, {
      method: "PUT",
    });
    if (createResponse.status !== 200) {
      throw new Error(`Koa upsert route failed with status ${createResponse.status}.`);
    }

    const readResponse = await fetch(`${baseUrl}/users/koa-1`);
    if (readResponse.status !== 200) {
      throw new Error(`Koa read route failed with status ${readResponse.status}.`);
    }
    const payload = await readResponse.json();
    if (payload?.name !== "atlas") {
      throw new Error("Koa route returned unexpected payload.");
    }
  } finally {
    await closeHttpServer(httpServer);
    await client.close();
  }
}

async function typeormMongoRepository(uri) {
  const UserEntity = new EntitySchema({
    name: "TypeormSmokeUser",
    columns: {
      _id: {
        type: "objectId",
        objectId: true,
        primary: true,
        generated: true,
      },
      name: {
        type: String,
      },
    },
  });

  const dataSource = new DataSource({
    type: "mongodb",
    url: uri,
    database: "node_smoke",
    entities: [UserEntity],
    synchronize: true,
    logging: false,
  });

  await dataSource.initialize();
  try {
    const repository = dataSource.getMongoRepository("TypeormSmokeUser");
    await repository.deleteMany({});
    const insertResult = await repository.insertOne({ name: "trinity" });
    if (insertResult.acknowledged !== true) {
      throw new Error("TypeORM repository insert did not acknowledge write.");
    }

    const found = await repository.findOneBy({ name: "trinity" });

    if (found?.name !== "trinity") {
      throw new Error("TypeORM repository CRUD value mismatch.");
    }
  } finally {
    await dataSource.destroy();
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

async function mongooseSessionWithTransaction(uri) {
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
      "MongooseSessionSmoke",
      schema,
      "mongoose_session_smoke"
    );
    await Model.deleteMany({});

    const session = await connection.startSession();
    try {
      await session.withTransaction(async () => {
        await Model.create([{ _id: "mongoose-session-1", value: "pending" }], {
          session,
        });

        const inSession = await Model.findById("mongoose-session-1")
          .session(session)
          .lean()
          .exec();
        if (inSession?.value !== "pending") {
          throw new Error("Mongoose session read in active transaction failed.");
        }

        const outsideSession = await Model.findById("mongoose-session-1").lean().exec();
        if (outsideSession !== null) {
          throw new Error(
            "Mongoose session leaked uncommitted document outside transaction."
          );
        }
      });
    } finally {
      await session.endSession();
    }

    const committed = await Model.findById("mongoose-session-1").lean().exec();
    if (committed?.value !== "pending") {
      throw new Error("Mongoose session transaction did not commit document.");
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

async function listenOnRandomPort(httpServer) {
  return await new Promise((resolve, reject) => {
    httpServer.once("error", reject);
    httpServer.listen(0, "127.0.0.1", () => {
      const address = httpServer.address();
      if (address === null || typeof address === "string") {
        reject(new Error("Failed to allocate local port for HTTP smoke server."));
        return;
      }
      resolve(address.port);
    });
  });
}

async function closeHttpServer(httpServer) {
  return await new Promise((resolve, reject) => {
    httpServer.close((error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });
}
