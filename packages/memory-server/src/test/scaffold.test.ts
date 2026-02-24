import { test } from "node:test";
import assert from "node:assert/strict";

import { startJongodbMemoryServer } from "../index.js";

test("startJongodbMemoryServer is scaffolded and explicitly not implemented", async () => {
  await assert.rejects(async () => {
    await startJongodbMemoryServer();
  }, /not implemented/i);
});
