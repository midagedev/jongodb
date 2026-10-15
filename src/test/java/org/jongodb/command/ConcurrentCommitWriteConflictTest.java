package org.jongodb.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.jongodb.engine.InMemoryEngineStore;
import org.junit.jupiter.api.Test;

class ConcurrentCommitWriteConflictTest {
    private static final int ROUNDS = 48;

    @Test
    void overlappingCommitsOfTheSameDocumentLetExactlyOneSucceed() throws Exception {
        final EngineBackedCommandStore store = new EngineBackedCommandStore(new InMemoryEngineStore());
        final CommandDispatcher dispatcher = new CommandDispatcher(store);
        final ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int round = 0; round < ROUNDS; round++) {
                final int id = round + 1;
                assertOk(dispatcher.dispatch(BsonDocument.parse(
                        "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":" + id + ",\"n\":0}]}")));
                final String sessionA = "session-a-" + round;
                final String sessionB = "session-b-" + round;
                assertOk(dispatcher.dispatch(transactionalUpdate(sessionA, id, 1)));
                assertOk(dispatcher.dispatch(transactionalUpdate(sessionB, id, 2)));

                final CountDownLatch ready = new CountDownLatch(2);
                final CountDownLatch start = new CountDownLatch(1);
                final List<Future<BsonDocument>> commits = new ArrayList<>(2);
                for (final String session : List.of(sessionA, sessionB)) {
                    commits.add(pool.submit(() -> commitAfter(dispatcher, session, ready, start)));
                }
                assertTrue(ready.await(10, TimeUnit.SECONDS), "committers did not arrive");
                start.countDown();

                int succeeded = 0;
                int conflicts = 0;
                for (final Future<BsonDocument> commit : commits) {
                    final BsonDocument response = commit.get(10, TimeUnit.SECONDS);
                    if (response.get("ok").asNumber().doubleValue() == 1.0) {
                        succeeded++;
                        continue;
                    }
                    assertEquals(112, response.getInt32("code").getValue(), response.toJson());
                    assertEquals("WriteConflict", response.getString("codeName").getValue());
                    conflicts++;
                }
                assertEquals(1, succeeded, "round " + round);
                assertEquals(1, conflicts, "round " + round);

                final List<BsonDocument> found =
                        store.find("app", "users", new BsonDocument("_id", new BsonInt32(id)));
                assertEquals(1, found.size());
                final int stored = found.get(0).getInt32("n").getValue();
                assertTrue(stored == 1 || stored == 2, "round " + round + " stored n=" + stored);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    private static BsonDocument commitAfter(
            final CommandDispatcher dispatcher,
            final String session,
            final CountDownLatch ready,
            final CountDownLatch start)
            throws Exception {
        ready.countDown();
        if (!start.await(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("commit did not start");
        }
        return dispatcher.dispatch(BsonDocument.parse(
                "{\"commitTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\""
                        + session
                        + "\"},\"txnNumber\":1,\"autocommit\":false}"));
    }

    private static BsonDocument transactionalUpdate(final String session, final int id, final int value) {
        return BsonDocument.parse(
                "{\"update\":\"users\",\"$db\":\"app\",\"updates\":[{\"q\":{\"_id\":"
                        + id
                        + "},\"u\":{\"$set\":{\"n\":"
                        + value
                        + "}}}],\"lsid\":{\"id\":\""
                        + session
                        + "\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}");
    }

    private static void assertOk(final BsonDocument response) {
        assertEquals(1.0, response.get("ok").asNumber().doubleValue(), response.toJson());
    }
}
