package org.jongodb.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.MongoCommandException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.Document;
import org.junit.jupiter.api.Test;

class WriteConflictRetryTest {
    @Test
    void secondSessionUpdateCommitReturnsWriteConflictWithTransientLabel() {
        try (TcpMongoServer server = TcpMongoServer.inMemoryReplicaSet("127.0.0.1", 0, "rs-write");
                MongoClient client = open(server)) {
            final MongoCollection<Document> users = client.getDatabase("app").getCollection("users");
            users.insertOne(new Document("_id", 1).append("n", 0));

            try (ClientSession first = client.startSession(); ClientSession second = client.startSession()) {
                first.startTransaction();
                second.startTransaction();
                users.updateOne(first, new Document("_id", 1), new Document("$set", new Document("n", 1)));
                users.updateOne(second, new Document("_id", 1), new Document("$set", new Document("n", 2)));
                first.commitTransaction();
                final MongoCommandException conflict =
                        assertThrows(MongoCommandException.class, second::commitTransaction);
                assertEquals(112, conflict.getErrorCode());
                assertEquals("WriteConflict", conflict.getErrorCodeName());
                assertTrue(conflict.hasErrorLabel("TransientTransactionError"));
            }

            assertEquals(1, users.find().first().getInteger("n"));
        }
    }

    @Test
    void transactionCommitAfterOutsideWriteReturnsWriteConflictWithTransientLabel() {
        try (TcpMongoServer server = TcpMongoServer.inMemoryReplicaSet("127.0.0.1", 0, "rs-write");
                MongoClient client = open(server)) {
            final MongoCollection<Document> users = client.getDatabase("app").getCollection("users");
            users.insertOne(new Document("_id", 1).append("n", 0));

            try (ClientSession session = client.startSession()) {
                session.startTransaction();
                users.updateOne(session, new Document("_id", 1), new Document("$inc", new Document("n", 1)));
                users.updateOne(new Document("_id", 1), new Document("$inc", new Document("n", 1)));
                final MongoCommandException conflict =
                        assertThrows(MongoCommandException.class, session::commitTransaction);
                assertEquals(112, conflict.getErrorCode());
                assertTrue(conflict.hasErrorLabel("TransientTransactionError"));
            }

            assertEquals(1, users.find().first().getInteger("n"));
        }
    }

    @Test
    void withTransactionRetriesWriteConflictAndCommits() throws Exception {
        try (TcpMongoServer server = TcpMongoServer.inMemoryReplicaSet("127.0.0.1", 0, "rs-write");
                MongoClient client = open(server)) {
            final MongoCollection<Document> users = client.getDatabase("app").getCollection("users");
            users.insertOne(new Document("_id", 1).append("n", 0));

            final AtomicInteger roles = new AtomicInteger();
            final CountDownLatch bothUpdated = new CountDownLatch(2);
            final CountDownLatch winnerCommitted = new CountDownLatch(1);
            final ExecutorService pool = Executors.newFixedThreadPool(2);
            try {
                final List<Future<?>> futures = new ArrayList<>();
                for (int index = 0; index < 2; index++) {
                    futures.add(pool.submit(() -> {
                        final int role = roles.getAndIncrement();
                        try (ClientSession session = client.startSession()) {
                            session.withTransaction(() -> {
                                users.updateOne(
                                        session, new Document("_id", 1), new Document("$inc", new Document("n", 1)));
                                bothUpdated.countDown();
                                try {
                                    if (!bothUpdated.await(10, TimeUnit.SECONDS)) {
                                        throw new IllegalStateException("peer did not update");
                                    }
                                    if (role != 0 && !winnerCommitted.await(10, TimeUnit.SECONDS)) {
                                        throw new IllegalStateException("winner did not commit");
                                    }
                                } catch (final InterruptedException interrupted) {
                                    Thread.currentThread().interrupt();
                                    throw new IllegalStateException("interrupted while waiting for the peer commit");
                                }
                                return null;
                            });
                        }
                        if (role == 0) {
                            winnerCommitted.countDown();
                        }
                        return null;
                    }));
                }
                for (final Future<?> future : futures) {
                    future.get(30, TimeUnit.SECONDS);
                }
            } finally {
                pool.shutdownNow();
            }

            assertEquals(2, users.find().first().getInteger("n"));
        }
    }

    private static MongoClient open(final TcpMongoServer server) {
        server.start();
        return MongoClients.create(server.connectionString("app"));
    }
}
