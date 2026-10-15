package org.jongodb.server;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.junit.jupiter.api.Test;

class FindAndModifyAtomicityTest {
    private static final int ROUNDS = 6;
    private static final int DOCUMENTS = 40;
    private static final int THREADS = 16;

    @FunctionalInterface
    private interface Claimer {
        List<Integer> claim(MongoDatabase database, MongoCollection<Document> items, int threadId);
    }

    @Test
    void findOneAndUpdateClaimsEachReadyDocumentOnceAcrossThreads() throws Exception {
        claimThroughServer((database, items, threadId) -> {
            final List<Integer> claimed = new ArrayList<>();
            while (true) {
                final Document document = items.findOneAndUpdate(
                        new Document("state", "ready"),
                        new Document("$set", new Document("state", "taken").append("by", threadId)));
                if (document == null) {
                    return claimed;
                }
                claimed.add(document.getInteger("_id"));
            }
        });
    }

    @Test
    void findOneAndReplaceClaimsEachReadyDocumentOnceAcrossThreads() throws Exception {
        claimThroughServer((database, items, threadId) -> {
            final List<Integer> claimed = new ArrayList<>();
            while (true) {
                final Document document = items.findOneAndReplace(
                        new Document("state", "ready"),
                        new Document("state", "taken").append("by", threadId));
                if (document == null) {
                    return claimed;
                }
                claimed.add(document.getInteger("_id"));
            }
        });
    }

    @Test
    void findAndModifyClaimsEachReadyDocumentOnceAcrossThreads() throws Exception {
        claimThroughServer((database, items, threadId) -> {
            final List<Integer> claimed = new ArrayList<>();
            while (true) {
                final Document response = database.runCommand(new Document("findAndModify", "items")
                        .append("query", new Document("state", "ready"))
                        .append("update", new Document("$set", new Document("state", "taken").append("by", threadId))));
                final Object value = response.get("value");
                if (value == null) {
                    return claimed;
                }
                claimed.add(((Document) value).getInteger("_id"));
            }
        });
    }

    private static void claimThroughServer(final Claimer claimer) throws Exception {
        try (TcpMongoServer server = TcpMongoServer.inMemory()) {
            server.start();
            try (MongoClient client = MongoClients.create(server.connectionString("app"))) {
                final MongoDatabase database = client.getDatabase("app");
                final MongoCollection<Document> items = database.getCollection("items");
                for (int round = 0; round < ROUNDS; round++) {
                    items.drop();
                    final List<Document> seed = new ArrayList<>(DOCUMENTS);
                    for (int id = 0; id < DOCUMENTS; id++) {
                        seed.add(new Document("_id", id).append("state", "ready"));
                    }
                    items.insertMany(seed);
                    assertEachDocumentClaimedOnce(database, items, claimer);
                }
            }
        }
    }

    private static void assertEachDocumentClaimedOnce(
            final MongoDatabase database, final MongoCollection<Document> items, final Claimer claimer)
            throws Exception {
        final ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        try {
            final List<Future<List<Integer>>> futures = new ArrayList<>(THREADS);
            for (int threadId = 0; threadId < THREADS; threadId++) {
                final int claimedBy = threadId;
                futures.add(pool.submit(() -> claimer.claim(database, items, claimedBy)));
            }
            final List<Integer> claimed = new ArrayList<>();
            for (final Future<List<Integer>> future : futures) {
                claimed.addAll(future.get(30, TimeUnit.SECONDS));
            }
            assertEquals(DOCUMENTS, claimed.size());
            assertEquals(DOCUMENTS, new HashSet<>(claimed).size());
        } finally {
            pool.shutdownNow();
        }

        final List<Document> remaining = items.find().into(new ArrayList<>());
        assertEquals(DOCUMENTS, remaining.size());
        final Set<Integer> ids = new HashSet<>();
        for (final Document document : remaining) {
            assertEquals("taken", document.getString("state"));
            ids.add(document.getInteger("_id"));
        }
        assertEquals(DOCUMENTS, ids.size());
    }
}
