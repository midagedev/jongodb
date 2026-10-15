package org.jongodb.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.mongodb.MongoCommandException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;

class TcpMongoServerChangeStreamTest {
    @Test
    void databaseWatchDeliversFilteredChangesWithUpdateLookupAndResumes() {
        try (TcpMongoServer server = TcpMongoServer.inMemoryReplicaSet("127.0.0.1", 0, "rs0")) {
            server.start();
            try (MongoClient client = MongoClients.create(server.connectionString("app"))) {
                final MongoDatabase db = client.getDatabase("app");

                final BsonDocument startToken;
                try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = openStream(db, null)) {
                    assertNull(cursor.tryNext());
                    startToken = cursor.getResumeToken();
                    assertNotNull(startToken);
                }

                db.getCollection("Account").insertOne(new Document("_id", 1).append("name", "a"));
                db.getCollection("Other").insertOne(new Document("_id", 9));
                db.getCollection("Account").updateOne(Filters.eq("_id", 1), Updates.set("name", "b"));
                db.getCollection("AccountCredential").insertOne(new Document("_id", 2));
                db.getCollection("Account").deleteOne(Filters.eq("_id", 1));

                final List<ChangeStreamDocument<Document>> events = new ArrayList<>();
                final BsonDocument afterFirst;
                try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = openStream(db, startToken)) {
                    events.add(cursor.tryNext());
                    afterFirst = cursor.getResumeToken();
                    drain(cursor, events, 3);
                }

                assertEquals(
                        List.of(OperationType.INSERT, OperationType.UPDATE, OperationType.INSERT, OperationType.DELETE),
                        events.stream().map(ChangeStreamDocument::getOperationType).toList());
                assertEquals("Account", events.get(0).getNamespace().getCollectionName());
                assertEquals("b", events.get(1).getFullDocument().getString("name"));
                assertEquals("AccountCredential", events.get(2).getNamespace().getCollectionName());
                assertEquals(1, events.get(3).getDocumentKey().getInt32("_id").getValue());
                assertNull(events.get(3).getFullDocument());
                assertNotNull(events.get(1).getClusterTime());

                try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = openStream(db, afterFirst)) {
                    final List<ChangeStreamDocument<Document>> resumed = new ArrayList<>();
                    drain(cursor, resumed, 3);
                    assertEquals(events.subList(1, 4).stream().map(ChangeStreamDocument::getOperationType).toList(),
                            resumed.stream().map(ChangeStreamDocument::getOperationType).toList());
                }
            }
        }
    }

    @Test
    void committedTransactionEmitsItsWritesAndAbortedEmitsNothing() {
        try (TcpMongoServer server = TcpMongoServer.inMemoryReplicaSet("127.0.0.1", 0, "rs0")) {
            server.start();
            try (MongoClient client = MongoClients.create(server.connectionString("app"))) {
                final MongoDatabase db = client.getDatabase("app");
                db.getCollection("Account").insertOne(new Document("_id", 1).append("n", 0));

                try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = openStream(db, null)) {
                    assertNull(cursor.tryNext());

                    try (ClientSession session = client.startSession()) {
                        session.startTransaction();
                        db.getCollection("Account").insertOne(session, new Document("_id", 5));
                        session.abortTransaction();

                        session.startTransaction();
                        db.getCollection("Account").updateOne(session, Filters.eq("_id", 1), Updates.inc("n", 1));
                        db.getCollection("Account").insertOne(session, new Document("_id", 2));
                        session.commitTransaction();
                    }

                    final List<ChangeStreamDocument<Document>> events = new ArrayList<>();
                    drain(cursor, events, 2);
                    assertEquals(
                            List.of(OperationType.UPDATE, OperationType.INSERT),
                            events.stream().map(ChangeStreamDocument::getOperationType).toList());
                    assertEquals(1, events.get(0).getFullDocument().getInteger("n"));
                    assertNull(cursor.tryNext());
                }
            }
        }
    }

    @Test
    void resumeTokenFromAnotherServerIsRejected() {
        try (TcpMongoServer server = TcpMongoServer.inMemoryReplicaSet("127.0.0.1", 0, "rs0")) {
            server.start();
            try (MongoClient client = MongoClients.create(server.connectionString("app"))) {
                final BsonDocument foreignToken = BsonDocument.parse("{_data: '00000000000000FF'}");
                final MongoCommandException error = assertThrows(
                        MongoCommandException.class,
                        () -> openStream(client.getDatabase("app"), foreignToken).tryNext());
                assertEquals(286, error.getErrorCode());
            }
        }
    }

    // Same shape as a database-wide consumer: $match on ns.coll, updateLookup, short await, optional resume.
    private static MongoChangeStreamCursor<ChangeStreamDocument<Document>> openStream(
            final MongoDatabase db, final BsonDocument resumeAfter) {
        final var stream = db.watch(List.of(Aggregates.match(Filters.in("ns.coll", "Account", "AccountCredential"))))
                .fullDocument(FullDocument.UPDATE_LOOKUP)
                .maxAwaitTime(100, TimeUnit.MILLISECONDS);
        if (resumeAfter != null) {
            stream.resumeAfter(resumeAfter);
        }
        return stream.cursor();
    }

    private static void drain(
            final MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor,
            final List<ChangeStreamDocument<Document>> into,
            final int count) {
        final int target = into.size() + count;
        for (int attempt = 0; attempt < 50 && into.size() < target; attempt++) {
            final ChangeStreamDocument<Document> event = cursor.tryNext();
            if (event != null) {
                into.add(event);
            }
        }
    }
}
