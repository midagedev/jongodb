package org.jongodb.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertManyOptions;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Test;

class InsertWriteErrorTest {
    @Test
    void insertOneDuplicateRaisesMongoWriteException() {
        try (TcpMongoServer server = TcpMongoServer.inMemory()) {
            server.start();
            try (MongoClient client = MongoClients.create(server.connectionString("app"))) {
                final MongoCollection<Document> items = client.getDatabase("app").getCollection("items");
                items.insertOne(new Document("_id", 1));
                final MongoWriteException duplicate = assertThrows(
                        MongoWriteException.class, () -> items.insertOne(new Document("_id", 1).append("n", 2)));
                assertEquals(11000, duplicate.getCode());
                assertEquals(1, items.countDocuments());
            }
        }
    }

    @Test
    void unorderedInsertManyRaisesMongoBulkWriteExceptionAndKeepsOtherDocuments() {
        try (TcpMongoServer server = TcpMongoServer.inMemory()) {
            server.start();
            try (MongoClient client = MongoClients.create(server.connectionString("app"))) {
                final MongoCollection<Document> items = client.getDatabase("app").getCollection("items");
                items.insertOne(new Document("_id", 2));
                final List<Document> batch = List.of(
                        new Document("_id", 4), new Document("_id", 2), new Document("_id", 5));
                final MongoBulkWriteException duplicate = assertThrows(
                        MongoBulkWriteException.class,
                        () -> items.insertMany(batch, new InsertManyOptions().ordered(false)));
                assertEquals(11000, duplicate.getWriteErrors().get(0).getCode());
                assertEquals(1, duplicate.getWriteErrors().get(0).getIndex());
                assertEquals(2, duplicate.getWriteResult().getInsertedCount());
                assertEquals(List.of(2, 4, 5), ids(items));
            }
        }
    }

    @Test
    void orderedInsertManyRaisesMongoBulkWriteExceptionAndStops() {
        try (TcpMongoServer server = TcpMongoServer.inMemory()) {
            server.start();
            try (MongoClient client = MongoClients.create(server.connectionString("app"))) {
                final MongoCollection<Document> items = client.getDatabase("app").getCollection("items");
                items.insertOne(new Document("_id", 2));
                final List<Document> batch =
                        List.of(new Document("_id", 6), new Document("_id", 2), new Document("_id", 7));
                final MongoBulkWriteException duplicate =
                        assertThrows(MongoBulkWriteException.class, () -> items.insertMany(batch));
                assertEquals(11000, duplicate.getWriteErrors().get(0).getCode());
                assertEquals(1, duplicate.getWriteErrors().get(0).getIndex());
                assertEquals(1, duplicate.getWriteResult().getInsertedCount());
                assertEquals(List.of(2, 6), ids(items));
            }
        }
    }

    private static List<Integer> ids(final MongoCollection<Document> items) {
        final List<Integer> ids = new ArrayList<>();
        for (final Document document : items.find().sort(new Document("_id", 1))) {
            ids.add(document.getInteger("_id"));
        }
        return ids;
    }
}
