package org.jongodb.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.bson.BsonDocument;
import org.jongodb.engine.InMemoryEngineStore;
import org.junit.jupiter.api.Test;

class EngineBackedCommandStoreTransactionSnapshotTest {
    @Test
    void snapshotForTransactionDefersWritableCloneUntilFirstWrite() {
        final EngineBackedCommandStore globalStore = new EngineBackedCommandStore(new InMemoryEngineStore());
        final CommandStore transactionStore = globalStore.snapshotForTransaction();

        final EngineBackedCommandStore.CopyOnWriteTransactionCommandStore copyOnWriteSnapshot =
                assertInstanceOf(EngineBackedCommandStore.CopyOnWriteTransactionCommandStore.class, transactionStore);

        assertFalse(copyOnWriteSnapshot.materializedWriteSnapshot());

        transactionStore.find("app", "users", new BsonDocument());
        transactionStore.aggregate("app", "users", List.of(BsonDocument.parse("{\"$match\":{}}")));
        assertFalse(copyOnWriteSnapshot.materializedWriteSnapshot());

        transactionStore.insert("app", "users", List.of(BsonDocument.parse("{\"_id\":1,\"name\":\"tx\"}")));
        assertTrue(copyOnWriteSnapshot.materializedWriteSnapshot());
    }

    @Test
    void publishReadOnlyCopyOnWriteSnapshotPreservesExistingGlobalState() {
        final EngineBackedCommandStore globalStore = new EngineBackedCommandStore(new InMemoryEngineStore());
        globalStore.insert("app", "users", List.of(BsonDocument.parse("{\"_id\":1,\"name\":\"outside\"}")));

        final CommandStore transactionStore = globalStore.snapshotForTransaction();
        final EngineBackedCommandStore.CopyOnWriteTransactionCommandStore copyOnWriteSnapshot =
                assertInstanceOf(EngineBackedCommandStore.CopyOnWriteTransactionCommandStore.class, transactionStore);

        assertEquals(
                1,
                transactionStore.find("app", "users", BsonDocument.parse("{\"_id\":1}")).size());
        assertFalse(copyOnWriteSnapshot.materializedWriteSnapshot());

        globalStore.publishTransactionSnapshot(transactionStore);
        final List<BsonDocument> globalUsers = globalStore.find("app", "users", new BsonDocument());

        assertEquals(1, globalUsers.size());
        assertEquals("outside", globalUsers.get(0).getString("name").getValue());
    }

    @Test
    void publishCopyOnWriteSnapshotAppliesTransactionWrites() {
        final EngineBackedCommandStore globalStore = new EngineBackedCommandStore(new InMemoryEngineStore());
        final CommandStore transactionStore = globalStore.snapshotForTransaction();

        final EngineBackedCommandStore.CopyOnWriteTransactionCommandStore copyOnWriteSnapshot =
                assertInstanceOf(EngineBackedCommandStore.CopyOnWriteTransactionCommandStore.class, transactionStore);

        transactionStore.insert("app", "users", List.of(BsonDocument.parse("{\"_id\":11,\"name\":\"committed\"}")));
        assertTrue(copyOnWriteSnapshot.materializedWriteSnapshot());

        globalStore.publishTransactionSnapshot(transactionStore);
        final List<BsonDocument> users = globalStore.find("app", "users", new BsonDocument());

        assertEquals(1, users.size());
        assertEquals("committed", users.get(0).getString("name").getValue());
    }
}
