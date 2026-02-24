package org.jongodb.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.jongodb.engine.InMemoryEngineStore;
import org.junit.jupiter.api.Test;

class CommandDispatcherE2ETest {
    @Test
    void helloCommandReturnsHandshakeShape() {
        final RecordingStore store = new RecordingStore();
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse("{\"hello\": 1, \"$db\": \"admin\"}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        assertEquals(true, response.getBoolean("isWritablePrimary").getValue());
        assertEquals(17, response.getInt32("maxWireVersion").getValue());
        assertEquals(30, response.getInt32("logicalSessionTimeoutMinutes").getValue());
    }

    @Test
    void pingCommandReturnsOk() {
        final RecordingStore store = new RecordingStore();
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse("{\"ping\": 1, \"$db\": \"admin\"}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
    }

    @Test
    void isMasterAliasReturnsHandshakeShape() {
        final RecordingStore store = new RecordingStore();
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse("{\"isMaster\": 1, \"$db\": \"admin\"}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        assertTrue(response.getBoolean("ismaster").getValue());
        assertTrue(response.getBoolean("isWritablePrimary").getValue());
    }

    @Test
    void buildInfoCommandReturnsCompatibilityFields() {
        final RecordingStore store = new RecordingStore();
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse("{\"buildInfo\": 1, \"$db\": \"admin\"}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        assertEquals("8.0.0-jongodb", response.getString("version").getValue());
        assertEquals(4, response.getArray("versionArray").size());
        assertEquals(16 * 1024 * 1024, response.getInt32("maxBsonObjectSize").getValue());
    }

    @Test
    void getParameterCommandReturnsRequestedSupportedFields() {
        final RecordingStore store = new RecordingStore();
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"getParameter\": 1, \"$db\": \"admin\", \"featureCompatibilityVersion\": 1, \"transactionLifetimeLimitSeconds\": 1}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        assertEquals(
                "8.0",
                response.getDocument("featureCompatibilityVersion")
                        .getString("version")
                        .getValue());
        assertEquals(60, response.getInt32("transactionLifetimeLimitSeconds").getValue());
    }

    @Test
    void getParameterCommandRejectsUnsupportedOption() {
        final RecordingStore store = new RecordingStore();
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument response = dispatcher.dispatch(
                BsonDocument.parse("{\"getParameter\": 1, \"$db\": \"admin\", \"unknownParameter\": 1}"));

        assertCommandError(response, "BadValue");
    }

    @Test
    void insertCommandCallsStoreAndReturnsCount() {
        final RecordingStore store = new RecordingStore();
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"a\"},{\"_id\":2,\"name\":\"b\"}]}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        assertEquals(2, response.getInt32("n").getValue());
        assertEquals("app", store.lastInsertDatabase);
        assertEquals("users", store.lastInsertCollection);
        assertEquals(2, store.lastInsertedDocuments.size());
        assertEquals("a", store.lastInsertedDocuments.get(0).getString("name").getValue());
    }

    @Test
    void findCommandCallsStoreAndReturnsCursorBatch() {
        final RecordingStore store = new RecordingStore();
        store.findResult = List.of(BsonDocument.parse("{\"_id\":1,\"name\":\"a\"}"));
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument response =
                dispatcher.dispatch(BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{\"name\":\"a\"}}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        final BsonDocument cursor = response.getDocument("cursor");
        assertEquals(Set.of("id", "ns", "firstBatch"), cursor.keySet());
        assertEquals(0L, cursor.getInt64("id").getValue());
        assertEquals("app.users", cursor.getString("ns").getValue());

        final BsonArray firstBatch = cursor.getArray("firstBatch");
        assertEquals(1, firstBatch.size());
        assertEquals("a", firstBatch.get(0).asDocument().getString("name").getValue());

        assertEquals("app", store.lastFindDatabase);
        assertEquals("users", store.lastFindCollection);
        assertNotNull(store.lastFindFilter);
        assertEquals("a", store.lastFindFilter.getString("name").getValue());
    }

    @Test
    void countDocumentsCommandCallsStoreAndReturnsCountShape() {
        final RecordingStore store = new RecordingStore();
        store.findResult = List.of(
                BsonDocument.parse("{\"_id\":1}"),
                BsonDocument.parse("{\"_id\":2}"),
                BsonDocument.parse("{\"_id\":3}"));
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"countDocuments\":\"users\",\"$db\":\"app\",\"filter\":{\"role\":\"member\"}}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        assertEquals(3L, response.getInt64("n").getValue());
        assertEquals(3L, response.getInt64("count").getValue());
        assertEquals("app", store.lastFindDatabase);
        assertEquals("users", store.lastFindCollection);
        assertEquals(
                "member",
                store.lastFindFilter.getString("role").getValue());
    }

    @Test
    void countDocumentsCommandSupportsSkipAndLimit() {
        final RecordingStore store = new RecordingStore();
        store.findResult = List.of(
                BsonDocument.parse("{\"_id\":1}"),
                BsonDocument.parse("{\"_id\":2}"),
                BsonDocument.parse("{\"_id\":3}"),
                BsonDocument.parse("{\"_id\":4}"));
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"countDocuments\":\"users\",\"$db\":\"app\",\"filter\":{},\"skip\":1,\"limit\":2}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        assertEquals(2L, response.getInt64("n").getValue());
    }

    @Test
    void countDocumentsCommandAcceptsLegacyQueryAlias() {
        final RecordingStore store = new RecordingStore();
        store.findResult = List.of(BsonDocument.parse("{\"_id\":1}"));
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"countDocuments\":\"users\",\"$db\":\"app\",\"query\":{\"role\":\"member\"}}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        assertEquals(1L, response.getInt64("n").getValue());
        assertEquals("member", store.lastFindFilter.getString("role").getValue());
    }

    @Test
    void countDocumentsCommandRejectsInvalidPayloadShapes() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new RecordingStore());

        final BsonDocument filterTypeMismatch = dispatcher.dispatch(BsonDocument.parse(
                "{\"countDocuments\":\"users\",\"filter\":1}"));
        assertCommandError(filterTypeMismatch, "TypeMismatch");

        final BsonDocument skipTypeMismatch = dispatcher.dispatch(BsonDocument.parse(
                "{\"countDocuments\":\"users\",\"filter\":{},\"skip\":1.5}"));
        assertCommandError(skipTypeMismatch, "TypeMismatch");

        final BsonDocument limitBadValue = dispatcher.dispatch(BsonDocument.parse(
                "{\"countDocuments\":\"users\",\"filter\":{},\"limit\":-1}"));
        assertCommandError(limitBadValue, "BadValue");
    }

    @Test
    void unsupportedQueryOperatorsReturnNotImplementedAcrossCrudCommands() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument insertResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"alice\"}]}"));
        assertEquals(1.0, insertResponse.get("ok").asNumber().doubleValue());

        final BsonDocument findResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"find\":\"users\",\"$db\":\"app\",\"filter\":{\"name\":{\"$foo\":1}}}"));
        assertCommandError(findResponse, 238, "NotImplemented");
        assertEquals(
                "UnsupportedFeature",
                findResponse.getArray("errorLabels").get(0).asString().getValue());

        final BsonDocument updateResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"update\":\"users\",\"$db\":\"app\",\"updates\":[{\"q\":{\"name\":{\"$foo\":1}},\"u\":{\"$set\":{\"active\":true}}}]}"));
        assertCommandError(updateResponse, 238, "NotImplemented");

        final BsonDocument deleteResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"delete\":\"users\",\"$db\":\"app\",\"deletes\":[{\"q\":{\"name\":{\"$foo\":1}},\"limit\":0}]}"));
        assertCommandError(deleteResponse, 238, "NotImplemented");

        final BsonDocument countDocumentsResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"countDocuments\":\"users\",\"$db\":\"app\",\"filter\":{\"name\":{\"$foo\":1}}}"));
        assertCommandError(countDocumentsResponse, 238, "NotImplemented");

        final BsonDocument replaceOneResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"replaceOne\":\"users\",\"$db\":\"app\",\"filter\":{\"name\":{\"$foo\":1}},\"replacement\":{\"name\":\"neo\"}}"));
        assertCommandError(replaceOneResponse, 238, "NotImplemented");

        final BsonDocument findOneAndUpdateResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"findOneAndUpdate\":\"users\",\"$db\":\"app\",\"filter\":{\"name\":{\"$foo\":1}},\"update\":{\"$set\":{\"active\":true}}}"));
        assertCommandError(findOneAndUpdateResponse, 238, "NotImplemented");

        final BsonDocument findOneAndReplaceResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"findOneAndReplace\":\"users\",\"$db\":\"app\",\"filter\":{\"name\":{\"$foo\":1}},\"replacement\":{\"name\":\"neo\"}}"));
        assertCommandError(findOneAndReplaceResponse, 238, "NotImplemented");
    }

    @Test
    void findGetMoreSupportsMultiBatchAndCursorConsumptionCleanup() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument insertResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"a\"},{\"_id\":2,\"name\":\"b\"},{\"_id\":3,\"name\":\"c\"}]}"));
        assertEquals(1.0, insertResponse.get("ok").asNumber().doubleValue());

        final BsonDocument findResponse = dispatcher.dispatch(
                BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{},\"batchSize\":2}"));
        assertEquals(1.0, findResponse.get("ok").asNumber().doubleValue());

        final BsonDocument findCursor = findResponse.getDocument("cursor");
        final long cursorId = findCursor.getInt64("id").getValue();
        assertTrue(cursorId > 0);
        assertEquals(2, findCursor.getArray("firstBatch").size());

        final BsonDocument getMoreResponse = dispatcher.dispatch(new BsonDocument()
                .append("getMore", new BsonInt64(cursorId))
                .append("collection", new BsonString("users"))
                .append("$db", new BsonString("app"))
                .append("batchSize", new BsonInt32(2)));
        assertEquals(1.0, getMoreResponse.get("ok").asNumber().doubleValue());

        final BsonDocument getMoreCursor = getMoreResponse.getDocument("cursor");
        assertEquals(0L, getMoreCursor.getInt64("id").getValue());
        assertEquals(1, getMoreCursor.getArray("nextBatch").size());
        assertEquals(
                "c",
                getMoreCursor
                        .getArray("nextBatch")
                        .get(0)
                        .asDocument()
                        .getString("name")
                        .getValue());

        final BsonDocument exhaustedResponse = dispatcher.dispatch(new BsonDocument()
                .append("getMore", new BsonInt64(cursorId))
                .append("collection", new BsonString("users"))
                .append("$db", new BsonString("app")));
        assertCursorNotFoundError(exhaustedResponse, cursorId);
    }

    @Test
    void killCursorsRemovesCursorAndSubsequentGetMoreFails() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument insertResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"a\"},{\"_id\":2,\"name\":\"b\"},{\"_id\":3,\"name\":\"c\"}]}"));
        assertEquals(1.0, insertResponse.get("ok").asNumber().doubleValue());

        final BsonDocument findResponse = dispatcher.dispatch(
                BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{},\"batchSize\":1}"));
        final long cursorId = findResponse.getDocument("cursor").getInt64("id").getValue();
        assertTrue(cursorId > 0);

        final BsonDocument killResponse = dispatcher.dispatch(new BsonDocument()
                .append("killCursors", new BsonString("users"))
                .append("$db", new BsonString("app"))
                .append(
                        "cursors",
                        new BsonArray(List.of(new BsonInt64(cursorId)))));
        assertEquals(1.0, killResponse.get("ok").asNumber().doubleValue());
        assertEquals(1, killResponse.getArray("cursorsKilled").size());
        assertEquals(cursorId, killResponse.getArray("cursorsKilled").get(0).asInt64().getValue());
        assertEquals(0, killResponse.getArray("cursorsNotFound").size());

        final BsonDocument getMoreResponse = dispatcher.dispatch(new BsonDocument()
                .append("getMore", new BsonInt64(cursorId))
                .append("collection", new BsonString("users"))
                .append("$db", new BsonString("app")));
        assertCursorNotFoundError(getMoreResponse, cursorId);

        final BsonDocument secondKillResponse = dispatcher.dispatch(new BsonDocument()
                .append("killCursors", new BsonString("users"))
                .append("$db", new BsonString("app"))
                .append(
                        "cursors",
                        new BsonArray(List.of(new BsonInt64(cursorId)))));
        assertEquals(1, secondKillResponse.getArray("cursorsNotFound").size());
        assertEquals(
                cursorId,
                secondKillResponse.getArray("cursorsNotFound").get(0).asInt64().getValue());
    }

    @Test
    void createIndexesCommandCallsStoreAndReturnsShape() {
        final RecordingStore store = new RecordingStore();
        store.createIndexesResult = new CommandStore.CreateIndexesResult(0, 1);
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"createIndexes\":\"users\",\"$db\":\"app\",\"indexes\":[{\"name\":\"email_1\",\"key\":{\"email\":1},\"unique\":true}]}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        assertEquals(false, response.getBoolean("createdCollectionAutomatically").getValue());
        assertEquals(0, response.getInt32("numIndexesBefore").getValue());
        assertEquals(1, response.getInt32("numIndexesAfter").getValue());
        assertEquals("app", store.lastCreateIndexesDatabase);
        assertEquals("users", store.lastCreateIndexesCollection);
        assertEquals(1, store.lastCreateIndexesRequests.size());
        assertEquals("email_1", store.lastCreateIndexesRequests.get(0).name());
        assertEquals(
                1,
                store.lastCreateIndexesRequests
                        .get(0)
                        .key()
                        .getInt32("email")
                        .getValue());
        assertTrue(store.lastCreateIndexesRequests.get(0).unique());
    }

    @Test
    void createIndexesCommandAcceptsAndForwardsSparsePartialCollationAndTtlOptions() {
        final RecordingStore store = new RecordingStore();
        store.createIndexesResult = new CommandStore.CreateIndexesResult(0, 1);
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"createIndexes\":\"users\",\"$db\":\"app\",\"indexes\":[{\"name\":\"email_1\",\"key\":{\"email\":1},\"sparse\":true,\"partialFilterExpression\":{\"email\":{\"$exists\":true}},\"collation\":{\"locale\":\"en\",\"strength\":2},\"expireAfterSeconds\":3600}]}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        assertEquals(1, store.lastCreateIndexesRequests.size());
        assertTrue(store.lastCreateIndexesRequests.get(0).sparse());
        assertEquals(
                true,
                store.lastCreateIndexesRequests
                        .get(0)
                        .partialFilterExpression()
                        .getDocument("email")
                        .getBoolean("$exists")
                        .getValue());
        assertEquals("en", store.lastCreateIndexesRequests.get(0).collation().getString("locale").getValue());
        assertEquals(2, store.lastCreateIndexesRequests.get(0).collation().getInt32("strength").getValue());
        assertEquals(3600L, store.lastCreateIndexesRequests.get(0).expireAfterSeconds());
    }

    @Test
    void createIndexesCommandValidatesSparsePartialCollationAndTtlOptionShape() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new RecordingStore());

        final BsonDocument sparseTypeMismatch = dispatcher.dispatch(BsonDocument.parse(
                "{\"createIndexes\":\"users\",\"$db\":\"app\",\"indexes\":[{\"name\":\"email_1\",\"key\":{\"email\":1},\"sparse\":\"true\"}]}"));
        assertCommandError(sparseTypeMismatch, "TypeMismatch");

        final BsonDocument partialTypeMismatch = dispatcher.dispatch(BsonDocument.parse(
                "{\"createIndexes\":\"users\",\"$db\":\"app\",\"indexes\":[{\"name\":\"email_1\",\"key\":{\"email\":1},\"partialFilterExpression\":1}]}"));
        assertCommandError(partialTypeMismatch, "TypeMismatch");

        final BsonDocument ttlTypeMismatch = dispatcher.dispatch(BsonDocument.parse(
                "{\"createIndexes\":\"users\",\"$db\":\"app\",\"indexes\":[{\"name\":\"email_1\",\"key\":{\"email\":1},\"expireAfterSeconds\":1.5}]}"));
        assertCommandError(ttlTypeMismatch, "TypeMismatch");

        final BsonDocument ttlBadValue = dispatcher.dispatch(BsonDocument.parse(
                "{\"createIndexes\":\"users\",\"$db\":\"app\",\"indexes\":[{\"name\":\"email_1\",\"key\":{\"email\":1},\"expireAfterSeconds\":-1}]}"));
        assertCommandError(ttlBadValue, "BadValue");

        final BsonDocument collationTypeMismatch = dispatcher.dispatch(BsonDocument.parse(
                "{\"createIndexes\":\"users\",\"$db\":\"app\",\"indexes\":[{\"name\":\"email_1\",\"key\":{\"email\":1},\"collation\":\"en\"}]}"));
        assertCommandError(collationTypeMismatch, "TypeMismatch");

        final BsonDocument collationLocaleTypeMismatch = dispatcher.dispatch(BsonDocument.parse(
                "{\"createIndexes\":\"users\",\"$db\":\"app\",\"indexes\":[{\"name\":\"email_1\",\"key\":{\"email\":1},\"collation\":{\"locale\":1}}]}"));
        assertCommandError(collationLocaleTypeMismatch, "TypeMismatch");
    }

    @Test
    void listIndexesCommandReturnsCursorWithIndexMetadata() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument createIndexResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"createIndexes\":\"users\",\"$db\":\"app\",\"indexes\":[{\"name\":\"email_1\",\"key\":{\"email\":1},\"unique\":true,\"sparse\":true,\"partialFilterExpression\":{\"email\":{\"$exists\":true}},\"collation\":{\"locale\":\"en\"},\"expireAfterSeconds\":3600}]}"));
        assertEquals(1.0, createIndexResponse.get("ok").asNumber().doubleValue());

        final BsonDocument listIndexesResponse =
                dispatcher.dispatch(BsonDocument.parse("{\"listIndexes\":\"users\",\"$db\":\"app\"}"));

        assertEquals(1.0, listIndexesResponse.get("ok").asNumber().doubleValue());
        final BsonDocument cursor = listIndexesResponse.getDocument("cursor");
        assertEquals("app.users", cursor.getString("ns").getValue());
        assertEquals(1, cursor.getArray("firstBatch").size());

        final BsonDocument index = cursor.getArray("firstBatch").get(0).asDocument();
        assertEquals(2, index.getInt32("v").getValue());
        assertEquals("email_1", index.getString("name").getValue());
        assertEquals("app.users", index.getString("ns").getValue());
        assertEquals(1, index.getDocument("key").getInt32("email").getValue());
        assertEquals(true, index.getBoolean("unique").getValue());
        assertEquals(true, index.getBoolean("sparse").getValue());
        assertEquals(
                true,
                index.getDocument("partialFilterExpression")
                        .getDocument("email")
                        .getBoolean("$exists")
                        .getValue());
        assertEquals("en", index.getDocument("collation").getString("locale").getValue());
        assertEquals(3600L, index.getInt64("expireAfterSeconds").getValue());
    }

    @Test
    void listIndexesCommandCallsStore() {
        final RecordingStore store = new RecordingStore();
        store.listIndexesResult = List.of(new CommandStore.IndexMetadata(
                "email_1",
                BsonDocument.parse("{\"email\":1}"),
                true,
                false,
                null,
                BsonDocument.parse("{\"locale\":\"en\"}"),
                null));
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"listIndexes\":\"users\",\"$db\":\"app\",\"cursor\":{\"batchSize\":5}}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        assertEquals("app", store.lastListIndexesDatabase);
        assertEquals("users", store.lastListIndexesCollection);
        assertEquals(1, response.getDocument("cursor").getArray("firstBatch").size());
        assertEquals(
                "en",
                response.getDocument("cursor")
                        .getArray("firstBatch")
                        .get(0)
                        .asDocument()
                        .getDocument("collation")
                        .getString("locale")
                        .getValue());
    }

    @Test
    void insertCommandRejectsDuplicateKeyAfterUniqueIndex() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument createIndexResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"createIndexes\":\"users\",\"$db\":\"app\",\"indexes\":[{\"name\":\"email_1\",\"key\":{\"email\":1},\"unique\":true}]}"));
        assertEquals(1.0, createIndexResponse.get("ok").asNumber().doubleValue());

        final BsonDocument firstInsert = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"email\":\"ada@example.com\"}]}"));
        assertEquals(1.0, firstInsert.get("ok").asNumber().doubleValue());

        final BsonDocument duplicateInsert = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":2,\"email\":\"ada@example.com\"}]}"));
        assertDuplicateKeyError(duplicateInsert);

        final BsonDocument findResponse =
                dispatcher.dispatch(BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{}}"));
        assertEquals(1, findResponse.getDocument("cursor").getArray("firstBatch").size());
    }

    @Test
    void updateCommandRejectsDuplicateKeyAfterUniqueIndex() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        dispatcher.dispatch(BsonDocument.parse(
                "{\"createIndexes\":\"users\",\"$db\":\"app\",\"indexes\":[{\"name\":\"email_1\",\"key\":{\"email\":1},\"unique\":true}]}"));
        dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"email\":\"ada@example.com\"},{\"_id\":2,\"email\":\"linus@example.com\"}]}"));

        final BsonDocument duplicateUpdate = dispatcher.dispatch(BsonDocument.parse(
                "{\"update\":\"users\",\"$db\":\"app\",\"updates\":[{\"q\":{\"_id\":2},\"u\":{\"$set\":{\"email\":\"ada@example.com\"}}}]}"));
        assertDuplicateKeyError(duplicateUpdate);

        final BsonDocument secondDocResponse = dispatcher.dispatch(
                BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{\"_id\":2}}"));
        assertEquals(1, secondDocResponse.getDocument("cursor").getArray("firstBatch").size());
        assertEquals(
                "linus@example.com",
                secondDocResponse
                        .getDocument("cursor")
                        .getArray("firstBatch")
                        .get(0)
                        .asDocument()
                        .getString("email")
                        .getValue());
    }

    @Test
    void updateCommandSupportsUpdateOneAndUpdateManySemantics() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));
        dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"role\":\"user\",\"tier\":0},{\"_id\":2,\"role\":\"user\",\"tier\":0}]}"));

        final BsonDocument updateOneResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"update\":\"users\",\"$db\":\"app\",\"updates\":[{\"q\":{\"role\":\"user\"},\"u\":{\"$set\":{\"tier\":1}}}]}"));
        assertEquals(1.0, updateOneResponse.get("ok").asNumber().doubleValue());
        assertEquals(1, updateOneResponse.getInt32("n").getValue());
        assertEquals(1, updateOneResponse.getInt32("nModified").getValue());

        final BsonDocument updateManyResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"update\":\"users\",\"$db\":\"app\",\"updates\":[{\"q\":{\"role\":\"user\"},\"u\":{\"$set\":{\"tier\":2}},\"multi\":true}]}"));
        assertEquals(1.0, updateManyResponse.get("ok").asNumber().doubleValue());
        assertEquals(2, updateManyResponse.getInt32("n").getValue());
        assertEquals(2, updateManyResponse.getInt32("nModified").getValue());
    }

    @Test
    void updateCommandSupportsReplaceOneSemantics() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));
        dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"before\",\"extra\":true}]}"));

        final BsonDocument replaceResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"update\":\"users\",\"$db\":\"app\",\"updates\":[{\"q\":{\"_id\":1},\"u\":{\"name\":\"after\"}}]}"));

        assertEquals(1.0, replaceResponse.get("ok").asNumber().doubleValue());
        assertEquals(1, replaceResponse.getInt32("n").getValue());
        assertEquals(1, replaceResponse.getInt32("nModified").getValue());

        final BsonDocument findResponse =
                dispatcher.dispatch(BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{\"_id\":1}}"));
        final BsonDocument replaced =
                findResponse.getDocument("cursor").getArray("firstBatch").get(0).asDocument();
        assertEquals(1, replaced.getInt32("_id").getValue());
        assertEquals("after", replaced.getString("name").getValue());
        assertTrue(!replaced.containsKey("extra"));
    }

    @Test
    void updateCommandReturnsUpsertedEntriesForOperatorAndReplacementUpserts() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument upsertResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"update\":\"users\",\"$db\":\"app\",\"updates\":[{\"q\":{\"email\":\"ada@example.com\"},\"u\":{\"$set\":{\"name\":\"Ada\"}},\"upsert\":true},{\"q\":{\"_id\":99},\"u\":{\"name\":\"Neo\"},\"upsert\":true}]}"));

        assertEquals(1.0, upsertResponse.get("ok").asNumber().doubleValue());
        assertEquals(2, upsertResponse.getInt32("n").getValue());
        assertEquals(0, upsertResponse.getInt32("nModified").getValue());
        assertEquals(2, upsertResponse.getArray("upserted").size());
        assertEquals(
                0,
                upsertResponse
                        .getArray("upserted")
                        .get(0)
                        .asDocument()
                        .getInt32("index")
                        .getValue());
        assertEquals(
                1,
                upsertResponse
                        .getArray("upserted")
                        .get(1)
                        .asDocument()
                        .getInt32("index")
                        .getValue());
        assertEquals(
                99,
                upsertResponse
                        .getArray("upserted")
                        .get(1)
                        .asDocument()
                        .getInt32("_id")
                        .getValue());

        final BsonDocument findResponse =
                dispatcher.dispatch(BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{}}"));
        assertEquals(2, findResponse.getDocument("cursor").getArray("firstBatch").size());
    }

    @Test
    void updateCommandCallsStoreAndReturnsWriteShape() {
        final RecordingStore store = new RecordingStore();
        store.updateResult = new CommandStore.UpdateResult(2, 1);
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"update\":\"users\",\"$db\":\"app\",\"updates\":[{\"q\":{\"role\":\"user\"},\"u\":{\"$set\":{\"role\":\"member\"}},\"multi\":true}]}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        assertEquals(2, response.getInt32("n").getValue());
        assertEquals(1, response.getInt32("nModified").getValue());
        assertEquals("app", store.lastUpdateDatabase);
        assertEquals("users", store.lastUpdateCollection);
        assertEquals(1, store.lastUpdateRequests.size());
        assertEquals(
                "user",
                store.lastUpdateRequests.get(0).query().getString("role").getValue());
        assertTrue(store.lastUpdateRequests.get(0).multi());
        assertTrue(!store.lastUpdateRequests.get(0).upsert());
    }

    @Test
    void deleteCommandCallsStoreAndReturnsWriteShape() {
        final RecordingStore store = new RecordingStore();
        store.deleteResult = 3;
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument response = dispatcher.dispatch(
                BsonDocument.parse("{\"delete\":\"users\",\"$db\":\"app\",\"deletes\":[{\"q\":{\"role\":\"inactive\"},\"limit\":0}]}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        assertEquals(3, response.getInt32("n").getValue());
        assertEquals("app", store.lastDeleteDatabase);
        assertEquals("users", store.lastDeleteCollection);
        assertEquals(1, store.lastDeleteRequests.size());
        assertEquals(
                "inactive",
                store.lastDeleteRequests.get(0).query().getString("role").getValue());
        assertEquals(0, store.lastDeleteRequests.get(0).limit());
    }

    @Test
    void replaceOneCommandCallsStoreAndReturnsWriteShape() {
        final RecordingStore store = new RecordingStore();
        store.updateResult = new CommandStore.UpdateResult(1, 1);
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"replaceOne\":\"users\",\"$db\":\"app\",\"filter\":{\"_id\":1},\"replacement\":{\"name\":\"after\"},\"upsert\":true,\"hint\":{\"_id\":1},\"collation\":{\"locale\":\"en\"}}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        assertEquals(1, response.getInt32("n").getValue());
        assertEquals(1, response.getInt32("nModified").getValue());
        assertEquals("app", store.lastUpdateDatabase);
        assertEquals("users", store.lastUpdateCollection);
        assertEquals(1, store.lastUpdateRequests.size());
        assertEquals(1, store.lastUpdateRequests.get(0).query().getInt32("_id").getValue());
        assertEquals("after", store.lastUpdateRequests.get(0).update().getString("name").getValue());
        assertTrue(!store.lastUpdateRequests.get(0).multi());
        assertEquals(true, store.lastUpdateRequests.get(0).upsert());
    }

    @Test
    void replaceOneCommandRejectsInvalidPayloadShapes() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new RecordingStore());

        final BsonDocument filterTypeMismatch = dispatcher.dispatch(BsonDocument.parse(
                "{\"replaceOne\":\"users\",\"filter\":1,\"replacement\":{\"name\":\"a\"}}"));
        assertCommandError(filterTypeMismatch, "TypeMismatch");

        final BsonDocument replacementTypeMismatch = dispatcher.dispatch(BsonDocument.parse(
                "{\"replaceOne\":\"users\",\"filter\":{},\"replacement\":1}"));
        assertCommandError(replacementTypeMismatch, "TypeMismatch");

        final BsonDocument replacementBadValue = dispatcher.dispatch(BsonDocument.parse(
                "{\"replaceOne\":\"users\",\"filter\":{},\"replacement\":{\"$set\":{\"name\":\"a\"}}}"));
        assertCommandError(replacementBadValue, "BadValue");
    }

    @Test
    void findOneAndUpdateCommandSupportsBeforeAndAfterSemantics() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));
        dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"before\",\"tier\":1}]}"));

        final BsonDocument beforeResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"findOneAndUpdate\":\"users\",\"$db\":\"app\",\"filter\":{\"_id\":1},\"update\":{\"$set\":{\"name\":\"after\"}}}"));
        assertEquals(1.0, beforeResponse.get("ok").asNumber().doubleValue());
        assertEquals(
                "before",
                beforeResponse.getDocument("value").getString("name").getValue());

        final BsonDocument afterResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"findOneAndUpdate\":\"users\",\"$db\":\"app\",\"filter\":{\"_id\":1},\"update\":{\"$set\":{\"name\":\"after-2\"}},\"returnDocument\":\"after\"}"));
        assertEquals(1.0, afterResponse.get("ok").asNumber().doubleValue());
        assertEquals(
                "after-2",
                afterResponse.getDocument("value").getString("name").getValue());
    }

    @Test
    void findOneAndUpdateCommandSupportsProjectionWithNestedInclude() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));
        dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"before\",\"email\":\"a@example.com\",\"profile\":{\"city\":\"seoul\",\"zip\":\"123\"}}]}"));

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"findOneAndUpdate\":\"users\",\"$db\":\"app\",\"filter\":{\"_id\":1},\"update\":{\"$set\":{\"name\":\"after\"}},\"returnDocument\":\"after\",\"projection\":{\"name\":1,\"profile.city\":1,\"_id\":0}}"));
        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        final BsonDocument value = response.getDocument("value");
        assertTrue(!value.containsKey("_id"));
        assertEquals("after", value.getString("name").getValue());
        assertTrue(!value.containsKey("email"));
        assertEquals("seoul", value.getDocument("profile").getString("city").getValue());
        assertTrue(!value.getDocument("profile").containsKey("zip"));
    }

    @Test
    void findOneAndUpdateCommandRejectsMixedProjectionModes() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));
        dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"before\",\"email\":\"a@example.com\"}]}"));

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"findOneAndUpdate\":\"users\",\"$db\":\"app\",\"filter\":{\"_id\":1},\"update\":{\"$set\":{\"name\":\"after\"}},\"projection\":{\"name\":1,\"email\":0}}"));
        assertCommandError(response, "BadValue");
    }

    @Test
    void findOneAndUpdateCommandRejectsInvalidPayloadShapes() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new RecordingStore());

        final BsonDocument filterTypeMismatch = dispatcher.dispatch(BsonDocument.parse(
                "{\"findOneAndUpdate\":\"users\",\"filter\":1,\"update\":{\"$set\":{\"name\":\"a\"}}}"));
        assertCommandError(filterTypeMismatch, "TypeMismatch");

        final BsonDocument replacementNotAllowed = dispatcher.dispatch(BsonDocument.parse(
                "{\"findOneAndUpdate\":\"users\",\"filter\":{},\"update\":{\"name\":\"a\"}}"));
        assertCommandError(replacementNotAllowed, "BadValue");

        final BsonDocument invalidReturnDocument = dispatcher.dispatch(BsonDocument.parse(
                "{\"findOneAndUpdate\":\"users\",\"filter\":{},\"update\":{\"$set\":{\"name\":\"a\"}},\"returnDocument\":\"later\"}"));
        assertCommandError(invalidReturnDocument, "BadValue");

        final BsonDocument unsupportedArrayFilters = dispatcher.dispatch(BsonDocument.parse(
                "{\"findOneAndUpdate\":\"users\",\"filter\":{},\"update\":{\"$set\":{\"name\":\"a\"}},\"arrayFilters\":[]}"));
        assertCommandError(unsupportedArrayFilters, "BadValue");
    }

    @Test
    void findOneAndReplaceCommandSupportsBeforeAndAfterSemantics() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));
        dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"before\",\"extra\":true}]}"));

        final BsonDocument beforeResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"findOneAndReplace\":\"users\",\"$db\":\"app\",\"filter\":{\"_id\":1},\"replacement\":{\"name\":\"after\"}}"));
        assertEquals(1.0, beforeResponse.get("ok").asNumber().doubleValue());
        assertEquals(
                "before",
                beforeResponse.getDocument("value").getString("name").getValue());

        final BsonDocument afterResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"findOneAndReplace\":\"users\",\"$db\":\"app\",\"filter\":{\"_id\":1},\"replacement\":{\"name\":\"after-2\"},\"returnDocument\":\"after\"}"));
        assertEquals(1.0, afterResponse.get("ok").asNumber().doubleValue());
        assertEquals(
                "after-2",
                afterResponse.getDocument("value").getString("name").getValue());

        final BsonDocument findResponse =
                dispatcher.dispatch(BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{\"_id\":1}}"));
        final BsonDocument stored =
                findResponse.getDocument("cursor").getArray("firstBatch").get(0).asDocument();
        assertEquals(1, stored.getInt32("_id").getValue());
        assertTrue(!stored.containsKey("extra"));
    }

    @Test
    void findOneAndReplaceCommandRejectsInvalidPayloadShapes() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new RecordingStore());

        final BsonDocument replacementTypeMismatch = dispatcher.dispatch(BsonDocument.parse(
                "{\"findOneAndReplace\":\"users\",\"filter\":{},\"replacement\":1}"));
        assertCommandError(replacementTypeMismatch, "TypeMismatch");

        final BsonDocument operatorReplacement = dispatcher.dispatch(BsonDocument.parse(
                "{\"findOneAndReplace\":\"users\",\"filter\":{},\"replacement\":{\"$set\":{\"name\":\"a\"}}}"));
        assertCommandError(operatorReplacement, "BadValue");

        final BsonDocument invalidReturnDocument = dispatcher.dispatch(BsonDocument.parse(
                "{\"findOneAndReplace\":\"users\",\"filter\":{},\"replacement\":{\"name\":\"a\"},\"returnDocument\":1}"));
        assertCommandError(invalidReturnDocument, "TypeMismatch");
    }

    @Test
    void findOneAndReplaceCommandSupportsUpsertWithReturnAfter() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"findOneAndReplace\":\"users\",\"$db\":\"app\",\"filter\":{\"_id\":91},\"replacement\":{\"name\":\"created\"},\"upsert\":true,\"returnDocument\":\"after\"}"));

        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        assertEquals(91, response.getDocument("value").getInt32("_id").getValue());
        assertEquals(
                "created",
                response.getDocument("value").getString("name").getValue());
    }

    @Test
    void findOneAndReplaceCommandSupportsProjectionExcludePaths() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));
        dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"before\",\"email\":\"a@example.com\",\"profile\":{\"city\":\"seoul\",\"zip\":\"123\"}}]}"));

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"findOneAndReplace\":\"users\",\"$db\":\"app\",\"filter\":{\"_id\":1},\"replacement\":{\"name\":\"after\",\"email\":\"b@example.com\",\"profile\":{\"city\":\"busan\",\"zip\":\"999\"}},\"returnDocument\":\"after\",\"projection\":{\"email\":0,\"profile.zip\":0}}"));
        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        final BsonDocument value = response.getDocument("value");
        assertEquals(1, value.getInt32("_id").getValue());
        assertEquals("after", value.getString("name").getValue());
        assertTrue(!value.containsKey("email"));
        assertEquals("busan", value.getDocument("profile").getString("city").getValue());
        assertTrue(!value.getDocument("profile").containsKey("zip"));
    }

    @Test
    void updateCommandRejectsInvalidPayloadShapes() {
        final RecordingStore store = new RecordingStore();
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument typeMismatchResponse = dispatcher.dispatch(
                BsonDocument.parse("{\"update\":\"users\",\"updates\":[{\"q\":1,\"u\":{\"$set\":{\"name\":\"a\"}}}]}"));
        assertCommandError(typeMismatchResponse, "TypeMismatch");

        final BsonDocument badValueResponse = dispatcher.dispatch(
                BsonDocument.parse("{\"update\":\"users\",\"updates\":[{\"q\":{},\"u\":{}}]}"));
        assertCommandError(badValueResponse, "BadValue");

        final BsonDocument arrayFiltersTypeMismatch = dispatcher.dispatch(BsonDocument.parse(
                "{\"update\":\"users\",\"updates\":[{\"q\":{},\"u\":{\"$set\":{\"a\":1}},\"arrayFilters\":{}}]}"));
        assertCommandError(arrayFiltersTypeMismatch, "TypeMismatch");

        final BsonDocument unsupportedArrayFilters = dispatcher.dispatch(BsonDocument.parse(
                "{\"update\":\"users\",\"updates\":[{\"q\":{},\"u\":{\"$set\":{\"a\":1}},\"arrayFilters\":[]}]}"));
        assertCommandError(unsupportedArrayFilters, "BadValue");

        final BsonDocument unsupportedPositionalPath = dispatcher.dispatch(BsonDocument.parse(
                "{\"update\":\"users\",\"updates\":[{\"q\":{},\"u\":{\"$set\":{\"items.$.qty\":1}}}]}"));
        assertCommandError(unsupportedPositionalPath, "BadValue");

        final BsonDocument replacementMultiTrue = dispatcher.dispatch(
                BsonDocument.parse("{\"update\":\"users\",\"updates\":[{\"q\":{},\"u\":{\"name\":\"a\"},\"multi\":true}]}"));
        assertCommandError(replacementMultiTrue, "BadValue");
    }

    @Test
    void deleteCommandRejectsInvalidPayloadShapes() {
        final RecordingStore store = new RecordingStore();
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument typeMismatchResponse = dispatcher.dispatch(
                BsonDocument.parse("{\"delete\":\"users\",\"deletes\":[{\"q\":{},\"limit\":\"1\"}]}"));
        assertCommandError(typeMismatchResponse, "TypeMismatch");

        final BsonDocument badValueResponse = dispatcher.dispatch(
                BsonDocument.parse("{\"delete\":\"users\",\"deletes\":[{\"q\":{},\"limit\":2}]}"));
        assertCommandError(badValueResponse, "BadValue");
    }

    @Test
    void crudCommandsAcceptCompatibleOptionShapes() {
        final RecordingStore store = new RecordingStore();
        store.findResult = List.of();
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument insertResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"ordered\":true,\"writeConcern\":{\"w\":\"majority\",\"j\":true,\"wtimeout\":1000},\"readConcern\":{\"level\":\"local\"},\"documents\":[{\"_id\":1,\"name\":\"a\"}]}"));
        assertEquals(1.0, insertResponse.get("ok").asNumber().doubleValue());

        final BsonDocument updateResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"update\":\"users\",\"$db\":\"app\",\"ordered\":false,\"writeConcern\":{\"w\":1},\"updates\":[{\"q\":{\"_id\":1},\"u\":{\"$set\":{\"name\":\"b\"}},\"hint\":{\"_id\":1},\"collation\":{\"locale\":\"en\"}}]}"));
        assertEquals(1.0, updateResponse.get("ok").asNumber().doubleValue());

        final BsonDocument deleteResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"delete\":\"users\",\"$db\":\"app\",\"ordered\":true,\"writeConcern\":{\"w\":1,\"wtimeout\":0},\"deletes\":[{\"q\":{\"_id\":1},\"limit\":1,\"hint\":\"_id_\",\"collation\":{\"locale\":\"en\"}}]}"));
        assertEquals(1.0, deleteResponse.get("ok").asNumber().doubleValue());

        final BsonDocument findResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"find\":\"users\",\"$db\":\"app\",\"filter\":{},\"readConcern\":{\"level\":\"local\"},\"hint\":{\"_id\":1},\"collation\":{\"locale\":\"en\"}}"));
        assertEquals(1.0, findResponse.get("ok").asNumber().doubleValue());
    }

    @Test
    void crudCommandsRejectInvalidOptionShapes() {
        final RecordingStore store = new RecordingStore();
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument invalidOrdered = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"ordered\":\"true\",\"documents\":[{\"_id\":1}]}"));
        assertCommandError(invalidOrdered, "TypeMismatch");

        final BsonDocument invalidWriteConcern = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"writeConcern\":\"majority\",\"documents\":[{\"_id\":1}]}"));
        assertCommandError(invalidWriteConcern, "TypeMismatch");

        final BsonDocument invalidHint = dispatcher.dispatch(BsonDocument.parse(
                "{\"update\":\"users\",\"$db\":\"app\",\"updates\":[{\"q\":{},\"u\":{\"$set\":{\"a\":1}},\"hint\":{}}]}"));
        assertCommandError(invalidHint, "BadValue");

        final BsonDocument invalidCollation = dispatcher.dispatch(BsonDocument.parse(
                "{\"delete\":\"users\",\"$db\":\"app\",\"deletes\":[{\"q\":{},\"limit\":1,\"collation\":\"en\"}]}"));
        assertCommandError(invalidCollation, "TypeMismatch");

        final BsonDocument invalidReadConcern = dispatcher.dispatch(BsonDocument.parse(
                "{\"find\":\"users\",\"$db\":\"app\",\"filter\":{},\"readConcern\":\"local\"}"));
        assertCommandError(invalidReadConcern, "TypeMismatch");
    }

    @Test
    void transactionLifecycleAllowsCommitAndClearsState() {
        final RecordingStore store = new RecordingStore();
        final CommandDispatcher dispatcher = new CommandDispatcher(store);

        final BsonDocument startResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"));
        assertEquals(1.0, startResponse.get("ok").asNumber().doubleValue());

        final BsonDocument inTxnResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"delete\":\"users\",\"$db\":\"app\",\"deletes\":[{\"q\":{\"_id\":1},\"limit\":1}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertEquals(1.0, inTxnResponse.get("ok").asNumber().doubleValue());

        final BsonDocument commitResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"commitTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertEquals(1.0, commitResponse.get("ok").asNumber().doubleValue());

        final BsonDocument secondCommitResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"commitTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertNoSuchTransactionError(secondCommitResponse, "UnknownTransactionCommitResult");
    }

    @Test
    void transactionalWritesAreNotVisibleOutsideBeforeCommit() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument startResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"txn-only\"}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"));
        assertEquals(1.0, startResponse.get("ok").asNumber().doubleValue());

        final BsonDocument outsideFind =
                dispatcher.dispatch(BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{}}"));
        assertEquals(0, outsideFind.getDocument("cursor").getArray("firstBatch").size());

        final BsonDocument inTransactionFind = dispatcher.dispatch(BsonDocument.parse(
                "{\"find\":\"users\",\"$db\":\"app\",\"filter\":{},\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertEquals(1, inTransactionFind.getDocument("cursor").getArray("firstBatch").size());
    }

    @Test
    void commitTransactionPublishesTransactionWrites() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument startResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"committed\"}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"));
        assertEquals(1.0, startResponse.get("ok").asNumber().doubleValue());

        final BsonDocument commitResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"commitTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertEquals(1.0, commitResponse.get("ok").asNumber().doubleValue());

        final BsonDocument outsideFind =
                dispatcher.dispatch(BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{}}"));
        assertEquals(1, outsideFind.getDocument("cursor").getArray("firstBatch").size());
    }

    @Test
    void replaceOneCommandHonorsTransactionVisibilityAndCommit() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument seedInsert = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"before\"}]}"));
        assertEquals(1.0, seedInsert.get("ok").asNumber().doubleValue());

        final BsonDocument replaceInTxn = dispatcher.dispatch(BsonDocument.parse(
                "{\"replaceOne\":\"users\",\"$db\":\"app\",\"filter\":{\"_id\":1},\"replacement\":{\"name\":\"after\"},\"lsid\":{\"id\":\"session-r1\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"));
        assertEquals(1.0, replaceInTxn.get("ok").asNumber().doubleValue());

        final BsonDocument outsideBeforeCommit = dispatcher.dispatch(
                BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{\"_id\":1}}"));
        assertEquals(
                "before",
                outsideBeforeCommit
                        .getDocument("cursor")
                        .getArray("firstBatch")
                        .get(0)
                        .asDocument()
                        .getString("name")
                        .getValue());

        final BsonDocument commitResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"commitTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\"session-r1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertEquals(1.0, commitResponse.get("ok").asNumber().doubleValue());

        final BsonDocument outsideAfterCommit = dispatcher.dispatch(
                BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{\"_id\":1}}"));
        assertEquals(
                "after",
                outsideAfterCommit
                        .getDocument("cursor")
                        .getArray("firstBatch")
                        .get(0)
                        .asDocument()
                        .getString("name")
                        .getValue());
    }

    @Test
    void countDocumentsCommandUsesTransactionalSnapshot() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument startInsert = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":10,\"name\":\"txn-doc\"}],\"lsid\":{\"id\":\"session-c1\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"));
        assertEquals(1.0, startInsert.get("ok").asNumber().doubleValue());

        final BsonDocument outsideCount = dispatcher.dispatch(
                BsonDocument.parse("{\"countDocuments\":\"users\",\"$db\":\"app\",\"filter\":{}}"));
        assertEquals(0L, outsideCount.getInt64("n").getValue());

        final BsonDocument transactionalCount = dispatcher.dispatch(BsonDocument.parse(
                "{\"countDocuments\":\"users\",\"$db\":\"app\",\"filter\":{},\"lsid\":{\"id\":\"session-c1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertEquals(1L, transactionalCount.getInt64("n").getValue());
    }

    @Test
    void aggregateCommandUsesTransactionalSnapshot() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument startInsert = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":20,\"name\":\"txn-agg\"}],\"lsid\":{\"id\":\"session-a1\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"));
        assertEquals(1.0, startInsert.get("ok").asNumber().doubleValue());

        final BsonDocument outsideAggregate = dispatcher.dispatch(BsonDocument.parse(
                "{\"aggregate\":\"users\",\"$db\":\"app\",\"pipeline\":[{\"$match\":{}}],\"cursor\":{}}"));
        assertEquals(0, outsideAggregate.getDocument("cursor").getArray("firstBatch").size());

        final BsonDocument transactionalAggregate = dispatcher.dispatch(BsonDocument.parse(
                "{\"aggregate\":\"users\",\"$db\":\"app\",\"pipeline\":[{\"$match\":{}}],\"cursor\":{},\"lsid\":{\"id\":\"session-a1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertEquals(1, transactionalAggregate.getDocument("cursor").getArray("firstBatch").size());

        final BsonDocument commitResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"commitTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\"session-a1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertEquals(1.0, commitResponse.get("ok").asNumber().doubleValue());

        final BsonDocument outsideAfterCommit = dispatcher.dispatch(BsonDocument.parse(
                "{\"aggregate\":\"users\",\"$db\":\"app\",\"pipeline\":[{\"$match\":{}}],\"cursor\":{}}"));
        assertEquals(1, outsideAfterCommit.getDocument("cursor").getArray("firstBatch").size());
    }

    @Test
    void abortTransactionDiscardsTransactionWrites() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument startResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"abort-me\"}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"));
        assertEquals(1.0, startResponse.get("ok").asNumber().doubleValue());

        final BsonDocument abortResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"abortTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertEquals(1.0, abortResponse.get("ok").asNumber().doubleValue());

        final BsonDocument outsideFind =
                dispatcher.dispatch(BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{}}"));
        assertEquals(0, outsideFind.getDocument("cursor").getArray("firstBatch").size());
    }

    @Test
    void abortTransactionPreservesOutOfTransactionWrites() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument startResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"txn-user\"}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"));
        assertEquals(1.0, startResponse.get("ok").asNumber().doubleValue());

        final BsonDocument outOfTransactionInsert = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"tenantMappings\",\"$db\":\"app\",\"documents\":[{\"_id\":101,\"tenantId\":\"t-1\"}]}"));
        assertEquals(1.0, outOfTransactionInsert.get("ok").asNumber().doubleValue());

        final BsonDocument abortResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"abortTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertEquals(1.0, abortResponse.get("ok").asNumber().doubleValue());

        final BsonDocument usersFind =
                dispatcher.dispatch(BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{}}"));
        assertEquals(0, usersFind.getDocument("cursor").getArray("firstBatch").size());

        final BsonDocument mappingFind = dispatcher.dispatch(BsonDocument.parse(
                "{\"find\":\"tenantMappings\",\"$db\":\"app\",\"filter\":{}}"));
        assertEquals(1, mappingFind.getDocument("cursor").getArray("firstBatch").size());
    }

    @Test
    void commitTransactionDoesNotDropOutOfTransactionWritesFromDifferentCollection() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument startResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"txn-user\"}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"));
        assertEquals(1.0, startResponse.get("ok").asNumber().doubleValue());

        final BsonDocument outOfTransactionInsert = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"tenantMappings\",\"$db\":\"app\",\"documents\":[{\"_id\":101,\"tenantId\":\"t-1\"}]}"));
        assertEquals(1.0, outOfTransactionInsert.get("ok").asNumber().doubleValue());

        final BsonDocument commitResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"commitTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertEquals(1.0, commitResponse.get("ok").asNumber().doubleValue());

        final BsonDocument usersFind =
                dispatcher.dispatch(BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{}}"));
        assertEquals(1, usersFind.getDocument("cursor").getArray("firstBatch").size());

        final BsonDocument mappingFind = dispatcher.dispatch(BsonDocument.parse(
                "{\"find\":\"tenantMappings\",\"$db\":\"app\",\"filter\":{}}"));
        assertEquals(1, mappingFind.getDocument("cursor").getArray("firstBatch").size());
    }

    @Test
    void commitTransactionDoesNotDropOutOfTransactionWritesInSameCollection() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument startResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"txn-user\"}],\"lsid\":{\"id\":\"session-2\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"));
        assertEquals(1.0, startResponse.get("ok").asNumber().doubleValue());

        final BsonDocument outOfTransactionInsert = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":2,\"name\":\"outside-user\"}]}"));
        assertEquals(1.0, outOfTransactionInsert.get("ok").asNumber().doubleValue());

        final BsonDocument commitResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"commitTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\"session-2\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertEquals(1.0, commitResponse.get("ok").asNumber().doubleValue());

        final BsonDocument usersFind =
                dispatcher.dispatch(BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{}}"));
        assertEquals(2, usersFind.getDocument("cursor").getArray("firstBatch").size());
    }

    @Test
    void commitTransactionUsesTransactionalVersionWhenSameIdIsChangedOutsideTransaction() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument seedResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"seed\"}]}"));
        assertEquals(1.0, seedResponse.get("ok").asNumber().doubleValue());

        final BsonDocument startResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"update\":\"users\",\"$db\":\"app\",\"updates\":[{\"q\":{\"_id\":1},\"u\":{\"$set\":{\"name\":\"txn\"}},\"multi\":false}],\"lsid\":{\"id\":\"session-3\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"));
        assertEquals(1.0, startResponse.get("ok").asNumber().doubleValue());

        final BsonDocument outOfTransactionUpdate = dispatcher.dispatch(BsonDocument.parse(
                "{\"update\":\"users\",\"$db\":\"app\",\"updates\":[{\"q\":{\"_id\":1},\"u\":{\"$set\":{\"name\":\"outside\"}},\"multi\":false}]}"));
        assertEquals(1.0, outOfTransactionUpdate.get("ok").asNumber().doubleValue());

        final BsonDocument commitResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"commitTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\"session-3\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertEquals(1.0, commitResponse.get("ok").asNumber().doubleValue());

        final BsonDocument usersFind =
                dispatcher.dispatch(BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{\"_id\":1}}"));
        assertEquals(
                "txn",
                usersFind.getDocument("cursor")
                        .getArray("firstBatch")
                        .get(0)
                        .asDocument()
                        .getString("name")
                        .getValue());
    }

    @Test
    void transactionStateTransitionsRequireValidLifecycle() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));

        final BsonDocument beforeStartResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"find\":\"users\",\"$db\":\"app\",\"filter\":{},\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertNoSuchTransactionError(beforeStartResponse, "TransientTransactionError");

        final BsonDocument startResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"));
        assertEquals(1.0, startResponse.get("ok").asNumber().doubleValue());

        final BsonDocument concurrentStartResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":2}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":2,\"autocommit\":false,\"startTransaction\":true}"));
        assertCommandError(concurrentStartResponse, "BadValue");

        final BsonDocument commitResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"commitTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertEquals(1.0, commitResponse.get("ok").asNumber().doubleValue());

        final BsonDocument afterCommitResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"find\":\"users\",\"$db\":\"app\",\"filter\":{},\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertNoSuchTransactionError(afterCommitResponse, "TransientTransactionError");
    }

    @Test
    void abortTransactionWithoutActiveTransactionReturnsNoSuchTransaction() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new RecordingStore());

        final BsonDocument abortResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"abortTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false}"));

        assertNoSuchTransactionError(abortResponse);
    }

    @Test
    void abortTransactionClearsActiveTransaction() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new RecordingStore());

        final BsonDocument startResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"));
        assertEquals(1.0, startResponse.get("ok").asNumber().doubleValue());

        final BsonDocument abortResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"abortTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertEquals(1.0, abortResponse.get("ok").asNumber().doubleValue());

        final BsonDocument postAbortResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"find\":\"users\",\"$db\":\"app\",\"filter\":{},\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertNoSuchTransactionError(postAbortResponse, "TransientTransactionError");
    }

    @Test
    void transactionalCommandRejectsMissingLsid() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new RecordingStore());

        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1}],\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"));

        assertCommandError(response, "TypeMismatch");
    }

    @Test
    void transactionStartRejectsInvalidFieldCombination() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new RecordingStore());

        final BsonDocument invalidStartTransaction = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":false}"));
        assertCommandError(invalidStartTransaction, "BadValue");

        final BsonDocument invalidAutocommit = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":true,\"startTransaction\":true}"));
        assertCommandError(invalidAutocommit, "BadValue");
    }

    @Test
    void transactionalCommandRejectsTxnNumberMismatch() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new RecordingStore());

        final BsonDocument startResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"));
        assertEquals(1.0, startResponse.get("ok").asNumber().doubleValue());

        final BsonDocument mismatchResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"update\":\"users\",\"$db\":\"app\",\"updates\":[{\"q\":{\"_id\":1},\"u\":{\"$set\":{\"name\":\"updated\"}}}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":2,\"autocommit\":false}"));
        assertNoSuchTransactionError(mismatchResponse, "TransientTransactionError");
    }

    @Test
    void transactionStartRejectsReusedTxnNumberAfterCompletion() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new RecordingStore());

        final BsonDocument startResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"));
        assertEquals(1.0, startResponse.get("ok").asNumber().doubleValue());

        final BsonDocument commitResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"commitTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertEquals(1.0, commitResponse.get("ok").asNumber().doubleValue());

        final BsonDocument reusedTxnNumberResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":2}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"));
        assertCommandError(reusedTxnNumberResponse, "BadValue");

        final BsonDocument lowerTxnNumberResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":3}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":0,\"autocommit\":false,\"startTransaction\":true}"));
        assertCommandError(lowerTxnNumberResponse, "BadValue");
    }

    @Test
    void transactionEnvelopeValidatesReadConcernPlacement() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new RecordingStore());

        final BsonDocument startResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"find\":\"users\",\"$db\":\"app\",\"filter\":{},\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true,\"readConcern\":{\"level\":\"local\"}}"));
        assertEquals(1.0, startResponse.get("ok").asNumber().doubleValue());

        final BsonDocument nonStartReadConcern = dispatcher.dispatch(BsonDocument.parse(
                "{\"find\":\"users\",\"$db\":\"app\",\"filter\":{},\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false,\"readConcern\":{\"level\":\"local\"}}"));
        assertCommandError(nonStartReadConcern, "BadValue");

        final BsonDocument commitWithReadConcern = dispatcher.dispatch(BsonDocument.parse(
                "{\"commitTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\"session-2\"},\"txnNumber\":1,\"autocommit\":false,\"readConcern\":{\"level\":\"local\"}}"));
        assertCommandError(commitWithReadConcern, "BadValue");
    }

    @Test
    void transactionEnvelopeValidatesWriteConcernPlacement() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new RecordingStore());

        final BsonDocument startWithWriteConcern = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1}],\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true,\"writeConcern\":{\"w\":1}}"));
        assertCommandError(startWithWriteConcern, "BadValue");

        final BsonDocument startResponse = dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":2}],\"lsid\":{\"id\":\"session-2\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"));
        assertEquals(1.0, startResponse.get("ok").asNumber().doubleValue());

        final BsonDocument commitWithWriteConcern = dispatcher.dispatch(BsonDocument.parse(
                "{\"commitTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\"session-2\"},\"txnNumber\":1,\"autocommit\":false,\"writeConcern\":{\"w\":\"majority\",\"j\":true,\"wtimeout\":1000}}"));
        assertEquals(1.0, commitWithWriteConcern.get("ok").asNumber().doubleValue());
    }

    @Test
    void transactionEnvelopeValidatesReadPreferenceRules() {
        final CommandDispatcher dispatcher = new CommandDispatcher(new RecordingStore());

        final BsonDocument secondaryReadPreference = dispatcher.dispatch(BsonDocument.parse(
                "{\"find\":\"users\",\"$db\":\"app\",\"filter\":{},\"lsid\":{\"id\":\"session-1\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true,\"readPreference\":{\"mode\":\"secondary\"}}"));
        assertCommandError(secondaryReadPreference, "BadValue");

        final BsonDocument primaryReadPreference = dispatcher.dispatch(BsonDocument.parse(
                "{\"find\":\"users\",\"$db\":\"app\",\"filter\":{},\"lsid\":{\"id\":\"session-2\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true,\"readPreference\":{\"mode\":\"primary\"}}"));
        assertEquals(1.0, primaryReadPreference.get("ok").asNumber().doubleValue());

        final BsonDocument commitWithReadPreference = dispatcher.dispatch(BsonDocument.parse(
                "{\"commitTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\"session-2\"},\"txnNumber\":1,\"autocommit\":false,\"readPreference\":{\"mode\":\"primary\"}}"));
        assertCommandError(commitWithReadPreference, "BadValue");
    }

    private static void assertCommandError(final BsonDocument response, final String codeName) {
        assertCommandError(response, 14, codeName);
    }

    private static void assertCommandError(final BsonDocument response, final int code, final String codeName) {
        assertEquals(0.0, response.get("ok").asNumber().doubleValue());
        assertEquals(code, response.getInt32("code").getValue());
        assertEquals(codeName, response.getString("codeName").getValue());
        assertNotNull(response.getString("errmsg"));
    }

    private static void assertNoSuchTransactionError(final BsonDocument response, final String... expectedLabels) {
        assertEquals(0.0, response.get("ok").asNumber().doubleValue());
        assertEquals(251, response.getInt32("code").getValue());
        assertEquals("NoSuchTransaction", response.getString("codeName").getValue());
        assertNotNull(response.getString("errmsg"));
        if (expectedLabels.length == 0) {
            assertTrue(!response.containsKey("errorLabels") || response.getArray("errorLabels").isEmpty());
            return;
        }

        assertEquals(expectedLabels.length, response.getArray("errorLabels").size());
        for (int index = 0; index < expectedLabels.length; index++) {
            assertEquals(
                    expectedLabels[index],
                    response.getArray("errorLabels").get(index).asString().getValue());
        }
    }

    private static void assertDuplicateKeyError(final BsonDocument response) {
        assertEquals(0.0, response.get("ok").asNumber().doubleValue());
        assertEquals(11000, response.getInt32("code").getValue());
        assertEquals("DuplicateKey", response.getString("codeName").getValue());
        assertNotNull(response.getString("errmsg"));
    }

    private static void assertCursorNotFoundError(final BsonDocument response, final long cursorId) {
        assertEquals(0.0, response.get("ok").asNumber().doubleValue());
        assertEquals(43, response.getInt32("code").getValue());
        assertEquals("CursorNotFound", response.getString("codeName").getValue());
        assertTrue(response.getString("errmsg").getValue().contains(Long.toString(cursorId)));
    }

    private static final class RecordingStore implements CommandStore {
        private String lastInsertDatabase;
        private String lastInsertCollection;
        private List<BsonDocument> lastInsertedDocuments = List.of();

        private String lastFindDatabase;
        private String lastFindCollection;
        private BsonDocument lastFindFilter;
        private List<BsonDocument> findResult = List.of();

        private String lastUpdateDatabase;
        private String lastUpdateCollection;
        private List<UpdateRequest> lastUpdateRequests = List.of();
        private UpdateResult updateResult = new UpdateResult(0, 0);

        private String lastCreateIndexesDatabase;
        private String lastCreateIndexesCollection;
        private List<IndexRequest> lastCreateIndexesRequests = List.of();
        private CreateIndexesResult createIndexesResult = new CreateIndexesResult(0, 0);

        private String lastListIndexesDatabase;
        private String lastListIndexesCollection;
        private List<IndexMetadata> listIndexesResult = List.of();

        private String lastDeleteDatabase;
        private String lastDeleteCollection;
        private List<DeleteRequest> lastDeleteRequests = List.of();
        private int deleteResult = 0;

        @Override
        public int insert(final String database, final String collection, final List<BsonDocument> documents) {
            lastInsertDatabase = database;
            lastInsertCollection = collection;
            lastInsertedDocuments = List.copyOf(documents);
            return documents.size();
        }

        @Override
        public List<BsonDocument> find(final String database, final String collection, final BsonDocument filter) {
            lastFindDatabase = database;
            lastFindCollection = collection;
            lastFindFilter = filter;
            return new ArrayList<>(findResult);
        }

        @Override
        public CreateIndexesResult createIndexes(
                final String database, final String collection, final List<IndexRequest> indexes) {
            lastCreateIndexesDatabase = database;
            lastCreateIndexesCollection = collection;
            lastCreateIndexesRequests = List.copyOf(indexes);
            return createIndexesResult;
        }

        @Override
        public List<IndexMetadata> listIndexes(final String database, final String collection) {
            lastListIndexesDatabase = database;
            lastListIndexesCollection = collection;
            return List.copyOf(listIndexesResult);
        }

        @Override
        public UpdateResult update(final String database, final String collection, final List<UpdateRequest> updates) {
            lastUpdateDatabase = database;
            lastUpdateCollection = collection;
            lastUpdateRequests = List.copyOf(updates);
            return updateResult;
        }

        @Override
        public int delete(final String database, final String collection, final List<DeleteRequest> deletes) {
            lastDeleteDatabase = database;
            lastDeleteCollection = collection;
            lastDeleteRequests = List.copyOf(deletes);
            return deleteResult;
        }
    }
}
