package org.jongodb.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.jongodb.engine.InMemoryEngineStore;
import org.junit.jupiter.api.Test;

class WriteErrorCompatibilityTest {
    @Test
    void incPromotesOverflowingInt32ToLong() {
        final CommandDispatcher dispatcher = dispatcher();
        insertNumber(dispatcher, "nums", new BsonInt32(Integer.MAX_VALUE));
        applyNumericUpdate(dispatcher, "nums", new BsonDocument("$inc", new BsonDocument("n", new BsonInt32(1))));
        assertEquals(2147483648L, readNumber(dispatcher, "nums").asInt64().getValue());

        insertNumber(dispatcher, "floor", new BsonInt32(Integer.MIN_VALUE));
        applyNumericUpdate(dispatcher, "floor", new BsonDocument("$inc", new BsonDocument("n", new BsonInt32(-1))));
        assertEquals(-2147483649L, readNumber(dispatcher, "floor").asInt64().getValue());

        insertNumber(dispatcher, "fits", new BsonInt32(10));
        applyNumericUpdate(dispatcher, "fits", new BsonDocument("$inc", new BsonDocument("n", new BsonInt32(1))));
        assertEquals(11, readNumber(dispatcher, "fits").asInt32().getValue());

        insertNumber(dispatcher, "wide", new BsonInt32(10));
        applyNumericUpdate(dispatcher, "wide", new BsonDocument("$inc", new BsonDocument("n", new BsonInt64(1))));
        assertEquals(11L, readNumber(dispatcher, "wide").asInt64().getValue());
    }

    @Test
    void mulPromotesOverflowingInt32ToLong() {
        final CommandDispatcher dispatcher = dispatcher();
        insertNumber(dispatcher, "nums", new BsonInt32(Integer.MAX_VALUE));
        applyNumericUpdate(dispatcher, "nums", new BsonDocument("$mul", new BsonDocument("n", new BsonInt32(2))));
        assertEquals(4294967294L, readNumber(dispatcher, "nums").asInt64().getValue());

        insertNumber(dispatcher, "fits", new BsonInt32(10));
        applyNumericUpdate(dispatcher, "fits", new BsonDocument("$mul", new BsonDocument("n", new BsonInt32(3))));
        assertEquals(30, readNumber(dispatcher, "fits").asInt32().getValue());
    }

    @Test
    void unorderedInsertReportsDuplicateAndContinues() {
        final CommandDispatcher dispatcher = dispatcher();
        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                """
                {"insert":"items","$db":"app","ordered":false,"documents":[{"_id":1},{"_id":1},{"_id":2}]}
                """));
        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        assertEquals(2, response.getInt32("n").getValue());
        final BsonDocument writeError = response.getArray("writeErrors").get(0).asDocument();
        assertEquals(1, writeError.getInt32("index").getValue());
        assertEquals(11000, writeError.getInt32("code").getValue());
        assertEquals(List.of(1, 2), insertedIds(dispatcher, "items"));
    }

    @Test
    void orderedInsertStopsAtFirstDuplicateInWriteErrors() {
        final CommandDispatcher dispatcher = dispatcher();
        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                """
                {"insert":"items","$db":"app","ordered":true,"documents":[{"_id":1},{"_id":1},{"_id":2}]}
                """));
        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
        assertEquals(1, response.getInt32("n").getValue());
        assertEquals(1, response.getArray("writeErrors").get(0).asDocument().getInt32("index").getValue());
        assertEquals(11000, response.getArray("writeErrors").get(0).asDocument().getInt32("code").getValue());
        assertFalse(response.containsKey("code"));
        assertEquals(List.of(1), insertedIds(dispatcher, "items"));
    }

    @Test
    void createIndexesSameSpecIsNoOpAndConflictsUseMeasuredCodes() {
        final CommandDispatcher dispatcher = dispatcher();
        final BsonDocument created = createIndexes(dispatcher, "items", "[{\"name\":\"a_1\",\"key\":{\"a\":1}}]");
        assertEquals(1.0, created.get("ok").asNumber().doubleValue());
        final int afterCreate = created.getInt32("numIndexesAfter").getValue();

        final BsonDocument again = createIndexes(dispatcher, "items", "[{\"name\":\"a_1\",\"key\":{\"a\":1}}]");
        assertEquals(1.0, again.get("ok").asNumber().doubleValue());
        assertEquals(afterCreate, again.getInt32("numIndexesAfter").getValue());

        final BsonDocument sameNameOtherKey =
                createIndexes(dispatcher, "items", "[{\"name\":\"a_1\",\"key\":{\"a\":-1}}]");
        assertCommandError(sameNameOtherKey, 86, "IndexKeySpecsConflict");
        assertTrue(sameNameOtherKey
                .getString("errmsg")
                .getValue()
                .startsWith("An existing index has the same name as the requested index."));

        final BsonDocument otherName = createIndexes(dispatcher, "items", "[{\"name\":\"a_plain\",\"key\":{\"a\":1}}]");
        assertCommandError(otherName, 85, "IndexOptionsConflict");
        assertEquals(
                "Index already exists with a different name: a_1", otherName.getString("errmsg").getValue());

        final BsonDocument ttl = createIndexes(
                dispatcher, "expires", "[{\"name\":\"at_1\",\"key\":{\"at\":1},\"expireAfterSeconds\":10}]");
        assertEquals(1.0, ttl.get("ok").asNumber().doubleValue());
        final BsonDocument ttlChange = createIndexes(
                dispatcher, "expires", "[{\"name\":\"at_1\",\"key\":{\"at\":1},\"expireAfterSeconds\":20}]");
        assertCommandError(ttlChange, 85, "IndexOptionsConflict");
        assertTrue(ttlChange
                .getString("errmsg")
                .getValue()
                .startsWith("An equivalent index already exists with the same name but different options."));

        final BsonDocument uniqueChange =
                createIndexes(dispatcher, "items", "[{\"name\":\"a_1\",\"key\":{\"a\":1},\"unique\":true}]");
        assertCommandError(uniqueChange, 86, "IndexKeySpecsConflict");

        final BsonDocument uniqueNamed = createIndexes(
                dispatcher, "mixed", "[{\"name\":\"a_unique\",\"key\":{\"a\":1},\"unique\":true}]");
        assertEquals(1.0, uniqueNamed.get("ok").asNumber().doubleValue());
        final BsonDocument plainNamed =
                createIndexes(dispatcher, "mixed", "[{\"name\":\"a_plain\",\"key\":{\"a\":1}}]");
        assertEquals(1.0, plainNamed.get("ok").asNumber().doubleValue());

        final BsonDocument batch = createIndexes(
                dispatcher,
                "batch",
                "[{\"name\":\"a_1\",\"key\":{\"a\":1}},{\"name\":\"a_1\",\"key\":{\"b\":1}}]");
        assertCommandError(batch, 86, "IndexKeySpecsConflict");
        assertEquals(List.of(), indexNames(dispatcher, "batch"));
    }

    @Test
    void uniqueIndexCollidesInt32AndInt64Keys() {
        final CommandDispatcher dispatcher = dispatcher();
        final BsonDocument created = createIndexes(
                dispatcher, "items", "[{\"name\":\"n_1\",\"key\":{\"n\":1},\"unique\":true}]");
        assertEquals(1.0, created.get("ok").asNumber().doubleValue());

        final BsonDocument first = insertFields(
                dispatcher, "items", new BsonDocument("_id", new BsonInt32(1)).append("n", new BsonInt32(1)));
        assertEquals(1.0, first.get("ok").asNumber().doubleValue());
        final BsonDocument collision = insertFields(
                dispatcher, "items", new BsonDocument("_id", new BsonInt32(2)).append("n", new BsonInt64(1)));
        assertEquals(1.0, collision.get("ok").asNumber().doubleValue());
        assertEquals(0, collision.getInt32("n").getValue());
        assertEquals(11000, collision.getArray("writeErrors").get(0).asDocument().getInt32("code").getValue());

        final BsonDocument nestedFirst = insertFields(
                dispatcher,
                "nested",
                new BsonDocument("_id", new BsonInt32(1))
                        .append("n", new BsonDocument("v", new BsonInt32(1))));
        assertEquals(1.0, createIndexes(
                        dispatcher, "nested", "[{\"name\":\"n_1\",\"key\":{\"n\":1},\"unique\":true}]")
                .get("ok")
                .asNumber()
                .doubleValue());
        assertEquals(1.0, nestedFirst.get("ok").asNumber().doubleValue());
        final BsonDocument nestedCollision = insertFields(
                dispatcher,
                "nested",
                new BsonDocument("_id", new BsonInt32(2))
                        .append("n", new BsonDocument("v", new BsonInt64(1))));
        assertEquals(11000, nestedCollision.getArray("writeErrors").get(0).asDocument().getInt32("code").getValue());
    }

    @Test
    void updateCommitWriteConflictCarriesTransientTransactionErrorLabel() {
        final CommandDispatcher dispatcher = dispatcher();
        dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"n\":0}]}"));
        final BsonDocument first = dispatcher.dispatch(BsonDocument.parse(
                """
                {"update":"users","$db":"app","updates":[{"q":{"_id":1},"u":{"$set":{"n":1}}}],
                 "lsid":{"id":"session-a"},"txnNumber":1,"autocommit":false,"startTransaction":true}
                """));
        assertEquals(1.0, first.get("ok").asNumber().doubleValue());
        final BsonDocument second = dispatcher.dispatch(BsonDocument.parse(
                """
                {"update":"users","$db":"app","updates":[{"q":{"_id":1},"u":{"$set":{"n":2}}}],
                 "lsid":{"id":"session-b"},"txnNumber":1,"autocommit":false,"startTransaction":true}
                """));
        assertEquals(1.0, second.get("ok").asNumber().doubleValue());
        assertEquals(
                1.0,
                dispatcher.dispatch(BsonDocument.parse(
                                "{\"commitTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\"session-a\"},\"txnNumber\":1,\"autocommit\":false}"))
                        .get("ok")
                        .asNumber()
                        .doubleValue());
        final BsonDocument conflict = dispatcher.dispatch(BsonDocument.parse(
                "{\"commitTransaction\":1,\"$db\":\"app\",\"lsid\":{\"id\":\"session-b\"},\"txnNumber\":1,\"autocommit\":false}"));
        assertCommandError(conflict, 112, "WriteConflict");
        assertEquals(
                "TransientTransactionError",
                conflict.getArray("errorLabels").get(0).asString().getValue());
    }

    private static CommandDispatcher dispatcher() {
        return new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));
    }

    private static void insertNumber(
            final CommandDispatcher dispatcher, final String collection, final BsonValue number) {
        final BsonDocument response = insertFields(
                dispatcher, collection, new BsonDocument("_id", new BsonInt32(1)).append("n", number));
        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
    }

    private static BsonDocument insertFields(
            final CommandDispatcher dispatcher, final String collection, final BsonDocument document) {
        return dispatcher.dispatch(new BsonDocument("insert", new BsonString(collection))
                .append("$db", new BsonString("app"))
                .append("documents", new BsonArray(List.of(document))));
    }

    private static void applyNumericUpdate(
            final CommandDispatcher dispatcher, final String collection, final BsonDocument update) {
        final BsonDocument spec = new BsonDocument("q", new BsonDocument("_id", new BsonInt32(1))).append("u", update);
        final BsonDocument response = dispatcher.dispatch(new BsonDocument("update", new BsonString(collection))
                .append("$db", new BsonString("app"))
                .append("updates", new BsonArray(List.of(spec))));
        assertEquals(1.0, response.get("ok").asNumber().doubleValue());
    }

    private static BsonValue readNumber(final CommandDispatcher dispatcher, final String collection) {
        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"find\":\"" + collection + "\",\"$db\":\"app\",\"filter\":{\"_id\":1}}"));
        return response.getDocument("cursor").getArray("firstBatch").get(0).asDocument().get("n");
    }

    private static List<Integer> insertedIds(final CommandDispatcher dispatcher, final String collection) {
        final BsonDocument response = dispatcher.dispatch(
                BsonDocument.parse("{\"find\":\"" + collection + "\",\"$db\":\"app\",\"filter\":{}}"));
        final List<Integer> ids = new ArrayList<>();
        for (final BsonValue value : response.getDocument("cursor").getArray("firstBatch")) {
            ids.add(value.asDocument().getInt32("_id").getValue());
        }
        ids.sort(Integer::compareTo);
        return ids;
    }

    private static BsonDocument createIndexes(
            final CommandDispatcher dispatcher, final String collection, final String indexesJson) {
        return dispatcher.dispatch(BsonDocument.parse(
                "{\"createIndexes\":\"" + collection + "\",\"$db\":\"app\",\"indexes\":" + indexesJson + "}"));
    }

    private static List<String> indexNames(final CommandDispatcher dispatcher, final String collection) {
        final BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
                "{\"listIndexes\":\"" + collection + "\",\"$db\":\"app\"}"));
        final List<String> names = new ArrayList<>();
        for (final BsonValue value : response.getDocument("cursor").getArray("firstBatch")) {
            names.add(value.asDocument().getString("name").getValue());
        }
        return names;
    }

    private static void assertCommandError(final BsonDocument response, final int code, final String codeName) {
        assertEquals(0.0, response.get("ok").asNumber().doubleValue());
        assertEquals(code, response.getInt32("code").getValue());
        assertEquals(codeName, response.getString("codeName").getValue());
    }
}
