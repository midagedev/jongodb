package org.jongodb.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.Decimal128;
import org.jongodb.engine.InMemoryEngineStore;
import org.junit.jupiter.api.Test;

class FindProjectionSortTest {
    @Test
    void findMatchesNumericValuesAcrossIntLongDoubleAndDecimal() {
        final CommandDispatcher dispatcher = dispatcher();
        final BsonArray documents = new BsonArray();
        documents.add(new BsonDocument("_id", new BsonInt32(1)).append("l", new BsonInt64(1)));
        documents.add(new BsonDocument("_id", new BsonInt32(2)).append("l", new BsonDouble(1.0)));
        documents.add(new BsonDocument("_id", new BsonInt32(3)).append("l", new BsonDecimal128(Decimal128.parse("1"))));
        documents.add(new BsonDocument("_id", new BsonInt32(4)).append("l", new BsonInt32(2)));
        dispatcher.dispatch(new BsonDocument("insert", new BsonString("items"))
                .append("$db", new BsonString("app"))
                .append("documents", documents));

        assertEquals(List.of(1, 2, 3), ids(find(dispatcher, "items", "{\"l\":1}")));
        assertEquals(List.of(4), ids(find(dispatcher, "items", "{\"l\":{\"$ne\":1}}")));
        assertEquals(List.of(1, 2, 3), ids(find(dispatcher, "items", "{\"l\":{\"$in\":[1]}}")));
        assertEquals(List.of(4), ids(find(dispatcher, "items", "{\"l\":{\"$nin\":[1]}}")));

        final BsonArray matched = batch(find(dispatcher, "items", "{\"l\":1}"));
        assertTrue(matched.get(0).asDocument().get("l").isInt64());
        assertTrue(matched.get(1).asDocument().get("l").isDouble());
        assertTrue(matched.get(2).asDocument().get("l").isDecimal128());
    }

    @Test
    void findProjectionAppliesInclusionExclusionSliceElemMatchAndPositional() {
        final CommandDispatcher dispatcher = dispatcher();
        dispatcher.dispatch(BsonDocument.parse(
                """
                {"insert":"orders","$db":"app","documents":[
                  {"_id":1,"name":"ada","secret":"x","profile":{"city":"seoul","zip":"1"},
                   "tags":["a","b","c","d"],
                   "scores":[{"n":1,"k":"a"},{"n":5,"k":"b"},{"n":3,"k":"c"}]},
                  {"_id":2,"name":"bea","secret":"y","profile":{"city":"busan","zip":"2"},
                   "tags":["e"],"scores":[{"n":0,"k":"z"}]}
                ]}
                """));

        final BsonDocument nameOnly = one(dispatch(
                dispatcher,
                "{\"find\":\"orders\",\"$db\":\"app\",\"filter\":{\"_id\":1},\"projection\":{\"name\":1}}"));
        assertEquals(List.of("_id", "name"), new ArrayList<>(nameOnly.keySet()));
        assertEquals("ada", nameOnly.getString("name").getValue());

        final BsonDocument withoutId = one(dispatch(
                dispatcher,
                "{\"find\":\"orders\",\"$db\":\"app\",\"filter\":{\"_id\":1},\"projection\":{\"name\":1,\"_id\":0}}"));
        assertEquals(List.of("name"), new ArrayList<>(withoutId.keySet()));

        final BsonDocument excluded = one(dispatch(
                dispatcher,
                "{\"find\":\"orders\",\"$db\":\"app\",\"filter\":{\"_id\":1},\"projection\":{\"secret\":0}}"));
        assertFalse(excluded.containsKey("secret"));
        assertEquals("ada", excluded.getString("name").getValue());
        assertEquals("seoul", excluded.getDocument("profile").getString("city").getValue());
        assertEquals(List.of("_id", "name", "profile", "tags", "scores"), new ArrayList<>(excluded.keySet()));

        final BsonDocument dotted = one(dispatch(
                dispatcher,
                "{\"find\":\"orders\",\"$db\":\"app\",\"filter\":{\"_id\":1},\"projection\":{\"profile.city\":1,\"_id\":0}}"));
        assertEquals(List.of("profile"), new ArrayList<>(dotted.keySet()));
        assertEquals(List.of("city"), new ArrayList<>(dotted.getDocument("profile").keySet()));
        assertEquals("seoul", dotted.getDocument("profile").getString("city").getValue());

        final BsonDocument dottedScores = one(dispatch(
                dispatcher,
                "{\"find\":\"orders\",\"$db\":\"app\",\"filter\":{\"_id\":1},\"projection\":{\"scores.n\":1,\"_id\":0}}"));
        final BsonArray projectedScores = dottedScores.getArray("scores");
        assertEquals(3, projectedScores.size());
        assertEquals(1, projectedScores.get(0).asDocument().getInt32("n").getValue());
        assertFalse(projectedScores.get(0).asDocument().containsKey("k"));

        final BsonDocument dottedExclude = one(dispatch(
                dispatcher,
                "{\"find\":\"orders\",\"$db\":\"app\",\"filter\":{\"_id\":1},\"projection\":{\"profile.zip\":0}}"));
        assertFalse(dottedExclude.getDocument("profile").containsKey("zip"));
        assertEquals("seoul", dottedExclude.getDocument("profile").getString("city").getValue());
        assertTrue(dottedExclude.containsKey("secret"));

        final BsonDocument sliced = one(dispatch(
                dispatcher,
                "{\"find\":\"orders\",\"$db\":\"app\",\"filter\":{\"_id\":1},\"projection\":{\"tags\":{\"$slice\":2}}}"));
        assertEquals(List.of("a", "b"), strings(sliced.getArray("tags")));
        assertTrue(sliced.containsKey("secret"));
        assertTrue(sliced.containsKey("_id"));

        final BsonDocument tail = one(dispatch(
                dispatcher,
                "{\"find\":\"orders\",\"$db\":\"app\",\"filter\":{\"_id\":1},\"projection\":{\"tags\":{\"$slice\":-2},\"_id\":0}}"));
        assertEquals(List.of("c", "d"), strings(tail.getArray("tags")));
        assertTrue(tail.containsKey("name"));

        final BsonDocument window = one(dispatch(
                dispatcher,
                "{\"find\":\"orders\",\"$db\":\"app\",\"filter\":{\"_id\":1},\"projection\":{\"tags\":{\"$slice\":[1,2]},\"_id\":0}}"));
        assertEquals(List.of("b", "c"), strings(window.getArray("tags")));

        final BsonDocument fromEnd = one(dispatch(
                dispatcher,
                "{\"find\":\"orders\",\"$db\":\"app\",\"filter\":{\"_id\":1},\"projection\":{\"tags\":{\"$slice\":[-3,2]},\"_id\":0}}"));
        assertEquals(List.of("b", "c"), strings(fromEnd.getArray("tags")));

        final BsonDocument sliceInclusion = one(dispatch(
                dispatcher,
                "{\"find\":\"orders\",\"$db\":\"app\",\"filter\":{\"_id\":1},\"projection\":{\"_id\":1,\"tags\":{\"$slice\":2}}}"));
        assertEquals(List.of("_id", "tags"), new ArrayList<>(sliceInclusion.keySet()));
        assertEquals(List.of("a", "b"), strings(sliceInclusion.getArray("tags")));

        final BsonDocument withName = one(dispatch(
                dispatcher,
                "{\"find\":\"orders\",\"$db\":\"app\",\"filter\":{\"_id\":1},\"projection\":{\"name\":1,\"tags\":{\"$slice\":1}}}"));
        assertEquals(List.of("_id", "name", "tags"), new ArrayList<>(withName.keySet()));
        assertEquals(List.of("a"), strings(withName.getArray("tags")));

        final BsonDocument elem = one(dispatch(
                dispatcher,
                "{\"find\":\"orders\",\"$db\":\"app\",\"filter\":{\"_id\":1},\"projection\":{\"scores\":{\"$elemMatch\":{\"n\":{\"$gt\":3}}}}}"));
        assertEquals(List.of("_id", "scores"), new ArrayList<>(elem.keySet()));
        assertEquals(1, elem.getArray("scores").size());
        assertEquals(5, elem.getArray("scores").get(0).asDocument().getInt32("n").getValue());
        assertEquals("b", elem.getArray("scores").get(0).asDocument().getString("k").getValue());

        final BsonDocument positional = one(dispatch(
                dispatcher,
                "{\"find\":\"orders\",\"$db\":\"app\",\"filter\":{\"scores.n\":5},\"projection\":{\"scores.$\":1}}"));
        assertEquals(1, positional.getInt32("_id").getValue());
        assertEquals(List.of("_id", "scores"), new ArrayList<>(positional.keySet()));
        assertEquals(5, positional.getArray("scores").get(0).asDocument().getInt32("n").getValue());

        final BsonDocument full = one(dispatch(
                dispatcher, "{\"find\":\"orders\",\"$db\":\"app\",\"filter\":{\"_id\":1},\"projection\":{}}"));
        assertTrue(full.containsKey("secret"));

        final BsonDocument first = dispatch(
                dispatcher,
                "{\"find\":\"orders\",\"$db\":\"app\",\"filter\":{},\"projection\":{\"name\":1,\"_id\":0},\"sort\":{\"_id\":1},\"batchSize\":1}");
        assertEquals("ada", first.getDocument("cursor").getArray("firstBatch").get(0).asDocument().getString("name").getValue());
        assertFalse(first.getDocument("cursor").getArray("firstBatch").get(0).asDocument().containsKey("secret"));
        final long cursorId = first.getDocument("cursor").getInt64("id").getValue();
        assertTrue(cursorId > 0);
        final BsonDocument more = dispatcher.dispatch(new BsonDocument("getMore", new BsonInt64(cursorId))
                .append("collection", new BsonString("orders"))
                .append("$db", new BsonString("app")));
        final BsonDocument second = more.getDocument("cursor").getArray("nextBatch").get(0).asDocument();
        assertEquals("bea", second.getString("name").getValue());
        assertFalse(second.containsKey("secret"));
    }

    @Test
    void findProjectionRejectsMixedModesWithLocation31253And31254() {
        final CommandDispatcher dispatcher = dispatcher();
        dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"orders\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"ada\",\"secret\":\"x\"}]}"));

        final BsonDocument exclusionInInclusion = dispatch(
                dispatcher,
                "{\"find\":\"orders\",\"$db\":\"app\",\"filter\":{},\"projection\":{\"name\":1,\"secret\":0}}");
        assertEquals(0.0, exclusionInInclusion.getDouble("ok").getValue());
        assertEquals(31254, exclusionInInclusion.getInt32("code").getValue());
        assertEquals("Location31254", exclusionInInclusion.getString("codeName").getValue());
        assertEquals(
                "Cannot do exclusion on field secret in inclusion projection",
                exclusionInInclusion.getString("errmsg").getValue());

        final BsonDocument inclusionInExclusion = dispatch(
                dispatcher,
                "{\"find\":\"orders\",\"$db\":\"app\",\"filter\":{},\"projection\":{\"secret\":0,\"name\":1}}");
        assertEquals(31253, inclusionInExclusion.getInt32("code").getValue());
        assertEquals("Location31253", inclusionInExclusion.getString("codeName").getValue());
        assertEquals(
                "Cannot do inclusion on field name in exclusion projection",
                inclusionInExclusion.getString("errmsg").getValue());

        final BsonDocument missingPositional = one(dispatch(
                dispatcher,
                "{\"find\":\"orders\",\"$db\":\"app\",\"filter\":{},\"projection\":{\"scores.$\":1}}"));
        assertEquals(List.of("_id"), new ArrayList<>(missingPositional.keySet()));

        dispatcher.dispatch(BsonDocument.parse(
                "{\"insert\":\"rows\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"scores\":\"nope\"},{\"_id\":2,\"scores\":[]}]}"));
        final BsonDocument scalarPositional = one(dispatch(
                dispatcher,
                "{\"find\":\"rows\",\"$db\":\"app\",\"filter\":{\"_id\":1},\"projection\":{\"scores.$\":1}}"));
        assertEquals("nope", scalarPositional.getString("scores").getValue());
        final BsonDocument emptyPositional = dispatch(
                dispatcher,
                "{\"find\":\"rows\",\"$db\":\"app\",\"filter\":{\"_id\":2},\"projection\":{\"scores.$\":1}}");
        assertEquals(51246, emptyPositional.getInt32("code").getValue());
        assertEquals("Location51246", emptyPositional.getString("codeName").getValue());
        assertEquals(
                "positional operator '.$' couldn't find a matching element in the array",
                emptyPositional.getString("errmsg").getValue());

        final BsonDocument notDocument = dispatch(
                dispatcher, "{\"find\":\"orders\",\"$db\":\"app\",\"filter\":{},\"projection\":1}");
        assertEquals("TypeMismatch", notDocument.getString("codeName").getValue());
    }

    @Test
    void findSortTreatsNullAndMissingAsEqualAndSortsArraysByMinOrMax() {
        final CommandDispatcher dispatcher = dispatcher();
        dispatcher.dispatch(BsonDocument.parse(
                """
                {"insert":"items","$db":"app","documents":[
                  {"_id":1,"a":null},
                  {"_id":2},
                  {"_id":3,"a":[]},
                  {"_id":4,"a":[3,1]},
                  {"_id":5,"a":2},
                  {"_id":6,"a":[4,0]}
                ]}
                """));

        assertEquals(List.of(3, 1, 2, 6, 4, 5), ids(dispatch(
                dispatcher, "{\"find\":\"items\",\"$db\":\"app\",\"filter\":{},\"sort\":{\"a\":1}}")));
        assertEquals(List.of(6, 4, 5, 1, 2, 3), ids(dispatch(
                dispatcher, "{\"find\":\"items\",\"$db\":\"app\",\"filter\":{},\"sort\":{\"a\":-1}}")));

        dispatcher.dispatch(BsonDocument.parse(
                """
                {"insert":"scores","$db":"app","documents":[
                  {"_id":1,"scores":[{"n":9},{"n":1}]},
                  {"_id":2,"scores":[{"n":4}]},
                  {"_id":3,"scores":[{"n":0},{"n":8}]}
                ]}
                """));
        assertEquals(List.of(3, 1, 2), ids(dispatch(
                dispatcher, "{\"find\":\"scores\",\"$db\":\"app\",\"filter\":{},\"sort\":{\"scores.n\":1}}")));
        assertEquals(List.of(1, 3, 2), ids(dispatch(
                dispatcher, "{\"find\":\"scores\",\"$db\":\"app\",\"filter\":{},\"sort\":{\"scores.n\":-1}}")));
    }

    private static CommandDispatcher dispatcher() {
        return new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));
    }

    private static BsonDocument dispatch(final CommandDispatcher dispatcher, final String json) {
        return dispatcher.dispatch(BsonDocument.parse(json));
    }

    private static BsonDocument find(final CommandDispatcher dispatcher, final String collection, final String filter) {
        return dispatch(
                dispatcher,
                "{\"find\":\"" + collection + "\",\"$db\":\"app\",\"filter\":" + filter + "}");
    }

    private static BsonArray batch(final BsonDocument response) {
        assertEquals(1.0, response.getDouble("ok").getValue(), response.toJson());
        return response.getDocument("cursor").getArray("firstBatch");
    }

    private static BsonDocument one(final BsonDocument response) {
        final BsonArray documents = batch(response);
        assertEquals(1, documents.size(), response.toJson());
        return documents.get(0).asDocument();
    }

    private static List<Integer> ids(final BsonDocument response) {
        final List<Integer> ids = new ArrayList<>();
        for (final BsonValue value : batch(response)) {
            ids.add(value.asDocument().getInt32("_id").getValue());
        }
        return ids;
    }

    private static List<String> strings(final BsonArray values) {
        final List<String> strings = new ArrayList<>();
        for (final BsonValue value : values) {
            strings.add(value.asString().getValue());
        }
        return strings;
    }
}
