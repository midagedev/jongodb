package org.jongodb.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.jongodb.engine.InMemoryEngineStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AdminCommandsE2ETest {
    private static final String LSID = "{\"id\":{\"$binary\":{\"base64\":\"AAAAAAAAAAAAAAAAAAAAAA==\",\"subType\":\"04\"}}}";

    private CommandDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore()));
    }

    private BsonDocument run(final String json) {
        return dispatcher.dispatch(BsonDocument.parse(json));
    }

    private static void assertError(final BsonDocument response, final String codeName) {
        assertEquals(0.0, response.get("ok").asNumber().doubleValue(), response.toJson());
        assertEquals(codeName, response.getString("codeName").getValue(), response.toJson());
    }

    private static void assertOk(final BsonDocument response) {
        assertEquals(1.0, response.get("ok").asNumber().doubleValue(), response.toJson());
    }

    private void insert(final String db, final String collection, final String documents) {
        assertOk(run("{\"insert\":\"" + collection + "\",\"$db\":\"" + db + "\",\"documents\":" + documents + "}"));
    }

    private List<String> collectionNames(final String db) {
        final List<String> names = new ArrayList<>();
        for (final BsonValue entry : run("{\"listCollections\":1,\"$db\":\"" + db + "\"}")
                .getDocument("cursor")
                .getArray("firstBatch")) {
            names.add(entry.asDocument().getString("name").getValue());
        }
        return names;
    }

    @Test
    void listDatabasesReportsSizesAndHonorsNameOnlyAndFilter() {
        insert("app", "c", "[{\"_id\":1,\"a\":\"x\"}]");
        assertOk(run("{\"create\":\"empty\",\"$db\":\"other\"}"));

        final BsonDocument all = run("{\"listDatabases\":1,\"$db\":\"admin\"}");
        assertOk(all);
        final BsonArray databases = all.getArray("databases");
        assertEquals(2, databases.size());
        final BsonDocument app = databases.get(0).asDocument();
        assertEquals("app", app.getString("name").getValue());
        assertTrue(app.get("sizeOnDisk").asNumber().longValue() > 0);
        assertFalse(app.getBoolean("empty").getValue());
        assertTrue(databases.get(1).asDocument().getBoolean("empty").getValue());
        assertEquals(app.get("sizeOnDisk").asNumber().longValue(), all.get("totalSize").asNumber().longValue());

        final BsonDocument names = run("{\"listDatabases\":1,\"nameOnly\":true,\"$db\":\"admin\"}");
        assertEquals("{\"name\": \"app\"}", names.getArray("databases").get(0).asDocument().toJson());
        assertFalse(names.containsKey("totalSize"));

        final BsonDocument filtered = run("{\"listDatabases\":1,\"filter\":{\"name\":\"other\"},\"$db\":\"admin\"}");
        assertEquals(1, filtered.getArray("databases").size());
    }

    @Test
    void readsDoNotMaterializeCollectionsSoCreateStillSucceeds() {
        run("{\"find\":\"ghost\",\"$db\":\"app\",\"filter\":{}}");
        assertEquals(List.of(), collectionNames("app"));
        assertOk(run("{\"create\":\"ghost\",\"$db\":\"app\"}"));
        assertEquals(List.of("ghost"), collectionNames("app"));
    }

    @Test
    void createRejectsDuplicatesAndSemanticOptions() {
        assertOk(run("{\"create\":\"c\",\"$db\":\"app\",\"validator\":{\"a\":1},\"capped\":false}"));
        assertError(run("{\"create\":\"c\",\"$db\":\"app\"}"), "NamespaceExists");
        assertEquals(48, run("{\"create\":\"c\",\"$db\":\"app\"}").getInt32("code").getValue());
        assertError(run("{\"create\":\"cap\",\"$db\":\"app\",\"capped\":true,\"size\":100}"), "NotImplemented");
        assertError(run("{\"create\":\"ts\",\"$db\":\"app\",\"timeseries\":{\"timeField\":\"t\"}}"), "NotImplemented");
        assertError(run("{\"create\":\"cl\",\"$db\":\"app\",\"clusteredIndex\":{\"key\":{\"_id\":1},\"unique\":true}}"),
                "NotImplemented");
        assertError(run("{\"create\":\"bad$name\",\"$db\":\"app\"}"), "InvalidNamespace");
        assertEquals(List.of("c"), collectionNames("app"));
    }

    @Test
    void dropIndexesAcceptsNameStarArrayAndKeyDocument() {
        insert("app", "c", "[{\"_id\":1,\"a\":1,\"b\":1,\"d\":1}]");
        assertOk(run("{\"createIndexes\":\"c\",\"$db\":\"app\",\"indexes\":["
                + "{\"key\":{\"a\":1},\"name\":\"a_1\"},{\"key\":{\"b\":1},\"name\":\"b_1\"},{\"key\":{\"d\":1},\"name\":\"d_1\"}]}"));

        final BsonDocument byName = run("{\"dropIndexes\":\"c\",\"$db\":\"app\",\"index\":\"a_1\"}");
        assertOk(byName);
        assertEquals(4, byName.getInt32("nIndexesWas").getValue());

        assertOk(run("{\"dropIndexes\":\"c\",\"$db\":\"app\",\"index\":{\"b\":1}}"));
        assertEquals(2, indexNames("c").size());

        assertError(run("{\"dropIndexes\":\"c\",\"$db\":\"app\",\"index\":\"a_1\"}"), "IndexNotFound");
        assertEquals(27, run("{\"dropIndexes\":\"c\",\"$db\":\"app\",\"index\":\"a_1\"}").getInt32("code").getValue());
        assertError(run("{\"dropIndexes\":\"c\",\"$db\":\"app\",\"index\":\"_id_\"}"), "InvalidOptions");
        assertError(run("{\"dropIndexes\":\"c\",\"$db\":\"app\",\"index\":[\"d_1\",\"nope\"]}"), "IndexNotFound");
        assertEquals(2, indexNames("c").size());

        assertOk(run("{\"dropIndexes\":\"c\",\"$db\":\"app\",\"index\":[\"d_1\"]}"));
        assertOk(run("{\"createIndexes\":\"c\",\"$db\":\"app\",\"indexes\":[{\"key\":{\"a\":1},\"name\":\"a_1\"}]}"));
        final BsonDocument star = run("{\"dropIndexes\":\"c\",\"$db\":\"app\",\"index\":\"*\"}");
        assertOk(star);
        assertTrue(star.containsKey("msg"));
        assertEquals(List.of("_id_"), indexNames("c"));

        assertError(run("{\"dropIndexes\":\"missing\",\"$db\":\"app\",\"index\":\"*\"}"), "NamespaceNotFound");
    }

    private List<String> indexNames(final String collection) {
        final List<String> names = new ArrayList<>();
        for (final BsonValue entry : run("{\"listIndexes\":\"" + collection + "\",\"$db\":\"app\"}")
                .getDocument("cursor")
                .getArray("firstBatch")) {
            names.add(entry.asDocument().getString("name").getValue());
        }
        return names;
    }

    @Test
    void renameCollectionMovesDocumentsIndexesAndAcrossDatabases() {
        insert("app", "from", "[{\"_id\":1,\"a\":1}]");
        assertOk(run("{\"createIndexes\":\"from\",\"$db\":\"app\",\"indexes\":[{\"key\":{\"a\":1},\"name\":\"a_1\"}]}"));
        insert("app", "taken", "[{\"_id\":9}]");

        assertError(run("{\"renameCollection\":\"app.from\",\"to\":\"app.taken\",\"$db\":\"admin\"}"), "NamespaceExists");
        assertError(run("{\"renameCollection\":\"app.nope\",\"to\":\"app.x\",\"$db\":\"admin\"}"), "NamespaceNotFound");
        assertError(run("{\"renameCollection\":\"app.from\",\"to\":\"app.from\",\"$db\":\"admin\"}"), "IllegalOperation");

        assertOk(run("{\"renameCollection\":\"app.from\",\"to\":\"app.taken\",\"dropTarget\":true,\"$db\":\"admin\"}"));
        assertEquals(List.of("taken"), collectionNames("app"));
        final BsonDocument found = run("{\"find\":\"taken\",\"$db\":\"app\",\"filter\":{}}");
        assertEquals(1, found.getDocument("cursor").getArray("firstBatch").size());
        assertEquals(1, found.getDocument("cursor").getArray("firstBatch").get(0).asDocument().getInt32("_id").getValue());
        assertEquals(List.of("_id_", "a_1"), indexNames("taken"));

        assertOk(run("{\"renameCollection\":\"app.taken\",\"to\":\"archive.moved\",\"$db\":\"admin\"}"));
        assertEquals(List.of(), collectionNames("app"));
        assertEquals(List.of("moved"), collectionNames("archive"));
        assertEquals(1, run("{\"count\":\"moved\",\"$db\":\"archive\",\"query\":{}}").get("n").asNumber().intValue());
    }

    @Test
    void collModChangesTtlBehaviourAndIgnoresValidator() {
        final MutableClock clock = new MutableClock();
        dispatcher = new CommandDispatcher(new EngineBackedCommandStore(new InMemoryEngineStore(clock)));
        assertOk(run("{\"createIndexes\":\"c\",\"$db\":\"app\",\"indexes\":[{\"key\":{\"at\":1},\"name\":\"at_1\",\"expireAfterSeconds\":100}]}"));
        assertOk(run("{\"insert\":\"c\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"at\":{\"$date\":0}}]}"));

        clock.millis = 50_000;
        assertEquals(1, countAll());

        final BsonDocument mod = run("{\"collMod\":\"c\",\"$db\":\"app\",\"index\":{\"name\":\"at_1\",\"expireAfterSeconds\":10},"
                + "\"validator\":{\"x\":1},\"validationLevel\":\"strict\"}");
        assertOk(mod);
        assertEquals(100, mod.get("expireAfterSeconds_old").asNumber().intValue());
        assertEquals(10, mod.get("expireAfterSeconds_new").asNumber().intValue());
        assertEquals(0, countAll());

        assertOk(run("{\"collMod\":\"c\",\"$db\":\"app\",\"index\":{\"keyPattern\":{\"at\":1},\"expireAfterSeconds\":1000}}"));
        assertEquals(1000, ttlOf("at_1"));

        assertError(run("{\"collMod\":\"c\",\"$db\":\"app\",\"index\":{\"name\":\"nope\",\"expireAfterSeconds\":1}}"), "IndexNotFound");
        assertError(run("{\"collMod\":\"c\",\"$db\":\"app\",\"index\":{\"name\":\"_id_\",\"expireAfterSeconds\":1}}"), "BadValue");
        assertError(run("{\"collMod\":\"c\",\"$db\":\"app\",\"viewOn\":\"x\"}"), "NotImplemented");
        assertError(run("{\"collMod\":\"missing\",\"$db\":\"app\"}"), "NamespaceNotFound");
    }

    private long ttlOf(final String indexName) {
        for (final BsonValue entry : run("{\"listIndexes\":\"c\",\"$db\":\"app\"}").getDocument("cursor").getArray("firstBatch")) {
            if (entry.asDocument().getString("name").getValue().equals(indexName)) {
                return entry.asDocument().get("expireAfterSeconds").asNumber().longValue();
            }
        }
        throw new AssertionError("index not listed: " + indexName);
    }

    private int countAll() {
        return run("{\"count\":\"c\",\"$db\":\"app\",\"query\":{}}").get("n").asNumber().intValue();
    }

    @Test
    void statsCommandsReportCountsSizesAndErrors() {
        insert("app", "c", "[{\"_id\":1,\"a\":\"xxxx\"},{\"_id\":2,\"a\":\"yyyy\"}]");
        assertOk(run("{\"createIndexes\":\"c\",\"$db\":\"app\",\"indexes\":[{\"key\":{\"a\":1},\"name\":\"a_1\"}]}"));

        final BsonDocument coll = run("{\"collStats\":\"c\",\"$db\":\"app\"}");
        assertOk(coll);
        assertEquals("app.c", coll.getString("ns").getValue());
        assertEquals(2, coll.get("count").asNumber().intValue());
        assertEquals(2, coll.get("nindexes").asNumber().intValue());
        assertTrue(coll.get("size").asNumber().longValue() > 0);
        assertEquals(coll.get("size").asNumber().longValue() / coll.get("count").asNumber().longValue(),
                coll.get("avgObjSize").asNumber().longValue());
        final BsonDocument scaled = run("{\"collStats\":\"c\",\"scale\":2,\"$db\":\"app\"}");
        assertEquals(coll.get("size").asNumber().longValue() / 2, scaled.get("size").asNumber().longValue());

        final BsonDocument db = run("{\"dbStats\":1,\"$db\":\"app\"}");
        assertOk(db);
        assertEquals("app", db.getString("db").getValue());
        assertEquals(1, db.get("collections").asNumber().intValue());
        assertEquals(2, db.get("objects").asNumber().intValue());
        assertEquals(2, db.get("indexes").asNumber().intValue());
        assertEquals(coll.get("size").asNumber().longValue(), db.get("dataSize").asNumber().longValue());

        assertError(run("{\"collStats\":\"missing\",\"$db\":\"app\"}"), "NamespaceNotFound");

        final BsonDocument stage = run("{\"aggregate\":\"c\",\"$db\":\"app\",\"pipeline\":["
                + "{\"$collStats\":{\"count\":{},\"storageStats\":{}}}],\"cursor\":{}}");
        assertOk(stage);
        final BsonDocument row = stage.getDocument("cursor").getArray("firstBatch").get(0).asDocument();
        assertEquals(2, row.get("count").asNumber().intValue());
        assertEquals(2, row.getDocument("storageStats").get("count").asNumber().intValue());
        assertEquals("app.c", row.getString("ns").getValue());
    }

    @Test
    void serverDiagnosticsReturnExpectedShapes() {
        final BsonDocument status = run("{\"serverStatus\":1,\"$db\":\"admin\"}");
        assertOk(status);
        assertEquals("mongod", status.getString("process").getValue());
        assertTrue(status.get("pid").asNumber().longValue() > 0);
        assertTrue(status.get("uptime").asNumber().doubleValue() >= 0);
        assertTrue(status.getDocument("connections").get("available").asNumber().intValue() > 0);
        assertTrue(status.containsKey("localTime") && status.containsKey("version") && status.containsKey("host"));

        assertOk(run("{\"hostInfo\":1,\"$db\":\"admin\"}"));
        final BsonDocument conn = run("{\"connectionStatus\":1,\"$db\":\"admin\"}");
        assertOk(conn);
        assertEquals(0, conn.getDocument("authInfo").getArray("authenticatedUsers").size());
    }

    @Test
    void replSetGetStatusDependsOnTopologyProfile() {
        assertError(run("{\"replSetGetStatus\":1,\"$db\":\"admin\"}"), "NoReplicationEnabled");
        assertEquals(76, run("{\"replSetGetStatus\":1,\"$db\":\"admin\"}").getInt32("code").getValue());

        final CommandDispatcher replicaSet = new CommandDispatcher(
                new EngineBackedCommandStore(new InMemoryEngineStore()),
                TopologyProfile.SINGLE_NODE_REPLICA_SET,
                "127.0.0.1:27017",
                "rs-test");
        final BsonDocument status = replicaSet.dispatch(BsonDocument.parse("{\"replSetGetStatus\":1,\"$db\":\"admin\"}"));
        assertOk(status);
        assertEquals("rs-test", status.getString("set").getValue());
        assertEquals(1, status.getArray("members").size());
        assertEquals("PRIMARY", status.getArray("members").get(0).asDocument().getString("stateStr").getValue());
        assertEquals(1, status.getInt32("myState").getValue());
    }

    @Test
    void currentOpAndLocalSessionsAggregatesReturnEmptyCursors() {
        for (final String stage : List.of("{\"$currentOp\":{\"allUsers\":true}}", "{\"$listLocalSessions\":{}}")) {
            final BsonDocument response = run("{\"aggregate\":1,\"$db\":\"admin\",\"pipeline\":[" + stage
                    + ",{\"$match\":{\"ns\":\"x\"}}],\"cursor\":{}}");
            assertOk(response);
            assertEquals(0, response.getDocument("cursor").getArray("firstBatch").size());
            assertEquals(0L, response.getDocument("cursor").get("id").asNumber().longValue());
        }
        assertError(run("{\"aggregate\":1,\"$db\":\"admin\",\"pipeline\":[{\"$match\":{}}],\"cursor\":{}}"), "BadValue");
        final BsonDocument kill = run("{\"killOp\":1,\"op\":123,\"$db\":\"admin\"}");
        assertOk(kill);
    }

    @Test
    void endingOrKillingSessionAbortsItsTransaction() {
        int txnNumber = 0;
        for (final String command : List.of("endSessions", "killSessions")) {
            txnNumber++;
            final String collection = "t_" + command;
            assertOk(run("{\"insert\":\"" + collection + "\",\"$db\":\"app\",\"documents\":[{\"_id\":1}],\"lsid\":" + LSID
                    + ",\"txnNumber\":{\"$numberLong\":\"" + txnNumber + "\"},\"autocommit\":false,\"startTransaction\":true}"));
            assertOk(run("{\"" + command + "\":[" + LSID + "],\"$db\":\"admin\"}"));
            assertError(
                    run("{\"commitTransaction\":1,\"$db\":\"admin\",\"lsid\":" + LSID
                            + ",\"txnNumber\":{\"$numberLong\":\"" + txnNumber + "\"},\"autocommit\":false}"),
                    "NoSuchTransaction");
            assertEquals(0, run("{\"count\":\"" + collection + "\",\"$db\":\"app\",\"query\":{}}").get("n").asNumber().intValue());
            // a fresh session lifetime may reuse txnNumber 1 only after endSessions forgot the session
            if (command.equals("endSessions")) {
                assertOk(run("{\"insert\":\"" + collection + "\",\"$db\":\"app\",\"documents\":[{\"_id\":2}],\"lsid\":" + LSID
                        + ",\"txnNumber\":{\"$numberLong\":\"" + txnNumber + "\"},\"autocommit\":false,\"startTransaction\":true}"));
                assertOk(run("{\"killAllSessions\":[],\"$db\":\"admin\"}"));
                assertError(
                        run("{\"commitTransaction\":1,\"$db\":\"admin\",\"lsid\":" + LSID
                                + ",\"txnNumber\":{\"$numberLong\":\"" + txnNumber + "\"},\"autocommit\":false}"),
                        "NoSuchTransaction");
            }
        }
        assertOk(run("{\"refreshSessions\":[" + LSID + "],\"$db\":\"admin\"}"));
        assertError(run("{\"endSessions\":1,\"$db\":\"admin\"}"), "TypeMismatch");
    }

    @Test
    void explainReportsCollscanAndExecutionStatsWithoutMutating() {
        insert("app", "c", "[{\"_id\":1,\"a\":1},{\"_id\":2,\"a\":2},{\"_id\":3,\"a\":2}]");

        final BsonDocument planOnly = run("{\"explain\":{\"find\":\"c\",\"filter\":{\"a\":2}},\"verbosity\":\"queryPlanner\",\"$db\":\"app\"}");
        assertOk(planOnly);
        assertEquals("COLLSCAN", planOnly.getDocument("queryPlanner").getDocument("winningPlan").getString("stage").getValue());
        assertEquals("app.c", planOnly.getDocument("queryPlanner").getString("namespace").getValue());
        assertFalse(planOnly.containsKey("executionStats"));

        final BsonDocument find = run("{\"explain\":{\"find\":\"c\",\"filter\":{\"a\":2}},\"verbosity\":\"executionStats\",\"$db\":\"app\"}");
        assertEquals(2, find.getDocument("executionStats").get("nReturned").asNumber().intValue());
        assertEquals(3, find.getDocument("executionStats").get("totalDocsExamined").asNumber().intValue());

        final BsonDocument limited = run("{\"explain\":{\"find\":\"c\",\"filter\":{},\"limit\":1,\"sort\":{\"a\":1}},\"verbosity\":\"executionStats\",\"$db\":\"app\"}");
        assertEquals(1, limited.getDocument("executionStats").get("nReturned").asNumber().intValue());
        assertEquals("SORT", limited.getDocument("queryPlanner").getDocument("winningPlan").getString("stage").getValue());

        final BsonDocument aggregate = run("{\"explain\":{\"aggregate\":\"c\",\"pipeline\":[{\"$match\":{\"a\":2}}],\"cursor\":{}},\"verbosity\":\"executionStats\",\"$db\":\"app\"}");
        assertOk(aggregate);
        assertEquals(2, aggregate.getDocument("executionStats").get("nReturned").asNumber().intValue());

        final BsonDocument count = run("{\"explain\":{\"count\":\"c\",\"query\":{\"a\":1}},\"$db\":\"app\"}");
        assertEquals(1, count.getDocument("executionStats").get("nReturned").asNumber().intValue());

        final BsonDocument update = run("{\"explain\":{\"update\":\"c\",\"updates\":[{\"q\":{\"a\":2},\"u\":{\"$set\":{\"a\":9}},\"multi\":true}]},\"verbosity\":\"executionStats\",\"$db\":\"app\"}");
        assertOk(update);
        assertEquals("UPDATE", update.getDocument("queryPlanner").getDocument("winningPlan").getString("stage").getValue());
        assertEquals(2, update.getDocument("executionStats").getDocument("executionStages").get("nWouldModify").asNumber().intValue());

        final BsonDocument delete = run("{\"explain\":{\"delete\":\"c\",\"deletes\":[{\"q\":{},\"limit\":0}]},\"verbosity\":\"executionStats\",\"$db\":\"app\"}");
        assertEquals(3, delete.getDocument("executionStats").getDocument("executionStages").get("nWouldDelete").asNumber().intValue());

        final BsonDocument out = run("{\"explain\":{\"aggregate\":\"c\",\"pipeline\":[{\"$out\":\"copy\"}],\"cursor\":{}},\"$db\":\"app\"}");
        assertOk(out);
        assertEquals(List.of("c"), collectionNames("app"));

        assertEquals(3, run("{\"count\":\"c\",\"$db\":\"app\",\"query\":{\"a\":{\"$in\":[1,2]}}}").get("n").asNumber().intValue());
        assertError(run("{\"explain\":{\"frobnicate\":\"c\"},\"$db\":\"app\"}"), "CommandNotFound");
        assertError(run("{\"explain\":{\"find\":\"c\"},\"verbosity\":\"loud\",\"$db\":\"app\"}"), "BadValue");
    }

    private static final class MutableClock extends Clock {
        long millis;

        @Override
        public java.time.ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(final java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }

        @Override
        public long millis() {
            return millis;
        }
    }
}
