package org.jongodb.obs;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

class CommandJournalInvariantCheckerTest {
    @Test
    void detectsSessionTransactionCursorAndIndexInvariantViolations() {
        final CommandJournal journal = new CommandJournal(16);

        journal.record(
                CorrelationContext.builder("r1", "insert").txnNumber(1L).build(),
                BsonDocument.parse("{\"insert\":\"users\",\"$db\":\"app\",\"txnNumber\":1,\"autocommit\":false}"),
                BsonDocument.parse("{\"ok\":0.0,\"errmsg\":\"lsid must be a document\"}"),
                "lsid must be a document");

        journal.record(
                CorrelationContext.builder("r2", "insert").sessionId("session-a").txnNumber(1L).build(),
                BsonDocument.parse(
                        "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1}],\"lsid\":{\"id\":\"session-a\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"),
                BsonDocument.parse("{\"ok\":1.0}"));

        journal.record(
                CorrelationContext.builder("r3", "find").sessionId("session-a").txnNumber(2L).build(),
                BsonDocument.parse(
                        "{\"find\":\"users\",\"$db\":\"app\",\"filter\":{},\"lsid\":{\"id\":\"session-a\"},\"txnNumber\":2,\"autocommit\":false}"),
                BsonDocument.parse("{\"ok\":0.0,\"errmsg\":\"find requires an active transaction\"}"),
                "find requires an active transaction");

        journal.record(
                CorrelationContext.of("r4", "find"),
                BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{}}"),
                BsonDocument.parse("{\"ok\":1.0,\"cursor\":{\"id\":0,\"firstBatch\":[]}}"));

        journal.record(
                CorrelationContext.of("r5", "createindexes"),
                BsonDocument.parse(
                        "{\"createIndexes\":\"users\",\"$db\":\"app\",\"indexes\":[{\"name\":\"name_1\",\"key\":{\"name\":1}}]}"),
                BsonDocument.parse(
                        "{\"ok\":1.0,\"createdCollectionAutomatically\":false,\"numIndexesBefore\":3,\"numIndexesAfter\":2}"));

        final CommandJournalInvariantChecker checker = new CommandJournalInvariantChecker();
        final CommandJournalInvariantChecker.InvariantReport report = checker.check(journal);
        final BsonDocument reportDocument = report.toDocument();

        assertEquals(4, report.violations().size());
        assertEquals(4, reportDocument.getInt32("violationCount").getValue());
        assertEquals(1, reportDocument.getDocument("byCategory").getInt32("session").getValue());
        assertEquals(1, reportDocument.getDocument("byCategory").getInt32("transaction").getValue());
        assertEquals(1, reportDocument.getDocument("byCategory").getInt32("cursor").getValue());
        assertEquals(1, reportDocument.getDocument("byCategory").getInt32("index").getValue());
        assertEquals(
                "SESSION_MISSING_LSID",
                reportDocument.getArray("violations").get(0).asDocument().getString("code").getValue());
        assertEquals(
                "TXN_COMMAND_NUMBER_MISMATCH",
                reportDocument.getArray("violations").get(1).asDocument().getString("code").getValue());
        assertEquals(
                "CURSOR_NAMESPACE_NOT_STRING",
                reportDocument.getArray("violations").get(2).asDocument().getString("code").getValue());
        assertEquals(
                "INDEX_COUNT_REGRESSED",
                reportDocument.getArray("violations").get(3).asDocument().getString("code").getValue());

        assertEquals(reportDocument, checker.check(journal).toDocument());
    }
}
