package org.jongodb.obs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

class DiagnosticTriageReporterTest {
    @Test
    void triageUsesInvariantViolationAsPrimaryRootCauseAndBuildsSessionPath() {
        final CommandJournal journal = new CommandJournal(8);

        journal.record(
                CorrelationContext.builder("r1", "insert").sessionId("session-a").txnNumber(1L).build(),
                BsonDocument.parse(
                        "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1}],\"lsid\":{\"id\":\"session-a\"},\"txnNumber\":1,\"autocommit\":false,\"startTransaction\":true}"),
                BsonDocument.parse("{\"ok\":1.0}"));

        journal.record(
                CorrelationContext.builder("r2", "find").sessionId("session-a").txnNumber(2L).build(),
                BsonDocument.parse(
                        "{\"find\":\"users\",\"$db\":\"app\",\"filter\":{},\"lsid\":{\"id\":\"session-a\"},\"txnNumber\":2,\"autocommit\":false}"),
                BsonDocument.parse("{\"ok\":0.0,\"errmsg\":\"find requires an active transaction\"}"),
                "find requires an active transaction");

        final CommandJournalInvariantChecker checker = new CommandJournalInvariantChecker();
        final DiagnosticTriageReporter reporter = new DiagnosticTriageReporter();
        final BsonDocument triage = reporter.report(journal, checker.check(journal));

        final BsonDocument summary = triage.getDocument("summary");
        assertEquals("failed", summary.getString("status").getValue());
        assertEquals("invariant_violation", summary.getString("rootCauseType").getValue());
        assertEquals("TXN_COMMAND_NUMBER_MISMATCH", summary.getString("violationCode").getValue());
        assertEquals(2L, summary.getInt64("anchorSequence").getValue());
        assertEquals("find", summary.getString("anchorCommandName").getValue());
        assertEquals(2, triage.getArray("relatedCommands").size());

        final BsonDocument sessionPath = triage.getDocument("sessionPath");
        assertEquals("session-a", sessionPath.getString("sessionId").getValue());
        assertEquals("in_progress", sessionPath.getString("terminalState").getValue());
        assertEquals(2, sessionPath.getArray("commands").size());
        assertEquals(
                "start",
                sessionPath.getArray("commands").get(0).asDocument().getString("phase").getValue());
    }

    @Test
    void triageFallsBackToLastCommandFailureWhenNoInvariantViolationsExist() {
        final CommandJournal journal = new CommandJournal(8);
        journal.record(
                CorrelationContext.of("r10", "ping"),
                BsonDocument.parse("{\"ping\":1,\"$db\":\"admin\"}"),
                BsonDocument.parse("{\"ok\":1.0}"));
        journal.record(
                CorrelationContext.of("r11", "doesnotexist"),
                BsonDocument.parse("{\"doesNotExist\":1,\"$db\":\"admin\"}"),
                BsonDocument.parse("{\"ok\":0.0,\"errmsg\":\"no such command: doesnotexist\"}"),
                "no such command: doesnotexist");

        final CommandJournalInvariantChecker checker = new CommandJournalInvariantChecker();
        final DiagnosticTriageReporter reporter = new DiagnosticTriageReporter();
        final BsonDocument triage = reporter.report(journal, checker.check(journal));

        final BsonDocument summary = triage.getDocument("summary");
        assertEquals("failed", summary.getString("status").getValue());
        assertEquals("command_failure", summary.getString("rootCauseType").getValue());
        assertTrue(summary.getString("likelyRootCause").getValue().contains("no such command"));
        assertEquals(2L, summary.getInt64("anchorSequence").getValue());
        assertEquals(2, triage.getArray("relatedCommands").size());
    }
}
