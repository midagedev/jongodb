package org.jongodb.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.bson.BsonDocument;
import org.jongodb.obs.CommandJournal;
import org.jongodb.obs.CorrelationContext;
import org.jongodb.obs.JsonLinesLogger;
import org.jongodb.wire.OpMsg;
import org.jongodb.wire.OpMsgCodec;
import org.junit.jupiter.api.Test;

class WireCommandIngressDiagnosticsArtifactsTest {
    private static final JsonLinesLogger NOOP_LOGGER = new JsonLinesLogger() {
        @Override
        public void log(
                final String level,
                final String message,
                final CorrelationContext correlationContext,
                final Map<String, ?> fields) {}

        @Override
        public void close() {}
    };

    @Test
    void failingCommandProducesJournalSnapshotAndReproArtifacts() {
        final CommandJournal journal = new CommandJournal(4);
        final WireCommandIngress ingress = WireCommandIngress.inMemory(NOOP_LOGGER, journal);
        final OpMsgCodec codec = new OpMsgCodec();

        final OpMsg successfulResponse =
                roundTrip(ingress, codec, 501, BsonDocument.parse("{\"ping\": 1, \"$db\": \"admin\"}"));
        assertEquals(1.0, successfulResponse.body().get("ok").asNumber().doubleValue());

        final BsonDocument failingCommand = BsonDocument.parse(
                "{\"doesNotExist\": 1, \"$db\": \"admin\", \"lsid\": {\"id\": \"session-7\"}, \"txnNumber\": 99}");
        final OpMsg failingResponse = roundTrip(ingress, codec, 502, failingCommand);
        assertEquals(0.0, failingResponse.body().get("ok").asNumber().doubleValue());
        assertTrue(failingResponse.body().getString("errmsg").getValue().contains("no such command"));

        final List<CommandJournal.Entry> entries = journal.entries();
        assertEquals(2, entries.size());
        assertFalse(entries.get(0).failed());

        final CommandJournal.Entry failedEntry = entries.get(1);
        assertTrue(failedEntry.failed());
        assertEquals(2L, failedEntry.sequence());
        assertEquals("502", failedEntry.correlationContext().requestId());
        assertEquals("doesnotexist", failedEntry.correlationContext().commandName());
        assertEquals("session-7", failedEntry.correlationContext().sessionId().orElseThrow());
        assertEquals(99L, failedEntry.correlationContext().txnNumber().orElseThrow());
        assertEquals("doesNotExist", failedEntry.commandInput().getFirstKey());
        assertEquals(0.0, failedEntry.commandOutput().get("ok").asNumber().doubleValue());
        assertTrue(failedEntry.error().contains("no such command"));

        final BsonDocument snapshotDocument = ingress.dumpDiagnosticSnapshotDocument();
        final BsonDocument snapshotFromJson = BsonDocument.parse(ingress.dumpDiagnosticSnapshotJson());
        final BsonDocument invariantReport = ingress.dumpInvariantReportDocument();
        final BsonDocument triageReport = ingress.dumpFailureTriageReportDocument();
        assertEquals(
                snapshotDocument.getDocument("journal").getInt32("size").getValue(),
                snapshotFromJson.getDocument("journal").getInt32("size").getValue());
        assertEquals(
                snapshotDocument.getDocument("journal").getInt32("capacity").getValue(),
                snapshotFromJson.getDocument("journal").getInt32("capacity").getValue());
        assertEquals(invariantReport.toJson(), ingress.dumpInvariantReportJson());
        assertEquals(triageReport.toJson(), ingress.dumpFailureTriageReportJson());

        final BsonDocument journalDocument = snapshotDocument.getDocument("journal");
        assertEquals(4, journalDocument.getInt32("capacity").getValue());
        assertEquals(2, journalDocument.getInt32("size").getValue());
        assertEquals(0L, journalDocument.getInt64("dropped").getValue());
        final BsonDocument failedSnapshot =
                journalDocument.getArray("entries").get(1).asDocument();
        assertEquals("doesnotexist", failedSnapshot.getString("commandName").getValue());
        assertTrue(failedSnapshot.getBoolean("failed").getValue());
        assertTrue(failedSnapshot.getString("error").getValue().contains("no such command"));
        assertEquals(0, snapshotDocument.getDocument("invariants").getInt32("violationCount").getValue());
        assertEquals(
                "command_failure",
                snapshotDocument.getDocument("triage").getDocument("summary").getString("rootCauseType").getValue());
        assertEquals(
                "doesnotexist",
                snapshotDocument.getDocument("triage").getDocument("summary").getString("anchorCommandName").getValue());
        assertEquals(
                snapshotDocument.getDocument("invariants"),
                invariantReport);
        assertEquals(
                snapshotDocument.getDocument("triage"),
                triageReport);

        final String reproJsonLines = ingress.exportReproJsonLines();
        final String[] reproLines = reproJsonLines.split("\\R");
        assertEquals(2, reproLines.length);
        assertEquals("doesNotExist", BsonDocument.parse(reproLines[1]).getFirstKey());

        final WireCommandIngress replayIngress = WireCommandIngress.inMemory();
        final OpMsg replayFirst = roundTrip(replayIngress, codec, 601, BsonDocument.parse(reproLines[0]));
        final OpMsg replaySecond = roundTrip(replayIngress, codec, 602, BsonDocument.parse(reproLines[1]));
        assertEquals(1.0, replayFirst.body().get("ok").asNumber().doubleValue());
        assertEquals(0.0, replaySecond.body().get("ok").asNumber().doubleValue());
    }

    private static OpMsg roundTrip(
            final WireCommandIngress ingress, final OpMsgCodec codec, final int requestId, final BsonDocument body) {
        final OpMsg request = new OpMsg(requestId, 0, 0, body);
        final byte[] responseBytes = ingress.handle(codec.encode(request));
        return codec.decode(responseBytes);
    }
}
