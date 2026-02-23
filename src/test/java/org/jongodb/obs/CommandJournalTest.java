package org.jongodb.obs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

class CommandJournalTest {
    @Test
    void ringBufferDropsOldestEntryWhenCapacityIsExceeded() {
        final CommandJournal journal = new CommandJournal(2);
        final CorrelationContext first = CorrelationContext.of("r1", "ping");
        final CorrelationContext second = CorrelationContext.of("r2", "insert");
        final CorrelationContext third = CorrelationContext.of("r3", "find");

        journal.record(first, BsonDocument.parse("{\"ping\": 1}"), BsonDocument.parse("{\"ok\": 1.0}"));
        journal.record(second, BsonDocument.parse("{\"insert\": \"x\"}"), BsonDocument.parse("{\"ok\": 1.0}"));
        journal.record(
                third,
                BsonDocument.parse("{\"find\": \"x\"}"),
                BsonDocument.parse("{\"ok\": 0.0, \"errmsg\": \"boom\"}"),
                "boom");

        assertEquals(2, journal.size());
        assertEquals(1L, journal.droppedCount());
        assertEquals(2L, journal.entries().get(0).sequence());
        assertEquals(3L, journal.entries().get(1).sequence());
        assertEquals("insert", journal.entries().get(0).correlationContext().commandName());
        assertEquals("find", journal.entries().get(1).correlationContext().commandName());

        final DiagnosticSnapshotDumper dumper = new DiagnosticSnapshotDumper();
        final BsonDocument snapshot = dumper.dumpDocument(journal);
        assertEquals(2, snapshot.getDocument("journal").getInt32("capacity").getValue());
        assertEquals(2, snapshot.getDocument("journal").getInt32("size").getValue());
        assertEquals(1L, snapshot.getDocument("journal").getInt64("dropped").getValue());

        final ReproExporter exporter = new ReproExporter();
        final String[] lines = exporter.exportJsonLines(journal).split("\\R");
        assertEquals(2, lines.length);
        assertEquals("insert", BsonDocument.parse(lines[0]).getFirstKey());
        assertEquals("find", BsonDocument.parse(lines[1]).getFirstKey());
        assertTrue(lines[1].contains("\"find\""));
    }
}
