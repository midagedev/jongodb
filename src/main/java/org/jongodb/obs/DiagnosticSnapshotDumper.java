package org.jongodb.obs;

import java.util.List;
import java.util.Objects;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;

/**
 * Serializes diagnostics state into deterministic BSON/JSON snapshot payloads.
 */
public final class DiagnosticSnapshotDumper {
    public BsonDocument dumpDocument(final CommandJournal journal) {
        Objects.requireNonNull(journal, "journal");

        final List<CommandJournal.Entry> entries = journal.entries();
        final BsonArray encodedEntries = new BsonArray(entries.size());
        for (CommandJournal.Entry entry : entries) {
            encodedEntries.add(toDocument(entry));
        }

        return new BsonDocument()
                .append(
                        "journal",
                        new BsonDocument()
                                .append("capacity", new BsonInt32(journal.capacity()))
                                .append("size", new BsonInt32(entries.size()))
                                .append("dropped", new BsonInt64(journal.droppedCount()))
                                .append("entries", encodedEntries));
    }

    public String dumpJson(final CommandJournal journal) {
        return dumpDocument(journal).toJson();
    }

    private static BsonDocument toDocument(final CommandJournal.Entry entry) {
        final CorrelationContext correlation = entry.correlationContext();
        final BsonDocument encoded = new BsonDocument()
                .append("sequence", new BsonInt64(entry.sequence()))
                .append("requestId", new BsonString(correlation.requestId()))
                .append("commandName", new BsonString(correlation.commandName()))
                .append("failed", BsonBoolean.valueOf(entry.failed()))
                .append("input", entry.commandInput());

        correlation.sessionId().ifPresent(sessionId -> encoded.append("sessionId", new BsonString(sessionId)));
        correlation.txnNumber().ifPresent(txnNumber -> encoded.append("txnNumber", new BsonInt64(txnNumber)));

        final BsonDocument output = entry.commandOutput();
        if (output != null) {
            encoded.append("output", output);
        }

        final String error = entry.error();
        if (error != null) {
            encoded.append("error", new BsonString(error));
        }
        return encoded;
    }
}
