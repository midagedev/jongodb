package org.jongodb.obs;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import org.bson.BsonDocument;

/**
 * Fixed-capacity ring buffer of recently handled commands for diagnostics.
 */
public final class CommandJournal {
    private final int capacity;
    private final Deque<Entry> entries;
    private long nextSequence;
    private long droppedCount;

    public CommandJournal(final int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be greater than zero");
        }
        this.capacity = capacity;
        this.entries = new ArrayDeque<>(capacity);
        this.nextSequence = 1L;
        this.droppedCount = 0L;
    }

    public int capacity() {
        return capacity;
    }

    public synchronized int size() {
        return entries.size();
    }

    public synchronized long droppedCount() {
        return droppedCount;
    }

    public synchronized List<Entry> entries() {
        return List.copyOf(entries);
    }

    public void record(
            final CorrelationContext correlationContext,
            final BsonDocument commandInput,
            final BsonDocument commandOutput) {
        record(correlationContext, commandInput, commandOutput, null);
    }

    public synchronized void record(
            final CorrelationContext correlationContext,
            final BsonDocument commandInput,
            final BsonDocument commandOutput,
            final String error) {
        final Entry entry = new Entry(
                nextSequence++,
                Objects.requireNonNull(correlationContext, "correlationContext"),
                cloneDocument(Objects.requireNonNull(commandInput, "commandInput")),
                cloneDocument(commandOutput),
                normalize(error));

        if (entries.size() == capacity) {
            entries.removeFirst();
            droppedCount++;
        }
        entries.addLast(entry);
    }

    public synchronized void clear() {
        entries.clear();
        droppedCount = 0L;
    }

    private static BsonDocument cloneDocument(final BsonDocument document) {
        if (document == null) {
            return null;
        }
        return document.clone();
    }

    private static String normalize(final String error) {
        if (error == null) {
            return null;
        }
        final String trimmed = error.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    public static final class Entry {
        private final long sequence;
        private final CorrelationContext correlationContext;
        private final BsonDocument commandInput;
        private final BsonDocument commandOutput;
        private final String error;

        private Entry(
                final long sequence,
                final CorrelationContext correlationContext,
                final BsonDocument commandInput,
                final BsonDocument commandOutput,
                final String error) {
            this.sequence = sequence;
            this.correlationContext = correlationContext;
            this.commandInput = commandInput;
            this.commandOutput = commandOutput;
            this.error = error;
        }

        public long sequence() {
            return sequence;
        }

        public CorrelationContext correlationContext() {
            return correlationContext;
        }

        public BsonDocument commandInput() {
            return commandInput.clone();
        }

        public BsonDocument commandOutput() {
            if (commandOutput == null) {
                return null;
            }
            return commandOutput.clone();
        }

        public String error() {
            return error;
        }

        public boolean failed() {
            return error != null;
        }
    }
}
