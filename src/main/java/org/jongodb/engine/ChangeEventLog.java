package org.jongodb.engine;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.bson.Document;

/**
 * Bounded journal of committed document changes, read by {@code $changeStream} cursors.
 *
 * <p>Sequences are contiguous and start at 1, so a sequence doubles as the resume position.
 * The oldest events are dropped once the journal exceeds its capacity; resuming from a dropped
 * position fails with {@link HistoryLostException}, the in-memory analogue of falling off the oplog.
 */
public final class ChangeEventLog {
    public static final int DEFAULT_CAPACITY = 100_000;

    private final List<Event> events = new ArrayList<>();
    private final int capacity;
    private final Clock clock;
    private long lastSequence;

    public ChangeEventLog(final Clock clock) {
        this(clock, DEFAULT_CAPACITY);
    }

    public ChangeEventLog(final Clock clock, final int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.clock = Objects.requireNonNull(clock, "clock");
        this.capacity = capacity;
    }

    public synchronized long lastSequence() {
        return lastSequence;
    }

    /** Last sequence whose event happened before the given cluster time (seconds, increment). */
    public synchronized long sequenceBefore(final long seconds, final long increment) {
        long before = events.isEmpty() ? lastSequence : events.get(0).sequence() - 1;
        for (final Event event : events) {
            final long eventSeconds = event.wallTimeMillis() / 1000L;
            if (eventSeconds > seconds || (eventSeconds == seconds && event.sequence() >= increment)) {
                return before;
            }
            before = event.sequence();
        }
        return before;
    }

    /**
     * Returns events after {@code afterSequence}, waiting up to {@code waitMillis} when none exist yet.
     * {@link Read#position()} is the sequence the next read should continue after.
     */
    public synchronized Read readAfter(final long afterSequence, final int limit, final long waitMillis)
            throws InterruptedException {
        ensureRetained(afterSequence);
        final long deadline = System.nanoTime() + Math.max(0L, waitMillis) * 1_000_000L;
        while (lastSequence <= afterSequence) {
            final long remainingMillis = (deadline - System.nanoTime()) / 1_000_000L;
            if (remainingMillis <= 0) {
                return new Read(List.of(), Math.max(afterSequence, lastSequence));
            }
            wait(remainingMillis);
        }
        ensureRetained(afterSequence);

        final int from = (int) (afterSequence + 1 - events.get(0).sequence());
        final int to = Math.min(events.size(), from + Math.max(1, limit));
        final List<Event> batch = List.copyOf(events.subList(from, to));
        return new Read(batch, batch.get(batch.size() - 1).sequence());
    }

    synchronized void recordInsert(final Namespace namespace, final Document document) {
        append(namespace, "insert", document.get("_id"), DocumentCopies.copy(document), null);
    }

    synchronized void recordUpdate(
            final Namespace namespace, final Document before, final Document after, final boolean replacement) {
        append(
                namespace,
                replacement ? "replace" : "update",
                after.get("_id"),
                DocumentCopies.copy(after),
                replacement ? null : updateDescription(before, after));
    }

    synchronized void recordDelete(final Namespace namespace, final Document before) {
        append(namespace, "delete", before.get("_id"), null, null);
    }

    /** Emits the per-document changes between two states of one collection, matched by {@code _id}. */
    synchronized void recordStateChange(
            final Namespace namespace, final List<Document> before, final List<Document> after) {
        final Map<Object, Document> beforeById = new HashMap<>();
        for (final Document document : before) {
            beforeById.put(document.get("_id"), document);
        }
        for (final Document document : after) {
            final Document previous = beforeById.remove(document.get("_id"));
            if (previous == null) {
                recordInsert(namespace, document);
            } else if (!previous.equals(document)) {
                recordUpdate(namespace, previous, document, false);
            }
        }
        for (final Document removed : beforeById.values()) {
            recordDelete(namespace, removed);
        }
    }

    private void append(
            final Namespace namespace,
            final String operationType,
            final Object documentId,
            final Document fullDocument,
            final Document updateDescription) {
        lastSequence++;
        events.add(new Event(
                lastSequence,
                clock.millis(),
                namespace,
                operationType,
                DocumentCopies.copyAny(documentId),
                fullDocument,
                updateDescription));
        // Trim in chunks so appends stay amortized O(1).
        if (events.size() > capacity + capacity / 4) {
            events.subList(0, events.size() - capacity).clear();
        }
        notifyAll();
    }

    private void ensureRetained(final long afterSequence) {
        if (afterSequence > lastSequence) {
            throw new HistoryLostException("resume position " + afterSequence + " is ahead of the change log");
        }
        if (!events.isEmpty() && events.get(0).sequence() > afterSequence + 1) {
            throw new HistoryLostException(
                    "resume position " + afterSequence + " is no longer in the change log");
        }
    }

    private static Document updateDescription(final Document before, final Document after) {
        final Document updatedFields = new Document();
        for (final Map.Entry<String, Object> entry : after.entrySet()) {
            if (!before.containsKey(entry.getKey())
                    || !Objects.deepEquals(before.get(entry.getKey()), entry.getValue())) {
                updatedFields.put(entry.getKey(), DocumentCopies.copyAny(entry.getValue()));
            }
        }
        final List<String> removedFields = new ArrayList<>();
        for (final String key : before.keySet()) {
            if (!after.containsKey(key)) {
                removedFields.add(key);
            }
        }
        return new Document("updatedFields", updatedFields)
                .append("removedFields", removedFields)
                .append("truncatedArrays", List.of());
    }

    public record Event(
            long sequence,
            long wallTimeMillis,
            Namespace namespace,
            String operationType,
            Object documentId,
            Document fullDocument,
            Document updateDescription) {}

    public record Read(List<Event> events, long position) {}

    public static final class HistoryLostException extends RuntimeException {
        HistoryLostException(final String message) {
            super(message);
        }
    }
}
