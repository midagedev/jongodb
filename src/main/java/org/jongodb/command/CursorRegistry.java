package org.jongodb.command;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.bson.BsonDocument;

final class CursorRegistry {
    private final Map<Long, CursorState> cursors = new HashMap<>();
    private long nextCursorId = 1L;

    synchronized FindRegistration openCursor(
            final String namespace, final List<BsonDocument> documents, final int firstBatchSize) {
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(documents, "documents");

        final int safeBatchSize = Math.max(0, firstBatchSize);
        final int firstBatchCount = Math.min(safeBatchSize, documents.size());
        final List<BsonDocument> firstBatch = cloneDocuments(documents, 0, firstBatchCount);
        final List<BsonDocument> remainingDocuments = cloneDocuments(documents, firstBatchCount, documents.size());
        if (remainingDocuments.isEmpty()) {
            return new FindRegistration(0L, firstBatch);
        }

        final long cursorId = allocateCursorId();
        cursors.put(cursorId, new CursorState(namespace, remainingDocuments));
        return new FindRegistration(cursorId, firstBatch);
    }

    synchronized GetMoreResult getMore(final long cursorId, final String namespace, final int batchSize) {
        Objects.requireNonNull(namespace, "namespace");
        final CursorState cursorState = cursors.get(cursorId);
        if (cursorState == null || !cursorState.namespace.equals(namespace)) {
            return GetMoreResult.notFound();
        }

        final int safeBatchSize = Math.max(0, batchSize);
        final int batchCount = Math.min(safeBatchSize, cursorState.remainingDocuments.size());
        final List<BsonDocument> nextBatch = List.copyOf(cursorState.remainingDocuments.subList(0, batchCount));
        if (batchCount >= cursorState.remainingDocuments.size()) {
            cursors.remove(cursorId);
            return new GetMoreResult(true, 0L, nextBatch);
        }

        cursorState.remainingDocuments = List.copyOf(
                cursorState.remainingDocuments.subList(batchCount, cursorState.remainingDocuments.size()));
        return new GetMoreResult(true, cursorId, nextBatch);
    }

    synchronized KillResult killCursors(final String namespace, final List<Long> cursorIds) {
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(cursorIds, "cursorIds");

        final List<Long> killed = new ArrayList<>();
        final List<Long> notFound = new ArrayList<>();
        for (final Long boxedCursorId : cursorIds) {
            if (boxedCursorId == null) {
                continue;
            }
            final long cursorId = boxedCursorId;
            final CursorState cursorState = cursors.get(cursorId);
            if (cursorState == null || !cursorState.namespace.equals(namespace)) {
                notFound.add(cursorId);
                continue;
            }
            cursors.remove(cursorId);
            killed.add(cursorId);
        }
        return new KillResult(List.copyOf(killed), List.copyOf(notFound));
    }

    private long allocateCursorId() {
        while (true) {
            final long candidate = nextCursorId;
            nextCursorId = candidate == Long.MAX_VALUE ? 1L : candidate + 1L;
            if (!cursors.containsKey(candidate)) {
                return candidate;
            }
        }
    }

    private static List<BsonDocument> cloneDocuments(
            final List<BsonDocument> documents, final int fromInclusive, final int toExclusive) {
        final List<BsonDocument> copies = new ArrayList<>(Math.max(0, toExclusive - fromInclusive));
        for (int index = fromInclusive; index < toExclusive; index++) {
            copies.add(documents.get(index).clone());
        }
        return List.copyOf(copies);
    }

    record FindRegistration(long cursorId, List<BsonDocument> firstBatch) {}

    record GetMoreResult(boolean found, long cursorId, List<BsonDocument> nextBatch) {
        private static GetMoreResult notFound() {
            return new GetMoreResult(false, 0L, List.of());
        }
    }

    record KillResult(List<Long> killedCursorIds, List<Long> notFoundCursorIds) {}

    private static final class CursorState {
        private final String namespace;
        private List<BsonDocument> remainingDocuments;

        private CursorState(final String namespace, final List<BsonDocument> remainingDocuments) {
            this.namespace = namespace;
            this.remainingDocuments = List.copyOf(remainingDocuments);
        }
    }
}
