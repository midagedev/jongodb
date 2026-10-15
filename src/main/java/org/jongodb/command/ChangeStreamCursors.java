package org.jongodb.command;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.jongodb.engine.AggregationPipeline;
import org.jongodb.engine.ChangeEventLog;

/**
 * {@code $changeStream} aggregate cursors over the global store's {@link ChangeEventLog}.
 *
 * <p>Resume tokens are {@code {_data: <hex sequence>}}; {@code postBatchResumeToken} is returned on every batch so
 * drivers can resume from the latest scanned position even when nothing matched.
 */
final class ChangeStreamCursors {
    // Disjoint from CursorRegistry ids (which count up from 1) so getMore/killCursors can route by id alone.
    private static final long FIRST_CURSOR_ID = 1L << 48;
    private static final long DEFAULT_AWAIT_MILLIS = 1_000L;
    private static final int MAX_EVENTS_PER_READ = 1_000;
    private static final long IDLE_TIMEOUT_MILLIS = 10 * 60 * 1_000L;
    private static final int CODE_HISTORY_LOST = 286;
    private static final int CODE_NOT_REPLICA_SET = 40573;
    private static final Set<String> FULL_DOCUMENT_MODES = Set.of("default", "updateLookup", "whenAvailable", "required");
    private static final DocumentCodec DOCUMENT_CODEC = new DocumentCodec();
    private static final CodecRegistry DOCUMENT_CODEC_REGISTRY = CodecRegistries.fromCodecs(DOCUMENT_CODEC);

    private final ChangeEventLog log;
    private final Map<Long, Cursor> cursors = new HashMap<>();
    private long nextCursorId = FIRST_CURSOR_ID;

    ChangeStreamCursors(final ChangeEventLog log) {
        this.log = log;
    }

    static boolean isChangeStream(final BsonDocument command) {
        final BsonValue pipeline = command.get("pipeline");
        if (pipeline == null || !pipeline.isArray() || pipeline.asArray().isEmpty()) {
            return false;
        }
        final BsonValue first = pipeline.asArray().get(0);
        return first.isDocument() && first.asDocument().containsKey("$changeStream");
    }

    synchronized boolean owns(final long cursorId) {
        return cursors.containsKey(cursorId);
    }

    synchronized boolean kill(final long cursorId) {
        return cursors.remove(cursorId) != null;
    }

    BsonDocument open(final String database, final BsonDocument command) {
        if (log == null) {
            return error("The $changeStream stage is only supported on replica sets", CODE_NOT_REPLICA_SET,
                    "Location40573");
        }
        final BsonValue target = command.get("aggregate");
        final String collection;
        if (target instanceof BsonString name) {
            collection = name.getValue();
        } else if (target != null && target.isNumber() && target.asNumber().intValue() == 1) {
            collection = null;
        } else {
            return CommandErrors.typeMismatch("aggregate must be a collection name or 1");
        }

        final List<BsonValue> stages = command.getArray("pipeline");
        final BsonValue specValue = stages.get(0).asDocument().get("$changeStream");
        if (!specValue.isDocument()) {
            return CommandErrors.typeMismatch("$changeStream must be a document");
        }
        final BsonDocument spec = specValue.asDocument();

        final String fullDocument = spec.containsKey("fullDocument")
                ? spec.getString("fullDocument", new BsonString("")).getValue()
                : "default";
        if (!FULL_DOCUMENT_MODES.contains(fullDocument)) {
            return CommandErrors.badValue("unsupported $changeStream fullDocument mode: " + fullDocument);
        }
        if (spec.containsKey("fullDocumentBeforeChange")
                && !"off".equals(spec.getString("fullDocumentBeforeChange", new BsonString("")).getValue())) {
            return CommandErrors.notImplemented("$changeStream fullDocumentBeforeChange is not supported");
        }
        final boolean allDatabases = spec.getBoolean("allChangesForCluster", org.bson.BsonBoolean.FALSE).getValue();
        if (allDatabases && !"admin".equals(database)) {
            return CommandErrors.badValue("allChangesForCluster is only allowed on the admin database");
        }

        final long start;
        try {
            start = startPosition(spec);
        } catch (final IllegalArgumentException exception) {
            return CommandErrors.badValue(exception.getMessage());
        }

        final List<Document> pipeline = new ArrayList<>();
        for (int index = 1; index < stages.size(); index++) {
            if (!stages.get(index).isDocument()) {
                return CommandErrors.typeMismatch("all entries in pipeline must be BSON documents");
            }
            pipeline.add(toDocument(stages.get(index).asDocument()));
        }

        final int batchSize = readBatchSize(command.get("cursor"));
        final String namespace = collection == null ? database + ".$cmd.aggregate" : database + "." + collection;
        final Cursor cursor = new Cursor(
                namespace, allDatabases ? null : database, collection, !"default".equals(fullDocument), pipeline, start);

        final BsonDocument response;
        try {
            response = nextBatch(cursor, batchSize, 0L, "firstBatch");
        } catch (final ChangeEventLog.HistoryLostException exception) {
            return error(exception.getMessage(), CODE_HISTORY_LOST, "ChangeStreamHistoryLost");
        } catch (final IllegalArgumentException exception) {
            return CommandExceptionMapper.fromIllegalArgument(exception);
        }
        final long cursorId = register(cursor);
        response.getDocument("cursor").put("id", new BsonInt64(cursorId));
        return response;
    }

    BsonDocument getMore(final long cursorId, final String namespace, final BsonDocument command) {
        final Cursor cursor;
        synchronized (this) {
            cursor = cursors.get(cursorId);
        }
        if (cursor == null || !cursor.namespace.equals(namespace)) {
            return CommandErrors.cursorNotFound(cursorId);
        }
        final BsonValue maxTime = command.get("maxTimeMS");
        final long awaitMillis = maxTime != null && maxTime.isNumber()
                ? maxTime.asNumber().longValue()
                : DEFAULT_AWAIT_MILLIS;
        final BsonValue batchSizeValue = command.get("batchSize");
        final int batchSize = batchSizeValue != null && batchSizeValue.isNumber() && batchSizeValue.asNumber().intValue() > 0
                ? batchSizeValue.asNumber().intValue()
                : MAX_EVENTS_PER_READ;
        try {
            final BsonDocument response = nextBatch(cursor, batchSize, awaitMillis, "nextBatch");
            response.getDocument("cursor").put("id", new BsonInt64(cursorId));
            return response;
        } catch (final ChangeEventLog.HistoryLostException exception) {
            kill(cursorId);
            return error(exception.getMessage(), CODE_HISTORY_LOST, "ChangeStreamHistoryLost");
        }
    }

    private BsonDocument nextBatch(final Cursor cursor, final int batchSize, final long awaitMillis, final String field) {
        final long deadline = System.currentTimeMillis() + awaitMillis;
        final BsonArray batch = new BsonArray();
        synchronized (cursor) {
            cursor.lastUsedMillis = System.currentTimeMillis();
            while (true) {
                final ChangeEventLog.Read read;
                try {
                    read = log.readAfter(
                            cursor.position,
                            Math.min(batchSize, MAX_EVENTS_PER_READ),
                            Math.max(0L, deadline - System.currentTimeMillis()));
                } catch (final InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    break;
                }
                final List<Document> scoped = new ArrayList<>();
                for (final ChangeEventLog.Event event : read.events()) {
                    if (cursor.covers(event)) {
                        scoped.add(eventDocument(event, cursor.includeUpdateFullDocument));
                    }
                }
                for (final Document document : AggregationPipeline.execute(scoped, cursor.pipeline)) {
                    batch.add(toBsonDocument(document));
                }
                cursor.position = read.position();
                if (!batch.isEmpty() || read.events().isEmpty() || System.currentTimeMillis() >= deadline) {
                    break;
                }
            }
        }
        final BsonDocument cursorDocument = new BsonDocument()
                .append("id", new BsonInt64(0L))
                .append("ns", new BsonString(cursor.namespace))
                .append(field, batch)
                .append("postBatchResumeToken", resumeToken(cursor.position));
        return new BsonDocument().append("cursor", cursorDocument).append("ok", new BsonDouble(1.0));
    }

    private long startPosition(final BsonDocument spec) {
        for (final String key : List.of("resumeAfter", "startAfter")) {
            if (spec.containsKey(key)) {
                return parseResumeToken(spec.get(key));
            }
        }
        if (spec.containsKey("startAtOperationTime")) {
            final BsonValue value = spec.get("startAtOperationTime");
            if (!value.isTimestamp()) {
                throw new IllegalArgumentException("startAtOperationTime must be a timestamp");
            }
            final BsonTimestamp timestamp = value.asTimestamp();
            return log.sequenceBefore(timestamp.getTime(), Integer.toUnsignedLong(timestamp.getInc()));
        }
        return log.lastSequence();
    }

    private synchronized long register(final Cursor cursor) {
        final long now = System.currentTimeMillis();
        cursors.values().removeIf(existing -> now - existing.lastUsedMillis > IDLE_TIMEOUT_MILLIS);
        final long cursorId = nextCursorId++;
        cursors.put(cursorId, cursor);
        return cursorId;
    }

    private static Document eventDocument(final ChangeEventLog.Event event, final boolean includeUpdateFullDocument) {
        final Document document = new Document("_id", new Document("_data", tokenData(event.sequence())))
                .append("operationType", event.operationType())
                .append("clusterTime", new BsonTimestamp(
                        (int) (event.wallTimeMillis() / 1000L), (int) event.sequence()))
                .append("wallTime", new Date(event.wallTimeMillis()));
        final boolean withFullDocument = event.fullDocument() != null
                && (!"update".equals(event.operationType()) || includeUpdateFullDocument);
        if (withFullDocument) {
            document.append("fullDocument", event.fullDocument());
        }
        document.append("ns", new Document("db", event.namespace().database())
                        .append("coll", event.namespace().collection()))
                .append("documentKey", new Document("_id", event.documentId()));
        if (event.updateDescription() != null) {
            document.append("updateDescription", event.updateDescription());
        }
        return document;
    }

    private static BsonDocument resumeToken(final long sequence) {
        return new BsonDocument("_data", new BsonString(tokenData(sequence)));
    }

    private static String tokenData(final long sequence) {
        return String.format(Locale.ROOT, "%016X", sequence);
    }

    private static long parseResumeToken(final BsonValue token) {
        if (token == null || !token.isDocument() || !token.asDocument().isString("_data")) {
            throw new IllegalArgumentException("resume token must be a document with a string _data field");
        }
        try {
            return Long.parseUnsignedLong(token.asDocument().getString("_data").getValue(), 16);
        } catch (final NumberFormatException exception) {
            throw new IllegalArgumentException("resume token was not issued by this server");
        }
    }

    private static int readBatchSize(final BsonValue cursorValue) {
        if (cursorValue == null || !cursorValue.isDocument()) {
            return MAX_EVENTS_PER_READ;
        }
        final BsonValue batchSize = cursorValue.asDocument().get("batchSize");
        if (batchSize == null || !batchSize.isNumber() || batchSize.asNumber().intValue() <= 0) {
            return MAX_EVENTS_PER_READ;
        }
        return batchSize.asNumber().intValue();
    }

    private static Document toDocument(final BsonDocument source) {
        return DOCUMENT_CODEC.decode(new BsonDocumentReader(source), DecoderContext.builder().build());
    }

    private static BsonDocument toBsonDocument(final Document source) {
        return source.toBsonDocument(BsonDocument.class, DOCUMENT_CODEC_REGISTRY);
    }

    private static BsonDocument error(final String message, final int code, final String codeName) {
        return new BsonDocument()
                .append("ok", new BsonDouble(0.0))
                .append("errmsg", new BsonString(message))
                .append("code", new BsonInt32(code))
                .append("codeName", new BsonString(codeName));
    }

    private static final class Cursor {
        private final String namespace;
        private final String database;
        private final String collection;
        private final boolean includeUpdateFullDocument;
        private final List<Document> pipeline;
        private long position;
        private long lastUsedMillis;

        private Cursor(
                final String namespace,
                final String database,
                final String collection,
                final boolean includeUpdateFullDocument,
                final List<Document> pipeline,
                final long position) {
            this.namespace = namespace;
            this.database = database;
            this.collection = collection;
            this.includeUpdateFullDocument = includeUpdateFullDocument;
            this.pipeline = List.copyOf(pipeline);
            this.position = position;
            this.lastUsedMillis = System.currentTimeMillis();
        }

        private boolean covers(final ChangeEventLog.Event event) {
            return (database == null || database.equals(event.namespace().database()))
                    && (collection == null || collection.equals(event.namespace().collection()));
        }
    }
}
