package org.jongodb.command;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.jongodb.engine.CollectionStore;
import org.jongodb.engine.EngineStore;

/**
 * Bridges command handlers (BSON documents) with the engine store (Document API).
 */
public final class EngineBackedCommandStore implements CommandStore {
    private static final DocumentCodec DOCUMENT_CODEC = new DocumentCodec();
    private static final CodecRegistry DOCUMENT_CODEC_REGISTRY = CodecRegistries.fromCodecs(DOCUMENT_CODEC);

    private final EngineStore engineStore;

    public EngineBackedCommandStore(final EngineStore engineStore) {
        this.engineStore = Objects.requireNonNull(engineStore, "engineStore");
    }

    @Override
    public int insert(final String database, final String collection, final List<BsonDocument> documents) {
        Objects.requireNonNull(documents, "documents");
        final CollectionStore collectionStore = engineStore.collection(database, collection);

        final List<Document> converted = new ArrayList<>(documents.size());
        for (final BsonDocument document : documents) {
            converted.add(toDocument(Objects.requireNonNull(document, "documents entries must not be null")));
        }

        collectionStore.insertMany(converted);
        return converted.size();
    }

    @Override
    public List<BsonDocument> find(final String database, final String collection, final BsonDocument filter) {
        final CollectionStore collectionStore = engineStore.collection(database, collection);
        final Document convertedFilter = filter == null ? new Document() : toDocument(filter);
        final List<Document> foundDocuments = collectionStore.find(convertedFilter);

        final List<BsonDocument> converted = new ArrayList<>(foundDocuments.size());
        for (final Document foundDocument : foundDocuments) {
            converted.add(toBsonDocument(foundDocument));
        }
        return List.copyOf(converted);
    }

    private static Document toDocument(final BsonDocument source) {
        return DOCUMENT_CODEC.decode(new BsonDocumentReader(source), DecoderContext.builder().build());
    }

    private static BsonDocument toBsonDocument(final Document source) {
        return source.toBsonDocument(BsonDocument.class, DOCUMENT_CODEC_REGISTRY);
    }
}
