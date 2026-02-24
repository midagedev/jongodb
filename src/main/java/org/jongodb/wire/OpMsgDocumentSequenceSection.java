package org.jongodb.wire;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.bson.BsonDocument;

/**
 * OP_MSG payload type 1 section (document sequence).
 * <p>
 * MongoDB drivers commonly use this for high-volume CRUD command payloads such as
 * {@code documents}, {@code updates}, and {@code deletes}.
 */
public final class OpMsgDocumentSequenceSection implements OpMsgSection {
    public static final byte KIND = 1;

    private final String identifier;
    private final List<BsonDocument> documents;

    public OpMsgDocumentSequenceSection(final String identifier, final List<BsonDocument> documents) {
        if (identifier == null || identifier.isEmpty()) {
            throw new IllegalArgumentException("identifier must not be null or empty");
        }
        Objects.requireNonNull(documents, "documents");

        final List<BsonDocument> copies = new ArrayList<>(documents.size());
        for (final BsonDocument document : documents) {
            copies.add(Objects.requireNonNull(document, "documents entries must not be null").clone());
        }
        this.identifier = identifier;
        this.documents = List.copyOf(copies);
    }

    @Override
    public byte kind() {
        return KIND;
    }

    public String identifier() {
        return identifier;
    }

    public List<BsonDocument> documents() {
        return documents;
    }
}
