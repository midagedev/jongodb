package org.jongodb.engine;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.bson.Document;

/**
 * Basic synchronized in-memory collection store.
 */
public final class InMemoryCollectionStore implements CollectionStore {
    private final List<Document> documents = new ArrayList<>();

    @Override
    public synchronized void insertMany(List<Document> documents) {
        Objects.requireNonNull(documents, "documents");

        for (Document document : documents) {
            if (document == null) {
                throw new IllegalArgumentException("documents must not contain null");
            }
            this.documents.add(DocumentCopies.copy(document));
        }
    }

    @Override
    public synchronized List<Document> findAll() {
        return copyMatchingDocuments(new Document());
    }

    @Override
    public synchronized List<Document> find(Document filter) {
        Document effectiveFilter = filter == null ? new Document() : DocumentCopies.copy(filter);
        return copyMatchingDocuments(effectiveFilter);
    }

    @Override
    public synchronized UpdateManyResult updateMany(Document filter, Document update) {
        Document effectiveFilter = filter == null ? new Document() : DocumentCopies.copy(filter);
        Document effectiveUpdate = update == null ? null : DocumentCopies.copy(update);
        UpdateApplier.ParsedUpdate parsedUpdate = UpdateApplier.parse(effectiveUpdate);

        List<Document> matchedDocuments = new ArrayList<>();
        for (Document document : documents) {
            if (QueryMatcher.matches(document, effectiveFilter)) {
                matchedDocuments.add(document);
            }
        }

        for (Document document : matchedDocuments) {
            UpdateApplier.validateApplicable(document, parsedUpdate);
        }

        long modifiedCount = 0;
        for (Document document : matchedDocuments) {
            if (UpdateApplier.apply(document, parsedUpdate)) {
                modifiedCount++;
            }
        }

        return new UpdateManyResult(matchedDocuments.size(), modifiedCount);
    }

    @Override
    public synchronized DeleteManyResult deleteMany(Document filter) {
        Document effectiveFilter = filter == null ? new Document() : DocumentCopies.copy(filter);

        long deletedCount = 0;
        Iterator<Document> iterator = documents.iterator();
        while (iterator.hasNext()) {
            Document document = iterator.next();
            if (QueryMatcher.matches(document, effectiveFilter)) {
                iterator.remove();
                deletedCount++;
            }
        }

        return new DeleteManyResult(deletedCount, deletedCount);
    }

    private List<Document> copyMatchingDocuments(Document filter) {
        List<Document> matches = new ArrayList<>();
        for (Document document : documents) {
            if (QueryMatcher.matches(document, filter)) {
                matches.add(DocumentCopies.copy(document));
            }
        }
        return matches;
    }
}
