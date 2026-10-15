package org.jongodb.command;

import java.util.ArrayList;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;

public final class DropIndexesCommandHandler implements CommandHandler {
    private final CommandStore store;

    public DropIndexesCommandHandler(final CommandStore store) {
        this.store = store;
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String database = CommandCanonicalizer.readDatabase(command);
        final String collection = AdminCommandSupport.string(command, "dropIndexes");
        if (collection == null) {
            return CommandErrors.typeMismatch("dropIndexes must be a string");
        }
        final BsonValue selector = command.get("index");
        if (selector == null) {
            return CommandErrors.failedToParse("BSON field 'dropIndexes.index' is missing but a required field");
        }
        if (!AdminCommandSupport.collectionExists(store, database, collection)) {
            return CommandErrors.namespaceNotFound(database + "." + collection);
        }

        final List<CommandStore.IndexMetadata> existing = store.listIndexes(database, collection);
        final boolean all = selector.isString() && "*".equals(selector.asString().getValue());
        final List<String> names = new ArrayList<>();
        if (all) {
            for (final CommandStore.IndexMetadata index : existing) {
                if (!"_id_".equals(index.name())) {
                    names.add(index.name());
                }
            }
        } else if (selector.isString()) {
            names.add(selector.asString().getValue());
        } else if (selector.isArray()) {
            for (final BsonValue entry : selector.asArray()) {
                if (!entry.isString()) {
                    return CommandErrors.typeMismatch("index name array must contain only strings");
                }
                names.add(entry.asString().getValue());
            }
        } else if (selector.isDocument()) {
            final String byKey = nameForKey(existing, selector.asDocument());
            if (byKey == null) {
                return CommandErrors.indexNotFound("can't find index with key: " + selector.asDocument().toJson());
            }
            names.add(byKey);
        } else {
            return CommandErrors.typeMismatch("index must be a string, array of strings, or key document");
        }

        for (final String name : names) {
            if ("_id_".equals(name)) {
                return CommandErrors.invalidOptions("cannot drop _id index");
            }
            if (existing.stream().noneMatch(index -> index.name().equals(name))) {
                return CommandErrors.indexNotFound("index not found with name [" + name + "]");
            }
        }

        store.dropIndexes(database, collection, List.copyOf(names));
        final BsonDocument response = new BsonDocument().append("nIndexesWas", new BsonInt32(existing.size()));
        if (all) {
            response.append("msg", new BsonString("non-_id indexes dropped for collection"));
        }
        return AdminCommandSupport.ok(response);
    }

    static String nameForKey(final List<CommandStore.IndexMetadata> indexes, final BsonDocument key) {
        for (final CommandStore.IndexMetadata index : indexes) {
            if (index.key().equals(key)) {
                return index.name();
            }
        }
        return null;
    }
}
