package org.jongodb.command;

import java.util.List;
import java.util.Set;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonValue;

public final class CollModCommandHandler implements CommandHandler {
    /** Accepted and ignored: document validation and pre/post images are not modelled. */
    private static final Set<String> IGNORED_OPTIONS = Set.of(
            "validator", "validationLevel", "validationAction", "changeStreamPreAndPostImages", "recordPreImages");
    private static final Set<String> INDEX_FIELDS = Set.of("name", "keyPattern", "expireAfterSeconds", "hidden");

    private final CommandStore store;

    public CollModCommandHandler(final CommandStore store) {
        this.store = store;
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String database = CommandCanonicalizer.readDatabase(command);
        final String collection = AdminCommandSupport.string(command, "collMod");
        if (collection == null) {
            return CommandErrors.typeMismatch("collMod must be a string");
        }
        for (final String key : command.keySet()) {
            if (key.equals("collMod")
                    || key.equals("index")
                    || IGNORED_OPTIONS.contains(key)
                    || AdminCommandSupport.ENVELOPE_FIELDS.contains(key)) {
                continue;
            }
            return CommandErrors.notImplemented("collMod option '" + key + "' is not supported");
        }
        if (!AdminCommandSupport.collectionExists(store, database, collection)) {
            return CommandErrors.namespaceNotFound(database + "." + collection);
        }

        final BsonValue indexValue = command.get("index");
        if (indexValue == null) {
            return AdminCommandSupport.ok(new BsonDocument());
        }
        if (!indexValue.isDocument()) {
            return CommandErrors.typeMismatch("collMod.index must be a document");
        }
        final BsonDocument index = indexValue.asDocument();
        for (final String key : index.keySet()) {
            if (!INDEX_FIELDS.contains(key)) {
                return CommandErrors.failedToParse("BSON field 'collMod.index." + key + "' is an unknown field.");
            }
        }
        if (index.containsKey("name") == index.containsKey("keyPattern")) {
            return CommandErrors.badValue("Cannot specify both key pattern and name, or neither.");
        }

        final List<CommandStore.IndexMetadata> existing = store.listIndexes(database, collection);
        final String name = index.containsKey("name")
                ? (index.get("name").isString() ? index.get("name").asString().getValue() : null)
                : (index.get("keyPattern").isDocument()
                        ? DropIndexesCommandHandler.nameForKey(existing, index.get("keyPattern").asDocument())
                        : null);
        final CommandStore.IndexMetadata target = name == null
                ? null
                : existing.stream().filter(i -> i.name().equals(name)).findFirst().orElse(null);
        if (target == null) {
            return CommandErrors.indexNotFound("cannot find index " + (name == null ? "" : name) + " for ns "
                    + database + "." + collection);
        }

        final BsonValue expire = index.get("expireAfterSeconds");
        if (expire == null) {
            return AdminCommandSupport.ok(new BsonDocument());
        }
        final Long newTtl;
        if (expire.isString() && "off".equals(expire.asString().getValue())) {
            newTtl = null;
        } else if (expire.isNumber() && expire.asNumber().doubleValue() >= 0
                && expire.asNumber().doubleValue() == Math.rint(expire.asNumber().doubleValue())) {
            newTtl = expire.asNumber().longValue();
        } else {
            return CommandErrors.badValue("expireAfterSeconds must be a non-negative integer or 'off'");
        }
        if (target.expireAfterSeconds() == null) {
            return CommandErrors.badValue("no expireAfterSeconds field to update");
        }

        final Long old = store.modifyIndexTtl(database, collection, target.name(), newTtl);
        final BsonDocument response = new BsonDocument();
        if (old != null) {
            response.append("expireAfterSeconds_old", new BsonInt64(old));
        }
        if (newTtl != null) {
            response.append("expireAfterSeconds_new", new BsonInt64(newTtl));
        }
        return AdminCommandSupport.ok(response);
    }
}
