package org.jongodb.command;

import java.util.Set;
import org.bson.BsonDocument;

public final class CreateCollectionCommandHandler implements CommandHandler {
    /** Options whose semantics change results; accepting them silently would make the server lie. */
    private static final Set<String> REJECTED_OPTIONS = Set.of(
            "capped", "size", "max", "timeseries", "clusteredIndex", "viewOn", "pipeline", "collation", "encryptedFields",
            "expireAfterSeconds");

    /** Options that are accepted and ignored: validation and storage tuning are not modelled. */
    private static final Set<String> IGNORED_OPTIONS = Set.of(
            "validator",
            "validationLevel",
            "validationAction",
            "storageEngine",
            "indexOptionDefaults",
            "autoIndexId",
            "changeStreamPreAndPostImages",
            "recordPreImages");

    private final CommandStore store;

    public CreateCollectionCommandHandler(final CommandStore store) {
        this.store = store;
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String database = CommandCanonicalizer.readDatabase(command);
        final String collection = AdminCommandSupport.string(command, "create");
        if (collection == null) {
            return CommandErrors.typeMismatch("create must be a string");
        }
        if (!AdminCommandSupport.isValidCollectionName(collection)) {
            return CommandErrors.invalidNamespace("Invalid collection name: " + collection);
        }
        for (final String key : command.keySet()) {
            if (key.equals("create") || IGNORED_OPTIONS.contains(key) || AdminCommandSupport.ENVELOPE_FIELDS.contains(key)) {
                continue;
            }
            if (REJECTED_OPTIONS.contains(key)) {
                if (key.equals("capped") && command.get(key).isBoolean() && !command.get(key).asBoolean().getValue()) {
                    continue;
                }
                return CommandErrors.notImplemented("create option '" + key + "' is not supported");
            }
            return CommandErrors.failedToParse("BSON field 'create." + key + "' is an unknown field.");
        }

        if (!store.createCollection(database, collection)) {
            return CommandErrors.namespaceExists("Collection already exists. NS: " + database + "." + collection);
        }
        return AdminCommandSupport.ok(new BsonDocument());
    }
}
