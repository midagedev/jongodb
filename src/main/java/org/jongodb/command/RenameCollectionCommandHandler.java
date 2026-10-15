package org.jongodb.command;

import org.bson.BsonDocument;
import org.bson.BsonValue;

public final class RenameCollectionCommandHandler implements CommandHandler {
    private final CommandStore store;

    public RenameCollectionCommandHandler(final CommandStore store) {
        this.store = store;
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final String from = AdminCommandSupport.string(command, "renameCollection");
        final String to = AdminCommandSupport.string(command, "to");
        if (from == null || to == null) {
            return CommandErrors.typeMismatch("renameCollection and to must be namespace strings");
        }
        final String[] source = split(from);
        final String[] target = split(to);
        if (source == null || target == null) {
            return CommandErrors.invalidNamespace("Invalid namespace specified '" + (source == null ? from : to) + "'");
        }
        if (!AdminCommandSupport.isValidCollectionName(target[1]) || target[1].startsWith("system.")) {
            return CommandErrors.illegalOperation("Invalid target namespace: " + to);
        }
        if (from.equals(to)) {
            return CommandErrors.illegalOperation("Can't rename a collection to itself");
        }
        final BsonValue dropTargetValue = command.get("dropTarget");
        final boolean dropTarget = dropTargetValue != null
                && (dropTargetValue.isBoolean()
                        ? dropTargetValue.asBoolean().getValue()
                        : dropTargetValue.isNumber() && dropTargetValue.asNumber().doubleValue() != 0);

        if (!AdminCommandSupport.collectionExists(store, source[0], source[1])) {
            return CommandErrors.namespaceNotFound(from);
        }
        if (AdminCommandSupport.collectionExists(store, target[0], target[1]) && !dropTarget) {
            return CommandErrors.namespaceExists("target namespace exists");
        }
        store.renameCollection(source[0], source[1], target[0], target[1], dropTarget);
        return AdminCommandSupport.ok(new BsonDocument());
    }

    private static String[] split(final String namespace) {
        final int dot = namespace.indexOf('.');
        if (dot <= 0 || dot == namespace.length() - 1) {
            return null;
        }
        return new String[] {namespace.substring(0, dot), namespace.substring(dot + 1)};
    }
}
