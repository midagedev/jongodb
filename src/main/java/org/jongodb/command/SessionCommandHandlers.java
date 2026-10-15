package org.jongodb.command;

import java.util.function.BiConsumer;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.jongodb.txn.SessionTransactionPool;

/**
 * {@code endSessions}, {@code killSessions}, {@code killAllSessions}, {@code refreshSessions}.
 * Sessions are only tracked while they hold a transaction, so these commands matter for that state alone.
 */
final class SessionCommandHandlers {
    private SessionCommandHandlers() {}

    static CommandHandler endSessions(final SessionTransactionPool pool) {
        return forEachSession("endSessions", pool, (p, id) -> p.endSession(id));
    }

    static CommandHandler killSessions(final SessionTransactionPool pool) {
        return forEachSession("killSessions", pool, (p, id) -> p.abortActive(id));
    }

    static CommandHandler refreshSessions() {
        return command -> {
            final BsonDocument error = validateSessionArray(command, "refreshSessions");
            return error != null ? error : AdminCommandSupport.ok(new BsonDocument());
        };
    }

    static CommandHandler killAllSessions(final SessionTransactionPool pool) {
        return command -> {
            pool.abortAll();
            return AdminCommandSupport.ok(new BsonDocument());
        };
    }

    private static CommandHandler forEachSession(
            final String name, final SessionTransactionPool pool, final BiConsumer<SessionTransactionPool, String> action) {
        return command -> {
            final BsonDocument error = validateSessionArray(command, name);
            if (error != null) {
                return error;
            }
            for (final BsonValue session : command.getArray(name)) {
                action.accept(pool, session.asDocument().toJson());
            }
            return AdminCommandSupport.ok(new BsonDocument());
        };
    }

    private static BsonDocument validateSessionArray(final BsonDocument command, final String name) {
        final BsonValue value = command.get(name);
        if (value == null || !value.isArray()) {
            return CommandErrors.typeMismatch(name + " must be an array");
        }
        for (final BsonValue session : value.asArray()) {
            if (!session.isDocument()) {
                return CommandErrors.typeMismatch(name + " entries must be session documents");
            }
        }
        return null;
    }
}
