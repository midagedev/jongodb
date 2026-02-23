package org.jongodb.command;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.jongodb.txn.SessionTransactionPool;
import org.jongodb.txn.TransactionCommandValidator;
import org.jongodb.txn.TransactionCommandValidator.ValidationResult;

public final class CommandDispatcher {
    private static final int CODE_COMMAND_NOT_FOUND = 59;
    private static final int CODE_INVALID_ARGUMENT = 14;
    private static final int CODE_NO_SUCH_TRANSACTION = 251;
    private static final int CODE_DUPLICATE_KEY = 11000;

    private final Map<String, CommandHandler> handlers;
    private final CommandStore globalStore;
    private final SessionTransactionPool sessionPool;
    private final TransactionCommandValidator transactionValidator;
    private final ThreadLocal<CommandStore> dispatchStore = new ThreadLocal<>();

    public CommandDispatcher(final CommandStore store) {
        this.globalStore = Objects.requireNonNull(store, "store");
        this.sessionPool = new SessionTransactionPool();
        this.transactionValidator = new TransactionCommandValidator(sessionPool);
        final CommandStore routedStore = new RoutingCommandStore(() -> {
            final CommandStore selectedStore = dispatchStore.get();
            return selectedStore == null ? globalStore : selectedStore;
        });

        final Map<String, CommandHandler> configuredHandlers = new HashMap<>();
        configuredHandlers.put("hello", new HelloCommandHandler());
        configuredHandlers.put("ping", new PingCommandHandler());
        configuredHandlers.put("insert", new InsertCommandHandler(routedStore));
        configuredHandlers.put("find", new FindCommandHandler(routedStore));
        configuredHandlers.put("createindexes", new CreateIndexesCommandHandler(routedStore));
        configuredHandlers.put("update", new UpdateCommandHandler(routedStore));
        configuredHandlers.put("delete", new DeleteCommandHandler(routedStore));
        configuredHandlers.put("committransaction", new CommitTransactionCommandHandler());
        configuredHandlers.put("aborttransaction", new AbortTransactionCommandHandler());
        this.handlers = Map.copyOf(configuredHandlers);
    }

    public BsonDocument dispatch(final BsonDocument command) {
        if (command == null || command.isEmpty()) {
            return error("command document must not be empty", CODE_INVALID_ARGUMENT, "BadValue");
        }

        final String commandName = command.getFirstKey().toLowerCase(Locale.ROOT);
        final CommandHandler handler = handlers.get(commandName);
        if (handler == null) {
            return error("no such command: " + commandName, CODE_COMMAND_NOT_FOUND, "CommandNotFound");
        }

        final ValidationResult validation = transactionValidator.validate(commandName, command);
        if (validation.error() != null) {
            return validation.error();
        }

        if (!validation.transactional()) {
            return dispatchWithStore(globalStore, handler, command);
        }

        if (validation.startTransaction()) {
            final CommandStore transactionStore = globalStore.snapshotForTransaction();
            final boolean started =
                    sessionPool.startTransaction(validation.sessionId(), validation.txnNumber(), transactionStore);
            if (!started) {
                return error("transaction already in progress for this session", CODE_INVALID_ARGUMENT, "BadValue");
            }
        }

        if (validation.commitTransaction()) {
            final SessionTransactionPool.ActiveTransaction activeTransaction =
                    sessionPool.activeTransaction(validation.sessionId(), validation.txnNumber());
            if (activeTransaction == null) {
                return noSuchTransactionError(commandName);
            }

            globalStore.publishTransactionSnapshot(activeTransaction.store());
            if (!sessionPool.clearTransaction(validation.sessionId(), validation.txnNumber())) {
                return noSuchTransactionError(commandName);
            }
            return handler.handle(command);
        }

        if (validation.abortTransaction()) {
            if (!sessionPool.clearTransaction(validation.sessionId(), validation.txnNumber())) {
                return noSuchTransactionError(commandName);
            }
            return handler.handle(command);
        }

        final CommandStore transactionStore = sessionPool.transactionStore(validation.sessionId(), validation.txnNumber());
        if (transactionStore == null) {
            return noSuchTransactionError(commandName);
        }
        return dispatchWithStore(transactionStore, handler, command);
    }

    static BsonDocument error(final String message, final int code, final String codeName) {
        return new BsonDocument()
                .append("ok", new BsonDouble(0.0))
                .append("errmsg", new BsonString(message))
                .append("code", new BsonInt32(code))
                .append("codeName", new BsonString(codeName));
    }

    static BsonDocument duplicateKeyError(final String message) {
        return error(message, CODE_DUPLICATE_KEY, "DuplicateKey");
    }

    private static BsonDocument noSuchTransactionError(final String commandName) {
        return error(commandName + " requires an active transaction", CODE_NO_SUCH_TRANSACTION, "NoSuchTransaction");
    }

    private BsonDocument dispatchWithStore(
            final CommandStore selectedStore, final CommandHandler handler, final BsonDocument command) {
        dispatchStore.set(selectedStore);
        try {
            return handler.handle(command);
        } finally {
            dispatchStore.remove();
        }
    }

    private static final class RoutingCommandStore implements CommandStore {
        private final Supplier<CommandStore> supplier;

        private RoutingCommandStore(final Supplier<CommandStore> supplier) {
            this.supplier = Objects.requireNonNull(supplier, "supplier");
        }

        @Override
        public int insert(final String database, final String collection, final java.util.List<BsonDocument> documents) {
            return delegate().insert(database, collection, documents);
        }

        @Override
        public java.util.List<BsonDocument> find(
                final String database, final String collection, final BsonDocument filter) {
            return delegate().find(database, collection, filter);
        }

        @Override
        public CommandStore snapshotForTransaction() {
            return delegate().snapshotForTransaction();
        }

        @Override
        public void publishTransactionSnapshot(final CommandStore snapshot) {
            delegate().publishTransactionSnapshot(snapshot);
        }

        @Override
        public CreateIndexesResult createIndexes(
                final String database, final String collection, final java.util.List<IndexRequest> indexes) {
            return delegate().createIndexes(database, collection, indexes);
        }

        @Override
        public UpdateResult update(final String database, final String collection, final java.util.List<UpdateRequest> updates) {
            return delegate().update(database, collection, updates);
        }

        @Override
        public int delete(final String database, final String collection, final java.util.List<DeleteRequest> deletes) {
            return delegate().delete(database, collection, deletes);
        }

        private CommandStore delegate() {
            final CommandStore delegate = supplier.get();
            return Objects.requireNonNull(delegate, "resolved command store");
        }
    }
}
