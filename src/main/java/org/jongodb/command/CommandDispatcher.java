package org.jongodb.command;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.bson.BsonDocument;
import org.jongodb.txn.SessionTransactionPool;
import org.jongodb.txn.TransactionCommandValidator;
import org.jongodb.txn.TransactionCommandValidator.ValidationResult;

public final class CommandDispatcher {
    private final Map<String, CommandHandler> handlers;
    private final CommandStore globalStore;
    private final SessionTransactionPool sessionPool;
    private final TransactionCommandValidator transactionValidator;
    private final ThreadLocal<CommandStore> dispatchStore = new ThreadLocal<>();

    public CommandDispatcher(final CommandStore store) {
        this.globalStore = Objects.requireNonNull(store, "store");
        this.sessionPool = new SessionTransactionPool();
        this.transactionValidator = new TransactionCommandValidator(sessionPool);
        final CursorRegistry cursorRegistry = new CursorRegistry();
        final CommandStore routedStore = new RoutingCommandStore(() -> {
            final CommandStore selectedStore = dispatchStore.get();
            return selectedStore == null ? globalStore : selectedStore;
        });

        final Map<String, CommandHandler> configuredHandlers = new HashMap<>();
        configuredHandlers.put("hello", new HelloCommandHandler());
        configuredHandlers.put("ismaster", new HelloCommandHandler());
        configuredHandlers.put("ping", new PingCommandHandler());
        configuredHandlers.put("buildinfo", new BuildInfoCommandHandler());
        configuredHandlers.put("getparameter", new GetParameterCommandHandler());
        configuredHandlers.put("insert", new InsertCommandHandler(routedStore));
        configuredHandlers.put("find", new FindCommandHandler(routedStore, cursorRegistry));
        configuredHandlers.put("aggregate", new AggregateCommandHandler(routedStore, cursorRegistry));
        configuredHandlers.put("getmore", new GetMoreCommandHandler(cursorRegistry));
        configuredHandlers.put("killcursors", new KillCursorsCommandHandler(cursorRegistry));
        configuredHandlers.put("createindexes", new CreateIndexesCommandHandler(routedStore));
        configuredHandlers.put("listindexes", new ListIndexesCommandHandler(routedStore, cursorRegistry));
        configuredHandlers.put("update", new UpdateCommandHandler(routedStore));
        configuredHandlers.put("delete", new DeleteCommandHandler(routedStore));
        configuredHandlers.put("findandmodify", new FindAndModifyCommandHandler(routedStore));
        configuredHandlers.put("committransaction", new CommitTransactionCommandHandler());
        configuredHandlers.put("aborttransaction", new AbortTransactionCommandHandler());
        this.handlers = Map.copyOf(configuredHandlers);
    }

    public BsonDocument dispatch(final BsonDocument command) {
        if (command == null || command.isEmpty()) {
            return CommandErrors.badValue("command document must not be empty");
        }

        final String commandName = command.getFirstKey().toLowerCase(Locale.ROOT);
        final CommandHandler handler = handlers.get(commandName);
        if (handler == null) {
            return CommandErrors.commandNotFound(commandName);
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
                return CommandErrors.badValue("transaction already in progress for this session");
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

    private static BsonDocument noSuchTransactionError(final String commandName) {
        if ("committransaction".equals(commandName)) {
            return CommandErrors.noSuchTransactionWithUnknownCommitResultLabel(commandName);
        }
        if ("aborttransaction".equals(commandName)) {
            return CommandErrors.noSuchTransaction(commandName);
        }
        return CommandErrors.noSuchTransactionWithTransientLabel(commandName);
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
        public java.util.List<BsonDocument> aggregate(
                final String database, final String collection, final java.util.List<BsonDocument> pipeline) {
            return delegate().aggregate(database, collection, pipeline);
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
        public java.util.List<IndexMetadata> listIndexes(final String database, final String collection) {
            return delegate().listIndexes(database, collection);
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
