package org.jongodb.txn;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.jongodb.command.CommandStore;

/**
 * Tracks active transaction number per logical session.
 */
public final class SessionTransactionPool {
    private final Map<String, ActiveTransaction> activeTransactions = new ConcurrentHashMap<>();

    public boolean hasActiveTransaction(final String sessionId) {
        Objects.requireNonNull(sessionId, "sessionId");
        return activeTransactions.containsKey(sessionId);
    }

    public boolean hasActiveTransaction(final String sessionId, final long txnNumber) {
        Objects.requireNonNull(sessionId, "sessionId");
        final ActiveTransaction activeTransaction = activeTransactions.get(sessionId);
        return activeTransaction != null && activeTransaction.txnNumber() == txnNumber;
    }

    public boolean startTransaction(final String sessionId, final long txnNumber, final CommandStore transactionStore) {
        Objects.requireNonNull(sessionId, "sessionId");
        Objects.requireNonNull(transactionStore, "transactionStore");
        return activeTransactions.putIfAbsent(sessionId, new ActiveTransaction(txnNumber, transactionStore)) == null;
    }

    public boolean clearTransaction(final String sessionId, final long txnNumber) {
        Objects.requireNonNull(sessionId, "sessionId");
        final ActiveTransaction activeTransaction = activeTransactions.get(sessionId);
        if (activeTransaction == null || activeTransaction.txnNumber() != txnNumber) {
            return false;
        }
        return activeTransactions.remove(sessionId, activeTransaction);
    }

    public CommandStore transactionStore(final String sessionId, final long txnNumber) {
        Objects.requireNonNull(sessionId, "sessionId");
        final ActiveTransaction activeTransaction = activeTransactions.get(sessionId);
        if (activeTransaction == null || activeTransaction.txnNumber() != txnNumber) {
            return null;
        }
        return activeTransaction.store();
    }

    public ActiveTransaction activeTransaction(final String sessionId, final long txnNumber) {
        Objects.requireNonNull(sessionId, "sessionId");
        final ActiveTransaction activeTransaction = activeTransactions.get(sessionId);
        if (activeTransaction == null || activeTransaction.txnNumber() != txnNumber) {
            return null;
        }
        return activeTransaction;
    }

    public record ActiveTransaction(long txnNumber, CommandStore store) {}
}
