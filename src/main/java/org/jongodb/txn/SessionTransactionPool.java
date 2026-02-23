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
    private final Map<String, Long> lastSeenTxnNumbers = new ConcurrentHashMap<>();

    public boolean hasActiveTransaction(final String sessionId) {
        Objects.requireNonNull(sessionId, "sessionId");
        return activeTransactions.containsKey(sessionId);
    }

    public boolean hasActiveTransaction(final String sessionId, final long txnNumber) {
        Objects.requireNonNull(sessionId, "sessionId");
        final ActiveTransaction activeTransaction = activeTransactions.get(sessionId);
        return activeTransaction != null && activeTransaction.txnNumber() == txnNumber;
    }

    public synchronized boolean startTransaction(
            final String sessionId, final long txnNumber, final CommandStore transactionStore) {
        Objects.requireNonNull(sessionId, "sessionId");
        Objects.requireNonNull(transactionStore, "transactionStore");
        if (activeTransactions.containsKey(sessionId)) {
            return false;
        }
        final Long lastSeenTxnNumber = lastSeenTxnNumbers.get(sessionId);
        if (lastSeenTxnNumber != null && txnNumber <= lastSeenTxnNumber) {
            return false;
        }
        activeTransactions.put(sessionId, new ActiveTransaction(txnNumber, transactionStore));
        lastSeenTxnNumbers.put(sessionId, txnNumber);
        return true;
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

    public boolean isTxnNumberReused(final String sessionId, final long txnNumber) {
        Objects.requireNonNull(sessionId, "sessionId");
        final Long lastSeenTxnNumber = lastSeenTxnNumbers.get(sessionId);
        return lastSeenTxnNumber != null && txnNumber <= lastSeenTxnNumber;
    }

    public record ActiveTransaction(long txnNumber, CommandStore store) {}
}
