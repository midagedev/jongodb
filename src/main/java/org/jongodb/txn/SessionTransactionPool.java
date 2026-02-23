package org.jongodb.txn;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks active transaction number per logical session.
 */
public final class SessionTransactionPool {
    private final Map<String, Long> activeTransactions = new ConcurrentHashMap<>();

    public boolean hasActiveTransaction(final String sessionId) {
        Objects.requireNonNull(sessionId, "sessionId");
        return activeTransactions.containsKey(sessionId);
    }

    public boolean hasActiveTransaction(final String sessionId, final long txnNumber) {
        Objects.requireNonNull(sessionId, "sessionId");
        final Long activeTxnNumber = activeTransactions.get(sessionId);
        return activeTxnNumber != null && activeTxnNumber == txnNumber;
    }

    public boolean startTransaction(final String sessionId, final long txnNumber) {
        Objects.requireNonNull(sessionId, "sessionId");
        return activeTransactions.putIfAbsent(sessionId, txnNumber) == null;
    }

    public boolean clearTransaction(final String sessionId, final long txnNumber) {
        Objects.requireNonNull(sessionId, "sessionId");
        return activeTransactions.remove(sessionId, txnNumber);
    }
}
