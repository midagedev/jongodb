package org.jongodb.testkit;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Deterministic transaction-focused scenarios for differential testing.
 */
public final class TransactionScenarioCatalog {
    private static final List<Scenario> SCENARIOS = List.of(
        scenario(
            "txn.start-commit-path",
            "start transaction, perform write, commit, then verify committed view",
            command(
                "insert",
                payload(
                    "collection",
                    "txn_start_commit",
                    "documents",
                    List.of(payload("_id", 1, "stage", "started")),
                    "lsid",
                    payload("id", "session-start-commit"),
                    "txnNumber",
                    1,
                    "autocommit",
                    false,
                    "startTransaction",
                    true
                )
            ),
            command(
                "update",
                payload(
                    "collection",
                    "txn_start_commit",
                    "updates",
                    List.of(
                        payload(
                            "q",
                            payload("_id", 1),
                            "u",
                            payload("$set", payload("stage", "committed"))
                        )
                    ),
                    "lsid",
                    payload("id", "session-start-commit"),
                    "txnNumber",
                    1,
                    "autocommit",
                    false
                )
            ),
            command(
                "commitTransaction",
                payload(
                    "lsid",
                    payload("id", "session-start-commit"),
                    "txnNumber",
                    1,
                    "autocommit",
                    false
                )
            ),
            command("find", payload("collection", "txn_start_commit", "filter", payload("_id", 1)))
        ),
        scenario(
            "txn.start-abort-path",
            "start transaction, perform write, abort, then verify rollback view",
            command(
                "insert",
                payload(
                    "collection",
                    "txn_start_abort",
                    "documents",
                    List.of(payload("_id", 1, "stage", "to-abort")),
                    "lsid",
                    payload("id", "session-start-abort"),
                    "txnNumber",
                    1,
                    "autocommit",
                    false,
                    "startTransaction",
                    true
                )
            ),
            command(
                "abortTransaction",
                payload(
                    "lsid",
                    payload("id", "session-start-abort"),
                    "txnNumber",
                    1,
                    "autocommit",
                    false
                )
            ),
            command("find", payload("collection", "txn_start_abort", "filter", payload("_id", 1)))
        ),
        scenario(
            "txn.no-such-transaction-path",
            "execute transactional command without active transaction",
            command(
                "find",
                payload(
                    "collection",
                    "txn_no_such_transaction",
                    "filter",
                    payload("_id", 1),
                    "lsid",
                    payload("id", "session-no-such-transaction"),
                    "txnNumber",
                    7,
                    "autocommit",
                    false
                )
            )
        ),
        scenario(
            "txn.lifecycle-transition-path",
            "commit one transaction, abort the next, and verify only committed writes persist",
            command(
                "insert",
                payload(
                    "collection",
                    "txn_lifecycle_transition",
                    "documents",
                    List.of(payload("_id", 1, "status", "committed")),
                    "lsid",
                    payload("id", "session-lifecycle"),
                    "txnNumber",
                    1,
                    "autocommit",
                    false,
                    "startTransaction",
                    true
                )
            ),
            command(
                "commitTransaction",
                payload(
                    "lsid",
                    payload("id", "session-lifecycle"),
                    "txnNumber",
                    1,
                    "autocommit",
                    false
                )
            ),
            command(
                "insert",
                payload(
                    "collection",
                    "txn_lifecycle_transition",
                    "documents",
                    List.of(payload("_id", 2, "status", "aborted")),
                    "lsid",
                    payload("id", "session-lifecycle"),
                    "txnNumber",
                    2,
                    "autocommit",
                    false,
                    "startTransaction",
                    true
                )
            ),
            command(
                "abortTransaction",
                payload(
                    "lsid",
                    payload("id", "session-lifecycle"),
                    "txnNumber",
                    2,
                    "autocommit",
                    false
                )
            ),
            command("find", payload("collection", "txn_lifecycle_transition", "filter", payload()))
        )
    );

    private TransactionScenarioCatalog() {
    }

    public static List<Scenario> scenarios() {
        return SCENARIOS;
    }

    private static Scenario scenario(String id, String description, ScenarioCommand... commands) {
        return new Scenario(id, description, List.of(commands));
    }

    private static ScenarioCommand command(String commandName, Map<String, Object> payload) {
        return new ScenarioCommand(commandName, payload);
    }

    private static Map<String, Object> payload(Object... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("keyValues length must be even");
        }
        Map<String, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            Object key = keyValues[i];
            if (!(key instanceof String keyString)) {
                throw new IllegalArgumentException("keyValues keys must be strings");
            }
            values.put(keyString, keyValues[i + 1]);
        }
        return Collections.unmodifiableMap(values);
    }
}
