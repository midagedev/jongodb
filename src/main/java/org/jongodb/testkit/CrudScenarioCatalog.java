package org.jongodb.testkit;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Deterministic CRUD-focused scenarios for differential testing.
 */
public final class CrudScenarioCatalog {
    private static final List<Scenario> SCENARIOS = List.of(
        scenario(
            "crud.insert-find",
            "insert documents and fetch a subset with find",
            command(
                "insert",
                payload(
                    "collection",
                    "users_insert_find",
                    "documents",
                    List.of(
                        payload("_id", 1, "name", "alpha", "tier", "gold"),
                        payload("_id", 2, "name", "beta", "tier", "silver")
                    )
                )
            ),
            command("find", payload("collection", "users_insert_find", "filter", payload("tier", "gold")))
        ),
        scenario(
            "crud.update-multi-set",
            "update multiple documents with $set",
            command(
                "insert",
                payload(
                    "collection",
                    "users_update_set",
                    "documents",
                    List.of(
                        payload("_id", 1, "status", "new"),
                        payload("_id", 2, "status", "new"),
                        payload("_id", 3, "status", "old")
                    )
                )
            ),
            command(
                "update",
                payload(
                    "collection",
                    "users_update_set",
                    "updates",
                    List.of(
                        payload(
                            "q",
                            payload("status", "new"),
                            "u",
                            payload("$set", payload("status", "ready")),
                            "multi",
                            true
                        )
                    )
                )
            ),
            command("find", payload("collection", "users_update_set", "filter", payload("status", "ready")))
        ),
        scenario(
            "crud.update-inc",
            "increment numeric fields with $inc",
            command(
                "insert",
                payload(
                    "collection",
                    "users_update_inc",
                    "documents",
                    List.of(
                        payload("_id", 1, "group", "a", "score", 1),
                        payload("_id", 2, "group", "a", "score", 4),
                        payload("_id", 3, "group", "b", "score", 7)
                    )
                )
            ),
            command(
                "update",
                payload(
                    "collection",
                    "users_update_inc",
                    "updates",
                    List.of(
                        payload("q", payload("group", "a"), "u", payload("$inc", payload("score", 2)), "multi", true)
                    )
                )
            ),
            command("find", payload("collection", "users_update_inc", "filter", payload("group", "a")))
        ),
        scenario(
            "crud.delete-one",
            "delete one matching document",
            command(
                "insert",
                payload(
                    "collection",
                    "users_delete_one",
                    "documents",
                    List.of(
                        payload("_id", 1, "kind", "temp"),
                        payload("_id", 2, "kind", "temp"),
                        payload("_id", 3, "kind", "stable")
                    )
                )
            ),
            command(
                "delete",
                payload(
                    "collection",
                    "users_delete_one",
                    "deletes",
                    List.of(payload("q", payload("kind", "temp"), "limit", 1))
                )
            ),
            command("find", payload("collection", "users_delete_one", "filter", payload("kind", "temp")))
        ),
        scenario(
            "crud.delete-many",
            "delete all matching documents",
            command(
                "insert",
                payload(
                    "collection",
                    "users_delete_many",
                    "documents",
                    List.of(
                        payload("_id", 1, "tag", "drop"),
                        payload("_id", 2, "tag", "drop"),
                        payload("_id", 3, "tag", "keep")
                    )
                )
            ),
            command(
                "delete",
                payload(
                    "collection",
                    "users_delete_many",
                    "deletes",
                    List.of(payload("q", payload("tag", "drop"), "limit", 0))
                )
            ),
            command("find", payload("collection", "users_delete_many", "filter", payload("tag", "drop")))
        ),
        scenario(
            "crud.create-indexes-duplicate-key",
            "create unique index then verify duplicate insert failure path",
            command(
                "insert",
                payload(
                    "collection",
                    "users_unique_email",
                    "documents",
                    List.of(
                        payload("_id", 1, "email", "alpha@example.com"),
                        payload("_id", 2, "email", "beta@example.com")
                    )
                )
            ),
            command(
                "createIndexes",
                payload(
                    "collection",
                    "users_unique_email",
                    "indexes",
                    List.of(payload("name", "email_1", "key", payload("email", 1), "unique", true))
                )
            ),
            command(
                "insert",
                payload(
                    "collection",
                    "users_unique_email",
                    "documents",
                    List.of(payload("_id", 3, "email", "alpha@example.com"))
                )
            )
        )
    );

    private CrudScenarioCatalog() {
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
