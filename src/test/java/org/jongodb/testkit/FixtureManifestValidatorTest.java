package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import org.junit.jupiter.api.Test;

class FixtureManifestValidatorTest {
    @Test
    void reportsFriendlyValidationErrors() {
        final String invalidJson =
                """
                {
                  "schemaVersion": "fixture-manifest.v2",
                  "source": { "uriAlias": "x" },
                  "profiles": {
                    "dev": {
                      "refreshMode": "full",
                      "collections": [
                        {
                          "database": "app",
                          "collection": "users",
                          "limit": 10
                        }
                      ]
                    },
                    "smoke": {
                      "refreshMode": "full",
                      "collections": [
                        {
                          "database": "app",
                          "collection": "orders",
                          "sort": { "_id": 1 },
                          "sample": { "size": 0, "seed": "smoke-seed" }
                        }
                      ]
                    },
                    "full": {
                      "refreshMode": "incremental",
                      "collections": [
                        {
                          "database": "app",
                          "collection": "orders"
                        }
                      ]
                    }
                  }
                }
                """;

        try {
            final FixtureManifest manifest = FixtureManifest.fromJson(invalidJson);
            final List<String> errors = FixtureManifestValidator.validate(manifest);

            assertTrue(errors.stream().anyMatch(message -> message.contains("schemaVersion must be")));
            assertTrue(errors.stream().anyMatch(message -> message.contains("source.uriAlias must be at least 2 characters")));
            assertTrue(errors.stream().anyMatch(message -> message.contains("profiles.dev.collections[0].sort is required")));
            assertTrue(errors.stream().anyMatch(message -> message.contains("profiles.smoke.collections[0].sample.size must be > 0")));
            assertTrue(errors.stream().anyMatch(message -> message.contains("profiles.full.collections[0].sort is required for incremental refresh mode")));
        } catch (final Exception e) {
            fail("expected validation error list, but parsing threw: " + e.getMessage());
        }
    }
}
