package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class FixtureExtractionPlannerTest {
    @Test
    void producesDeterministicFingerprintPerProfile() throws Exception {
        final FixtureManifest manifest =
                FixtureManifestLoader.load(java.nio.file.Path.of("testkit/fixture/manifests/baseline-dev-smoke-full.json"));

        final FixtureExtractionPlan first = FixtureExtractionPlanner.plan(
                manifest,
                FixtureManifest.ScenarioProfile.DEV);
        final FixtureExtractionPlan second = FixtureExtractionPlanner.plan(
                manifest,
                FixtureManifest.ScenarioProfile.DEV);
        final FixtureExtractionPlan smoke = FixtureExtractionPlanner.plan(
                manifest,
                FixtureManifest.ScenarioProfile.SMOKE);

        assertEquals(first.fingerprint(), second.fingerprint());
        assertNotEquals(first.fingerprint(), smoke.fingerprint());
        assertEquals(FixtureManifest.SCHEMA_VERSION, first.schemaVersion());
        assertEquals("dev", first.profile().value());
        assertTrue(first.collections().size() >= 1);
    }

    @Test
    void failsWhenManifestDoesNotMeetSchemaContract() {
        final String invalidManifestJson =
                """
                {
                  "schemaVersion": "fixture-manifest.v1",
                  "source": { "uriAlias": "prod-core" },
                  "profiles": {
                    "dev": {
                      "refreshMode": "full",
                      "collections": [
                        { "database": "app", "collection": "users", "limit": 10 }
                      ]
                    }
                  }
                }
                """;
        final FixtureManifest manifest = FixtureManifest.fromJson(invalidManifestJson);

        final FixtureManifestValidationException error = assertThrows(
                FixtureManifestValidationException.class,
                () -> FixtureExtractionPlanner.plan(manifest, FixtureManifest.ScenarioProfile.DEV));
        assertTrue(error.getMessage().contains("profiles.smoke is required"));
        assertTrue(error.getMessage().contains("profiles.full is required"));
    }
}
