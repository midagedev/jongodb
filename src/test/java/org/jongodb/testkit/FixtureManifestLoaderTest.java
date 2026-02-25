package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

class FixtureManifestLoaderTest {
    @Test
    void loadsJsonTemplateManifests() throws Exception {
        final List<Path> templates = List.of(
                Path.of("testkit/fixture/manifests/baseline-dev-smoke-full.json"),
                Path.of("testkit/fixture/manifests/incremental-events.json"),
                Path.of("testkit/fixture/manifests/sensitive-field-rules.json"));

        for (final Path template : templates) {
            final FixtureManifest manifest = FixtureManifestLoader.load(template);
            assertEquals(FixtureManifest.SCHEMA_VERSION, manifest.schemaVersion());
            assertEquals(3, manifest.profiles().size());
            assertNotNull(manifest.profile(FixtureManifest.ScenarioProfile.DEV));
            assertNotNull(manifest.profile(FixtureManifest.ScenarioProfile.SMOKE));
            assertNotNull(manifest.profile(FixtureManifest.ScenarioProfile.FULL));
        }
    }

    @Test
    void returnsClearErrorForMissingManifestPath() {
        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> FixtureManifestLoader.load(Path.of("testkit/fixture/manifests/missing.json")));
        assertTrue(error.getMessage().contains("manifest path does not exist"));
    }
}
