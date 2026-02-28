package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression pack for official UTF shard failures from run 22513632933.
 *
 * <p>Mapped failure IDs:
 *
 * <p>- crud/tests/unified/db-aggregate.json::1:aggregate-with-listlocalsessions
 * <p>- crud/tests/unified/db-aggregate.json::2:aggregate-with-listlocalsessions-and-allowdiskuse
 * <p>- crud/tests/unified/db-aggregate.yml::1:aggregate-with-listlocalsessions
 * <p>- crud/tests/unified/db-aggregate.yml::2:aggregate-with-listlocalsessions-and-allowdiskuse
 */
class ListLocalSessionsRegressionPackTest {
    private static final String FAILURE_ID_JSON_CASE_1 =
            "crud/tests/unified/db-aggregate.json::1:aggregate-with-listlocalsessions";
    private static final String FAILURE_ID_JSON_CASE_2 =
            "crud/tests/unified/db-aggregate.json::2:aggregate-with-listlocalsessions-and-allowdiskuse";
    private static final String FAILURE_ID_YML_CASE_1 =
            "crud/tests/unified/db-aggregate.yml::1:aggregate-with-listlocalsessions";
    private static final String FAILURE_ID_YML_CASE_2 =
            "crud/tests/unified/db-aggregate.yml::2:aggregate-with-listlocalsessions-and-allowdiskuse";

    private static final List<String> OFFICIAL_FAILURE_IDS = List.of(
            FAILURE_ID_JSON_CASE_1,
            FAILURE_ID_JSON_CASE_2,
            FAILURE_ID_YML_CASE_1,
            FAILURE_ID_YML_CASE_2);

    private static final List<String> OFFICIAL_MANIFEST_RESOURCES = List.of(
            "testkit/official-suite-artifacts/22513632933/shard-0/baseline/failure-replay-bundles/manifest.json",
            "testkit/official-suite-artifacts/22513632933/shard-1/baseline/failure-replay-bundles/manifest.json");

    @TempDir
    Path tempDir;

    @Test
    void officialReplayBundleFixtureCapturesFourHistoricalListLocalSessionsFailures() throws Exception {
        final List<DeterministicReplayBundles.Bundle> bundles = loadOfficialReplayBundles();
        assertEquals(
                OFFICIAL_FAILURE_IDS,
                bundles.stream().map(DeterministicReplayBundles.Bundle::failureId).toList());

        for (final DeterministicReplayBundles.Bundle bundle : bundles) {
            assertEquals("aggregate", bundle.commands().get(0).commandName());
            final Map<String, Object> payload = bundle.commands().get(0).payload();
            final List<?> pipeline = asList(payload.get("pipeline"), bundle.failureId() + " pipeline");
            assertEquals(4, pipeline.size());

            final Map<String, Object> firstStage = asMap(pipeline.get(0), bundle.failureId() + " first stage");
            assertTrue(firstStage.containsKey("$limit"));
            assertEquals(0, asNumber(firstStage.get("$limit"), bundle.failureId() + " first stage $limit").intValue());

            if (bundle.failureId().contains("allowdiskuse")) {
                assertEquals(Boolean.TRUE, payload.get("allowDiskUse"));
            } else {
                assertFalse(payload.containsKey("allowDiskUse"));
            }
        }
    }

    @Test
    void importerNormalizesOfficialFailureCasesToValidAggregateLimitAndWireExecution() throws Exception {
        final UnifiedSpecImporter.ImportResult result = importOfficialListLocalSessionsCorpus();
        assertEquals(4, result.importedCount());
        assertEquals(0, result.unsupportedCount());

        final Map<String, Scenario> scenariosById = new LinkedHashMap<>();
        for (final UnifiedSpecImporter.ImportedScenario imported : result.importedScenarios()) {
            scenariosById.put(imported.caseId(), imported.scenario());
        }
        assertEquals(OFFICIAL_FAILURE_IDS, scenariosById.keySet().stream().sorted().toList());

        final WireCommandIngressBackend backend = new WireCommandIngressBackend("wire");
        for (final String failureId : OFFICIAL_FAILURE_IDS) {
            final Scenario scenario = scenariosById.get(failureId);
            assertNotNull(scenario, "missing imported scenario for " + failureId);
            assertEquals(1, scenario.commands().size());

            final ScenarioCommand command = scenario.commands().get(0);
            assertEquals("aggregate", command.commandName());
            final Map<String, Object> payload = command.payload();
            final List<?> pipeline = asList(payload.get("pipeline"), failureId + " pipeline");
            assertEquals(4, pipeline.size());

            final Map<String, Object> firstStage = asMap(pipeline.get(0), failureId + " first stage");
            assertTrue(firstStage.containsKey("$limit"));
            assertEquals(1, asNumber(firstStage.get("$limit"), failureId + " first stage $limit").intValue());

            if (failureId.contains("allowdiskuse")) {
                assertEquals(Boolean.TRUE, payload.get("allowDiskUse"));
            } else {
                assertFalse(payload.containsKey("allowDiskUse"));
            }

            final ScenarioOutcome outcome = backend.execute(scenario);
            assertTrue(outcome.success(), failureId + " should execute successfully: "
                    + outcome.errorMessage().orElse("expected success"));
        }
    }

    @Test
    void jsonAndYamlVariantsStayPayloadEquivalentForListLocalSessionsCases() throws Exception {
        final UnifiedSpecImporter.ImportResult result = importOfficialListLocalSessionsCorpus();

        final Map<String, Map<String, Object>> payloadByFailureId = new LinkedHashMap<>();
        for (final UnifiedSpecImporter.ImportedScenario imported : result.importedScenarios()) {
            payloadByFailureId.put(imported.caseId(), imported.scenario().commands().get(0).payload());
        }

        assertEquals(
                payloadByFailureId.get(FAILURE_ID_JSON_CASE_1),
                payloadByFailureId.get(FAILURE_ID_YML_CASE_1));
        assertEquals(
                payloadByFailureId.get(FAILURE_ID_JSON_CASE_2),
                payloadByFailureId.get(FAILURE_ID_YML_CASE_2));
    }

    private UnifiedSpecImporter.ImportResult importOfficialListLocalSessionsCorpus() throws IOException {
        final Path corpusRoot = tempDir.resolve("official-corpus");
        final Path aggregateJson = corpusRoot.resolve("crud/tests/unified/db-aggregate.json");
        final Path aggregateYml = corpusRoot.resolve("crud/tests/unified/db-aggregate.yml");
        Files.createDirectories(aggregateJson.getParent());
        Files.writeString(aggregateJson, aggregateJsonFixture());
        Files.writeString(aggregateYml, aggregateYmlFixture());
        return new UnifiedSpecImporter().importCorpus(corpusRoot);
    }

    private static String aggregateJsonFixture() {
        return """
                {
                  "database_name": "admin",
                  "collection_name": "users",
                  "tests": [
                    {
                      "description": "aggregate with listLocalSessions",
                      "operations": [
                        {
                          "name": "aggregate",
                          "arguments": {
                            "pipeline": [
                              {"$listLocalSessions": {}},
                              {"$limit": 1},
                              {"$addFields": {"dummy": "dummy field"}},
                              {"$project": {"_id": 0, "dummy": 1}}
                            ]
                          }
                        }
                      ]
                    },
                    {
                      "description": "aggregate with listLocalSessions and allowDiskUse",
                      "operations": [
                        {
                          "name": "aggregate",
                          "arguments": {
                            "allowDiskUse": true,
                            "pipeline": [
                              {"$listLocalSessions": {}},
                              {"$limit": 1},
                              {"$addFields": {"dummy": "dummy field"}},
                              {"$project": {"_id": 0, "dummy": 1}}
                            ]
                          }
                        }
                      ]
                    }
                  ]
                }
                """;
    }

    private static String aggregateYmlFixture() {
        return """
                database_name: admin
                collection_name: users
                tests:
                  - description: aggregate with listLocalSessions
                    operations:
                      - name: aggregate
                        arguments:
                          pipeline:
                            - $listLocalSessions: {}
                            - $limit: 1
                            - $addFields:
                                dummy: dummy field
                            - $project:
                                _id: 0
                                dummy: 1
                  - description: aggregate with listLocalSessions and allowDiskUse
                    operations:
                      - name: aggregate
                        arguments:
                          allowDiskUse: true
                          pipeline:
                            - $listLocalSessions: {}
                            - $limit: 1
                            - $addFields:
                                dummy: dummy field
                            - $project:
                                _id: 0
                                dummy: 1
                """;
    }

    private static List<DeterministicReplayBundles.Bundle> loadOfficialReplayBundles() throws Exception {
        final List<DeterministicReplayBundles.Bundle> bundles = new ArrayList<>();
        for (final String manifestResource : OFFICIAL_MANIFEST_RESOURCES) {
            final Path manifestPath = repositoryResource(manifestResource);
            final Path bundleDir = manifestPath.getParent();
            final DeterministicReplayBundles.Manifest manifest = DeterministicReplayBundles.readManifest(bundleDir);
            for (final DeterministicReplayBundles.ManifestEntry failure : manifest.failures()) {
                bundles.add(DeterministicReplayBundles.readBundle(bundleDir, failure.failureId()));
            }
        }
        bundles.sort(Comparator.comparing(DeterministicReplayBundles.Bundle::failureId));
        return List.copyOf(bundles);
    }

    private static Path repositoryResource(final String resourcePath) {
        final Path resolved = Path.of("src/test/resources").resolve(resourcePath).normalize();
        assertTrue(Files.exists(resolved), "missing test resource: " + resolved);
        return resolved;
    }

    private static List<?> asList(final Object value, final String fieldName) {
        assertTrue(value instanceof List<?>, fieldName + " must be a list");
        return (List<?>) value;
    }

    private static Map<String, Object> asMap(final Object value, final String fieldName) {
        assertTrue(value instanceof Map<?, ?>, fieldName + " must be an object");
        final Map<?, ?> raw = (Map<?, ?>) value;
        final Map<String, Object> normalized = new LinkedHashMap<>();
        for (final Map.Entry<?, ?> entry : raw.entrySet()) {
            normalized.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        return normalized;
    }

    private static Number asNumber(final Object value, final String fieldName) {
        assertTrue(value instanceof Number, fieldName + " must be numeric");
        return (Number) value;
    }
}
