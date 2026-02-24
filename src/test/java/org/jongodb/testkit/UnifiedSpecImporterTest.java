package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class UnifiedSpecImporterTest {
    @TempDir
    Path tempDir;

    @Test
    void importsJsonYamlAndClassifiesSkippedAndUnsupported() throws IOException {
        Files.writeString(
                tempDir.resolve("crud.json"),
                """
                {
                  "database_name": "app",
                  "collection_name": "users",
                  "tests": [
                    {
                      "description": "insert and find",
                      "operations": [
                        {"name": "insertOne", "arguments": {"document": {"_id": 1, "name": "alice"}}},
                        {"name": "find", "arguments": {"filter": {"name": "alice"}}}
                      ]
                    },
                    {
                      "description": "skip me",
                      "skipReason": "requires replica set",
                      "operations": [
                        {"name": "find", "arguments": {"filter": {}}}
                      ]
                    },
                    {
                      "description": "unsupported op",
                      "operations": [
                        {"name": "renameCollection", "arguments": {}}
                      ]
                    }
                  ]
                }
                """);

        Files.writeString(
                tempDir.resolve("aggregate.yaml"),
                """
                database_name: app
                collection_name: users
                tests:
                  - description: aggregate case
                    operations:
                      - name: aggregate
                        arguments:
                          pipeline:
                            - $match:
                                name: alice
                """);

        final UnifiedSpecImporter importer = new UnifiedSpecImporter();
        final UnifiedSpecImporter.ImportResult result = importer.importCorpus(tempDir);

        assertEquals(2, result.importedCount());
        assertEquals(1, result.skippedCount());
        assertEquals(1, result.unsupportedCount());

        final List<UnifiedSpecImporter.ImportedScenario> imported = result.importedScenarios();
        assertEquals(2, imported.size());

        final Scenario insertFindScenario = imported.stream()
                .map(UnifiedSpecImporter.ImportedScenario::scenario)
                .filter(scenario -> scenario.description().equals("insert and find"))
                .findFirst()
                .orElseThrow();
        assertEquals(2, insertFindScenario.commands().size());
        assertEquals("insert", insertFindScenario.commands().get(0).commandName());
        assertEquals("users", insertFindScenario.commands().get(0).payload().get("insert"));
        assertEquals("app", insertFindScenario.commands().get(0).payload().get("$db"));

        final Scenario aggregateScenario = imported.stream()
                .map(UnifiedSpecImporter.ImportedScenario::scenario)
                .filter(scenario -> scenario.description().equals("aggregate case"))
                .findFirst()
                .orElseThrow();
        assertEquals("aggregate", aggregateScenario.commands().get(0).commandName());

        assertTrue(result.skippedCases().stream().anyMatch(skipped ->
                skipped.kind() == UnifiedSpecImporter.SkipKind.SKIPPED
                        && skipped.reason().contains("requires replica set")));
        assertTrue(result.skippedCases().stream().anyMatch(skipped ->
                skipped.kind() == UnifiedSpecImporter.SkipKind.UNSUPPORTED
                        && skipped.reason().contains("unsupported UTF operation")));
    }

    @Test
    void marksInvalidCasesWhenOperationsAreMissing() throws IOException {
        Files.writeString(
                tempDir.resolve("invalid.json"),
                """
                {
                  "database_name": "app",
                  "collection_name": "users",
                  "tests": [
                    {"description": "missing operations"}
                  ]
                }
                """);

        final UnifiedSpecImporter importer = new UnifiedSpecImporter();
        final UnifiedSpecImporter.ImportResult result = importer.importCorpus(tempDir);

        assertEquals(0, result.importedCount());
        assertEquals(1, result.skippedCount());
        assertEquals(0, result.unsupportedCount());
        assertEquals(UnifiedSpecImporter.SkipKind.INVALID, result.skippedCases().get(0).kind());
    }

    @Test
    void importsBulkWriteOperationWhenOrderedIsSupported() throws IOException {
        Files.writeString(
                tempDir.resolve("bulk-write.json"),
                """
                {
                  "database_name": "app",
                  "collection_name": "users",
                  "tests": [
                    {
                      "description": "bulk write ordered true",
                      "operations": [
                        {
                          "name": "bulkWrite",
                          "arguments": {
                            "ordered": true,
                            "requests": [
                              {"insertOne": {"document": {"_id": 1, "name": "alice"}}},
                              {"updateOne": {"filter": {"_id": 1}, "update": {"$set": {"tier": 1}}}},
                              {"deleteMany": {"filter": {"tier": 0}}}
                            ]
                          }
                        }
                      ]
                    }
                  ]
                }
                """);

        final UnifiedSpecImporter importer = new UnifiedSpecImporter();
        final UnifiedSpecImporter.ImportResult result = importer.importCorpus(tempDir);

        assertEquals(1, result.importedCount());
        assertEquals(0, result.skippedCount());
        assertEquals(0, result.unsupportedCount());

        final Scenario scenario = result.importedScenarios().get(0).scenario();
        assertEquals(1, scenario.commands().size());
        assertEquals("bulkWrite", scenario.commands().get(0).commandName());
        assertEquals("users", scenario.commands().get(0).payload().get("bulkWrite"));
        assertEquals("app", scenario.commands().get(0).payload().get("$db"));
        assertEquals(true, scenario.commands().get(0).payload().get("ordered"));
        final Object operationsValue = scenario.commands().get(0).payload().get("operations");
        assertTrue(operationsValue instanceof List<?>);
        final List<?> operations = (List<?>) operationsValue;
        assertEquals(3, operations.size());
        assertTrue(operations.get(0) instanceof java.util.Map<?, ?>);
        final java.util.Map<?, ?> firstOperation = (java.util.Map<?, ?>) operations.get(0);
        assertTrue(firstOperation.containsKey("insertOne"));
    }

    @Test
    void marksBulkWriteOrderedFalseAsUnsupported() throws IOException {
        Files.writeString(
                tempDir.resolve("bulk-write-unsupported.json"),
                """
                {
                  "database_name": "app",
                  "collection_name": "users",
                  "tests": [
                    {
                      "description": "bulk write ordered false",
                      "operations": [
                        {
                          "name": "bulkWrite",
                          "arguments": {
                            "ordered": false,
                            "requests": [
                              {"insertOne": {"document": {"_id": 1, "name": "alice"}}}
                            ]
                          }
                        }
                      ]
                    }
                  ]
                }
                """);

        final UnifiedSpecImporter importer = new UnifiedSpecImporter();
        final UnifiedSpecImporter.ImportResult result = importer.importCorpus(tempDir);

        assertEquals(0, result.importedCount());
        assertEquals(1, result.unsupportedCount());
        assertTrue(result.skippedCases().stream().anyMatch(skipped ->
                skipped.kind() == UnifiedSpecImporter.SkipKind.UNSUPPORTED
                        && skipped.reason().contains("bulkWrite option: ordered=false")));
    }

    @Test
    void marksKnownUnsupportedQueryUpdateFeaturesAsUnsupported() throws IOException {
        Files.writeString(
                tempDir.resolve("unsupported-query-update.json"),
                """
                {
                  "database_name": "app",
                  "collection_name": "users",
                  "tests": [
                    {
                      "description": "insertMany ordered false",
                      "operations": [
                        {
                          "name": "insertMany",
                          "arguments": {
                            "ordered": false,
                            "documents": [{"_id": 1}, {"_id": 1}]
                          }
                        }
                      ]
                    },
                    {
                      "description": "update with arrayFilters",
                      "operations": [
                        {
                          "name": "updateOne",
                          "arguments": {
                            "filter": {"_id": 1},
                            "update": {"$set": {"items.$[e].qty": 1}},
                            "arrayFilters": [{"e.qty": {"$gt": 2}}]
                          }
                        }
                      ]
                    },
                    {
                      "description": "update pipeline",
                      "operations": [
                        {
                          "name": "updateMany",
                          "arguments": {
                            "filter": {"_id": 1},
                            "update": [{"$set": {"a": 1}}]
                          }
                        }
                      ]
                    }
                  ]
                }
                """);

        final UnifiedSpecImporter importer = new UnifiedSpecImporter();
        final UnifiedSpecImporter.ImportResult result = importer.importCorpus(tempDir);

        assertEquals(0, result.importedCount());
        assertEquals(3, result.unsupportedCount());
        assertTrue(result.skippedCases().stream().allMatch(skipped ->
                skipped.kind() == UnifiedSpecImporter.SkipKind.UNSUPPORTED));
    }

    @Test
    void marksKnownUnsupportedAggregationFeaturesAsUnsupported() throws IOException {
        Files.writeString(
                tempDir.resolve("unsupported-aggregation.json"),
                """
                {
                  "database_name": "app",
                  "collection_name": "users",
                  "tests": [
                    {
                      "description": "aggregate with out",
                      "operations": [
                        {
                          "name": "aggregate",
                          "arguments": {
                            "pipeline": [{"$out": "archive"}]
                          }
                        }
                      ]
                    },
                    {
                      "description": "aggregate with merge",
                      "operations": [
                        {
                          "name": "aggregate",
                          "arguments": {
                            "pipeline": [{"$merge": {"into": "archive"}}]
                          }
                        }
                      ]
                    },
                    {
                      "description": "aggregate with bypass validation",
                      "operations": [
                        {
                          "name": "aggregate",
                          "arguments": {
                            "pipeline": [{"$match": {"_id": 1}}],
                            "bypassDocumentValidation": true
                          }
                        }
                      ]
                    }
                  ]
                }
                """);

        final UnifiedSpecImporter importer = new UnifiedSpecImporter();
        final UnifiedSpecImporter.ImportResult result = importer.importCorpus(tempDir);

        assertEquals(0, result.importedCount());
        assertEquals(3, result.unsupportedCount());
        assertTrue(result.skippedCases().stream().allMatch(skipped ->
                skipped.kind() == UnifiedSpecImporter.SkipKind.UNSUPPORTED));
    }
}
