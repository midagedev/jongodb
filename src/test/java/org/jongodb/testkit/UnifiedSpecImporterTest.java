package org.jongodb.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
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
    void skipsFileLevelRunOnRequirementsEntries() throws IOException {
        Files.writeString(
                tempDir.resolve("run-on-file-level.json"),
                """
                {
                  "runOnRequirements": [{"minServerVersion": "8.0"}],
                  "database_name": "app",
                  "collection_name": "users",
                  "tests": [
                    {
                      "description": "find gated by file-level runOn",
                      "operations": [
                        {"name": "find", "arguments": {"filter": {"_id": 1}}}
                      ]
                    }
                  ]
                }
                """);

        final UnifiedSpecImporter importer = new UnifiedSpecImporter();
        final UnifiedSpecImporter.ImportResult result = importer.importCorpus(tempDir);

        assertEquals(0, result.importedCount());
        assertEquals(1, result.skippedCount());
        assertEquals(0, result.unsupportedCount());
        assertEquals(UnifiedSpecImporter.SkipKind.SKIPPED, result.skippedCases().get(0).kind());
        assertTrue(result.skippedCases().get(0).reason().contains("runOnRequirements not evaluated"));
    }

    @Test
    void evaluatesFileLevelRunOnRequirementsWhenRuntimeContextIsProvided() throws IOException {
        Files.writeString(
                tempDir.resolve("run-on-evaluated.json"),
                """
                {
                  "runOnRequirements": [{"minServerVersion": "7.0", "topologies": ["replicaset"]}],
                  "database_name": "app",
                  "collection_name": "users",
                  "tests": [
                    {
                      "description": "find gated by evaluated runOn",
                      "operations": [
                        {"name": "find", "arguments": {"filter": {"_id": 1}}}
                      ]
                    }
                  ]
                }
                """);

        final UnifiedSpecImporter importer = new UnifiedSpecImporter();
        final UnifiedSpecImporter.ImportResult matched = importer.importCorpus(
                tempDir,
                UnifiedSpecImporter.RunOnContext.evaluated("7.0.4", "replicaset", false, false));
        assertEquals(1, matched.importedCount());
        assertEquals(0, matched.skippedCount());

        final UnifiedSpecImporter.ImportResult unmatched = importer.importCorpus(
                tempDir,
                UnifiedSpecImporter.RunOnContext.evaluated("6.0.14", "single", false, false));
        assertEquals(0, unmatched.importedCount());
        assertEquals(1, unmatched.skippedCount());
        assertTrue(unmatched.skippedCases().get(0).reason().contains("runOnRequirements not satisfied"));
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
    void importsBulkWriteOrderedFalseForExecutionParity() throws IOException {
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

        assertEquals(1, result.importedCount());
        assertEquals(0, result.unsupportedCount());
        final Scenario scenario = result.importedScenarios().get(0).scenario();
        assertEquals("bulkWrite", scenario.commands().get(0).commandName());
        assertEquals(Boolean.FALSE, scenario.commands().get(0).payload().get("ordered"));
    }

    @Test
    void importsBulkWriteInsertOneWithDotOrDollarKeys() throws IOException {
        Files.writeString(
                tempDir.resolve("bulk-write-dot-dollar.json"),
                """
                {
                  "database_name": "app",
                  "collection_name": "users",
                  "tests": [
                    {
                      "description": "bulk write insertOne with dotted key",
                      "operations": [
                        {
                          "name": "bulkWrite",
                          "arguments": {
                            "requests": [
                              {"insertOne": {"document": {"_id": 1, "a.b": 1}}},
                              {"insertOne": {"document": {"_id": 2, "nested": {"$x": 2}}}}
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
        assertEquals(0, result.unsupportedCount());

        final Scenario scenario = result.importedScenarios().get(0).scenario();
        assertEquals("bulkWrite", scenario.commands().get(0).commandName());
        final Object operationsValue = scenario.commands().get(0).payload().get("operations");
        assertTrue(operationsValue instanceof List<?>);
        final List<?> operations = (List<?>) operationsValue;
        assertEquals(2, operations.size());
    }

    @Test
    void importsAggregateListLocalSessionsAsDeterministicSubset() throws IOException {
        Files.writeString(
                tempDir.resolve("db-aggregate-list-local-sessions.json"),
                """
                {
                  "database_name": "app",
                  "collection_name": "users",
                  "createEntities": [
                    {"database": {"id": "database0", "databaseName": "admin"}}
                  ],
                  "tests": [
                    {
                      "description": "db aggregate listLocalSessions",
                      "operations": [
                        {
                          "name": "aggregate",
                          "object": "database0",
                          "arguments": {
                            "pipeline": [
                              {"$listLocalSessions": {}},
                              {"$limit": 1},
                              {"$project": {"_id": 0}}
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
        assertEquals(0, result.unsupportedCount());

        final Scenario scenario = result.importedScenarios().get(0).scenario();
        assertEquals("aggregate", scenario.commands().get(0).commandName());
        final Object pipelineValue = scenario.commands().get(0).payload().get("pipeline");
        assertTrue(pipelineValue instanceof List<?>);
        final List<?> pipeline = (List<?>) pipelineValue;
        assertEquals(3, pipeline.size());
        assertTrue(pipeline.get(0) instanceof java.util.Map<?, ?>);
        assertTrue(((java.util.Map<?, ?>) pipeline.get(0)).containsKey("$limit"));
        assertEquals(0, ((java.util.Map<?, ?>) pipeline.get(0)).get("$limit"));

        final WireCommandIngressBackend backend = new WireCommandIngressBackend("wire");
        final ScenarioOutcome outcome = backend.execute(scenario);
        assertTrue(outcome.success(), outcome.errorMessage().orElse("expected success"));
    }

    @Test
    void importsArrayFiltersUpdateSubsetWhileKeepingOtherUnsupportedCasesSkipped() throws IOException {
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
                    }
                  ]
                }
                """);

        final UnifiedSpecImporter importer = new UnifiedSpecImporter();
        final UnifiedSpecImporter.ImportResult result = importer.importCorpus(tempDir);

        assertEquals(1, result.importedCount());
        assertEquals(1, result.unsupportedCount());
        assertTrue(result.skippedCases().stream().allMatch(skipped ->
                skipped.kind() == UnifiedSpecImporter.SkipKind.UNSUPPORTED));

        final Scenario scenario = result.importedScenarios().get(0).scenario();
        assertEquals("update", scenario.commands().get(0).commandName());
        final Object updatesValue = scenario.commands().get(0).payload().get("updates");
        assertTrue(updatesValue instanceof List<?>);
        final Object updateEntry = ((List<?>) updatesValue).get(0);
        assertTrue(updateEntry instanceof java.util.Map<?, ?>);
        assertTrue(((java.util.Map<?, ?>) updateEntry).containsKey("arrayFilters"));
    }

    @Test
    void importsUpdatePipelineSubsetAndExecutesThroughWireBackend() throws IOException {
        Files.writeString(
                tempDir.resolve("update-pipeline-subset.json"),
                """
                {
                  "database_name": "app",
                  "collection_name": "users",
                  "tests": [
                    {
                      "description": "update pipeline subset",
                      "operations": [
                        {"name": "insertMany", "arguments": {"documents": [{"_id": 1, "name": "before", "legacy": true}]}},
                        {"name": "updateMany", "arguments": {"filter": {"_id": 1}, "update": [{"$set": {"name": "after"}}, {"$unset": "legacy"}]}},
                        {"name": "find", "arguments": {"filter": {"_id": 1}}}
                      ]
                    }
                  ]
                }
                """);

        final UnifiedSpecImporter importer = new UnifiedSpecImporter();
        final UnifiedSpecImporter.ImportResult result = importer.importCorpus(tempDir);

        assertEquals(1, result.importedCount());
        assertEquals(0, result.unsupportedCount());

        final WireCommandIngressBackend backend = new WireCommandIngressBackend("wire");
        final ScenarioOutcome outcome = backend.execute(result.importedScenarios().get(0).scenario());
        assertTrue(outcome.success(), outcome.errorMessage().orElse("expected success"));

        @SuppressWarnings("unchecked")
        final Map<String, Object> findResult = outcome.commandResults().get(2);
        @SuppressWarnings("unchecked")
        final Map<String, Object> cursor = (Map<String, Object>) findResult.get("cursor");
        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> firstBatch = (List<Map<String, Object>>) cursor.get("firstBatch");
        assertEquals(1, firstBatch.size());
        assertEquals("after", firstBatch.get(0).get("name"));
        assertTrue(!firstBatch.get(0).containsKey("legacy"));
    }

    @Test
    void marksUnsupportedUpdatePipelineStagesAsUnsupported() throws IOException {
        Files.writeString(
                tempDir.resolve("update-pipeline-unsupported.json"),
                """
                {
                  "database_name": "app",
                  "collection_name": "users",
                  "tests": [
                    {
                      "description": "update pipeline uses unsupported stages",
                      "operations": [
                        {
                          "name": "updateOne",
                          "arguments": {
                            "filter": {"_id": 1},
                            "update": [{"$project": {"x": 1}}, {"$addFields": {"foo": 1}}]
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
                        && skipped.reason().contains("unsupported UTF update pipeline form outside subset")));
    }

    @Test
    void importsOutAndMergeStagesAndBypassValidationFalse() throws IOException {
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
                            "bypassDocumentValidation": false
                          }
                        }
                      ]
                    },
                    {
                      "description": "aggregate with bypass validation true",
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

        assertEquals(3, result.importedCount());
        assertEquals(1, result.unsupportedCount());
        final Scenario outScenario = result.importedScenarios().stream()
                .map(UnifiedSpecImporter.ImportedScenario::scenario)
                .filter(scenario -> scenario.description().equals("aggregate with out"))
                .findFirst()
                .orElseThrow();
        final Scenario mergeScenario = result.importedScenarios().stream()
                .map(UnifiedSpecImporter.ImportedScenario::scenario)
                .filter(scenario -> scenario.description().equals("aggregate with merge"))
                .findFirst()
                .orElseThrow();
        final Scenario bypassFalseScenario = result.importedScenarios().stream()
                .map(UnifiedSpecImporter.ImportedScenario::scenario)
                .filter(scenario -> scenario.description().equals("aggregate with bypass validation"))
                .findFirst()
                .orElseThrow();
        assertEquals(1, outScenario.commands().size());
        assertEquals("aggregate", outScenario.commands().get(0).commandName());
        assertEquals("aggregate", mergeScenario.commands().get(0).commandName());
        assertEquals(Boolean.FALSE, bypassFalseScenario.commands().get(0).payload().get("bypassDocumentValidation"));
        assertTrue(result.skippedCases().stream().allMatch(skipped ->
                skipped.kind() == UnifiedSpecImporter.SkipKind.UNSUPPORTED
                        && skipped.reason().contains("unsupported UTF aggregate option: bypassDocumentValidation")));
    }

    @Test
    void importsCrudParityOperationsMappedToCommandLayer() throws IOException {
        Files.writeString(
                tempDir.resolve("crud-parity.json"),
                """
                {
                  "database_name": "app",
                  "collection_name": "users",
                  "tests": [
                    {
                      "description": "crud parity operations",
                      "operations": [
                        {"name": "countDocuments", "arguments": {"filter": {"active": true}, "limit": 2}},
                        {"name": "replaceOne", "arguments": {"filter": {"_id": 1}, "replacement": {"name": "neo"}, "upsert": true}},
                        {"name": "findOneAndUpdate", "arguments": {"filter": {"_id": 1}, "update": {"$set": {"tier": 2}}, "returnDocument": "after"}},
                        {"name": "findOneAndReplace", "arguments": {"filter": {"_id": 1}, "replacement": {"name": "trinity"}, "returnDocument": "before"}}
                      ]
                    }
                  ]
                }
                """);

        final UnifiedSpecImporter importer = new UnifiedSpecImporter();
        final UnifiedSpecImporter.ImportResult result = importer.importCorpus(tempDir);

        assertEquals(1, result.importedCount());
        assertEquals(0, result.unsupportedCount());

        final Scenario scenario = result.importedScenarios().get(0).scenario();
        assertEquals(4, scenario.commands().size());
        assertEquals("countDocuments", scenario.commands().get(0).commandName());
        assertEquals("replaceOne", scenario.commands().get(1).commandName());
        assertEquals("findOneAndUpdate", scenario.commands().get(2).commandName());
        assertEquals("findOneAndReplace", scenario.commands().get(3).commandName());
        assertEquals("users", scenario.commands().get(0).payload().get("countDocuments"));
        assertEquals("users", scenario.commands().get(1).payload().get("replaceOne"));
    }

    @Test
    void importsCountDistinctAndFindOneAndDeleteOperations() throws IOException {
        Files.writeString(
                tempDir.resolve("crud-more.json"),
                """
                {
                  "database_name": "app",
                  "collection_name": "users",
                  "tests": [
                    {
                      "description": "count distinct and findOneAndDelete",
                      "operations": [
                        {"name": "count", "arguments": {"query": {"active": true}, "limit": 3}},
                        {"name": "distinct", "arguments": {"fieldName": "tag", "filter": {"active": true}}},
                        {"name": "findOneAndDelete", "arguments": {"filter": {"_id": 1}, "sort": {"_id": 1}, "projection": {"_id": 1}}}
                      ]
                    }
                  ]
                }
                """);

        final UnifiedSpecImporter importer = new UnifiedSpecImporter();
        final UnifiedSpecImporter.ImportResult result = importer.importCorpus(tempDir);

        assertEquals(1, result.importedCount());
        assertEquals(0, result.unsupportedCount());

        final Scenario scenario = result.importedScenarios().get(0).scenario();
        assertEquals(3, scenario.commands().size());
        assertEquals("countDocuments", scenario.commands().get(0).commandName());
        assertEquals("distinct", scenario.commands().get(1).commandName());
        assertEquals("findAndModify", scenario.commands().get(2).commandName());

        assertEquals("users", scenario.commands().get(0).payload().get("countDocuments"));
        assertEquals(true, ((java.util.Map<?, ?>) scenario.commands().get(0).payload().get("query")).get("active"));
        assertEquals("tag", scenario.commands().get(1).payload().get("key"));
        assertEquals("users", scenario.commands().get(1).payload().get("distinct"));
        assertEquals("users", scenario.commands().get(2).payload().get("findAndModify"));
        assertEquals(true, scenario.commands().get(2).payload().get("remove"));
        assertTrue(scenario.commands().get(2).payload().containsKey("fields"));
    }

    @Test
    void importsRunCommandSubsetAndExecutesThroughWireBackend() throws IOException {
        Files.writeString(
                tempDir.resolve("run-command.json"),
                """
                {
                  "database_name": "app",
                  "collection_name": "users",
                  "tests": [
                    {
                      "description": "runCommand subset",
                      "operations": [
                        {"name": "insertMany", "arguments": {"documents": [
                          {"_id": 1, "active": true},
                          {"_id": 2, "active": false}
                        ]}},
                        {"name": "runCommand", "arguments": {"command": {"count": "users", "query": {"active": true}}}},
                        {"name": "runCommand", "arguments": {"command": {"ping": 1}}}
                      ]
                    }
                  ]
                }
                """);

        final UnifiedSpecImporter importer = new UnifiedSpecImporter();
        final UnifiedSpecImporter.ImportResult result = importer.importCorpus(tempDir);
        assertEquals(1, result.importedCount());
        assertEquals(0, result.unsupportedCount());

        final Scenario scenario = result.importedScenarios().get(0).scenario();
        assertEquals(3, scenario.commands().size());
        assertEquals("count", scenario.commands().get(1).commandName());
        assertEquals("users", scenario.commands().get(1).payload().get("commandValue"));
        assertEquals("ping", scenario.commands().get(2).commandName());

        final WireCommandIngressBackend backend = new WireCommandIngressBackend("wire");
        final ScenarioOutcome outcome = backend.execute(scenario);
        assertTrue(outcome.success(), outcome.errorMessage().orElse("expected success"));

        final Map<String, Object> countResult = outcome.commandResults().get(1);
        assertEquals(1L, ((Number) countResult.get("n")).longValue());
        assertEquals(1L, ((Number) countResult.get("count")).longValue());
        final Map<String, Object> pingResult = outcome.commandResults().get(2);
        assertEquals(1.0, ((Number) pingResult.get("ok")).doubleValue());
    }

    @Test
    void importsRunCommandBuildInfoAndListIndexesSubset() throws IOException {
        Files.writeString(
                tempDir.resolve("run-command-buildinfo-listindexes.json"),
                """
                {
                  "database_name": "app",
                  "collection_name": "users",
                  "tests": [
                    {
                      "description": "runCommand buildInfo and listIndexes subset",
                      "operations": [
                        {"name": "createIndex", "arguments": {"key": {"name": 1}, "name": "name_1"}},
                        {"name": "runCommand", "arguments": {"command": {"buildInfo": 1}}},
                        {"name": "runCommand", "arguments": {"command": {"listIndexes": "users"}}}
                      ]
                    }
                  ]
                }
                """);

        final UnifiedSpecImporter importer = new UnifiedSpecImporter();
        final UnifiedSpecImporter.ImportResult result = importer.importCorpus(tempDir);
        assertEquals(1, result.importedCount());
        assertEquals(0, result.unsupportedCount());

        final Scenario scenario = result.importedScenarios().get(0).scenario();
        assertEquals(3, scenario.commands().size());
        assertEquals("buildInfo", scenario.commands().get(1).commandName());
        assertEquals("listIndexes", scenario.commands().get(2).commandName());

        final WireCommandIngressBackend backend = new WireCommandIngressBackend("wire");
        final ScenarioOutcome outcome = backend.execute(scenario);
        assertTrue(outcome.success(), outcome.errorMessage().orElse("expected success"));

        final Map<String, Object> buildInfoResult = outcome.commandResults().get(1);
        assertEquals(1.0, ((Number) buildInfoResult.get("ok")).doubleValue());
        assertTrue(outcome.commandResults().get(2).containsKey("cursor"));
    }

    @Test
    void marksUnsupportedRunCommandNameAsUnsupported() throws IOException {
        Files.writeString(
                tempDir.resolve("run-command-unsupported.json"),
                """
                {
                  "database_name": "app",
                  "collection_name": "users",
                  "tests": [
                    {
                      "description": "runCommand unsupported command",
                      "operations": [
                        {"name": "runCommand", "arguments": {"command": {"replSetGetStatus": 1}}}
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
                        && skipped.reason().contains("unsupported UTF runCommand command")));
    }

    @Test
    void importsClientBulkWriteAndExecutesThroughWireBackend() throws IOException {
        Files.writeString(
                tempDir.resolve("client-bulk-write.json"),
                """
                {
                  "database_name": "app",
                  "collection_name": "users",
                  "tests": [
                    {
                      "description": "clientBulkWrite subset",
                      "operations": [
                        {"name": "clientBulkWrite", "arguments": {"models": [
                          {"insertOne": {"namespace": "app.users", "document": {"_id": 1, "name": "alpha"}}},
                          {"updateOne": {"namespace": "app.users", "filter": {"_id": 1}, "update": {"$set": {"name": "beta"}}}}
                        ]}},
                        {"name": "countDocuments", "arguments": {"filter": {}}}
                      ]
                    }
                  ]
                }
                """);

        final UnifiedSpecImporter importer = new UnifiedSpecImporter();
        final UnifiedSpecImporter.ImportResult result = importer.importCorpus(tempDir);
        assertEquals(1, result.importedCount());
        assertEquals(0, result.unsupportedCount());

        final Scenario scenario = result.importedScenarios().get(0).scenario();
        assertEquals(2, scenario.commands().size());
        assertEquals("bulkWrite", scenario.commands().get(0).commandName());
        assertEquals("users", scenario.commands().get(0).payload().get("bulkWrite"));
        assertEquals("app", scenario.commands().get(0).payload().get("$db"));

        final WireCommandIngressBackend backend = new WireCommandIngressBackend("wire");
        final ScenarioOutcome outcome = backend.execute(scenario);
        assertTrue(outcome.success(), outcome.errorMessage().orElse("expected success"));

        final Map<String, Object> countResult = outcome.commandResults().get(1);
        assertEquals(1L, ((Number) countResult.get("n")).longValue());
        assertEquals(1L, ((Number) countResult.get("count")).longValue());
    }

    @Test
    void marksClientBulkWriteMixedNamespacesAsUnsupported() throws IOException {
        Files.writeString(
                tempDir.resolve("client-bulk-write-mixed-ns.json"),
                """
                {
                  "database_name": "app",
                  "collection_name": "users",
                  "tests": [
                    {
                      "description": "clientBulkWrite mixed namespace",
                      "operations": [
                        {"name": "clientBulkWrite", "arguments": {"models": [
                          {"insertOne": {"namespace": "app.users", "document": {"_id": 1}}},
                          {"insertOne": {"namespace": "app.audit", "document": {"_id": 2}}}
                        ]}}
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
                        && skipped.reason().contains("unsupported UTF clientBulkWrite mixed namespaces")));
    }

    @Test
    void importsClientBulkWriteOrderedFalseForExecutionParity() throws IOException {
        Files.writeString(
                tempDir.resolve("client-bulk-write-unordered.json"),
                """
                {
                  "database_name": "app",
                  "collection_name": "users",
                  "tests": [
                    {
                      "description": "clientBulkWrite ordered false",
                      "operations": [
                        {"name": "clientBulkWrite", "arguments": {"ordered": false, "models": [
                          {"insertOne": {"namespace": "app.users", "document": {"_id": 1}}}
                        ]}}
                      ]
                    }
                  ]
                }
                """);

        final UnifiedSpecImporter importer = new UnifiedSpecImporter();
        final UnifiedSpecImporter.ImportResult result = importer.importCorpus(tempDir);

        assertEquals(1, result.importedCount());
        assertEquals(0, result.unsupportedCount());
        final Scenario scenario = result.importedScenarios().get(0).scenario();
        assertEquals("bulkWrite", scenario.commands().get(0).commandName());
        assertEquals(Boolean.FALSE, scenario.commands().get(0).payload().get("ordered"));
    }

    @Test
    void marksClientBulkWriteVerboseResultsAsUnsupported() throws IOException {
        Files.writeString(
                tempDir.resolve("client-bulk-write-verbose-results.json"),
                """
                {
                  "database_name": "app",
                  "collection_name": "users",
                  "tests": [
                    {
                      "description": "clientBulkWrite verboseResults true",
                      "operations": [
                        {"name": "clientBulkWrite", "arguments": {"verboseResults": true, "models": [
                          {"insertOne": {"namespace": "app.users", "document": {"_id": 1}}}
                        ]}}
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
                        && skipped.reason().contains("unsupported UTF clientBulkWrite option: verboseResults=true")));
    }

    @Test
    void importsCountAliasWithQueryAndExecutesThroughWireBackend() throws IOException {
        Files.writeString(
                tempDir.resolve("count-alias-integration.json"),
                """
                {
                  "database_name": "app",
                  "collection_name": "users",
                  "tests": [
                    {
                      "description": "count alias importer and execution",
                      "operations": [
                        {"name": "insertMany", "arguments": {"documents": [
                          {"_id": 1, "active": true},
                          {"_id": 2, "active": true},
                          {"_id": 3, "active": false}
                        ]}},
                        {"name": "count", "arguments": {"query": {"active": true}, "skip": 1, "limit": 1}}
                      ]
                    }
                  ]
                }
                """);

        final UnifiedSpecImporter importer = new UnifiedSpecImporter();
        final UnifiedSpecImporter.ImportResult result = importer.importCorpus(tempDir);
        assertEquals(1, result.importedCount());
        assertEquals(0, result.unsupportedCount());

        final Scenario scenario = result.importedScenarios().get(0).scenario();
        assertEquals(2, scenario.commands().size());
        final ScenarioCommand countCommand = scenario.commands().get(1);
        assertEquals("countDocuments", countCommand.commandName());
        assertTrue(countCommand.payload().containsKey("query"));
        assertTrue(!countCommand.payload().containsKey("filter"));

        final WireCommandIngressBackend backend = new WireCommandIngressBackend("wire");
        final ScenarioOutcome outcome = backend.execute(scenario);
        assertTrue(outcome.success(), outcome.errorMessage().orElse("expected success"));

        final Map<String, Object> countResult = outcome.commandResults().get(1);
        assertEquals(1L, ((Number) countResult.get("n")).longValue());
        assertEquals(1L, ((Number) countResult.get("count")).longValue());
    }

    @Test
    void importsFindOneAndDeleteAndExecutesThroughWireBackend() throws IOException {
        Files.writeString(
                tempDir.resolve("find-one-and-delete-integration.json"),
                """
                {
                  "database_name": "app",
                  "collection_name": "users",
                  "tests": [
                    {
                      "description": "findOneAndDelete importer and execution",
                      "operations": [
                        {"name": "insertMany", "arguments": {"documents": [
                          {"_id": 1, "name": "alpha"},
                          {"_id": 2, "name": "beta"}
                        ]}},
                        {"name": "findOneAndDelete", "arguments": {"filter": {"_id": 1}, "projection": {"name": 1, "_id": 0}}},
                        {"name": "countDocuments", "arguments": {"filter": {}}}
                      ]
                    }
                  ]
                }
                """);

        final UnifiedSpecImporter importer = new UnifiedSpecImporter();
        final UnifiedSpecImporter.ImportResult result = importer.importCorpus(tempDir);
        assertEquals(1, result.importedCount());
        assertEquals(0, result.unsupportedCount());

        final Scenario scenario = result.importedScenarios().get(0).scenario();
        assertEquals(3, scenario.commands().size());
        final ScenarioCommand findOneAndDelete = scenario.commands().get(1);
        assertEquals("findAndModify", findOneAndDelete.commandName());
        assertEquals(true, findOneAndDelete.payload().get("remove"));
        assertTrue(findOneAndDelete.payload().containsKey("fields"));

        final WireCommandIngressBackend backend = new WireCommandIngressBackend("wire");
        final ScenarioOutcome outcome = backend.execute(scenario);
        assertTrue(outcome.success(), outcome.errorMessage().orElse("expected success"));

        final Map<String, Object> findAndModifyResult = outcome.commandResults().get(1);
        @SuppressWarnings("unchecked")
        final Map<String, Object> value = (Map<String, Object>) findAndModifyResult.get("value");
        assertEquals("alpha", value.get("name"));
        assertTrue(!value.containsKey("_id"));

        final Map<String, Object> countResult = outcome.commandResults().get(2);
        assertEquals(1L, ((Number) countResult.get("n")).longValue());
        assertEquals(1L, ((Number) countResult.get("count")).longValue());
    }

    @Test
    void appliesTransactionEnvelopeWithCreateEntitiesSessions() throws IOException {
        Files.writeString(
                tempDir.resolve("transactions.yml"),
                """
                schemaVersion: "1.3"
                createEntities:
                  - client:
                      id: client0
                  - database:
                      id: database0
                      client: client0
                      databaseName: tx-db
                  - collection:
                      id: collection0
                      database: database0
                      collectionName: tx-coll
                  - session:
                      id: session0
                      client: client0
                tests:
                  - description: txn envelope
                    operations:
                      - object: session0
                        name: startTransaction
                        arguments:
                          readConcern:
                            level: majority
                      - object: collection0
                        name: insertOne
                        arguments:
                          session: session0
                          document:
                            _id: 1
                      - object: collection0
                        name: countDocuments
                        arguments:
                          session: session0
                          filter: {}
                      - object: session0
                        name: commitTransaction
                """);

        final UnifiedSpecImporter importer = new UnifiedSpecImporter();
        final UnifiedSpecImporter.ImportResult result = importer.importCorpus(tempDir);

        assertEquals(1, result.importedCount());
        assertEquals(0, result.unsupportedCount());

        final Scenario scenario = result.importedScenarios().get(0).scenario();
        assertEquals(3, scenario.commands().size());

        final ScenarioCommand first = scenario.commands().get(0);
        assertEquals("insert", first.commandName());
        assertEquals("tx-coll", first.payload().get("insert"));
        assertEquals("tx-db", first.payload().get("$db"));
        assertEquals(true, first.payload().get("startTransaction"));
        assertEquals(false, first.payload().get("autocommit"));
        assertEquals(1L, first.payload().get("txnNumber"));

        final Object lsid = first.payload().get("lsid");
        assertTrue(lsid instanceof java.util.Map<?, ?>);
        final java.util.Map<?, ?> lsidMap = (java.util.Map<?, ?>) lsid;
        assertEquals("session0", lsidMap.get("id"));
        assertTrue(first.payload().containsKey("readConcern"));

        final ScenarioCommand second = scenario.commands().get(1);
        assertEquals("countDocuments", second.commandName());
        assertEquals(false, second.payload().get("autocommit"));
        assertEquals(1L, second.payload().get("txnNumber"));

        final ScenarioCommand third = scenario.commands().get(2);
        assertEquals("commitTransaction", third.commandName());
        assertEquals("admin", third.payload().get("$db"));
        assertEquals(1L, third.payload().get("txnNumber"));
    }

    @Test
    void appliesTransactionEnvelopeToFindOneAndDelete() throws IOException {
        Files.writeString(
                tempDir.resolve("find-one-delete-transaction.yml"),
                """
                schemaVersion: "1.3"
                createEntities:
                  - client:
                      id: client0
                  - database:
                      id: database0
                      client: client0
                      databaseName: tx-db
                  - collection:
                      id: collection0
                      database: database0
                      collectionName: tx-coll
                  - session:
                      id: session0
                      client: client0
                tests:
                  - description: txn findOneAndDelete envelope
                    operations:
                      - object: session0
                        name: startTransaction
                      - object: collection0
                        name: findOneAndDelete
                        arguments:
                          session: session0
                          filter:
                            _id: 1
                      - object: session0
                        name: commitTransaction
                """);

        final UnifiedSpecImporter importer = new UnifiedSpecImporter();
        final UnifiedSpecImporter.ImportResult result = importer.importCorpus(tempDir);

        assertEquals(1, result.importedCount());
        assertEquals(0, result.unsupportedCount());

        final Scenario scenario = result.importedScenarios().get(0).scenario();
        assertEquals(2, scenario.commands().size());

        final ScenarioCommand first = scenario.commands().get(0);
        assertEquals("findAndModify", first.commandName());
        assertEquals(true, first.payload().get("remove"));
        assertEquals(true, first.payload().get("startTransaction"));
        assertEquals(false, first.payload().get("autocommit"));
        assertEquals(1L, first.payload().get("txnNumber"));
        final Object lsid = first.payload().get("lsid");
        assertTrue(lsid instanceof java.util.Map<?, ?>);
        assertEquals("session0", ((java.util.Map<?, ?>) lsid).get("id"));

        final ScenarioCommand second = scenario.commands().get(1);
        assertEquals("commitTransaction", second.commandName());
        assertEquals("admin", second.payload().get("$db"));
        assertEquals(1L, second.payload().get("txnNumber"));
    }

    @Test
    void marksFailPointAsUnsupportedByPolicy() throws IOException {
        Files.writeString(
                tempDir.resolve("failpoint.yml"),
                """
                tests:
                  - description: failpoint policy
                    operations:
                      - object: testRunner
                        name: failPoint
                        arguments:
                          failPoint:
                            configureFailPoint: failCommand
                            mode: alwaysOn
                """);

        final UnifiedSpecImporter importer = new UnifiedSpecImporter();
        final UnifiedSpecImporter.ImportResult result = importer.importCorpus(tempDir);

        assertEquals(0, result.importedCount());
        assertEquals(1, result.unsupportedCount());
        assertTrue(result.skippedCases().stream().anyMatch(skipped ->
                skipped.kind() == UnifiedSpecImporter.SkipKind.UNSUPPORTED
                        && skipped.reason().contains("unsupported-by-policy UTF operation: failPoint")));
    }

    @Test
    void strictProfileMarksTargetedFailPointAsUnsupported() throws IOException {
        Files.writeString(
                tempDir.resolve("targeted-failpoint-strict.yml"),
                """
                tests:
                  - description: targeted failpoint strict profile
                    operations:
                      - object: testRunner
                        name: targetedFailPoint
                        arguments:
                          failPoint:
                            configureFailPoint: failCommand
                            mode: off
                """);

        final UnifiedSpecImporter importer = new UnifiedSpecImporter();
        final UnifiedSpecImporter.ImportResult result = importer.importCorpus(tempDir);

        assertEquals(0, result.importedCount());
        assertEquals(1, result.unsupportedCount());
        assertTrue(result.skippedCases().stream().anyMatch(skipped ->
                skipped.kind() == UnifiedSpecImporter.SkipKind.UNSUPPORTED
                        && skipped.reason().contains("unsupported UTF operation: targetedFailPoint")));
    }

    @Test
    void compatProfileImportsFailPointDisableOperationsAsNoop() throws IOException {
        Files.writeString(
                tempDir.resolve("failpoint-compat.yml"),
                """
                tests:
                  - description: failpoint compat mode off
                    operations:
                      - object: testRunner
                        name: failPoint
                        arguments:
                          failPoint:
                            configureFailPoint: failCommand
                            mode: off
                      - object: testRunner
                        name: targetedFailPoint
                        arguments:
                          failPoint:
                            configureFailPoint: failCommand
                            mode:
                              times: 0
                      - object: db
                        name: runCommand
                        arguments:
                          command:
                            ping: 1
                """);

        final UnifiedSpecImporter importer = new UnifiedSpecImporter(UnifiedSpecImporter.ImportProfile.COMPAT);
        final UnifiedSpecImporter.ImportResult result = importer.importCorpus(tempDir);

        assertEquals(1, result.importedCount());
        assertEquals(0, result.unsupportedCount());
        final Scenario scenario = result.importedScenarios().get(0).scenario();
        assertEquals(3, scenario.commands().size());
        assertEquals("ping", scenario.commands().get(0).commandName());
        assertEquals("ping", scenario.commands().get(1).commandName());
        assertEquals("ping", scenario.commands().get(2).commandName());
    }

    @Test
    void compatProfileRejectsUnsupportedFailPointModes() throws IOException {
        Files.writeString(
                tempDir.resolve("failpoint-compat-unsupported.yml"),
                """
                tests:
                  - description: failpoint compat mode unsupported
                    operations:
                      - object: testRunner
                        name: failPoint
                        arguments:
                          failPoint:
                            configureFailPoint: failCommand
                            mode: alwaysOn
                """);

        final UnifiedSpecImporter importer = new UnifiedSpecImporter(UnifiedSpecImporter.ImportProfile.COMPAT);
        final UnifiedSpecImporter.ImportResult result = importer.importCorpus(tempDir);

        assertEquals(0, result.importedCount());
        assertEquals(1, result.unsupportedCount());
        assertTrue(result.skippedCases().stream().anyMatch(skipped ->
                skipped.kind() == UnifiedSpecImporter.SkipKind.UNSUPPORTED
                        && skipped.reason().contains("unsupported UTF failPoint mode for compat profile")));
    }
}
