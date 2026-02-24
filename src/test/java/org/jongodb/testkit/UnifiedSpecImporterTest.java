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
        assertEquals(true, ((java.util.Map<?, ?>) scenario.commands().get(0).payload().get("filter")).get("active"));
        assertEquals("tag", scenario.commands().get(1).payload().get("key"));
        assertEquals("users", scenario.commands().get(1).payload().get("distinct"));
        assertEquals("users", scenario.commands().get(2).payload().get("findAndModify"));
        assertEquals(true, scenario.commands().get(2).payload().get("remove"));
        assertTrue(scenario.commands().get(2).payload().containsKey("fields"));
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
}
