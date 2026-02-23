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
}
