package org.jongodb.spring.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class InProcessIntegrationTemplateTest {
    @Test
    void seedFindAndResetFlowWorks() {
        final InProcessIntegrationTemplate template = new InProcessIntegrationTemplate();
        template.seedDocuments(
                "app",
                "users",
                List.of(
                        BsonDocument.parse("{\"_id\":1,\"name\":\"alpha\"}"),
                        BsonDocument.parse("{\"_id\":2,\"name\":\"beta\"}")));

        final BsonDocument beforeReset = template.findAll("app", "users");
        assertEquals(2, beforeReset.getDocument("cursor").getArray("firstBatch").size());

        template.reset();
        final BsonDocument afterReset = template.findAll("app", "users");
        assertEquals(0, afterReset.getDocument("cursor").getArray("firstBatch").size());
    }

    @Test
    void writesTraceArtifactsOnFailureWhenEnabled(@TempDir final Path tempDir) throws Exception {
        final InProcessIntegrationTemplate template = new InProcessIntegrationTemplate();
        template.enableFailureTraceArtifacts(tempDir);

        final BsonDocument failed = template.runCommand("{\"doesNotExist\":1,\"$db\":\"admin\"}");
        assertEquals(0.0d, failed.getNumber("ok").doubleValue());

        final List<Path> files = Files.list(tempDir).toList();
        assertTrue(files.stream().anyMatch(path -> path.getFileName().toString().endsWith("-snapshot.json")));
        assertTrue(files.stream().anyMatch(path -> path.getFileName().toString().endsWith("-invariant.json")));
        assertTrue(files.stream().anyMatch(path -> path.getFileName().toString().endsWith("-triage.json")));
        assertTrue(files.stream().anyMatch(path -> path.getFileName().toString().endsWith("-repro.jsonl")));
        assertTrue(files.stream().anyMatch(path -> path.getFileName().toString().endsWith("-response.json")));
    }
}
