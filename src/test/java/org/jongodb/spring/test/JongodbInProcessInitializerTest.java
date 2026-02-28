package org.jongodb.spring.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;

final class JongodbInProcessInitializerTest {
    @Test
    void registersTemplateBeanAndConfiguresTraceArtifacts(@TempDir final Path tempDir) throws Exception {
        final AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.getEnvironment().getPropertySources().addFirst(new MapPropertySource(
                "jongodbInProcessProps",
                java.util.Map.of(
                        JongodbInProcessInitializer.TRACE_ARTIFACTS_ENABLED_PROPERTY, "true",
                        JongodbInProcessInitializer.TRACE_ARTIFACTS_DIR_PROPERTY, tempDir.toString())));

        new JongodbInProcessInitializer().initialize(context);
        context.refresh();

        final InProcessIntegrationTemplate template =
                context.getBean(JongodbInProcessInitializer.TEMPLATE_BEAN_NAME, InProcessIntegrationTemplate.class);
        assertNotNull(template);

        final BsonDocument ping = template.runCommand("{\"ping\":1,\"$db\":\"admin\"}");
        assertEquals(1.0d, ping.getNumber("ok").doubleValue());

        final BsonDocument failed = template.runCommand("{\"doesNotExist\":1,\"$db\":\"admin\"}");
        assertEquals(0.0d, failed.getNumber("ok").doubleValue());

        final List<Path> traces = Files.list(tempDir).toList();
        assertTrue(traces.stream().anyMatch(path -> path.getFileName().toString().endsWith("-triage.json")));

        context.close();
    }
}
