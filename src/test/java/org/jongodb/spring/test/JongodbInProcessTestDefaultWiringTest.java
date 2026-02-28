package org.jongodb.spring.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@JongodbInProcessTest(classes = JongodbInProcessTestDefaultWiringTest.TestConfig.class)
final class JongodbInProcessTestDefaultWiringTest {
    @Autowired
    private InProcessIntegrationTemplate template;

    @Autowired
    private ApplicationContext context;

    @Test
    void providesInProcessTemplateWithResetSupport() {
        assertNotNull(template);

        template.seedDocuments(
                "app",
                "users",
                List.of(BsonDocument.parse("{\"_id\":1,\"name\":\"alpha\"}")));
        final BsonDocument beforeReset = template.findAll("app", "users");
        assertEquals(1, beforeReset.getDocument("cursor").getArray("firstBatch").size());

        JongodbInProcessResetSupport.reset(context);

        final BsonDocument afterReset = template.findAll("app", "users");
        assertEquals(0, afterReset.getDocument("cursor").getArray("firstBatch").size());
    }

    @Configuration
    static class TestConfig {}
}
