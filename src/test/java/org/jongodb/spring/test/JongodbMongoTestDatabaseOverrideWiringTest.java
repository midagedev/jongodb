package org.jongodb.spring.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.BsonDocument;
import org.jongodb.server.TcpMongoServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestPropertySource(properties = "jongodb.test.database=ordersdb")
@JongodbMongoTest(classes = JongodbMongoTestDatabaseOverrideWiringTest.TestConfig.class)
final class JongodbMongoTestDatabaseOverrideWiringTest {
    @Autowired
    private Environment environment;

    @Autowired
    @Qualifier(JongodbMongoInitializer.SERVER_BEAN_NAME)
    private TcpMongoServer server;

    @Test
    void wiresMongoUriHostPortAndOverriddenDatabase() {
        final String uri = environment.getProperty("spring.data.mongodb.uri");
        final String host = environment.getProperty("spring.data.mongodb.host");
        final String port = environment.getProperty("spring.data.mongodb.port");

        assertNotNull(uri);
        assertNotNull(host);
        assertNotNull(port);
        assertTrue(uri.endsWith("/ordersdb"));
        assertEquals(server.host(), host);
        assertEquals(server.port(), Integer.parseInt(port));

        try (MongoClient client = MongoClients.create(uri)) {
            final BsonDocument ping = BsonDocument.parse(
                    client.getDatabase("ordersdb").runCommand(BsonDocument.parse("{\"ping\":1}")).toJson());
            assertEquals(1.0, ping.getNumber("ok").doubleValue(), 0.0);
        }
    }

    @Configuration
    static class TestConfig {}
}
