package org.jongodb.spring.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.BsonDocument;
import org.jongodb.server.TcpMongoServer;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;

final class JongodbMongoInitializerTest {
    @Test
    void initializesMongoPropertiesAndClosesServerOnContextClose() {
        final AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.getEnvironment()
                .getPropertySources()
                .addFirst(new MapPropertySource("jongodbTestProps", java.util.Map.of("jongodb.test.database", "sampledb")));

        final JongodbMongoInitializer initializer = new JongodbMongoInitializer();
        initializer.initialize(context);
        context.refresh();

        final String uri = context.getEnvironment().getProperty("spring.data.mongodb.uri");
        final String host = context.getEnvironment().getProperty("spring.data.mongodb.host");
        final String port = context.getEnvironment().getProperty("spring.data.mongodb.port");

        assertNotNull(uri);
        assertNotNull(host);
        assertNotNull(port);
        assertTrue(uri.endsWith("/sampledb"));

        final TcpMongoServer server = context.getBean(JongodbMongoInitializer.SERVER_BEAN_NAME, TcpMongoServer.class);
        assertTrue(server.isRunning());
        assertEquals(host, server.host());
        assertEquals(Integer.parseInt(port), server.port());

        try (MongoClient client = MongoClients.create(uri)) {
            final BsonDocument ping = BsonDocument.parse(client.getDatabase("sampledb").runCommand(BsonDocument.parse("{\"ping\":1}")).toJson());
            assertEquals(1.0, ping.getNumber("ok").doubleValue(), 0.0);
        }

        context.close();
        assertFalse(server.isRunning());
    }
}
