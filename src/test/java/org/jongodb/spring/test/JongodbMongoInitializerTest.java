package org.jongodb.spring.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.BsonDocument;
import org.jongodb.server.TcpMongoServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;

final class JongodbMongoInitializerTest {
    @AfterEach
    void cleanupSharedServer() {
        JongodbMongoInitializer.stopSharedServer();
    }

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

    @Test
    void sharedServerModeReusesPortAcrossContextsAndClosesAfterLastContext() {
        final AnnotationConfigApplicationContext first = new AnnotationConfigApplicationContext();
        first.getEnvironment()
                .getPropertySources()
                .addFirst(new MapPropertySource(
                        "jongodbShared1",
                        java.util.Map.of(
                                "jongodb.test.database", "shared1",
                                "jongodb.test.sharedServer", "true")));
        new JongodbMongoInitializer().initialize(first);
        first.refresh();

        final TcpMongoServer firstServer = first.getBean(JongodbMongoInitializer.SERVER_BEAN_NAME, TcpMongoServer.class);
        final String firstUri = first.getEnvironment().getProperty("spring.data.mongodb.uri");
        assertNotNull(firstUri);
        assertTrue(firstUri.endsWith("/shared1"));

        final AnnotationConfigApplicationContext second = new AnnotationConfigApplicationContext();
        second.getEnvironment()
                .getPropertySources()
                .addFirst(new MapPropertySource(
                        "jongodbShared2",
                        java.util.Map.of(
                                "jongodb.test.database", "shared2",
                                "jongodb.test.sharedServer", "true")));
        new JongodbMongoInitializer().initialize(second);
        second.refresh();

        final TcpMongoServer secondServer = second.getBean(JongodbMongoInitializer.SERVER_BEAN_NAME, TcpMongoServer.class);
        final String secondUri = second.getEnvironment().getProperty("spring.data.mongodb.uri");
        assertNotNull(secondUri);
        assertTrue(secondUri.endsWith("/shared2"));
        assertSame(firstServer, secondServer);
        assertEquals(firstServer.port(), secondServer.port());
        assertTrue(firstServer.isRunning());

        first.close();
        assertTrue(secondServer.isRunning());

        second.close();
        assertFalse(secondServer.isRunning());
    }

    @Test
    void resetSupportClearsDataWithoutRestartingServer() {
        final AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.getEnvironment()
                .getPropertySources()
                .addFirst(new MapPropertySource("jongodbResetProps", java.util.Map.of("jongodb.test.database", "resetdb")));
        new JongodbMongoInitializer().initialize(context);
        context.refresh();

        final TcpMongoServer server = context.getBean(JongodbMongoInitializer.SERVER_BEAN_NAME, TcpMongoServer.class);
        final String uri = context.getEnvironment().getProperty("spring.data.mongodb.uri");
        assertNotNull(uri);

        try (MongoClient client = MongoClients.create(uri)) {
            final BsonDocument insert = BsonDocument.parse(client.getDatabase("resetdb")
                    .runCommand(BsonDocument.parse("{\"insert\":\"users\",\"documents\":[{\"_id\":1,\"name\":\"before\"}]}"))
                    .toJson());
            assertEquals(1.0, insert.getNumber("ok").doubleValue(), 0.0);

            final BsonDocument beforeReset = BsonDocument.parse(client.getDatabase("resetdb")
                    .runCommand(BsonDocument.parse("{\"find\":\"users\",\"filter\":{}}"))
                    .toJson());
            assertEquals(1, beforeReset.getDocument("cursor").getArray("firstBatch").size());

            JongodbMongoResetSupport.reset(context);
            assertTrue(server.isRunning());

            final BsonDocument afterReset = BsonDocument.parse(client.getDatabase("resetdb")
                    .runCommand(BsonDocument.parse("{\"find\":\"users\",\"filter\":{}}"))
                    .toJson());
            assertEquals(0, afterReset.getDocument("cursor").getArray("firstBatch").size());
        }

        context.close();
        assertFalse(server.isRunning());
    }
}
