package org.jongodb.spring.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.bson.BsonDocument;
import org.jongodb.server.TcpMongoServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.DynamicPropertyRegistry;

final class JongodbMongoDynamicPropertySupportTest {
    @AfterEach
    void tearDown() {
        JongodbMongoDynamicPropertySupport.stop();
    }

    @Test
    void registersDynamicMongoPropertiesAndStartsSharedServer() {
        final RecordingDynamicPropertyRegistry registry = new RecordingDynamicPropertyRegistry();
        final TcpMongoServer server = JongodbMongoDynamicPropertySupport.register(registry, "dynodb");

        assertTrue(server.isRunning());
        final String uri = registry.stringValue("spring.data.mongodb.uri");
        assertNotNull(uri);
        assertTrue(uri.endsWith("/dynodb"));
        assertEquals(server.host(), registry.stringValue("spring.data.mongodb.host"));
        assertEquals(server.port(), registry.intValue("spring.data.mongodb.port"));

        try (MongoClient client = MongoClients.create(uri)) {
            final BsonDocument ping = BsonDocument.parse(client.getDatabase("dynodb").runCommand(BsonDocument.parse("{\"ping\":1}")).toJson());
            assertEquals(1.0, ping.getNumber("ok").doubleValue(), 0.0);
        }
    }

    @Test
    void reusesSharedServerAcrossMultipleRegistrations() {
        final RecordingDynamicPropertyRegistry first = new RecordingDynamicPropertyRegistry();
        final RecordingDynamicPropertyRegistry second = new RecordingDynamicPropertyRegistry();

        final TcpMongoServer firstServer = JongodbMongoDynamicPropertySupport.register(first);
        final TcpMongoServer secondServer = JongodbMongoDynamicPropertySupport.register(second, "anotherdb");
        assertSame(firstServer, secondServer);
        assertTrue(firstServer.isRunning());

        JongodbMongoDynamicPropertySupport.stop();
        assertFalse(firstServer.isRunning());
    }

    private static final class RecordingDynamicPropertyRegistry implements DynamicPropertyRegistry {
        private final Map<String, Supplier<Object>> values = new LinkedHashMap<>();

        @Override
        public void add(final String name, final Supplier<Object> valueSupplier) {
            values.put(name, valueSupplier);
        }

        private String stringValue(final String key) {
            final Supplier<Object> supplier = values.get(key);
            return supplier == null ? null : String.valueOf(supplier.get());
        }

        private int intValue(final String key) {
            final Supplier<Object> supplier = values.get(key);
            final Object value = supplier == null ? null : supplier.get();
            if (value instanceof Number number) {
                return number.intValue();
            }
            return Integer.parseInt(String.valueOf(value));
        }
    }
}
