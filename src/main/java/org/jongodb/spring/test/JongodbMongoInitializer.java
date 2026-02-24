package org.jongodb.spring.test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jongodb.server.TcpMongoServer;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.core.env.MapPropertySource;

/**
 * Spring test initializer that starts an in-memory {@link TcpMongoServer} and wires MongoDB
 * properties for the test context.
 */
public final class JongodbMongoInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    public static final String DEFAULT_DATABASE = "test";
    public static final String DATABASE_PROPERTY = "jongodb.test.database";
    public static final String SERVER_BEAN_NAME = "jongodbTcpMongoServer";

    @Override
    public void initialize(final ConfigurableApplicationContext context) {
        Objects.requireNonNull(context, "context");

        final String database = normalizeDatabase(context.getEnvironment().getProperty(DATABASE_PROPERTY));
        final TcpMongoServer server = TcpMongoServer.inMemory();
        server.start();

        final Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("spring.data.mongodb.uri", server.connectionString(database));
        properties.put("spring.data.mongodb.host", server.host());
        properties.put("spring.data.mongodb.port", Integer.toString(server.port()));

        context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("jongodbInMemoryMongo", properties));
        context.getBeanFactory().registerSingleton(SERVER_BEAN_NAME, server);
        context.addApplicationListener(event -> {
            if (event instanceof ContextClosedEvent) {
                server.close();
            }
        });
    }

    private static String normalizeDatabase(final String candidate) {
        if (candidate == null || candidate.isBlank()) {
            return DEFAULT_DATABASE;
        }
        return candidate.trim();
    }
}
