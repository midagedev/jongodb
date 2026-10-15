package org.jongodb.spring.embedded;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jongodb.command.EngineBackedCommandStore;
import org.jongodb.command.TopologyProfile;
import org.jongodb.engine.EngineStateFiles;
import org.jongodb.engine.InMemoryEngineStore;
import org.jongodb.server.TcpMongoServer;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

/**
 * Runs jongodb inside an application's own JVM as its MongoDB, for local environments rather than tests.
 *
 * <p>Spring Boot picks it up from {@code META-INF/spring.factories}, so an application needs no code change: put the
 * jongodb jar on its classpath and set {@code jongodb.embedded.enabled=true}; without that property it does nothing.
 * The server starts, and loads its initial state, before any bean is created, so startup code already sees the data.
 *
 * <p>Properties:
 * <ul>
 *   <li>{@code jongodb.embedded.host} / {@code .port} — listen address, default {@code 127.0.0.1:27017}; a fixed port
 *       lets mongosh and other processes share this server</li>
 *   <li>{@code jongodb.embedded.topology} — {@code singleNodeReplicaSet} (default) or {@code standalone}</li>
 *   <li>{@code jongodb.embedded.replica-set-name} — default {@code rs0}</li>
 *   <li>{@code jongodb.embedded.uri-properties} — comma-separated property names to point at this server, each
 *       optionally {@code name:database}; default {@code spring.data.mongodb.uri}</li>
 *   <li>{@code jongodb.embedded.load-path} — {@code mongodump}-layout directory to load at startup</li>
 *   <li>{@code jongodb.embedded.persist-path} — loaded at startup when present (and no load-path), written on close</li>
 * </ul>
 */
public final class JongodbEmbeddedInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    public static final String PREFIX = "jongodb.embedded.";
    public static final String SERVER_BEAN_NAME = "jongodbEmbeddedServer";

    @Override
    public void initialize(final ConfigurableApplicationContext context) {
        Objects.requireNonNull(context, "context");
        final ConfigurableEnvironment environment = context.getEnvironment();
        if (!environment.getProperty(PREFIX + "enabled", Boolean.class, false)) {
            return;
        }
        final String host = environment.getProperty(PREFIX + "host", "127.0.0.1");
        final int port = environment.getProperty(PREFIX + "port", Integer.class, 27017);
        final TopologyProfile topology =
                TopologyProfile.parse(environment.getProperty(PREFIX + "topology", "singleNodeReplicaSet"));
        final String replicaSetName = environment.getProperty(PREFIX + "replica-set-name", "rs0");
        final Path loadPath = path(environment.getProperty(PREFIX + "load-path"));
        final Path persistPath = path(environment.getProperty(PREFIX + "persist-path"));

        final InMemoryEngineStore engine = new InMemoryEngineStore();
        final Path initialState = loadPath != null
                ? loadPath
                : persistPath != null && Files.isDirectory(persistPath) ? persistPath : null;
        if (initialState != null) {
            EngineStateFiles.load(engine, initialState);
        }
        final TcpMongoServer server =
                new TcpMongoServer(new EngineBackedCommandStore(engine), host, port, topology, replicaSetName);
        server.start();

        final Map<String, Object> properties = new LinkedHashMap<>();
        for (final String entry : environment.getProperty(PREFIX + "uri-properties", "spring.data.mongodb.uri").split(",")) {
            final String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            final int separator = trimmed.indexOf(':');
            final String property = separator < 0 ? trimmed : trimmed.substring(0, separator);
            final String database = separator < 0 ? "" : trimmed.substring(separator + 1);
            properties.put(property, server.connectionString(database));
        }
        environment.getPropertySources().addFirst(new MapPropertySource("jongodbEmbedded", properties));
        context.getBeanFactory().registerSingleton(SERVER_BEAN_NAME, server);
        context.addApplicationListener(event -> {
            if (event instanceof ContextClosedEvent) {
                server.close();
                if (persistPath != null) {
                    EngineStateFiles.export(engine, persistPath);
                }
            }
        });
    }

    private static Path path(final String value) {
        return value == null || value.isBlank() ? null : Path.of(value.trim());
    }
}
