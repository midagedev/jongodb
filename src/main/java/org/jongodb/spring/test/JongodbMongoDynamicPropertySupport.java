package org.jongodb.spring.test;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.jongodb.server.TcpMongoServer;
import org.springframework.test.context.DynamicPropertyRegistry;

/**
 * Helper for Spring tests using {@code @DynamicPropertySource} style configuration.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * @DynamicPropertySource
 * static void mongoProps(DynamicPropertyRegistry registry) {
 *   JongodbMongoDynamicPropertySupport.register(registry);
 * }
 * }</pre>
 */
public final class JongodbMongoDynamicPropertySupport {
    private static final Object LOCK = new Object();
    private static final AtomicReference<TcpMongoServer> SHARED_SERVER = new AtomicReference<>();
    private static final AtomicBoolean SHUTDOWN_HOOK_REGISTERED = new AtomicBoolean(false);

    private JongodbMongoDynamicPropertySupport() {}

    public static TcpMongoServer register(final DynamicPropertyRegistry registry) {
        return register(registry, JongodbMongoInitializer.DEFAULT_DATABASE);
    }

    public static TcpMongoServer register(final DynamicPropertyRegistry registry, final String database) {
        Objects.requireNonNull(registry, "registry");
        final String normalizedDatabase = normalizeDatabase(database);
        final TcpMongoServer server = ensureRunningServer();

        registry.add("spring.data.mongodb.uri", () -> server.connectionString(normalizedDatabase));
        registry.add("spring.data.mongodb.host", server::host);
        registry.add("spring.data.mongodb.port", server::port);
        return server;
    }

    public static void stop() {
        synchronized (LOCK) {
            final TcpMongoServer server = SHARED_SERVER.getAndSet(null);
            if (server != null) {
                server.close();
            }
        }
    }

    private static TcpMongoServer ensureRunningServer() {
        synchronized (LOCK) {
            final TcpMongoServer existing = SHARED_SERVER.get();
            if (existing != null && existing.isRunning()) {
                return existing;
            }
            if (existing != null) {
                existing.close();
            }

            final TcpMongoServer created = TcpMongoServer.inMemory();
            created.start();
            SHARED_SERVER.set(created);
            registerShutdownHookOnce();
            return created;
        }
    }

    private static void registerShutdownHookOnce() {
        if (!SHUTDOWN_HOOK_REGISTERED.compareAndSet(false, true)) {
            return;
        }
        Runtime.getRuntime().addShutdownHook(new Thread(JongodbMongoDynamicPropertySupport::stop, "jongodb-tcp-shutdown"));
    }

    private static String normalizeDatabase(final String database) {
        if (database == null || database.isBlank()) {
            return JongodbMongoInitializer.DEFAULT_DATABASE;
        }
        return database.trim();
    }
}
