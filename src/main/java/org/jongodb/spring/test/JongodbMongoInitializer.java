package org.jongodb.spring.test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jongodb.server.TcpMongoServer;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.core.env.MapPropertySource;

/**
 * Spring test initializer that starts an in-memory {@link TcpMongoServer} and wires MongoDB
 * properties for the test context.
 *
 * <p>By default each context gets a dedicated server. Set {@link #SHARED_SERVER_PROPERTY} to
 * {@code true} to reuse one shared in-process server across multiple Spring contexts in the same JVM.
 */
public final class JongodbMongoInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    public static final String DEFAULT_DATABASE = "test";
    public static final String DATABASE_PROPERTY = "jongodb.test.database";
    public static final String SHARED_SERVER_PROPERTY = "jongodb.test.sharedServer";
    public static final String SERVER_BEAN_NAME = "jongodbTcpMongoServer";
    private static final Object SHARED_SERVER_LOCK = new Object();
    private static final AtomicBoolean SHARED_SHUTDOWN_HOOK_REGISTERED = new AtomicBoolean(false);
    private static TcpMongoServer sharedServer;
    private static int sharedServerRefCount;

    @Override
    public void initialize(final ConfigurableApplicationContext context) {
        Objects.requireNonNull(context, "context");

        final String database = normalizeDatabase(context.getEnvironment().getProperty(DATABASE_PROPERTY));
        final boolean sharedServerEnabled = parseSharedServerEnabled(context.getEnvironment().getProperty(SHARED_SERVER_PROPERTY));
        final TcpMongoServer server = sharedServerEnabled ? acquireSharedServer() : startDedicatedServer();

        final Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("spring.data.mongodb.uri", server.connectionString(database));
        properties.put("spring.data.mongodb.host", server.host());
        properties.put("spring.data.mongodb.port", Integer.toString(server.port()));

        context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("jongodbInMemoryMongo", properties));
        context.getBeanFactory().registerSingleton(SERVER_BEAN_NAME, server);
        context.addApplicationListener(event -> {
            if (event instanceof ContextClosedEvent) {
                if (sharedServerEnabled) {
                    releaseSharedServer();
                } else {
                    server.close();
                }
            }
        });
    }

    /**
     * Explicit cleanup hook for suites that opt into shared-server mode.
     *
     * <p>Shared servers are normally closed when the last shared Spring context closes.
     * This method is useful in test teardown blocks to aggressively release the shared endpoint.
     */
    public static void stopSharedServer() {
        synchronized (SHARED_SERVER_LOCK) {
            closeSharedServerLocked();
        }
    }

    private static TcpMongoServer startDedicatedServer() {
        final TcpMongoServer server = TcpMongoServer.inMemory();
        server.start();
        return server;
    }

    private static TcpMongoServer acquireSharedServer() {
        synchronized (SHARED_SERVER_LOCK) {
            if (sharedServer != null && sharedServer.isRunning()) {
                sharedServerRefCount++;
                return sharedServer;
            }
            closeSharedServerLocked();
            sharedServer = TcpMongoServer.inMemory();
            sharedServer.start();
            sharedServerRefCount = 1;
            registerSharedShutdownHookOnce();
            return sharedServer;
        }
    }

    private static void releaseSharedServer() {
        synchronized (SHARED_SERVER_LOCK) {
            if (sharedServerRefCount <= 0) {
                closeSharedServerLocked();
                return;
            }
            sharedServerRefCount--;
            if (sharedServerRefCount == 0) {
                closeSharedServerLocked();
            }
        }
    }

    private static void registerSharedShutdownHookOnce() {
        if (!SHARED_SHUTDOWN_HOOK_REGISTERED.compareAndSet(false, true)) {
            return;
        }
        Runtime.getRuntime().addShutdownHook(new Thread(JongodbMongoInitializer::stopSharedServer, "jongodb-shared-tcp-shutdown"));
    }

    private static void closeSharedServerLocked() {
        final TcpMongoServer previous = sharedServer;
        sharedServer = null;
        sharedServerRefCount = 0;
        if (previous != null) {
            previous.close();
        }
    }

    private static String normalizeDatabase(final String candidate) {
        if (candidate == null || candidate.isBlank()) {
            return DEFAULT_DATABASE;
        }
        return candidate.trim();
    }

    private static boolean parseSharedServerEnabled(final String rawValue) {
        if (rawValue == null || rawValue.isBlank()) {
            return false;
        }
        return Boolean.parseBoolean(rawValue.trim());
    }
}
