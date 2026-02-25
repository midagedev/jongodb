package org.jongodb.spring.test;

import java.util.Objects;
import org.jongodb.server.TcpMongoServer;
import org.springframework.context.ApplicationContext;

/**
 * Test-only reset helpers for Spring integrations using {@link JongodbMongoInitializer}.
 */
public final class JongodbMongoResetSupport {
    private JongodbMongoResetSupport() {}

    /**
     * Resets the in-memory Mongo state backing the current Spring context without restarting the context.
     */
    public static void reset(final ApplicationContext context) {
        Objects.requireNonNull(context, "context");
        final TcpMongoServer server = context.getBean(JongodbMongoInitializer.SERVER_BEAN_NAME, TcpMongoServer.class);
        reset(server);
    }

    /**
     * Resets the provided in-memory TCP server without closing its socket endpoint.
     */
    public static void reset(final TcpMongoServer server) {
        Objects.requireNonNull(server, "server");
        server.reset();
    }
}
