package org.jongodb.server;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import org.jongodb.command.TopologyProfile;

/**
 * Command-line launcher for starting a {@link TcpMongoServer} in a dedicated JVM process.
 *
 * <p>Ready signal is emitted as a single stdout line: {@code JONGODB_URI=<mongodb-uri>}.
 */
public final class TcpMongoServerLauncher {
    private static final String READY_PREFIX = "JONGODB_URI=";
    private static final String FAILURE_PREFIX = "JONGODB_START_FAILURE=";

    private TcpMongoServerLauncher() {}

    public static void main(final String[] args) throws Exception {
        final LaunchConfig config = LaunchConfig.parse(args);
        final CountDownLatch stopLatch = new CountDownLatch(1);
        final TcpMongoServer server;
        try {
            server = new TcpMongoServer(
                    config.host(),
                    config.port(),
                    config.topologyProfile(),
                    config.replicaSetName());
            server.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    server.close();
                } finally {
                    stopLatch.countDown();
                }
            }, "jongodb-tcp-server-shutdown"));
        } catch (final Exception exception) {
            System.err.println(FAILURE_PREFIX + exception.getMessage());
            exception.printStackTrace(System.err);
            System.exit(1);
            return;
        }

        System.out.println(READY_PREFIX + server.connectionString(config.database()));
        System.out.flush();

        try {
            stopLatch.await();
        } catch (final InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    private record LaunchConfig(
            String host,
            int port,
            String database,
            TopologyProfile topologyProfile,
            String replicaSetName) {
        private static LaunchConfig parse(final String[] args) {
            String host = "127.0.0.1";
            int port = 0;
            String database = "test";
            TopologyProfile topologyProfile = TopologyProfile.STANDALONE;
            String replicaSetName = "jongodb-rs0";

            for (final String arg : args) {
                if (arg == null || arg.isBlank()) {
                    continue;
                }
                if (arg.startsWith("--host=")) {
                    host = requireValue(arg, "--host=");
                    continue;
                }
                if (arg.startsWith("--port=")) {
                    port = parsePort(requireValue(arg, "--port="));
                    continue;
                }
                if (arg.startsWith("--database=")) {
                    database = requireValue(arg, "--database=");
                    continue;
                }
                if (arg.startsWith("--topology-profile=")) {
                    topologyProfile = TopologyProfile.parse(requireValue(arg, "--topology-profile="));
                    continue;
                }
                if (arg.startsWith("--replica-set-name=")) {
                    replicaSetName = requireValue(arg, "--replica-set-name=");
                    continue;
                }
                throw new IllegalArgumentException("unsupported argument: " + arg);
            }

            return new LaunchConfig(host, port, database, topologyProfile, replicaSetName);
        }

        private static String requireValue(final String arg, final String prefix) {
            final String value = Objects.requireNonNull(arg, "arg").substring(prefix.length()).trim();
            if (value.isEmpty()) {
                throw new IllegalArgumentException("argument value is empty for " + prefix);
            }
            return value;
        }

        private static int parsePort(final String value) {
            try {
                final int parsed = Integer.parseInt(value);
                if (parsed < 0 || parsed > 65535) {
                    throw new IllegalArgumentException("port must be between 0 and 65535: " + parsed);
                }
                return parsed;
            } catch (final NumberFormatException numberFormatException) {
                throw new IllegalArgumentException("invalid port: " + value, numberFormatException);
            }
        }
    }
}
