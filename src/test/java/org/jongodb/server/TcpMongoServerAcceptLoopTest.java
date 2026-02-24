package org.jongodb.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.jongodb.command.EngineBackedCommandStore;
import org.jongodb.engine.InMemoryEngineStore;
import org.junit.jupiter.api.Test;

final class TcpMongoServerAcceptLoopTest {
    @Test
    void acceptLoopRetriesAfterTransientFailures() throws Exception {
        final ScriptedServerSocket serverSocket = new ScriptedServerSocket();
        serverSocket.enqueue(new IOException("transient accept failure"));
        serverSocket.enqueue(new InertSocket(10101));

        try (TcpMongoServer server = new TcpMongoServer(
                new EngineBackedCommandStore(new InMemoryEngineStore()),
                "127.0.0.1",
                serverSocket,
                4,
                1L,
                4L)) {
            server.start();

            waitUntil(() -> serverSocket.acceptCount() >= 2, 2_000L, "accept loop should retry after transient failure");
            assertTrue(server.isRunning(), "server should remain running after transient accept failure");
        }
    }

    @Test
    void acceptLoopStopsAfterConfiguredFailureThreshold() throws Exception {
        final ScriptedServerSocket serverSocket = new ScriptedServerSocket();
        serverSocket.enqueue(new IOException("accept failure-1"));
        serverSocket.enqueue(new IOException("accept failure-2"));
        serverSocket.enqueue(new IOException("accept failure-3"));

        try (TcpMongoServer server = new TcpMongoServer(
                new EngineBackedCommandStore(new InMemoryEngineStore()),
                "127.0.0.1",
                serverSocket,
                3,
                1L,
                4L)) {
            server.start();

            waitUntil(() -> !server.isRunning(), 2_000L, "server should stop after repeated accept failures");
            assertFalse(server.isRunning());
            assertEquals(3, serverSocket.acceptCount());
        }
    }

    private static void waitUntil(
            final BooleanSupplier condition,
            final long timeoutMillis,
            final String failureMessage) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10L);
        }
        if (condition.getAsBoolean()) {
            return;
        }
        fail(failureMessage);
    }

    private static final class ScriptedServerSocket extends ServerSocket {
        private final Queue<Object> scriptedAcceptResults = new ConcurrentLinkedQueue<>();
        private final AtomicInteger acceptCount = new AtomicInteger();
        private final CountDownLatch closeSignal = new CountDownLatch(1);

        private ScriptedServerSocket() throws IOException {
            super();
        }

        private void enqueue(final Object result) {
            scriptedAcceptResults.add(result);
        }

        private int acceptCount() {
            return acceptCount.get();
        }

        @Override
        public Socket accept() throws IOException {
            acceptCount.incrementAndGet();
            final Object scripted = scriptedAcceptResults.poll();
            if (scripted instanceof IOException ioException) {
                throw ioException;
            }
            if (scripted instanceof Socket socket) {
                return socket;
            }
            try {
                closeSignal.await(5, TimeUnit.SECONDS);
            } catch (final InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                throw new IOException("accept interrupted", interruptedException);
            }
            throw new IOException("server socket closed");
        }

        @Override
        public synchronized void close() throws IOException {
            closeSignal.countDown();
            super.close();
        }

        @Override
        public int getLocalPort() {
            return 27017;
        }
    }

    private static final class InertSocket extends Socket {
        private final int port;
        private final InputStream input = new ByteArrayInputStream(new byte[0]);
        private final OutputStream output = new ByteArrayOutputStream();
        private volatile boolean closed;

        private InertSocket(final int port) {
            this.port = port;
        }

        @Override
        public InputStream getInputStream() {
            return input;
        }

        @Override
        public OutputStream getOutputStream() {
            return output;
        }

        @Override
        public void setTcpNoDelay(final boolean on) {}

        @Override
        public int getPort() {
            return port;
        }

        @Override
        public synchronized void close() {
            closed = true;
        }

        @Override
        public boolean isClosed() {
            return closed;
        }
    }
}
