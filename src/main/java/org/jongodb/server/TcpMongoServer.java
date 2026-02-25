package org.jongodb.server;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.jongodb.command.CommandDispatcher;
import org.jongodb.command.CommandStore;
import org.jongodb.command.EngineBackedCommandStore;
import org.jongodb.command.TopologyProfile;
import org.jongodb.engine.InMemoryEngineStore;
import org.jongodb.wire.OpMsg;
import org.jongodb.wire.OpMsgCodec;

/**
 * In-memory TCP bridge server for MongoDB wire requests.
 *
 * <p>This class is intended for integration-test bootstrap, so Spring test contexts can connect
 * using regular {@code mongodb://} URIs while requests are handled by {@link CommandDispatcher}.
 */
public final class TcpMongoServer implements AutoCloseable {
    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 0;
    private static final int HEADER_LENGTH = 16;
    private static final int OP_REPLY_FIELDS_LENGTH = 20;
    private static final int OP_REPLY = 1;
    private static final int OP_QUERY = 2004;
    private static final int OP_MSG = 2013;
    private static final int DEFAULT_MAX_ACCEPT_FAILURES = 8;
    private static final long DEFAULT_ACCEPT_BACKOFF_BASE_MILLIS = 10L;
    private static final long DEFAULT_ACCEPT_BACKOFF_MAX_MILLIS = 500L;
    private static final TopologyProfile DEFAULT_TOPOLOGY_PROFILE = TopologyProfile.STANDALONE;
    private static final String DEFAULT_REPLICA_SET_NAME = "jongodb-rs0";

    private final CommandDispatcher dispatcher;
    private final CommandStore commandStore;
    private final OpMsgCodec opMsgCodec = new OpMsgCodec();
    private final AtomicInteger responseRequestId = new AtomicInteger(1);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ServerSocket serverSocket;
    private final List<Socket> clientSockets = new CopyOnWriteArrayList<>();
    private final Thread acceptThread;
    private final String host;
    private final TopologyProfile topologyProfile;
    private final String replicaSetName;
    private final int maxConsecutiveAcceptFailures;
    private final long acceptBackoffBaseMillis;
    private final long acceptBackoffMaxMillis;

    public static TcpMongoServer inMemory() {
        return new TcpMongoServer();
    }

    public static TcpMongoServer inMemory(final String host, final int port) {
        return new TcpMongoServer(host, port);
    }

    public static TcpMongoServer inMemoryReplicaSet(final String host, final int port) {
        return new TcpMongoServer(host, port, TopologyProfile.SINGLE_NODE_REPLICA_SET, DEFAULT_REPLICA_SET_NAME);
    }

    public static TcpMongoServer inMemoryReplicaSet(final String host, final int port, final String replicaSetName) {
        return new TcpMongoServer(host, port, TopologyProfile.SINGLE_NODE_REPLICA_SET, replicaSetName);
    }

    public TcpMongoServer() {
        this(
                new EngineBackedCommandStore(new InMemoryEngineStore()),
                DEFAULT_HOST,
                DEFAULT_PORT,
                DEFAULT_TOPOLOGY_PROFILE,
                DEFAULT_REPLICA_SET_NAME);
    }

    public TcpMongoServer(final String host, final int port) {
        this(
                new EngineBackedCommandStore(new InMemoryEngineStore()),
                host,
                port,
                DEFAULT_TOPOLOGY_PROFILE,
                DEFAULT_REPLICA_SET_NAME);
    }

    public TcpMongoServer(
            final String host,
            final int port,
            final TopologyProfile topologyProfile,
            final String replicaSetName) {
        this(new EngineBackedCommandStore(new InMemoryEngineStore()), host, port, topologyProfile, replicaSetName);
    }

    public TcpMongoServer(final CommandStore commandStore) {
        this(commandStore, DEFAULT_HOST, DEFAULT_PORT, DEFAULT_TOPOLOGY_PROFILE, DEFAULT_REPLICA_SET_NAME);
    }

    public TcpMongoServer(final CommandStore commandStore, final String host, final int port) {
        this(commandStore, host, port, DEFAULT_TOPOLOGY_PROFILE, DEFAULT_REPLICA_SET_NAME);
    }

    public TcpMongoServer(
            final CommandStore commandStore,
            final String host,
            final int port,
            final TopologyProfile topologyProfile,
            final String replicaSetName) {
        this(
                Objects.requireNonNull(commandStore, "commandStore"),
                normalizeHost(host),
                newServerSocket(normalizeHost(host), normalizePort(port)),
                Objects.requireNonNull(topologyProfile, "topologyProfile"),
                normalizeReplicaSetName(replicaSetName),
                DEFAULT_MAX_ACCEPT_FAILURES,
                DEFAULT_ACCEPT_BACKOFF_BASE_MILLIS,
                DEFAULT_ACCEPT_BACKOFF_MAX_MILLIS);
    }

    TcpMongoServer(
            final CommandStore commandStore,
            final String host,
            final ServerSocket serverSocket,
            final TopologyProfile topologyProfile,
            final String replicaSetName,
            final int maxConsecutiveAcceptFailures,
            final long acceptBackoffBaseMillis,
            final long acceptBackoffMaxMillis) {
        this.host = normalizeHost(host);
        this.serverSocket = Objects.requireNonNull(serverSocket, "serverSocket");
        this.topologyProfile = Objects.requireNonNull(topologyProfile, "topologyProfile");
        this.replicaSetName = normalizeReplicaSetName(replicaSetName);
        this.commandStore = Objects.requireNonNull(commandStore, "commandStore");
        this.dispatcher = new CommandDispatcher(
                this.commandStore,
                this.topologyProfile,
                this.host + ":" + this.serverSocket.getLocalPort(),
                this.replicaSetName);
        this.maxConsecutiveAcceptFailures = normalizeMaxAcceptFailures(maxConsecutiveAcceptFailures);
        this.acceptBackoffBaseMillis = normalizeBackoff(acceptBackoffBaseMillis, "acceptBackoffBaseMillis");
        this.acceptBackoffMaxMillis = normalizeBackoff(acceptBackoffMaxMillis, "acceptBackoffMaxMillis");
        if (this.acceptBackoffMaxMillis < this.acceptBackoffBaseMillis) {
            throw new IllegalArgumentException("acceptBackoffMaxMillis must be >= acceptBackoffBaseMillis");
        }
        this.acceptThread = new Thread(this::acceptLoop, "jongodb-tcp-accept");
        this.acceptThread.setDaemon(true);
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            acceptThread.start();
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    public String host() {
        return host;
    }

    public int port() {
        return serverSocket.getLocalPort();
    }

    public TopologyProfile topologyProfile() {
        return topologyProfile;
    }

    public String replicaSetName() {
        return replicaSetName;
    }

    public String connectionString(final String database) {
        final String normalizedDatabase = normalizeDatabase(database);
        final String base = "mongodb://" + host + ":" + port() + "/" + normalizedDatabase;
        if (topologyProfile.replicaSetSemanticsEnabled()) {
            return base + "?replicaSet=" + replicaSetName;
        }
        return base;
    }

    /**
     * Test-only fast reset hook that clears in-memory command state without restarting the TCP server.
     */
    public void reset() {
        commandStore.reset();
    }

    @Override
    public void close() {
        running.set(false);
        closeQuietly(serverSocket);
        for (final Socket socket : clientSockets) {
            closeQuietly(socket);
        }
        try {
            acceptThread.join(1000L);
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static ServerSocket newServerSocket(final String host, final int port) {
        try {
            return new ServerSocket(port, 50, InetAddress.getByName(host));
        } catch (final IOException ioException) {
            throw new IllegalStateException(
                    "failed to allocate TCP server socket host=" + host + " port=" + port, ioException);
        }
    }

    private static void closeQuietly(final AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (final Exception ignore) {
            // no-op: test teardown should be best-effort.
        }
    }

    private void acceptLoop() {
        int consecutiveFailures = 0;
        long firstFailureNanos = 0L;
        while (running.get()) {
            final Socket socket;
            try {
                socket = serverSocket.accept();
                consecutiveFailures = 0;
                firstFailureNanos = 0L;
            } catch (final IOException ioException) {
                if (isExpectedAcceptShutdown(ioException)) {
                    return;
                }
                consecutiveFailures++;
                if (firstFailureNanos == 0L) {
                    firstFailureNanos = System.nanoTime();
                }
                final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - firstFailureNanos);
                if (consecutiveFailures >= maxConsecutiveAcceptFailures) {
                    running.set(false);
                    System.err.println(
                            "jongodb tcp accept terminal failure"
                                    + " port=" + serverSocket.getLocalPort()
                                    + " attempts=" + consecutiveFailures
                                    + " elapsedMs=" + elapsedMillis
                                    + " error=" + ioException.getMessage());
                    closeQuietly(serverSocket);
                    return;
                }
                System.err.println(
                        "jongodb tcp accept transient failure"
                                + " port=" + serverSocket.getLocalPort()
                                + " attempt=" + consecutiveFailures
                                + " elapsedMs=" + elapsedMillis
                                + " retryBackoffMs=" + nextBackoffMillis(consecutiveFailures)
                                + " error=" + ioException.getMessage());
                if (!sleepBeforeAcceptRetry(consecutiveFailures)) {
                    running.set(false);
                    return;
                }
                continue;
            }

            clientSockets.add(socket);
            final Thread worker = new Thread(
                    () -> handleClient(socket),
                    "jongodb-tcp-client-" + socket.getPort());
            worker.setDaemon(true);
            worker.start();
        }
    }

    private boolean isExpectedAcceptShutdown(final IOException ioException) {
        return !running.get() || serverSocket.isClosed();
    }

    private boolean sleepBeforeAcceptRetry(final int consecutiveFailures) {
        try {
            Thread.sleep(nextBackoffMillis(consecutiveFailures));
            return true;
        } catch (final InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private long nextBackoffMillis(final int consecutiveFailures) {
        long computed = acceptBackoffBaseMillis;
        for (int index = 1; index < consecutiveFailures; index++) {
            if (computed >= acceptBackoffMaxMillis / 2L) {
                return acceptBackoffMaxMillis;
            }
            computed *= 2L;
        }
        return Math.min(computed, acceptBackoffMaxMillis);
    }

    private void handleClient(final Socket socket) {
        try (Socket client = socket;
                BufferedInputStream input = new BufferedInputStream(client.getInputStream());
                BufferedOutputStream output = new BufferedOutputStream(client.getOutputStream())) {
            client.setTcpNoDelay(true);

            while (running.get()) {
                final byte[] request = readMessage(input);
                if (request == null) {
                    break;
                }

                final byte[] response;
                try {
                    response = handleRequest(request);
                } catch (final RuntimeException runtimeException) {
                    System.err.println("jongodb tcp request failed: " + runtimeException.getMessage());
                    runtimeException.printStackTrace();
                    break;
                }
                output.write(response);
                output.flush();
            }
        } catch (final IOException ioException) {
            if (running.get()) {
                System.err.println("jongodb tcp client io failure: " + ioException.getMessage());
            }
        } finally {
            clientSockets.remove(socket);
        }
    }

    private byte[] handleRequest(final byte[] request) {
        final int opcode = readIntLE(request, 12);
        if (opcode == OP_MSG) {
            return handleOpMsg(request);
        }
        if (opcode == OP_QUERY) {
            return handleOpQuery(request);
        }
        throw new IllegalArgumentException("unsupported opcode: " + opcode);
    }

    private byte[] handleOpMsg(final byte[] request) {
        final OpMsg opMsg = opMsgCodec.decode(request);
        final BsonDocument responseBody = dispatcher.dispatch(opMsg.body());
        final OpMsg response = new OpMsg(responseRequestId.getAndIncrement(), opMsg.requestId(), 0, responseBody);
        return opMsgCodec.encode(response);
    }

    private byte[] handleOpQuery(final byte[] request) {
        final int requestId = readIntLE(request, 4);
        int cursor = HEADER_LENGTH;
        cursor += 4; // flags
        final int namespaceOffset = cursor;
        cursor = readCString(request, cursor); // fullCollectionName
        final String fullCollectionName = readCStringValue(request, namespaceOffset, cursor);
        final String namespaceDatabase = parseDatabaseFromNamespace(fullCollectionName);
        cursor += 4; // numberToSkip
        cursor += 4; // numberToReturn

        final int documentLength = readIntLE(request, cursor);
        if (documentLength <= 0 || cursor + documentLength > request.length) {
            throw new IllegalArgumentException("invalid OP_QUERY document length: " + documentLength);
        }
        final byte[] queryBytes = java.util.Arrays.copyOfRange(request, cursor, cursor + documentLength);
        final BsonDocument queryDocument = new RawBsonDocument(queryBytes);
        final BsonDocument dispatchDocument = withNamespaceDatabase(queryDocument, namespaceDatabase);
        final BsonDocument responseBody = dispatcher.dispatch(dispatchDocument);
        return encodeOpReply(requestId, responseBody);
    }

    private byte[] encodeOpReply(final int responseTo, final BsonDocument document) {
        final byte[] bodyBytes = encodeBson(document);
        final int totalLength = HEADER_LENGTH + OP_REPLY_FIELDS_LENGTH + bodyBytes.length;
        return ByteBuffer.allocate(totalLength)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(totalLength)
                .putInt(responseRequestId.getAndIncrement())
                .putInt(responseTo)
                .putInt(OP_REPLY)
                .putInt(0) // responseFlags
                .putLong(0L) // cursorId
                .putInt(0) // startingFrom
                .putInt(1) // numberReturned
                .put(bodyBytes)
                .array();
    }

    private static byte[] encodeBson(final BsonDocument document) {
        final BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        try (BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer)) {
            new BsonDocumentCodec().encode(
                    writer,
                    document,
                    EncoderContext.builder().isEncodingCollectibleDocument(true).build());
        }
        return outputBuffer.toByteArray();
    }

    private static byte[] readMessage(final BufferedInputStream input) throws IOException {
        final byte[] headerLengthBytes = new byte[4];
        final int first = input.read();
        if (first == -1) {
            return null;
        }

        headerLengthBytes[0] = (byte) first;
        if (!readFully(input, headerLengthBytes, 1, 3)) {
            return null;
        }

        final int messageLength = littleEndianInt(headerLengthBytes);
        if (messageLength < HEADER_LENGTH) {
            throw new IllegalArgumentException("invalid message length: " + messageLength);
        }

        final byte[] message = new byte[messageLength];
        System.arraycopy(headerLengthBytes, 0, message, 0, 4);
        if (!readFully(input, message, 4, messageLength - 4)) {
            return null;
        }
        return message;
    }

    private static boolean readFully(
            final BufferedInputStream input,
            final byte[] target,
            final int offset,
            final int length)
            throws IOException {
        int total = 0;
        while (total < length) {
            final int read = input.read(target, offset + total, length - total);
            if (read == -1) {
                return false;
            }
            total += read;
        }
        return true;
    }

    private static int readCString(final byte[] bytes, final int offset) {
        int index = offset;
        while (index < bytes.length && bytes[index] != 0) {
            index++;
        }
        if (index >= bytes.length) {
            throw new IllegalArgumentException("malformed cstring in wire request");
        }
        return index + 1;
    }

    private static String readCStringValue(final byte[] bytes, final int offset, final int nextOffset) {
        if (nextOffset <= offset) {
            return "";
        }
        return new String(bytes, offset, nextOffset - offset - 1, StandardCharsets.UTF_8);
    }

    private static String parseDatabaseFromNamespace(final String fullCollectionName) {
        if (fullCollectionName == null || fullCollectionName.isBlank()) {
            return null;
        }
        final int delimiter = fullCollectionName.indexOf('.');
        if (delimiter <= 0) {
            return null;
        }
        final String database = fullCollectionName.substring(0, delimiter);
        return database.isBlank() ? null : database;
    }

    private static BsonDocument withNamespaceDatabase(final BsonDocument commandDocument, final String database) {
        if (database == null || commandDocument.containsKey("$db")) {
            return commandDocument;
        }
        final BsonDocument mutable = new BsonDocument();
        for (final String key : commandDocument.keySet()) {
            mutable.put(key, commandDocument.get(key));
        }
        return mutable.append("$db", new BsonString(database));
    }

    private static int readIntLE(final byte[] bytes, final int offset) {
        if (offset < 0 || offset + 4 > bytes.length) {
            throw new IllegalArgumentException("unable to read int32 at offset " + offset);
        }
        return (bytes[offset] & 0xff)
                | ((bytes[offset + 1] & 0xff) << 8)
                | ((bytes[offset + 2] & 0xff) << 16)
                | ((bytes[offset + 3] & 0xff) << 24);
    }

    private static int littleEndianInt(final byte[] bytes) {
        return (bytes[0] & 0xff)
                | ((bytes[1] & 0xff) << 8)
                | ((bytes[2] & 0xff) << 16)
                | ((bytes[3] & 0xff) << 24);
    }

    private static String normalizeDatabase(final String database) {
        if (database == null || database.isBlank()) {
            return "test";
        }
        return database.trim();
    }

    private static String normalizeHost(final String host) {
        if (host == null || host.isBlank()) {
            return DEFAULT_HOST;
        }
        return host.trim();
    }

    private static String normalizeReplicaSetName(final String replicaSetName) {
        if (replicaSetName == null || replicaSetName.isBlank()) {
            return DEFAULT_REPLICA_SET_NAME;
        }
        return replicaSetName.trim();
    }

    private static int normalizePort(final int port) {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port must be between 0 and 65535: " + port);
        }
        return port;
    }

    private static int normalizeMaxAcceptFailures(final int maxConsecutiveAcceptFailures) {
        if (maxConsecutiveAcceptFailures <= 0) {
            throw new IllegalArgumentException("maxConsecutiveAcceptFailures must be > 0");
        }
        return maxConsecutiveAcceptFailures;
    }

    private static long normalizeBackoff(final long backoffMillis, final String fieldName) {
        if (backoffMillis <= 0L) {
            throw new IllegalArgumentException(fieldName + " must be > 0");
        }
        return backoffMillis;
    }
}
