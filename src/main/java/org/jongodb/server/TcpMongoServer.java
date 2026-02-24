package org.jongodb.server;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.jongodb.command.CommandDispatcher;
import org.jongodb.command.CommandStore;
import org.jongodb.command.EngineBackedCommandStore;
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

    private final CommandDispatcher dispatcher;
    private final OpMsgCodec opMsgCodec = new OpMsgCodec();
    private final AtomicInteger responseRequestId = new AtomicInteger(1);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ServerSocket serverSocket;
    private final List<Socket> clientSockets = new CopyOnWriteArrayList<>();
    private final Thread acceptThread;
    private final String host;

    public static TcpMongoServer inMemory() {
        return new TcpMongoServer();
    }

    public static TcpMongoServer inMemory(final String host, final int port) {
        return new TcpMongoServer(host, port);
    }

    public TcpMongoServer() {
        this(new EngineBackedCommandStore(new InMemoryEngineStore()), DEFAULT_HOST, DEFAULT_PORT);
    }

    public TcpMongoServer(final String host, final int port) {
        this(new EngineBackedCommandStore(new InMemoryEngineStore()), host, port);
    }

    public TcpMongoServer(final CommandStore commandStore) {
        this(commandStore, DEFAULT_HOST, DEFAULT_PORT);
    }

    public TcpMongoServer(final CommandStore commandStore, final String host, final int port) {
        Objects.requireNonNull(commandStore, "commandStore");
        final String normalizedHost = normalizeHost(host);
        final int normalizedPort = normalizePort(port);
        this.dispatcher = new CommandDispatcher(commandStore);
        this.host = normalizedHost;
        this.serverSocket = newServerSocket(normalizedHost, normalizedPort);
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

    public String connectionString(final String database) {
        final String normalizedDatabase = normalizeDatabase(database);
        return "mongodb://" + host + ":" + port() + "/" + normalizedDatabase;
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

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
        while (running.get()) {
            final Socket socket;
            try {
                socket = serverSocket.accept();
            } catch (final IOException ioException) {
                if (running.get()) {
                    throw new IllegalStateException("failed to accept tcp client", ioException);
                }
                return;
            }

            clientSockets.add(socket);
            final Thread worker = new Thread(
                    () -> handleClient(socket),
                    "jongodb-tcp-client-" + socket.getPort());
            worker.setDaemon(true);
            worker.start();
        }
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
        cursor = readCString(request, cursor); // fullCollectionName
        cursor += 4; // numberToSkip
        cursor += 4; // numberToReturn

        final int documentLength = readIntLE(request, cursor);
        if (documentLength <= 0 || cursor + documentLength > request.length) {
            throw new IllegalArgumentException("invalid OP_QUERY document length: " + documentLength);
        }
        final byte[] queryBytes = java.util.Arrays.copyOfRange(request, cursor, cursor + documentLength);
        final BsonDocument queryDocument = new RawBsonDocument(queryBytes);
        final BsonDocument responseBody = dispatcher.dispatch(queryDocument);
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

    private static int normalizePort(final int port) {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port must be between 0 and 65535: " + port);
        }
        return port;
    }
}
