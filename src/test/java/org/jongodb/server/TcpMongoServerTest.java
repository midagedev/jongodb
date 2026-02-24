package org.jongodb.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.jongodb.command.TopologyProfile;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

final class TcpMongoServerTest {
    @Test
    void supportsMongoClientPingAndCrudThroughTcpUri() {
        try (TcpMongoServer server = TcpMongoServer.inMemory()) {
            server.start();

            try (MongoClient client = MongoClients.create(server.connectionString("app"))) {
                final BsonDocument ping = BsonDocument.parse(client.getDatabase("app").runCommand(BsonDocument.parse("{\"ping\":1}")).toJson());
                assertEquals(1.0, ping.getNumber("ok").doubleValue(), 0.0);

                final MongoCollection<org.bson.Document> users = client.getDatabase("app").getCollection("users");
                final ObjectId id = new ObjectId();
                users.insertOne(new org.bson.Document("_id", id).append("name", "alice"));
                final org.bson.Document found = users.find(new org.bson.Document("_id", id)).first();
                assertNotNull(found);
                assertEquals("alice", found.getString("name"));
            }
        }
    }

    @Test
    void replicaSetProfileEmitsReplicaSetUriAndHandshakeFields() {
        try (TcpMongoServer server = TcpMongoServer.inMemoryReplicaSet("127.0.0.1", 0, "rs-test")) {
            server.start();

            assertEquals(TopologyProfile.SINGLE_NODE_REPLICA_SET, server.topologyProfile());
            assertEquals(
                    "mongodb://" + server.host() + ":" + server.port() + "/app?replicaSet=rs-test",
                    server.connectionString("app"));

            try (MongoClient client = MongoClients.create(server.connectionString("app"))) {
                final BsonDocument hello = BsonDocument.parse(client.getDatabase("admin")
                        .runCommand(BsonDocument.parse("{\"hello\":1}"))
                        .toJson());
                assertEquals(1.0, hello.getNumber("ok").doubleValue(), 0.0);
                assertEquals("rs-test", hello.getString("setName").getValue());
                assertEquals(server.host() + ":" + server.port(), hello.getString("primary").getValue());
            }
        }
    }

    @Test
    void supportsLegacyOpQueryCommandPath() throws IOException {
        try (TcpMongoServer server = TcpMongoServer.inMemory()) {
            server.start();

            try (Socket socket = new Socket(server.host(), server.port());
                    BufferedOutputStream output = new BufferedOutputStream(socket.getOutputStream());
                    BufferedInputStream input = new BufferedInputStream(socket.getInputStream())) {
                final byte[] request = encodeOpQueryCommand(42, "admin.$cmd", BsonDocument.parse("{\"ping\":1,\"$db\":\"test\"}"));
                output.write(request);
                output.flush();

                final byte[] response = readMessage(input);
                assertNotNull(response);
                assertEquals(1, readIntLE(response, 12)); // OP_REPLY
                final BsonDocument responseDoc = decodeReplyDocument(response);
                assertEquals(1.0, responseDoc.getNumber("ok").doubleValue(), 0.0);
            }
        }
    }

    @Test
    void opQueryUsesNamespaceDatabaseWhenPayloadDbIsMissing() throws IOException {
        try (TcpMongoServer server = TcpMongoServer.inMemory()) {
            server.start();

            try (MongoClient client = MongoClients.create(server.connectionString("app"))) {
                client.getDatabase("app").runCommand(BsonDocument.parse(
                        "{\"insert\":\"users\",\"documents\":[{\"_id\":1,\"name\":\"alice\"}]}"));
            }

            try (Socket socket = new Socket(server.host(), server.port());
                    BufferedOutputStream output = new BufferedOutputStream(socket.getOutputStream());
                    BufferedInputStream input = new BufferedInputStream(socket.getInputStream())) {
                final byte[] request = encodeOpQueryCommand(43, "app.$cmd", BsonDocument.parse("{\"count\":\"users\",\"query\":{}}"));
                output.write(request);
                output.flush();

                final byte[] response = readMessage(input);
                assertNotNull(response);
                final BsonDocument responseDoc = decodeReplyDocument(response);
                assertEquals(1.0, responseDoc.getNumber("ok").doubleValue(), 0.0);
                assertEquals(1L, responseDoc.getInt64("n").getValue());
                assertEquals(1L, responseDoc.getInt64("count").getValue());
            }
        }
    }

    @Test
    void opQueryPayloadDbOverridesNamespaceDatabase() throws IOException {
        try (TcpMongoServer server = TcpMongoServer.inMemory()) {
            server.start();

            try (MongoClient client = MongoClients.create(server.connectionString("app"))) {
                client.getDatabase("app").runCommand(BsonDocument.parse(
                        "{\"insert\":\"users\",\"documents\":[{\"_id\":1,\"name\":\"alice\"}]}"));
            }

            try (Socket socket = new Socket(server.host(), server.port());
                    BufferedOutputStream output = new BufferedOutputStream(socket.getOutputStream());
                    BufferedInputStream input = new BufferedInputStream(socket.getInputStream())) {
                final byte[] request = encodeOpQueryCommand(
                        44,
                        "admin.$cmd",
                        BsonDocument.parse("{\"count\":\"users\",\"$db\":\"app\",\"query\":{}}"));
                output.write(request);
                output.flush();

                final byte[] response = readMessage(input);
                assertNotNull(response);
                final BsonDocument responseDoc = decodeReplyDocument(response);
                assertEquals(1.0, responseDoc.getNumber("ok").doubleValue(), 0.0);
                assertEquals(1L, responseDoc.getInt64("n").getValue());
            }
        }
    }

    private static byte[] encodeOpQueryCommand(final int requestId, final String namespace, final BsonDocument command) {
        final byte[] namespaceBytes = namespace.getBytes(StandardCharsets.UTF_8);
        final byte[] commandBytes = encodeBson(command);
        final int messageLength = 16 + 4 + namespaceBytes.length + 1 + 4 + 4 + commandBytes.length;

        return ByteBuffer.allocate(messageLength)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(messageLength)
                .putInt(requestId)
                .putInt(0)
                .putInt(2004) // OP_QUERY
                .putInt(0) // flags
                .put(namespaceBytes)
                .put((byte) 0)
                .putInt(0) // numberToSkip
                .putInt(1) // numberToReturn
                .put(commandBytes)
                .array();
    }

    private static byte[] readMessage(final BufferedInputStream input) throws IOException {
        final byte[] sizeBytes = new byte[4];
        if (!readFully(input, sizeBytes, 0, 4)) {
            return null;
        }
        final int size = readIntLE(sizeBytes, 0);
        final byte[] message = new byte[size];
        System.arraycopy(sizeBytes, 0, message, 0, 4);
        if (!readFully(input, message, 4, size - 4)) {
            throw new IOException("incomplete response");
        }
        return message;
    }

    private static BsonDocument decodeReplyDocument(final byte[] message) {
        final int documentOffset = 16 + 20;
        final int documentLength = readIntLE(message, documentOffset);
        final byte[] document = java.util.Arrays.copyOfRange(message, documentOffset, documentOffset + documentLength);
        return new RawBsonDocument(document);
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

    private static boolean readFully(final BufferedInputStream input, final byte[] target, final int offset, final int length)
            throws IOException {
        int readTotal = 0;
        while (readTotal < length) {
            final int read = input.read(target, offset + readTotal, length - readTotal);
            if (read == -1) {
                return false;
            }
            readTotal += read;
        }
        return true;
    }

    private static int readIntLE(final byte[] bytes, final int offset) {
        return (bytes[offset] & 0xff)
                | ((bytes[offset + 1] & 0xff) << 8)
                | ((bytes[offset + 2] & 0xff) << 16)
                | ((bytes[offset + 3] & 0xff) << 24);
    }
}
