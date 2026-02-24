package org.jongodb.wire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.junit.jupiter.api.Test;

class OpMsgCodecTest {
    private final OpMsgCodec codec = new OpMsgCodec();

    @Test
    void roundTripsBodyDocument() {
        final BsonDocument body = BsonDocument.parse("{\"ping\": 1, \"$db\": \"admin\"}");
        final OpMsg original = new OpMsg(42, 0, 0, body);

        final byte[] encoded = codec.encode(original);
        final OpMsg decoded = codec.decode(encoded);

        assertEquals(42, decoded.requestId());
        assertEquals(0, decoded.responseTo());
        assertEquals(0, decoded.flagBits());
        assertEquals(body, decoded.body());
        assertEquals(0, decoded.sections().size());
    }

    @Test
    void rejectsUnsupportedSectionKind() {
        final BsonDocument body = BsonDocument.parse("{\"ping\": 1, \"$db\": \"admin\"}");
        final OpMsg original = new OpMsg(10, 0, 0, body);

        final byte[] encoded = codec.encode(original);
        encoded[20] = 2; // First section kind byte: 16-byte header + 4-byte flags.

        assertThrows(UnsupportedOperationException.class, () -> codec.decode(encoded));
    }

    @Test
    void decodesDocumentSequenceAndMergesIntoCommandBody() {
        final BsonDocument body = BsonDocument.parse("{\"insert\": \"tokens\", \"ordered\": true, \"$db\": \"account\"}");
        final BsonDocument first = BsonDocument.parse("{\"_id\": 1, \"value\": \"a\"}");
        final BsonDocument second = BsonDocument.parse("{\"_id\": 2, \"value\": \"b\"}");

        final byte[] message = encodeOpMsgWithDocumentSequence(99, 0, body, "documents", first, second);
        final OpMsg decoded = codec.decode(message);

        assertEquals("tokens", decoded.body().getString("insert").getValue());
        assertEquals(2, decoded.body().getArray("documents").size());
        assertEquals(first, decoded.body().getArray("documents").get(0).asDocument());
        assertEquals(second, decoded.body().getArray("documents").get(1).asDocument());
        assertEquals(1, decoded.sections().size());
        assertEquals(OpMsgDocumentSequenceSection.KIND, decoded.sections().get(0).kind());
    }

    @Test
    void decodesChecksumWhenFlagIsPresent() {
        final BsonDocument body = BsonDocument.parse("{\"ping\": 1, \"$db\": \"admin\"}");
        final byte[] bodyBytes = encodeDocument(body);
        final int messageLength = 16 + 4 + 1 + bodyBytes.length + 4;

        final byte[] message = ByteBuffer.allocate(messageLength)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(messageLength)
                .putInt(7)
                .putInt(0)
                .putInt(OpMsg.OP_CODE)
                .putInt(1) // checksumPresent
                .put((byte) 0)
                .put(bodyBytes)
                .putInt(0x12345678)
                .array();

        final OpMsg decoded = codec.decode(message);
        assertEquals(1, decoded.flagBits());
        assertEquals(body, decoded.body());
    }

    private static byte[] encodeOpMsgWithDocumentSequence(
            final int requestId,
            final int responseTo,
            final BsonDocument body,
            final String identifier,
            final BsonDocument... documents) {
        final byte[] bodyBytes = encodeDocument(body);
        final byte[] identifierBytes = identifier.getBytes(StandardCharsets.UTF_8);

        int documentsLength = 0;
        final byte[][] encodedDocuments = new byte[documents.length][];
        for (int index = 0; index < documents.length; index++) {
            encodedDocuments[index] = encodeDocument(documents[index]);
            documentsLength += encodedDocuments[index].length;
        }

        final int sequencePayloadSize = 4 + identifierBytes.length + 1 + documentsLength;
        final int messageLength = 16 + 4 + 1 + bodyBytes.length + 1 + sequencePayloadSize;

        final ByteBuffer buffer = ByteBuffer.allocate(messageLength).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(messageLength);
        buffer.putInt(requestId);
        buffer.putInt(responseTo);
        buffer.putInt(OpMsg.OP_CODE);
        buffer.putInt(0);
        buffer.put((byte) 0);
        buffer.put(bodyBytes);
        buffer.put((byte) OpMsgDocumentSequenceSection.KIND);
        buffer.putInt(sequencePayloadSize);
        buffer.put(identifierBytes);
        buffer.put((byte) 0);
        for (final byte[] encodedDocument : encodedDocuments) {
            buffer.put(encodedDocument);
        }
        return buffer.array();
    }

    private static byte[] encodeDocument(final BsonDocument document) {
        final BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        try (BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer)) {
            new BsonDocumentCodec()
                    .encode(writer, document, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
            return outputBuffer.toByteArray();
        }
    }
}
