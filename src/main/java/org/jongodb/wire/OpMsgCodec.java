package org.jongodb.wire;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

public final class OpMsgCodec {
    private static final int HEADER_LENGTH = 16;
    private static final int FLAG_BITS_LENGTH = 4;
    private static final byte BODY_SECTION_KIND = 0;

    public byte[] encode(final OpMsg message) {
        if (!message.sections().isEmpty()) {
            throw new UnsupportedOperationException("Non-body OP_MSG sections are not encoded by this skeleton codec.");
        }

        final byte[] bodyBytes = encodeDocument(message.body());
        final int messageLength = HEADER_LENGTH + FLAG_BITS_LENGTH + 1 + bodyBytes.length;

        final ByteBuffer buffer = ByteBuffer.allocate(messageLength).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(messageLength);
        buffer.putInt(message.requestId());
        buffer.putInt(message.responseTo());
        buffer.putInt(OpMsg.OP_CODE);
        buffer.putInt(message.flagBits());
        buffer.put(BODY_SECTION_KIND);
        buffer.put(bodyBytes);
        return buffer.array();
    }

    public OpMsg decode(final byte[] messageBytes) {
        if (messageBytes == null || messageBytes.length < HEADER_LENGTH + FLAG_BITS_LENGTH + 1 + 5) {
            throw new IllegalArgumentException("OP_MSG bytes are too short.");
        }

        final ByteBuffer buffer = ByteBuffer.wrap(messageBytes).order(ByteOrder.LITTLE_ENDIAN);
        final int messageLength = buffer.getInt();
        if (messageLength != messageBytes.length) {
            throw new IllegalArgumentException("messageLength does not match the provided byte array length.");
        }

        final int requestId = buffer.getInt();
        final int responseTo = buffer.getInt();
        final int opCode = buffer.getInt();
        if (opCode != OpMsg.OP_CODE) {
            throw new IllegalArgumentException("Unsupported opCode: " + opCode);
        }

        final int flagBits = buffer.getInt();
        final byte sectionKind = buffer.get();
        if (sectionKind != BODY_SECTION_KIND) {
            throw new UnsupportedOperationException("Only kind 0 body sections are currently supported.");
        }

        final byte[] bodyBytes = readCurrentDocumentBytes(buffer);
        if (buffer.hasRemaining()) {
            // A full OP_MSG can include additional sections/checksum; this skeleton intentionally limits scope.
            throw new UnsupportedOperationException("Additional OP_MSG sections/checksum are not yet supported.");
        }

        final BsonDocument body = new RawBsonDocument(bodyBytes);
        return new OpMsg(requestId, responseTo, flagBits, body);
    }

    private static byte[] readCurrentDocumentBytes(final ByteBuffer buffer) {
        if (buffer.remaining() < Integer.BYTES) {
            throw new IllegalArgumentException("Missing BSON body length.");
        }

        final int start = buffer.position();
        final int documentLength = buffer.getInt(start);
        if (documentLength < 5) {
            throw new IllegalArgumentException("Invalid BSON body length: " + documentLength);
        }
        if (documentLength > buffer.remaining()) {
            throw new IllegalArgumentException("Declared BSON body length exceeds available bytes.");
        }

        final byte[] documentBytes = new byte[documentLength];
        buffer.get(documentBytes);
        return documentBytes;
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
