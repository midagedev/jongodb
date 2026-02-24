package org.jongodb.wire;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.bson.BsonBinaryWriter;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

public final class OpMsgCodec {
    private static final int HEADER_LENGTH = 16;
    private static final int FLAG_BITS_LENGTH = 4;
    private static final int CHECKSUM_LENGTH = 4;
    private static final byte BODY_SECTION_KIND = 0;
    private static final byte DOCUMENT_SEQUENCE_SECTION_KIND = 1;
    private static final int CHECKSUM_PRESENT_FLAG = 1;

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
        final int checksumLength = (flagBits & CHECKSUM_PRESENT_FLAG) == CHECKSUM_PRESENT_FLAG ? CHECKSUM_LENGTH : 0;
        final int payloadLimit = messageLength - checksumLength;
        if (payloadLimit <= HEADER_LENGTH + FLAG_BITS_LENGTH) {
            throw new IllegalArgumentException("OP_MSG payload is empty.");
        }
        buffer.limit(payloadLimit);

        BsonDocument body = null;
        final List<OpMsgSection> sections = new ArrayList<>();
        while (buffer.position() < payloadLimit) {
            final byte sectionKind = buffer.get();
            if (sectionKind == BODY_SECTION_KIND) {
                if (body != null) {
                    throw new UnsupportedOperationException("Multiple OP_MSG body sections are not supported.");
                }
                body = new RawBsonDocument(readCurrentDocumentBytes(buffer, payloadLimit));
                continue;
            }

            if (sectionKind == DOCUMENT_SEQUENCE_SECTION_KIND) {
                sections.add(readDocumentSequenceSection(buffer, payloadLimit));
                continue;
            }
            throw new UnsupportedOperationException("Unsupported OP_MSG section kind: " + sectionKind);
        }

        if (body == null) {
            throw new UnsupportedOperationException("OP_MSG body section (kind 0) is required.");
        }

        final BsonDocument mergedBody = new BsonDocument();
        for (final String key : body.keySet()) {
            mergedBody.put(key, body.get(key));
        }
        for (final OpMsgSection section : sections) {
            final OpMsgDocumentSequenceSection documentSequenceSection = (OpMsgDocumentSequenceSection) section;
            mergeDocumentSequenceField(
                    mergedBody, documentSequenceSection.identifier(), documentSequenceSection.documents());
        }

        // Touch checksum bytes for structural validation when checksumPresent is set.
        if (checksumLength == CHECKSUM_LENGTH) {
            try {
                ByteBuffer.wrap(messageBytes)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .position(payloadLimit)
                        .getInt();
            } catch (final BufferUnderflowException e) {
                throw new IllegalArgumentException("Missing OP_MSG checksum bytes.", e);
            }
        }

        return new OpMsg(requestId, responseTo, flagBits, mergedBody, sections);
    }

    private static OpMsgDocumentSequenceSection readDocumentSequenceSection(
            final ByteBuffer buffer, final int payloadLimit) {
        if (buffer.position() + Integer.BYTES > payloadLimit) {
            throw new IllegalArgumentException("Missing OP_MSG document sequence size.");
        }

        final int sectionStart = buffer.position();
        final int sectionSize = buffer.getInt();
        if (sectionSize < Integer.BYTES + 1) {
            throw new IllegalArgumentException("Invalid OP_MSG document sequence size: " + sectionSize);
        }

        final int sectionEnd = sectionStart + sectionSize;
        if (sectionEnd > payloadLimit) {
            throw new IllegalArgumentException("OP_MSG document sequence exceeds payload boundary.");
        }

        final String identifier = readCString(buffer, sectionEnd);
        final List<BsonDocument> documents = new ArrayList<>();
        while (buffer.position() < sectionEnd) {
            documents.add(new RawBsonDocument(readCurrentDocumentBytes(buffer, sectionEnd)));
        }
        return new OpMsgDocumentSequenceSection(identifier, documents);
    }

    private static String readCString(final ByteBuffer buffer, final int limitExclusive) {
        final int start = buffer.position();
        int cursor = start;
        while (cursor < limitExclusive && buffer.get(cursor) != 0) {
            cursor++;
        }
        if (cursor >= limitExclusive) {
            throw new IllegalArgumentException("Unterminated C-string in OP_MSG section.");
        }

        final byte[] identifierBytes = new byte[cursor - start];
        for (int index = 0; index < identifierBytes.length; index++) {
            identifierBytes[index] = buffer.get(start + index);
        }
        buffer.position(cursor + 1); // Skip terminating zero byte.
        return new String(identifierBytes, StandardCharsets.UTF_8);
    }

    private static void mergeDocumentSequenceField(
            final BsonDocument body, final String identifier, final List<BsonDocument> documents) {
        final BsonValue existing = body.get(identifier);
        final BsonArray merged = new BsonArray();
        if (existing != null) {
            if (!existing.isArray()) {
                throw new UnsupportedOperationException(
                        "Document sequence field '" + identifier + "' conflicts with a non-array command field.");
            }
            for (final BsonValue existingValue : existing.asArray()) {
                merged.add(existingValue);
            }
        }

        for (final BsonDocument document : documents) {
            merged.add(document);
        }
        body.put(identifier, merged);
    }

    private static byte[] readCurrentDocumentBytes(final ByteBuffer buffer, final int limitExclusive) {
        if (buffer.position() + Integer.BYTES > limitExclusive) {
            throw new IllegalArgumentException("Missing BSON body length.");
        }

        final int start = buffer.position();
        final int documentLength = buffer.getInt(start);
        if (documentLength < 5) {
            throw new IllegalArgumentException("Invalid BSON body length: " + documentLength);
        }
        if (start + documentLength > limitExclusive) {
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
