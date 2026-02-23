package org.jongodb.wire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.bson.BsonDocument;
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
        encoded[20] = 1; // First section kind byte: 16-byte header + 4-byte flags.

        assertThrows(UnsupportedOperationException.class, () -> codec.decode(encoded));
    }
}
