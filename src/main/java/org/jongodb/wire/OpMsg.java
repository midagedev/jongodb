package org.jongodb.wire;

import java.util.List;
import org.bson.BsonDocument;

public final class OpMsg {
    public static final int OP_CODE = 2013;

    private final int requestId;
    private final int responseTo;
    private final int flagBits;
    private final BsonDocument body;
    private final List<OpMsgSection> sections;

    public OpMsg(final int requestId, final int responseTo, final int flagBits, final BsonDocument body) {
        this(requestId, responseTo, flagBits, body, List.of());
    }

    public OpMsg(
            final int requestId,
            final int responseTo,
            final int flagBits,
            final BsonDocument body,
            final List<OpMsgSection> sections) {
        if (body == null) {
            throw new IllegalArgumentException("body must not be null");
        }
        this.requestId = requestId;
        this.responseTo = responseTo;
        this.flagBits = flagBits;
        this.body = body;
        this.sections = List.copyOf(sections);
    }

    public int requestId() {
        return requestId;
    }

    public int responseTo() {
        return responseTo;
    }

    public int flagBits() {
        return flagBits;
    }

    public BsonDocument body() {
        return body;
    }

    public List<OpMsgSection> sections() {
        return sections;
    }
}
