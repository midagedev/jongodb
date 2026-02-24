package org.jongodb.command;

import java.util.Objects;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.types.ObjectId;

public final class HelloCommandHandler implements CommandHandler {
    private final TopologyProfile topologyProfile;
    private final String primaryAddress;
    private final String replicaSetName;
    private final BsonDocument topologyVersion;

    public HelloCommandHandler() {
        this(TopologyProfile.STANDALONE, "127.0.0.1:27017", "jongodb-rs0");
    }

    public HelloCommandHandler(
            final TopologyProfile topologyProfile,
            final String primaryAddress,
            final String replicaSetName) {
        this.topologyProfile = Objects.requireNonNull(topologyProfile, "topologyProfile");
        this.primaryAddress = requireText(primaryAddress, "primaryAddress");
        this.replicaSetName = requireText(replicaSetName, "replicaSetName");
        this.topologyVersion = new BsonDocument()
                .append("processId", new BsonObjectId(new ObjectId()))
                .append("counter", new BsonInt64(1L));
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final BsonDocument response = new BsonDocument()
                .append("isWritablePrimary", BsonBoolean.TRUE)
                .append("ismaster", BsonBoolean.TRUE)
                .append("helloOk", BsonBoolean.TRUE)
                .append("minWireVersion", new BsonInt32(0))
                .append("maxWireVersion", new BsonInt32(17))
                .append("maxBsonObjectSize", new BsonInt32(16 * 1024 * 1024))
                .append("maxMessageSizeBytes", new BsonInt32(48_000_000))
                .append("maxWriteBatchSize", new BsonInt32(100_000))
                .append("logicalSessionTimeoutMinutes", new BsonInt32(30))
                .append("connectionId", new BsonInt32(1))
                .append("ok", new BsonDouble(1.0));
        if (topologyProfile.replicaSetSemanticsEnabled()) {
            response.append("setName", new BsonString(replicaSetName));
            response.append("primary", new BsonString(primaryAddress));
            response.append("me", new BsonString(primaryAddress));
            response.append("hosts", new BsonArray(java.util.List.of(new BsonString(primaryAddress))));
            response.append("secondary", BsonBoolean.FALSE);
            response.append("arbiterOnly", BsonBoolean.FALSE);
            response.append("topologyVersion", topologyVersion.clone());
        }
        return response;
    }

    private static String requireText(final String value, final String fieldName) {
        final String normalized = value == null ? null : value.trim();
        if (normalized == null || normalized.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return normalized;
    }
}
