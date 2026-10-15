package org.jongodb.command;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

class UpdatePipelineSubsetExpressionTest {
    @Test
    void updatePipelineAllowsDateAndStringExpressions() {
        final BsonArray pipeline = new BsonArray(List.of(BsonDocument.parse(
                "{\"$set\":{\"name\":{\"$concat\":[\"$name\",\"-deleted\"]},"
                        + "\"at\":\"$$NOW\","
                        + "\"day\":{\"$dateToString\":{\"format\":\"%Y-%m-%d\",\"date\":\"$createdAt\","
                        + "\"timezone\":\"Asia/Seoul\"}}}}")));

        final UpdatePipelineSubset.ParseResult parsed = UpdatePipelineSubset.parse(pipeline);

        assertNull(parsed.error());
        assertNotNull(parsed.updatePipeline());
    }
}
