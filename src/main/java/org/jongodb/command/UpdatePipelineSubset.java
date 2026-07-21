package org.jongodb.command;

import java.util.Set;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/** Validation for the stages MongoDB permits in aggregation-pipeline updates. */
final class UpdatePipelineSubset {
    private static final Set<String> SUPPORTED_STAGES = Set.of(
            "$addFields", "$set", "$project", "$unset", "$replaceRoot", "$replaceWith");

    private UpdatePipelineSubset() {}

    static ParseResult parse(final BsonArray pipeline) {
        if (pipeline == null || pipeline.isEmpty()) {
            return ParseResult.error(CommandErrors.badValue("update pipeline must not be empty"));
        }

        final BsonArray copied = new BsonArray();
        for (final BsonValue stageValue : pipeline) {
            if (!stageValue.isDocument()) {
                return ParseResult.error(CommandErrors.typeMismatch("update pipeline stages must be documents"));
            }
            final BsonDocument stage = stageValue.asDocument();
            if (stage.size() != 1) {
                return ParseResult.error(
                        CommandErrors.badValue("update pipeline stage must contain exactly one operator"));
            }
            final String stageName = stage.getFirstKey();
            if (!SUPPORTED_STAGES.contains(stageName)) {
                return ParseResult.error(
                        CommandErrors.badValue("unsupported update pipeline stage: " + stageName));
            }
            copied.add(stage.clone());
        }
        return ParseResult.success(copied);
    }

    static final class ParseResult {
        private final BsonArray updatePipeline;
        private final BsonDocument error;

        private ParseResult(final BsonArray updatePipeline, final BsonDocument error) {
            this.updatePipeline = updatePipeline;
            this.error = error;
        }

        static ParseResult success(final BsonArray updatePipeline) {
            return new ParseResult(updatePipeline, null);
        }

        static ParseResult error(final BsonDocument error) {
            return new ParseResult(null, error);
        }

        BsonArray updatePipeline() {
            return updatePipeline == null ? null : updatePipeline.clone();
        }

        BsonDocument error() {
            return error;
        }
    }
}
