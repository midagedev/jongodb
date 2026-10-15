package org.jongodb.command;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;

/**
 * {@code explain} for find, aggregate, count, distinct, update, delete and findAndModify.
 *
 * <p>The engine has no query planner or index scans, so every plan is an honest {@code COLLSCAN}. Read commands are
 * executed for {@code executionStats}; write commands are never executed, they only count the documents they would hit.
 */
public final class ExplainCommandHandler implements CommandHandler {
    private static final Set<String> VERBOSITIES = Set.of("queryPlanner", "executionStats", "allPlansExecution");
    private static final Set<String> WRITE_STAGES = Set.of("$out", "$merge");

    private final CommandStore store;
    private final String address;

    public ExplainCommandHandler(final CommandStore store, final String address) {
        this.store = store;
        this.address = address;
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        try {
            return explain(command);
        } catch (final IllegalArgumentException exception) {
            return CommandExceptionMapper.fromIllegalArgument(exception);
        }
    }

    private BsonDocument explain(final BsonDocument command) {
        final BsonValue explained = command.get("explain");
        if (explained == null || !explained.isDocument() || explained.asDocument().isEmpty()) {
            return CommandErrors.typeMismatch("explain must be a command document");
        }
        final BsonValue verbosityValue = command.get("verbosity");
        final String verbosity = verbosityValue == null ? "allPlansExecution" : AdminCommandSupport.string(command, "verbosity");
        if (verbosity == null || !VERBOSITIES.contains(verbosity)) {
            return CommandErrors.badValue("verbosity string must be one of {'queryPlanner', 'executionStats', 'allPlansExecution'}");
        }

        final BsonDocument inner = explained.asDocument();
        final String name = inner.getFirstKey();
        final String database = inner.containsKey("$db") ? CommandCanonicalizer.readDatabase(inner)
                : CommandCanonicalizer.readDatabase(command);
        final BsonValue target = inner.get(name);
        if (target == null || !target.isString()) {
            return CommandErrors.typeMismatch(name + " must be a string");
        }
        final String collection = target.asString().getValue();

        final Plan plan;
        switch (name.toLowerCase(Locale.ROOT)) {
            case "find" -> plan = findPlan(database, collection, inner);
            case "count" -> plan = countPlan(database, collection, inner);
            case "distinct" -> plan = readPlan(database, collection, document(inner, "query"), null);
            case "aggregate" -> plan = aggregatePlan(database, collection, inner);
            case "update" -> plan = writePlan("UPDATE", database, collection, updateFilters(inner));
            case "delete" -> plan = writePlan("DELETE", database, collection, deleteFilters(inner));
            case "findandmodify" -> plan = writePlan(
                    inner.containsKey("remove") && inner.get("remove").isBoolean()
                                    && inner.get("remove").asBoolean().getValue()
                            ? "DELETE"
                            : "UPDATE",
                    database,
                    collection,
                    List.of(document(inner, "query")));
            default -> {
                return CommandErrors.commandNotFound(name.toLowerCase(Locale.ROOT));
            }
        }
        if (plan.error() != null) {
            return plan.error();
        }

        final BsonDocument collscan = new BsonDocument()
                .append("stage", new BsonString("COLLSCAN"))
                .append("direction", new BsonString("forward"));
        if (!plan.filter().isEmpty()) {
            collscan.append("filter", plan.filter());
        }
        final BsonDocument winningPlan = plan.rootStage() == null
                ? collscan
                : plan.rootStage().clone().append("inputStage", collscan);

        final BsonDocument queryPlanner = new BsonDocument()
                .append("namespace", new BsonString(database + "." + collection))
                .append("indexFilterSet", BsonBoolean.FALSE)
                .append("parsedQuery", plan.filter())
                .append("winningPlan", winningPlan)
                .append("rejectedPlans", new BsonArray());

        final BsonDocument response = new BsonDocument()
                .append("explainVersion", new BsonString("1"))
                .append("queryPlanner", queryPlanner);
        if (!"queryPlanner".equals(verbosity)) {
            final BsonDocument stages = winningPlan.clone();
            stages.append("nReturned", count(plan.returned())).append("docsExamined", count(plan.examined()));
            final String rootStage = plan.rootStage() == null ? "" : plan.rootStage().getString("stage").getValue();
            if (rootStage.equals("DELETE") || rootStage.equals("UPDATE")) {
                stages.append(rootStage.equals("DELETE") ? "nWouldDelete" : "nWouldModify", count(plan.matched()));
            }
            response.append("executionStats", new BsonDocument()
                    .append("executionSuccess", BsonBoolean.TRUE)
                    .append("nReturned", count(plan.returned()))
                    .append("executionTimeMillis", new BsonInt32(0))
                    .append("totalKeysExamined", new BsonInt32(0))
                    .append("totalDocsExamined", count(plan.examined()))
                    .append("executionStages", stages));
        }
        // the wire layer hands over immutable raw documents, so the echoed command is rebuilt rather than cloned
        final BsonDocument echoed = new BsonDocument();
        inner.forEach(echoed::put);
        response.append("command", echoed.append("$db", new BsonString(database)))
                .append("serverInfo", new BsonDocument()
                        .append("host", new BsonString(address))
                        .append("version", new BsonString(BuildInfoCommandHandler.VERSION))
                        .append("gitVersion", new BsonString("jongodb")))
                .append("serverParameters", new BsonDocument());
        return AdminCommandSupport.ok(response);
    }

    private Plan findPlan(final String database, final String collection, final BsonDocument inner) {
        final BsonDocument sort = inner.containsKey("sort") && inner.get("sort").isDocument() ? inner.getDocument("sort") : null;
        final Plan base = readPlan(database, collection, document(inner, "filter"), sort);
        long returned = Math.max(0, base.returned() - longValue(inner.get("skip")));
        final long limit = Math.abs(longValue(inner.get("limit")));
        if (limit > 0) {
            returned = Math.min(returned, limit);
        }
        return new Plan(base.filter(), base.rootStage(), returned, base.examined(), 0, null);
    }

    private Plan countPlan(final String database, final String collection, final BsonDocument inner) {
        final Plan base = readPlan(database, collection, document(inner, "query"), null);
        long returned = Math.max(0, base.returned() - longValue(inner.get("skip")));
        final long limit = Math.abs(longValue(inner.get("limit")));
        if (limit > 0) {
            returned = Math.min(returned, limit);
        }
        return new Plan(base.filter(), new BsonDocument("stage", new BsonString("COUNT")), returned, base.examined(), 0, null);
    }

    private Plan aggregatePlan(final String database, final String collection, final BsonDocument inner) {
        final BsonValue pipelineValue = inner.get("pipeline");
        if (pipelineValue == null || !pipelineValue.isArray()) {
            return Plan.error(CommandErrors.typeMismatch("pipeline must be an array"));
        }
        final List<BsonDocument> pipeline = new ArrayList<>();
        BsonDocument filter = new BsonDocument();
        boolean mutates = false;
        for (final BsonValue stage : pipelineValue.asArray()) {
            if (!stage.isDocument()) {
                return Plan.error(CommandErrors.typeMismatch("all entries in pipeline must be BSON documents"));
            }
            pipeline.add(stage.asDocument());
            mutates |= stage.asDocument().keySet().stream().anyMatch(WRITE_STAGES::contains);
        }
        if (!pipeline.isEmpty() && pipeline.get(0).containsKey("$match") && pipeline.get(0).get("$match").isDocument()) {
            filter = pipeline.get(0).getDocument("$match");
        }
        final long examined = store.find(database, collection, new BsonDocument()).size();
        if (mutates) {
            return new Plan(filter, null, 0, examined, 0, null);
        }
        try {
            return new Plan(filter, null, store.aggregate(database, collection, pipeline).size(), examined, 0, null);
        } catch (final IllegalArgumentException exception) {
            return Plan.error(CommandExceptionMapper.fromIllegalArgument(exception));
        }
    }

    private Plan readPlan(final String database, final String collection, final BsonDocument filter, final BsonDocument sort) {
        final long examined = store.find(database, collection, new BsonDocument()).size();
        final long returned = store.find(database, collection, filter).size();
        final BsonDocument root = sort == null || sort.isEmpty()
                ? null
                : new BsonDocument("stage", new BsonString("SORT")).append("sortPattern", sort);
        return new Plan(filter, root, returned, examined, 0, null);
    }

    private Plan writePlan(final String stage, final String database, final String collection, final List<BsonDocument> filters) {
        long matched = 0;
        for (final BsonDocument filter : filters) {
            matched += store.find(database, collection, filter).size();
        }
        final long examined = store.find(database, collection, new BsonDocument()).size();
        final BsonDocument filter = filters.size() == 1 ? filters.get(0) : new BsonDocument();
        return new Plan(filter, new BsonDocument("stage", new BsonString(stage)), 0, examined, matched, null);
    }

    private static List<BsonDocument> updateFilters(final BsonDocument inner) {
        return statementFilters(inner, "updates");
    }

    private static List<BsonDocument> deleteFilters(final BsonDocument inner) {
        return statementFilters(inner, "deletes");
    }

    private static List<BsonDocument> statementFilters(final BsonDocument inner, final String key) {
        final List<BsonDocument> filters = new ArrayList<>();
        final BsonValue statements = inner.get(key);
        if (statements != null && statements.isArray()) {
            for (final BsonValue statement : statements.asArray()) {
                if (statement.isDocument()) {
                    filters.add(document(statement.asDocument(), "q"));
                }
            }
        }
        return filters;
    }

    private static BsonDocument document(final BsonDocument source, final String key) {
        final BsonValue value = source.get(key);
        return value != null && value.isDocument() ? value.asDocument() : new BsonDocument();
    }

    private static BsonInt32 count(final long value) {
        return new BsonInt32((int) Math.min(value, Integer.MAX_VALUE));
    }

    private static long longValue(final BsonValue value) {
        return value != null && value.isNumber() ? value.asNumber().longValue() : 0;
    }

    private record Plan(
            BsonDocument filter, BsonDocument rootStage, long returned, long examined, long matched, BsonDocument error) {
        static Plan error(final BsonDocument error) {
            return new Plan(null, null, 0, 0, 0, error);
        }
    }
}
