package org.jongodb.command;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;

public final class GetParameterCommandHandler implements CommandHandler {
    private static final Set<String> METADATA_FIELDS =
            Set.of(
                    "$db",
                    "lsid",
                    "$clusterTime",
                    "$readPreference",
                    "comment",
                    "apiVersion",
                    "apiStrict",
                    "apiDeprecationErrors");

    private static final Map<String, Supplier<BsonValue>> SUPPORTED_PARAMETERS = buildSupportedParameters();

    @Override
    public BsonDocument handle(final BsonDocument command) {
        final BsonValue selector = command.get("getParameter");
        if (selector == null) {
            return CommandErrors.badValue("getParameter must be specified");
        }

        final boolean requestAll;
        if (selector.isString()) {
            if (!"*".equals(selector.asString().getValue())) {
                return CommandErrors.badValue("getParameter must be 1, true, or \"*\"");
            }
            requestAll = true;
        } else if (selector.isBoolean()) {
            if (!selector.asBoolean().getValue()) {
                return CommandErrors.badValue("getParameter must be 1, true, or \"*\"");
            }
            requestAll = false;
        } else {
            final Long numericSelector = readIntegralLong(selector);
            if (numericSelector == null) {
                return CommandErrors.typeMismatch("getParameter must be 1, true, or \"*\"");
            }
            if (numericSelector != 1L) {
                return CommandErrors.badValue("getParameter must be 1, true, or \"*\"");
            }
            requestAll = false;
        }

        final List<String> requestedParameterNames = collectRequestedParameters(command, requestAll);
        if (requestedParameterNames.isEmpty()) {
            return CommandErrors.badValue("getParameter requires at least one parameter name");
        }

        final BsonDocument response = new BsonDocument();
        for (final String parameterName : requestedParameterNames) {
            final Supplier<BsonValue> supplier = SUPPORTED_PARAMETERS.get(parameterName);
            if (supplier == null) {
                return CommandErrors.badValue("no option found to get: " + parameterName);
            }
            response.append(parameterName, supplier.get());
        }
        response.append("ok", new BsonDouble(1.0));
        return response;
    }

    private static List<String> collectRequestedParameters(final BsonDocument command, final boolean requestAll) {
        if (requestAll) {
            return List.copyOf(SUPPORTED_PARAMETERS.keySet());
        }

        final List<String> requestedParameterNames = new ArrayList<>();
        for (final Map.Entry<String, BsonValue> entry : command.entrySet()) {
            final String key = entry.getKey();
            if ("getParameter".equals(key) || METADATA_FIELDS.contains(key) || key.startsWith("$")) {
                continue;
            }
            if (!isSelector(entry.getValue())) {
                continue;
            }
            requestedParameterNames.add(key);
        }
        return requestedParameterNames;
    }

    private static boolean isSelector(final BsonValue value) {
        if (value.isBoolean()) {
            return value.asBoolean().getValue();
        }
        final Long parsedNumeric = readIntegralLong(value);
        return parsedNumeric != null && parsedNumeric == 1L;
    }

    private static Long readIntegralLong(final BsonValue value) {
        if (value.isInt32()) {
            return (long) value.asInt32().getValue();
        }
        if (value.isInt64()) {
            return value.asInt64().getValue();
        }
        if (value.isDouble()) {
            final double doubleValue = value.asDouble().getValue();
            if (!Double.isFinite(doubleValue) || Math.rint(doubleValue) != doubleValue) {
                return null;
            }
            if (doubleValue < Long.MIN_VALUE || doubleValue > Long.MAX_VALUE) {
                return null;
            }
            return (long) doubleValue;
        }
        return null;
    }

    private static Map<String, Supplier<BsonValue>> buildSupportedParameters() {
        final Map<String, Supplier<BsonValue>> parameters = new LinkedHashMap<>();
        parameters.put(
                "featureCompatibilityVersion",
                () -> new BsonDocument().append("version", new BsonString("8.0")));
        parameters.put("transactionLifetimeLimitSeconds", () -> new BsonInt32(60));
        parameters.put("internalQueryMaxBlockingSortMemoryUsageBytes", () -> new BsonInt64(104_857_600L));
        return Map.copyOf(parameters);
    }
}
