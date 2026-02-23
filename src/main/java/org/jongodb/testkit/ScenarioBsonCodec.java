package org.jongodb.testkit;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;

/**
 * Shared command/request/response BSON conversions for scenario execution backends.
 */
final class ScenarioBsonCodec {
    private static final String COLLECTION_FIELD = "collection";
    private static final String COMMAND_VALUE_FIELD = "commandValue";

    private ScenarioBsonCodec() {
    }

    static BsonDocument toCommandDocument(ScenarioCommand command, String defaultDatabase) {
        Objects.requireNonNull(command, "command");
        String commandName = command.commandName();
        Map<String, Object> payload = command.payload();

        String consumedKey = null;
        Object commandValue = null;
        if (payload.containsKey(commandName)) {
            consumedKey = commandName;
            commandValue = payload.get(commandName);
        } else if (payload.containsKey(COLLECTION_FIELD)) {
            consumedKey = COLLECTION_FIELD;
            commandValue = payload.get(COLLECTION_FIELD);
        } else if (payload.containsKey(COMMAND_VALUE_FIELD)) {
            consumedKey = COMMAND_VALUE_FIELD;
            commandValue = payload.get(COMMAND_VALUE_FIELD);
        }
        if (commandValue == null) {
            commandValue = 1;
        }

        BsonDocument document = new BsonDocument();
        document.put(commandName, toBsonValue(commandValue));

        boolean hasDatabase = false;
        for (Map.Entry<String, Object> entry : payload.entrySet()) {
            String key = Objects.requireNonNull(entry.getKey(), "payload key");
            if (key.equals(consumedKey)) {
                continue;
            }
            document.put(key, toBsonValue(entry.getValue()));
            if ("$db".equals(key)) {
                hasDatabase = true;
            }
        }
        if (!hasDatabase) {
            document.put("$db", new BsonString(requireText(defaultDatabase, "defaultDatabase")));
        }
        return document;
    }

    static BsonDocument toRealMongodCommandDocument(ScenarioCommand command, String defaultDatabase) {
        BsonDocument commandDocument = toCommandDocument(command, defaultDatabase);
        commandDocument.remove("$db");
        commandDocument.remove("lsid");
        ensureTxnNumberIsLong(commandDocument);
        return commandDocument;
    }

    static boolean isSuccess(BsonDocument response) {
        Objects.requireNonNull(response, "response");
        BsonValue okValue = response.get("ok");
        if (okValue == null || !okValue.isNumber()) {
            return false;
        }
        if (okValue.asNumber().doubleValue() != 1.0d) {
            return false;
        }
        return !hasWriteErrors(response) && !hasWriteConcernError(response);
    }

    static String formatFailure(String commandName, int commandIndex, BsonDocument response) {
        Objects.requireNonNull(response, "response");
        StringBuilder message = new StringBuilder();
        message.append("command '")
            .append(commandName)
            .append("' failed at index ")
            .append(commandIndex);

        String errorMessage = extractErrorMessage(response);
        if (errorMessage != null) {
            message.append(": ").append(errorMessage);
        } else {
            message.append(": ").append(response.toJson());
        }

        Integer errorCode = extractErrorCode(response);
        String errorCodeName = extractErrorCodeName(response);
        if (errorCode != null || errorCodeName != null) {
            message.append(" (");
            boolean needsComma = false;
            if (errorCode != null) {
                message.append("code=").append(errorCode);
                needsComma = true;
            }
            if (errorCodeName != null) {
                if (needsComma) {
                    message.append(", ");
                }
                message.append("codeName=").append(errorCodeName);
            }
            message.append(')');
        }
        return message.toString();
    }

    static Map<String, Object> toJavaMap(BsonDocument document) {
        Objects.requireNonNull(document, "document");
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<String, BsonValue> entry : document.entrySet()) {
            result.put(entry.getKey(), toJavaValue(entry.getValue()));
        }
        return result;
    }

    private static Integer readInteger(BsonValue value) {
        if (value == null || !value.isNumber()) {
            return null;
        }
        return value.asNumber().intValue();
    }

    private static String readString(BsonDocument document, String key) {
        BsonValue value = document.get(key);
        if (value == null || !value.isString()) {
            return null;
        }
        return value.asString().getValue();
    }

    private static boolean hasWriteErrors(BsonDocument response) {
        BsonDocument writeError = firstWriteError(response);
        return writeError != null;
    }

    private static boolean hasWriteConcernError(BsonDocument response) {
        return writeConcernError(response) != null;
    }

    private static String extractErrorMessage(BsonDocument response) {
        String topLevelMessage = readString(response, "errmsg");
        if (topLevelMessage != null) {
            return topLevelMessage;
        }
        BsonDocument writeError = firstWriteError(response);
        if (writeError != null) {
            String writeErrorMessage = readString(writeError, "errmsg");
            if (writeErrorMessage != null) {
                return writeErrorMessage;
            }
        }
        BsonDocument writeConcernError = writeConcernError(response);
        if (writeConcernError != null) {
            return readString(writeConcernError, "errmsg");
        }
        return null;
    }

    private static Integer extractErrorCode(BsonDocument response) {
        Integer topLevelCode = readInteger(response.get("code"));
        if (topLevelCode != null) {
            return topLevelCode;
        }
        BsonDocument writeError = firstWriteError(response);
        if (writeError != null) {
            Integer writeErrorCode = readInteger(writeError.get("code"));
            if (writeErrorCode != null) {
                return writeErrorCode;
            }
        }
        BsonDocument writeConcernError = writeConcernError(response);
        if (writeConcernError != null) {
            return readInteger(writeConcernError.get("code"));
        }
        return null;
    }

    private static String extractErrorCodeName(BsonDocument response) {
        String topLevelCodeName = readString(response, "codeName");
        if (topLevelCodeName != null) {
            return topLevelCodeName;
        }
        BsonDocument writeError = firstWriteError(response);
        if (writeError != null) {
            String writeErrorCodeName = readString(writeError, "codeName");
            if (writeErrorCodeName != null) {
                return writeErrorCodeName;
            }
        }
        BsonDocument writeConcernError = writeConcernError(response);
        if (writeConcernError != null) {
            return readString(writeConcernError, "codeName");
        }
        return null;
    }

    private static BsonDocument firstWriteError(BsonDocument response) {
        BsonValue writeErrors = response.get("writeErrors");
        if (writeErrors == null || !writeErrors.isArray()) {
            return null;
        }
        BsonArray writeErrorsArray = writeErrors.asArray();
        if (writeErrorsArray.isEmpty()) {
            return null;
        }
        BsonValue first = writeErrorsArray.get(0);
        if (first == null || !first.isDocument()) {
            return null;
        }
        return first.asDocument();
    }

    private static BsonDocument writeConcernError(BsonDocument response) {
        BsonValue writeConcernError = response.get("writeConcernError");
        if (writeConcernError == null || !writeConcernError.isDocument()) {
            return null;
        }
        return writeConcernError.asDocument();
    }

    private static void ensureTxnNumberIsLong(BsonDocument commandDocument) {
        BsonValue txnNumberValue = commandDocument.get("txnNumber");
        if (txnNumberValue == null || !txnNumberValue.isNumber() || txnNumberValue.isInt64()) {
            return;
        }
        commandDocument.put("txnNumber", new BsonInt64(txnNumberValue.asNumber().longValue()));
    }

    private static Object toJavaValue(BsonValue value) {
        if (value == null || value.isNull()) {
            return null;
        }
        if (value.isDocument()) {
            return toJavaMap(value.asDocument());
        }
        if (value.isArray()) {
            List<Object> results = new ArrayList<>();
            for (BsonValue item : value.asArray()) {
                results.add(toJavaValue(item));
            }
            return results;
        }
        if (value.isString()) {
            return value.asString().getValue();
        }
        if (value.isBoolean()) {
            return value.asBoolean().getValue();
        }
        if (value.isInt32()) {
            return value.asInt32().getValue();
        }
        if (value.isInt64()) {
            return value.asInt64().getValue();
        }
        if (value.isDouble()) {
            return value.asDouble().getValue();
        }
        if (value.isDecimal128()) {
            return value.asDecimal128().decimal128Value().bigDecimalValue();
        }
        if (value.isDateTime()) {
            return value.asDateTime().getValue();
        }
        if (value.isObjectId()) {
            return value.asObjectId().getValue().toHexString();
        }
        return value.toString();
    }

    private static BsonValue toBsonValue(Object value) {
        if (value == null) {
            return BsonNull.VALUE;
        }
        if (value instanceof BsonValue bsonValue) {
            return bsonValue;
        }
        if (value instanceof String s) {
            return new BsonString(s);
        }
        if (value instanceof Boolean b) {
            return BsonBoolean.valueOf(b);
        }
        if (value instanceof Integer n) {
            return new BsonInt32(n);
        }
        if (value instanceof Long n) {
            return new BsonInt64(n);
        }
        if (value instanceof Double n) {
            return new BsonDouble(n);
        }
        if (value instanceof Float n) {
            return new BsonDouble(n.doubleValue());
        }
        if (value instanceof Short n) {
            return new BsonInt32(n.intValue());
        }
        if (value instanceof Byte n) {
            return new BsonInt32(n.intValue());
        }
        if (value instanceof BigDecimal decimal) {
            return new org.bson.BsonDecimal128(new org.bson.types.Decimal128(decimal));
        }
        if (value instanceof Map<?, ?> map) {
            return toBsonDocument(map);
        }
        if (value instanceof Collection<?> collection) {
            BsonArray array = new BsonArray();
            for (Object item : collection) {
                array.add(toBsonValue(item));
            }
            return array;
        }
        throw new IllegalArgumentException("unsupported payload value type: " + value.getClass().getName());
    }

    private static BsonDocument toBsonDocument(Map<?, ?> source) {
        BsonDocument document = new BsonDocument();
        for (Map.Entry<?, ?> entry : source.entrySet()) {
            Object key = Objects.requireNonNull(entry.getKey(), "document key");
            document.put(String.valueOf(key), toBsonValue(entry.getValue()));
        }
        return document;
    }

    private static String requireText(String value, String fieldName) {
        String normalized = value == null ? null : value.trim();
        if (normalized == null || normalized.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return normalized;
    }
}
