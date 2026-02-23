package org.jongodb.command;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;

public final class CommandDispatcher {
    private static final int CODE_COMMAND_NOT_FOUND = 59;
    private static final int CODE_INVALID_ARGUMENT = 14;

    private final Map<String, CommandHandler> handlers;

    public CommandDispatcher(final CommandStore store) {
        final Map<String, CommandHandler> configuredHandlers = new HashMap<>();
        configuredHandlers.put("hello", new HelloCommandHandler());
        configuredHandlers.put("ping", new PingCommandHandler());
        configuredHandlers.put("insert", new InsertCommandHandler(store));
        configuredHandlers.put("find", new FindCommandHandler(store));
        configuredHandlers.put("update", new UpdateCommandHandler(store));
        configuredHandlers.put("delete", new DeleteCommandHandler(store));
        this.handlers = Map.copyOf(configuredHandlers);
    }

    public BsonDocument dispatch(final BsonDocument command) {
        if (command == null || command.isEmpty()) {
            return error("command document must not be empty", CODE_INVALID_ARGUMENT, "BadValue");
        }

        final String commandName = command.getFirstKey().toLowerCase(Locale.ROOT);
        final CommandHandler handler = handlers.get(commandName);
        if (handler == null) {
            return error("no such command: " + commandName, CODE_COMMAND_NOT_FOUND, "CommandNotFound");
        }

        return handler.handle(command);
    }

    static BsonDocument error(final String message, final int code, final String codeName) {
        return new BsonDocument()
                .append("ok", new BsonDouble(0.0))
                .append("errmsg", new BsonString(message))
                .append("code", new BsonInt32(code))
                .append("codeName", new BsonString(codeName));
    }
}
