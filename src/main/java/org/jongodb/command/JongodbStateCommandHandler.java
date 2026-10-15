package org.jongodb.command;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.jongodb.engine.EngineStateFiles;
import org.jongodb.engine.InMemoryEngineStore;

/**
 * jongodb-specific state control: {@code {jongodbState: <action>, name?, path?}}.
 *
 * <ul>
 *   <li>{@code save}/{@code restore}/{@code drop} — named in-memory snapshots of every database</li>
 *   <li>{@code list} — snapshot names</li>
 *   <li>{@code export}/{@code import} — the whole state as a {@code mongodump}-layout directory</li>
 * </ul>
 *
 * <p>Restores replace all data and surface to change streams as per-document inserts, updates, and deletes.
 * Open transactions are not aborted; they commit against the restored state.
 */
final class JongodbStateCommandHandler implements CommandHandler {
    private final InMemoryEngineStore engine;
    private final Map<String, InMemoryEngineStore> snapshots = new ConcurrentHashMap<>();

    JongodbStateCommandHandler(final InMemoryEngineStore engine) {
        this.engine = engine;
    }

    @Override
    public BsonDocument handle(final BsonDocument command) {
        if (engine == null) {
            return CommandErrors.notImplemented("jongodbState requires the in-memory engine");
        }
        final BsonValue actionValue = command.get("jongodbState");
        if (!(actionValue instanceof BsonString action)) {
            return CommandErrors.typeMismatch("jongodbState must be an action string");
        }
        try {
            return switch (action.getValue()) {
                case "save" -> {
                    snapshots.put(requireString(command, "name"), engine.snapshot());
                    yield ok();
                }
                case "restore" -> {
                    final String name = requireString(command, "name");
                    final InMemoryEngineStore snapshot = snapshots.get(name);
                    if (snapshot == null) {
                        yield CommandErrors.badValue("no jongodbState snapshot named '" + name + "'");
                    }
                    engine.restoreFrom(snapshot);
                    yield ok();
                }
                case "drop" -> {
                    snapshots.remove(requireString(command, "name"));
                    yield ok();
                }
                case "list" -> {
                    final BsonArray names = new BsonArray();
                    snapshots.keySet().stream().sorted().forEach(name -> names.add(new BsonString(name)));
                    yield ok().append("snapshots", names);
                }
                case "export" -> {
                    EngineStateFiles.export(engine, Path.of(requireString(command, "path")));
                    yield ok();
                }
                case "import" -> {
                    EngineStateFiles.load(engine, Path.of(requireString(command, "path")));
                    yield ok();
                }
                default -> CommandErrors.badValue("unknown jongodbState action: " + action.getValue());
            };
        } catch (final IllegalArgumentException | java.io.UncheckedIOException exception) {
            return CommandErrors.badValue(exception.getMessage());
        }
    }

    private static String requireString(final BsonDocument command, final String key) {
        final BsonValue value = command.get(key);
        if (!(value instanceof BsonString string) || string.getValue().isBlank()) {
            throw new IllegalArgumentException("jongodbState requires a non-empty string '" + key + "'");
        }
        return string.getValue();
    }

    private static BsonDocument ok() {
        return new BsonDocument("ok", new BsonDouble(1.0));
    }
}
