package org.jongodb.testkit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.bson.BsonDocument;
import org.jongodb.server.WireCommandIngress;
import org.jongodb.wire.OpMsg;
import org.jongodb.wire.OpMsgCodec;

/**
 * In-process backend adapter that executes scenario commands through {@link WireCommandIngress}.
 */
public final class WireCommandIngressBackend implements DifferentialBackend {
    private static final String DEFAULT_DATABASE = "testkit";

    private final String name;
    private final Supplier<WireCommandIngress> ingressFactory;

    public WireCommandIngressBackend(String name) {
        this(name, WireCommandIngress::inMemory);
    }

    public WireCommandIngressBackend(String name, Supplier<WireCommandIngress> ingressFactory) {
        this.name = requireText(name, "name");
        this.ingressFactory = Objects.requireNonNull(ingressFactory, "ingressFactory");
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public ScenarioOutcome execute(Scenario scenario) {
        Objects.requireNonNull(scenario, "scenario");
        final WireCommandIngress ingress;
        try {
            ingress = Objects.requireNonNull(ingressFactory.get(), "ingressFactory result");
        } catch (RuntimeException exception) {
            return ScenarioOutcome.failure("failed to create ingress: " + exception.getMessage());
        }

        final OpMsgCodec codec = new OpMsgCodec();
        int requestId = 1_000;
        final List<Map<String, Object>> commandResults = new ArrayList<>(scenario.commands().size());
        for (int i = 0; i < scenario.commands().size(); i++) {
            final ScenarioCommand command = scenario.commands().get(i);
            final BsonDocument commandDocument;
            try {
                commandDocument = ScenarioBsonCodec.toCommandDocument(command, DEFAULT_DATABASE);
            } catch (RuntimeException exception) {
                return ScenarioOutcome.failure("invalid command payload for " + command.commandName() + ": " + exception.getMessage());
            }

            final OpMsg request = new OpMsg(requestId++, 0, 0, commandDocument);
            final OpMsg response;
            try {
                response = codec.decode(ingress.handle(codec.encode(request)));
            } catch (RuntimeException exception) {
                return ScenarioOutcome.failure(
                    "wire execution failed for " + command.commandName() + ": " + exception.getMessage()
                );
            }

            final BsonDocument responseBody = response.body();
            commandResults.add(ScenarioBsonCodec.toJavaMap(responseBody));
            if (!ScenarioBsonCodec.isSuccess(responseBody)) {
                return ScenarioOutcome.failure(ScenarioBsonCodec.formatFailure(command.commandName(), i, responseBody));
            }
        }
        return ScenarioOutcome.success(commandResults);
    }

    private static String requireText(String value, String fieldName) {
        String normalized = value == null ? null : value.trim();
        if (normalized == null || normalized.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return normalized;
    }
}
