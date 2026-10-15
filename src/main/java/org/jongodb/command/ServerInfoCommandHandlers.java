package org.jongodb.command;

import java.lang.management.ManagementFactory;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;

/**
 * Diagnostic commands that describe the server process rather than stored data.
 */
final class ServerInfoCommandHandlers {
    private static final int MAX_CONNECTIONS = 51_200;

    private final long startMillis = System.currentTimeMillis();
    private final TopologyProfile topologyProfile;
    private final String address;
    private final String replicaSetName;

    ServerInfoCommandHandlers(final TopologyProfile topologyProfile, final String address, final String replicaSetName) {
        this.topologyProfile = topologyProfile;
        this.address = address;
        this.replicaSetName = replicaSetName;
    }

    CommandHandler serverStatus() {
        return command -> {
            final long now = System.currentTimeMillis();
            final long uptimeMillis = now - startMillis;
            // ponytail: the TCP layer does not report open sockets here, so connections is a fixed single connection
            return AdminCommandSupport.ok(new BsonDocument()
                    .append("host", new BsonString(address))
                    .append("version", new BsonString(BuildInfoCommandHandler.VERSION))
                    .append("process", new BsonString("mongod"))
                    .append("pid", new BsonInt64(ProcessHandle.current().pid()))
                    .append("uptime", new BsonDouble(uptimeMillis / 1000))
                    .append("uptimeMillis", new BsonInt64(uptimeMillis))
                    .append("uptimeEstimate", new BsonInt64(uptimeMillis / 1000))
                    .append("localTime", new BsonDateTime(now))
                    .append("connections", new BsonDocument()
                            .append("current", new BsonInt32(1))
                            .append("available", new BsonInt32(MAX_CONNECTIONS - 1))
                            .append("totalCreated", new BsonInt32(1))));
        };
    }

    CommandHandler hostInfo() {
        return command -> {
            final java.lang.management.OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
            final long memBytes = Runtime.getRuntime().maxMemory();
            return AdminCommandSupport.ok(new BsonDocument()
                    .append("system", new BsonDocument()
                            .append("currentTime", new BsonDateTime(System.currentTimeMillis()))
                            .append("hostname", new BsonString(address))
                            .append("cpuAddrSize", new BsonInt32(64))
                            .append("memSizeMB", new BsonInt64(memBytes >> 20))
                            .append("memLimitMB", new BsonInt64(memBytes >> 20))
                            .append("numCores", new BsonInt32(os.getAvailableProcessors()))
                            .append("cpuArch", new BsonString(os.getArch()))
                            .append("numaEnabled", BsonBoolean.FALSE))
                    .append("os", new BsonDocument()
                            .append("type", new BsonString(os.getName()))
                            .append("name", new BsonString(os.getName()))
                            .append("version", new BsonString(os.getVersion())))
                    .append("extra", new BsonDocument()));
        };
    }

    CommandHandler connectionStatus() {
        return command -> AdminCommandSupport.ok(new BsonDocument()
                .append("authInfo", new BsonDocument()
                        .append("authenticatedUsers", new BsonArray())
                        .append("authenticatedUserRoles", new BsonArray())
                        .append("authenticatedUserPrivileges", new BsonArray())));
    }

    CommandHandler replSetGetStatus() {
        return command -> {
            if (!topologyProfile.replicaSetSemanticsEnabled()) {
                return CommandErrors.noReplicationEnabled("not running with --replSet");
            }
            final long now = System.currentTimeMillis();
            final BsonTimestamp optime = new BsonTimestamp((int) (now / 1000), 1);
            final BsonDocument optimeDocument = new BsonDocument()
                    .append("ts", optime)
                    .append("t", new BsonInt64(1));
            final BsonDocument member = new BsonDocument()
                    .append("_id", new BsonInt32(0))
                    .append("name", new BsonString(address))
                    .append("health", new BsonDouble(1.0))
                    .append("state", new BsonInt32(1))
                    .append("stateStr", new BsonString("PRIMARY"))
                    .append("uptime", new BsonInt64((now - startMillis) / 1000))
                    .append("optime", optimeDocument)
                    .append("optimeDate", new BsonDateTime(now))
                    .append("lastAppliedWallTime", new BsonDateTime(now))
                    .append("lastDurableWallTime", new BsonDateTime(now))
                    .append("syncSourceHost", new BsonString(""))
                    .append("syncSourceId", new BsonInt32(-1))
                    .append("infoMessage", new BsonString(""))
                    .append("electionTime", new BsonTimestamp((int) (startMillis / 1000), 1))
                    .append("electionDate", new BsonDateTime(startMillis))
                    .append("configVersion", new BsonInt32(1))
                    .append("configTerm", new BsonInt32(1))
                    .append("self", BsonBoolean.TRUE)
                    .append("lastHeartbeatMessage", new BsonString(""));
            return AdminCommandSupport.ok(new BsonDocument()
                    .append("set", new BsonString(replicaSetName))
                    .append("date", new BsonDateTime(now))
                    .append("myState", new BsonInt32(1))
                    .append("term", new BsonInt64(1))
                    .append("syncSourceHost", new BsonString(""))
                    .append("syncSourceId", new BsonInt32(-1))
                    .append("heartbeatIntervalMillis", new BsonInt64(2000))
                    .append("majorityVoteCount", new BsonInt32(1))
                    .append("writeMajorityCount", new BsonInt32(1))
                    .append("votingMembersCount", new BsonInt32(1))
                    .append("writableVotingMembersCount", new BsonInt32(1))
                    .append("optimes", new BsonDocument()
                            .append("lastCommittedOpTime", optimeDocument)
                            .append("appliedOpTime", optimeDocument)
                            .append("durableOpTime", optimeDocument))
                    .append("members", new BsonArray(java.util.List.of(member))));
        };
    }

    /** No operation runs long enough to be observed, so there is never anything to kill. */
    static CommandHandler killOp() {
        return command -> AdminCommandSupport.ok(new BsonDocument("info", new BsonString("attempting to kill op")));
    }
}
