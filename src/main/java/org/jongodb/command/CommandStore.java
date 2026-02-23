package org.jongodb.command;

import java.util.List;
import org.bson.BsonDocument;

public interface CommandStore {
    int insert(String database, String collection, List<BsonDocument> documents);

    List<BsonDocument> find(String database, String collection, BsonDocument filter);
}
