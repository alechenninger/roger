package io.github.alechenninger.roger;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Optional;

public class EarliestOplogEntry implements StartOperationTime {
  private final MongoClient client;

  private static final Bson NOT_NOOP = Filters.ne("op", "n");

  public EarliestOplogEntry(MongoClient client) {
    this.client = client;
  }

  @Override
  public Optional<BsonTimestamp> startFrom() {
    final MongoDatabase local = client.getDatabase("local");
    final MongoCollection<Document> oplog = local.getCollection("oplog.rs");
    final Document first = oplog.find(NOT_NOOP)
        .sort(Sorts.ascending("$natural"))
        .limit(1)
        .first();
    return Optional.ofNullable(first).map((op) -> op.get("ts", BsonTimestamp.class));
  }
}
