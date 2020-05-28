/*
 * Copyright (C) 2020  Alec Henninger
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

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

public class EarliestOplogEntry implements TimestampProvider {
  private final MongoClient client;

  private static final Bson NOT_NOOP = Filters.ne("op", "n");

  public EarliestOplogEntry(MongoClient client) {
    this.client = client;
  }

  @Override
  public Optional<BsonTimestamp> timestamp() {
    final MongoDatabase local = client.getDatabase("local");
    final MongoCollection<Document> oplog = local.getCollection("oplog.rs");
    final Document first = oplog.find(NOT_NOOP)
        .sort(Sorts.ascending("$natural"))
        .limit(1)
        .first();
    return Optional.ofNullable(first).map((op) -> op.get("ts", BsonTimestamp.class));
  }
}
