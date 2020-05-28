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

package io.github.alechenninger.roger.testing;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;

import java.io.IOException;

public class MongoDb implements BeforeAllCallback, AfterAllCallback {
  private MongoDBContainer server;
  private MongoClient client;

  private static final Logger log = LoggerFactory.getLogger(MongoDb.class);

  private static final CodecRegistry pojoCodecRegistry =
      fromRegistries(
          MongoClientSettings.getDefaultCodecRegistry(),
          fromProviders(PojoCodecProvider.builder().automatic(true).build()));

  public static MongoDb replicaSet() {
    return new MongoDb();
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    start();
  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    stop();
  }

  public MongoDatabase getDatabase(String db) {
    return client.getDatabase(db);
  }

  public MongoClient client() {
    return client;
  }

  public void start() throws IOException {
    server = new MongoDBContainer("mongo:4.2.6");
    server.start();
    client = MongoClients.create(MongoClientSettings.builder()
        .applyConnectionString(new ConnectionString(server.getReplicaSetUrl()))
        .codecRegistry(pojoCodecRegistry)
        .build());
  }

  public void stop() {
    client.close();
    server.stop();
  }

  public void dropDatabase(String db) {
    client.getDatabase(db).drop();
  }

}
