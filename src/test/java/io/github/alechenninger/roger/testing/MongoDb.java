package io.github.alechenninger.roger.testing;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.Conventions;
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
          fromProviders(PojoCodecProvider.builder()
              .conventions(Conventions.DEFAULT_CONVENTIONS)
              .automatic(true)
              g.build()));

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
    server = new MongoDBContainer();
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
