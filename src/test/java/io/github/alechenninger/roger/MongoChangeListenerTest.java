package io.github.alechenninger.roger;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

import com.google.common.collect.ImmutableMap;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import io.github.alechenninger.roger.testing.Closer;
import io.github.alechenninger.roger.testing.MongoDb;
import org.awaitility.Awaitility;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class MongoChangeListenerTest {
  @RegisterExtension
  static MongoDb mongo = MongoDb.replicaSet();

  @RegisterExtension
  static Closer closer = new Closer();

  MongoDatabase db = mongo.getDatabase("db");
  RefreshOnce refreshStrategy = closer.closeItAfterTest(new RefreshOnce());
  MongoChangeListenerFactory listenerFactory = new MongoChangeListenerFactory(
      Duration.ofMinutes(5),
      refreshStrategy,
      Duration.ofSeconds(1), new MongoListenerLockService(
      Clock.systemUTC(),
          db.getCollection("listenerLocks", BsonDocument.class),
      "test",
      Duration.ofMinutes(5)));

  @AfterEach
  void cleanUp() throws IOException {
    db.drop();
  }

  @Test
  void listensToInserts() {
    List<TestDoc> log = new ArrayList<>();
    ChangeConsumer<TestDoc> logIt = log::add;
    
    final MongoCollection<TestDoc> collection = db.getCollection("test", TestDoc.class);
    closer.closeItAfterTest(listenerFactory.onChangeTo(collection, logIt));
    collection.insertOne(new TestDoc());

    Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> log, hasSize(1));
  }

  @Test
  void listensToUpdates() {
    List<Document> log = new ArrayList<>();
    ChangeConsumer<Document> logIt = log::add;

    final MongoCollection<Document> collection = db.getCollection("test");
    collection.insertOne(new Document("_id", "test"));

    closer.closeItAfterTest(listenerFactory.onChangeTo(collection, logIt));

    collection.updateOne(Filters.eq("_id", "test"), Updates.set("foo", "bar"));

    Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .until(
            () -> log,
            contains(equalTo(new Document(ImmutableMap.of("foo", "bar")))));
  }

  @Test
  void listensToUpdatesWithParsing() {
    List<TestDoc> log = new ArrayList<>();
    ChangeConsumer<TestDoc> logIt = log::add;

    final MongoCollection<TestDoc> collection = db.getCollection("test", TestDoc.class);
    TestDoc testDoc = new TestDoc();
    testDoc._id = "test";
    collection.insertOne(testDoc);

    closer.closeItAfterTest(listenerFactory.onChangeTo(collection, logIt));

    final UpdateResult result = collection.updateOne(Filters.eq("_id", "test"), Updates.set("foo", "bar"));

    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(
            () -> log,
            contains(equalTo(new TestDoc(null, "bar"))));
  }

  @Test
  void listensToReplacements() {
    List<Document> log = new ArrayList<>();
    ChangeConsumer<Document> logIt = log::add;

    final MongoCollection<Document> collection = db.getCollection("test");
    collection.insertOne(new Document("_id", "test"));

    closer.closeItAfterTest(listenerFactory.onChangeTo(collection, logIt));

    collection.replaceOne(Filters.eq("_id", "test"), new Document(ImmutableMap.of("_id", "test", "foo", "bar")));

    Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .until(
            () -> log,
            contains(equalTo(new Document(ImmutableMap.of("_id", "test", "foo", "bar")))));
  }

  static class RefreshOnce implements MongoChangeListenerFactory.RefreshStrategy {
    final ExecutorService executor = Executors.newSingleThreadExecutor();

    @Override
    public void scheduleInBackground(Runnable refresh, Duration leaseTime) {
      executor.submit(refresh);
    }

    @Override
    public void close() throws IOException {
      executor.shutdown();
    }
  }

  public static class TestDoc {
    String _id = UUID.randomUUID().toString();
    String foo;

    public TestDoc() {}

    public TestDoc(String id, String foo) {
      this._id = id;
      this.foo = foo;
    }

    public String get_id() {
      return _id;
    }

    public void set_id(String _id) {
      this._id = _id;
    }

    public String getFoo() {
      return foo;
    }

    public void setFoo(String foo) {
      this.foo = foo;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TestDoc testDoc = (TestDoc) o;

      if (_id != null ? !_id.equals(testDoc._id) : testDoc._id != null) return false;
      return foo != null ? foo.equals(testDoc.foo) : testDoc.foo == null;
    }

    @Override
    public int hashCode() {
      int result = _id != null ? _id.hashCode() : 0;
      result = 31 * result + (foo != null ? foo.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return "TestDoc{" +
          "_id='" + _id + '\'' +
          ", foo='" + foo + '\'' +
          '}';
    }
  }
}
