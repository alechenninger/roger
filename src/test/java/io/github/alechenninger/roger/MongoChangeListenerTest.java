package io.github.alechenninger.roger;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
import org.bson.codecs.pojo.annotations.BsonId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

class MongoChangeListenerTest {
  @RegisterExtension
  static MongoDb mongo = MongoDb.replicaSet();

  @RegisterExtension
  static Closer closer = new Closer();

  static Random random = new Random();

  // Each test will invalidate the change stream for its database, so use a new database each time.
  String testDb = Long.toUnsignedString(random.nextLong(), Character.MAX_RADIX);
  MongoDatabase db = mongo.getDatabase(testDb);
  EverySecond refreshStrategy = closer.closeItAfterTest(new EverySecond());
  MongoChangeListenerFactory listenerFactory = new MongoChangeListenerFactory(
      Duration.ofMinutes(5),
      refreshStrategy,
      Duration.ofSeconds(1), new MongoListenerLockService(
      Clock.systemUTC(),
          db.getCollection("listenerLocks", BsonDocument.class),
      "test",
      Duration.ofMinutes(5)));
  StartOperationTime earliestOplogEntry = new EarliestOplogEntry(mongo.client());

  @AfterEach
  void cleanUp() {
    db.drop();
  }

  @Test
  void listensToInserts() {
    List<TestDoc> log = new ArrayList<>();
    ChangeConsumer<TestDoc> logIt = log::add;

    final MongoCollection<TestDoc> collection = db.getCollection("test", TestDoc.class);
    closer.closeItAfterTest(listenerFactory.onChangeTo(collection, logIt, earliestOplogEntry));
    collection.insertOne(new TestDoc());

    Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> log, hasSize(1));
  }

  @Test
  void listensToUpdates() {
    List<Document> log = new ArrayList<>();
    ChangeConsumer<Document> logIt = log::add;

    final MongoCollection<Document> collection = db.getCollection("test");
    collection.insertOne(new Document("_id", "test"));

    closer.closeItAfterTest(listenerFactory.onChangeTo(collection, logIt, earliestOplogEntry));

    collection.updateOne(Filters.eq("_id", "test"), Updates.set("foo", "bar"));

    Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .until(
            () -> log.stream().skip(1).collect(Collectors.toList()),
            contains(equalTo(new Document(ImmutableMap.of("foo", "bar")))));
  }

  @Test
  void listensToUpdatesWithParsing() {
    List<TestDoc> log = new ArrayList<>();
    ChangeConsumer<TestDoc> logIt = log::add;

    final MongoCollection<TestDoc> collection = db.getCollection("test", TestDoc.class);
    TestDoc testDoc = new TestDoc("test", null);
    collection.insertOne(testDoc);

    closer.closeItAfterTest(listenerFactory.onChangeTo(collection, logIt, earliestOplogEntry));

    final UpdateResult result = collection.updateOne(Filters.eq("_id", "test"), Updates.set("foo", "bar"));

    assertNotNull(result);

    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(
            () -> log.stream().skip(1).collect(Collectors.toList()),
            contains(equalTo(new TestDoc(null, "bar"))));
  }

  @Test
  void listensToReplacements() {
    List<Document> log = new ArrayList<>();
    ChangeConsumer<Document> logIt = log::add;

    final MongoCollection<Document> collection = db.getCollection("test");
    collection.insertOne(new Document("_id", "test"));
    collection.replaceOne(Filters.eq("_id", "test"), new Document(ImmutableMap.of("_id", "test", "foo", "bar")));

    closer.closeItAfterTest(listenerFactory.onChangeTo(collection, logIt, earliestOplogEntry));

    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(
            () -> log.stream().skip(1).collect(Collectors.toList()),
            contains(equalTo(new Document(ImmutableMap.of("_id", "test", "foo", "bar")))));
  }

  static class EverySecond implements MongoChangeListenerFactory.RefreshStrategy {
    final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    @Override
    public void scheduleInBackground(Runnable refresh, Duration leaseTime) {
      executor.scheduleAtFixedRate(refresh, 0, 1, TimeUnit.SECONDS);
    }

    @Override
    public void close() throws IOException {
      executor.shutdown();
      try {
        executor.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException ignored) {
        // ignored
      }
      executor.shutdownNow();
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

    @BsonId
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
