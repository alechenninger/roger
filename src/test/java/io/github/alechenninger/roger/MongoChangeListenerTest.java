/*
 * MIT License
 *
 * Copyright (c) 2020 Alec Henninger
 */

package io.github.alechenninger.roger;

import static io.github.alechenninger.roger.DecodingConsumer.decoded;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.result.UpdateResult;
import io.github.alechenninger.roger.MongoChangeListenerFactory.RefreshStrategy;
import io.github.alechenninger.roger.testing.Defer;
import io.github.alechenninger.roger.testing.MongoDb;
import io.github.alechenninger.roger.testing.WatchedLogger;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.awaitility.Awaitility;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MongoChangeListenerTest {

  @RegisterExtension
  static MongoDb mongo = MongoDb.replicaSet();

  @RegisterExtension
  static Defer defer = new Defer();

  @RegisterExtension
  static WatchedLogger listenerLogs = new WatchedLogger(MongoChangeListener.class);

  static Random random = new Random();

  private static final Logger LOG = LoggerFactory.getLogger(MongoChangeListenerTest.class);

  // Each test will invalidate the change stream for its database, so use a new database each time.
  String testDb = Long.toUnsignedString(random.nextLong(), Character.MAX_RADIX);
  MongoDatabase db = defer.that(mongo.getDatabase(testDb), MongoDatabase::drop);
  ScheduledRefresh refreshStrategy = defer.close(ScheduledRefresh.every(Duration.ofMillis(100)));
  MongoListenerLockService lockService = new MongoListenerLockService(
      Clock.systemUTC(),
      db.getCollection("listenerLocks", BsonDocument.class),
      "test",
      Duration.ofMillis(500));
  MongoChangeListenerFactory listenerFactory = new MongoChangeListenerFactory(
      refreshStrategy,
      Duration.ofMillis(200),
      lockService);
  TimestampProvider earliestOplogEntry = new EarliestOplogEntry(mongo.client());

  @Test
  void listensToInserts() {
    List<TestDoc> log = new ArrayList<>();
    ChangeConsumer<TestDoc> logIt = (change, tok) -> log.add(change.getFullDocument());

    final MongoCollection<TestDoc> collection = db.getCollection("test", TestDoc.class);
    defer.close(listenerFactory.onChangeTo(collection, logIt, earliestOplogEntry));
    collection.insertOne(TestDoc.randomId());

    Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> log, hasSize(1));
  }

  @Test
  void listensToUpdates() {
    List<Document> log = new ArrayList<>();
    ChangeConsumer<Document> logIt = decoded(
        db.getCodecRegistry().get(Document.class),
        (it, tok) -> log.add(it));

    final MongoCollection<Document> collection = db.getCollection("test");
    collection.insertOne(new Document("_id", "test"));

    defer.close(listenerFactory.onChangeTo(collection, logIt, earliestOplogEntry));

    collection.updateOne(Filters.eq("_id", "test"), Updates.set("foo", "bar"));

    Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .until(
            () -> log.stream().skip(1).collect(Collectors.toList()),
            contains(equalTo(new Document(ImmutableMap.of("foo", "bar")))));
  }

  @Test
  void listensToUpdatesAsPojo() {
    List<TestDoc> log = new ArrayList<>();
    ChangeConsumer<TestDoc> logIt = decoded(
        db.getCodecRegistry().get(TestDoc.class),
        (it, tok) -> log.add(it));

    final MongoCollection<TestDoc> collection = db.getCollection("test", TestDoc.class);
    TestDoc testDoc = new TestDoc("test", null);
    collection.insertOne(testDoc);

    defer.close(listenerFactory.onChangeTo(collection, logIt, earliestOplogEntry));

    final UpdateResult result = collection
        .updateOne(Filters.eq("_id", "test"), Updates.set("foo", "bar"));

    assertEquals(1, result.getModifiedCount());

    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(
            () -> log.stream().skip(1).collect(Collectors.toList()),
            contains(equalTo(new TestDoc(null, "bar"))));
  }

  @Test
  void listensToReplacements() {
    List<Document> log = new ArrayList<>();
    ChangeConsumer<Document> logIt = (change, tok) -> log.add(change.getFullDocument());

    final MongoCollection<Document> collection = db.getCollection("test");
    collection.insertOne(new Document("_id", "test"));
    collection.replaceOne(Filters.eq("_id", "test"),
        new Document(ImmutableMap.of("_id", "test", "foo", "bar")));

    defer.close(listenerFactory.onChangeTo(collection, logIt, earliestOplogEntry));

    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(
            () -> log.stream().skip(1).collect(Collectors.toList()),
            contains(equalTo(new Document(ImmutableMap.of("_id", "test", "foo", "bar")))));
  }

  @Test
  void retriesExceptions() {
    List<Document> log = new ArrayList<>();

    class TakesTwoTries implements ChangeConsumer<Document> {

      int count = 0;

      @Override
      public void accept(ChangeStreamDocument<Document> change, Long aLong) {
        if (count++ == 0) {
          throw new SimulatedException();
        }

        log.add(change.getFullDocument());
      }
    }

    final MongoCollection<Document> collection = db.getCollection("test");
    collection.insertOne(new Document("_id", "test"));

    defer.close(listenerFactory.onChangeTo(collection, new TakesTwoTries(), earliestOplogEntry));

    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(
            () -> log,
            contains(equalTo(new Document(ImmutableMap.of("_id", "test")))));
  }

  @Test
  void continuesListeningAfterCursorStopsWaitingWithoutWaitingForRefresh() {
    MongoChangeListenerFactory listenerFactory = new MongoChangeListenerFactory(
        // Never refresh after first, so we can be sure lock refresh isn't covering up for us
        new JustOnce(),
        /* max wait */ Duration.ofMillis(100),
        lockService);

    List<Document> log = new ArrayList<>();
    ChangeConsumer<Document> logIt = (change, tok) -> log.add(change.getFullDocument());
    final MongoCollection<Document> collection = db.getCollection("test");

    defer.close(listenerFactory.onChangeTo(collection, logIt, earliestOplogEntry));

    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> listenerLogs.contains(MongoChangeListener.CURSOR_MAX_WAIT));

    collection.insertOne(new Document("_id", "test"));

    Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> log, hasSize(1));
  }

  @Test
  @DisplayName("changes are only processed once among all listeners under load with participating consumer")
  void changesAreOnlyProcessedOnceAmongAllListenersUnderLoadWithParticipatingConsumer() {
    // A stress test:
    // Create many listeners with different ids (all competing for lock).
    // Give each listener some random chance of skipping refreshes or pausing while processing
    // changes.
    // Log each received change idempotently, checking fencing token.
    // Ensure all changes received only once, in order.

    IdempotentFencedLogWithSimulatedDelays logIt = new IdempotentFencedLogWithSimulatedDelays();
    MongoCollection<Document> collection = db.getCollection("test");

    for (int i = 0; i < 100; i++) {
      MongoListenerLockService lockService = new MongoListenerLockService(
          Clock.systemUTC(),
          db.getCollection("listenerLocks", BsonDocument.class),
          String.valueOf(i),
          Duration.ofMillis(50));
      MongoChangeListenerFactory listenerFactory = new MongoChangeListenerFactory(
          defer.close(new RandomlyCrashing(Duration.ofMillis(40))),
          Duration.ofMillis(40),
          lockService);

      defer.close(listenerFactory.onChangeTo(collection, logIt, earliestOplogEntry));
    }

    List<Document> expected = IntStream.range(0, 100)
        .mapToObj(i -> new Document("_id", String.valueOf(i)))
        .peek(collection::insertOne)
        .collect(Collectors.toList());

    Awaitility.await()
        .atMost(Duration.ofSeconds(60))
        .until(
            () -> logIt.log,
            equalTo(expected));
  }

  static class SimulatedException extends RuntimeException {

    public SimulatedException() {
    }

    public SimulatedException(String message) {
      super(message);
    }
  }

  public static class TestDoc {

    private String id;
    private String foo;

    public TestDoc() {
    }

    public static TestDoc randomId() {
      return new TestDoc(UUID.randomUUID().toString(), null);
    }

    public TestDoc(String id, String foo) {
      this.id = id;
      this.foo = foo;
    }

    public String getId() {
      return id;
    }

    public void setId(String _id) {
      this.id = _id;
    }

    public String getFoo() {
      return foo;
    }

    public void setFoo(String foo) {
      this.foo = foo;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TestDoc testDoc = (TestDoc) o;

      if (id != null ? !id.equals(testDoc.id) : testDoc.id != null) {
        return false;
      }
      return foo != null ? foo.equals(testDoc.foo) : testDoc.foo == null;
    }

    @Override
    public int hashCode() {
      int result = id != null ? id.hashCode() : 0;
      result = 31 * result + (foo != null ? foo.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return "TestDoc{" +
          "id='" + id + '\'' +
          ", foo='" + foo + '\'' +
          '}';
    }

  }

  private static class JustOnce implements RefreshStrategy {

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @Override
    public void scheduleInBackground(Runnable refresh, Duration leaseTime) {
      executor.execute(refresh);
    }

    @Override
    public void close() throws IOException {
      executor.shutdown();
    }

  }

  static class RandomlyCrashing implements RefreshStrategy {

    private final ScheduledRefresh scheduled;
    private int periodsUntilRecovery = 0;

    private static final Logger log = LoggerFactory.getLogger(RandomlyCrashing.class);

    RandomlyCrashing(Duration period) {
      scheduled = ScheduledRefresh.every(period);
    }

    @Override
    public void scheduleInBackground(Runnable refresh, Duration leaseTime) {
      scheduled.scheduleInBackground(() -> {
        // Simulate crash that eventually recovers
        if (periodsUntilRecovery > 0) {
          periodsUntilRecovery--;
          return;
        }
        if (random.nextInt(100) > 20) {
          log.info("Simulated crash");
          periodsUntilRecovery = 3;
          return;
        }
        refresh.run();
      }, leaseTime);
    }

    @Override
    public void close() throws IOException {
      scheduled.close();
    }
  }

  /**
   * Has properties of a production-like change consumer, namely:
   * <ul>
   *   <li>Only updates log if fencing token is equal to or greater than last used token</li>
   *   <li>Idempotently adds to log–if last element is the same, does not add again.</li>
   *   <li>Log and token reads and writes are performed in an isolated transaction</li>
   * </ul>
   */
  private static class IdempotentFencedLogWithSimulatedDelays implements ChangeConsumer<Document> {

    private final List<Document> log;
    private long lastToken;

    public IdempotentFencedLogWithSimulatedDelays() {
      this.log = Collections.synchronizedList(new ArrayList<>());
      this.lastToken = -1;
    }

    @Override
    public void accept(ChangeStreamDocument<Document> change, Long tok) {
      try {
        Thread.sleep(random.nextInt(100));
      } catch (InterruptedException ignored) {
      }

      synchronized (log) {
        if (tok >= lastToken) {
          lastToken = tok;
          // Changes must be idempotent regardless of lock ownership
          if (!log.isEmpty() && log.get(log.size() - 1).equals(change.getFullDocument())) {
            LOG.info("Detected retry");
            return;
          }
          log.add(change.getFullDocument());
        } else {
          throw new LostLockException("Old token");
        }
      }

      try {
        Thread.sleep(random.nextInt(100));
      } catch (InterruptedException ignored) {
      }
    }
  }
}




