package io.github.alechenninger.roger;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Clock;
import java.time.Duration;
import java.util.UUID;
import java.util.function.BiConsumer;

public class MongoChangeListenerFactory {
  private final Duration leaseTime;
  private final RefreshStrategy refreshStrategy;
  private final Duration maxAwaitTime;
  private final MongoListenerLockService lock;

  private static final Logger log = LoggerFactory.getLogger(MongoChangeListenerFactory.class);

  public MongoChangeListenerFactory(Duration leaseTime, RefreshStrategy refreshStrategy,
      Duration maxAwaitTime, MongoListenerLockService lockService) {
    this.leaseTime = leaseTime;
    this.refreshStrategy = refreshStrategy;
    this.maxAwaitTime = maxAwaitTime;
    this.lock = lockService;
  }

  public static MongoChangeListenerFactory withDefaults(
      MongoDatabase db, RefreshStrategy refreshStrategy) {
    return new MongoChangeListenerFactory(
        Duration.ofMinutes(5), refreshStrategy, Duration.ofSeconds(5), new MongoListenerLockService(
        Clock.systemUTC(),
            db.getCollection("listenerLocks", BsonDocument.class),
        UUID.randomUUID().toString(),
        Duration.ofMinutes(5)));
  }

  public <T> Closeable onChangeTo(MongoCollection<T> collection,
      BiConsumer<ChangeStreamDocument<T>, Long> callback, TimestampProvider initialStartTime) {
    MongoChangeListener<T> listener = new MongoChangeListener<>(
        lock, callback, maxAwaitTime, collection, initialStartTime);

    refreshStrategy.scheduleInBackground(listener::startOrRefresh, leaseTime);

    return listener;
  }

  interface RefreshStrategy extends Closeable {
    void scheduleInBackground(Runnable refresh, Duration leaseTime);
  }

}
