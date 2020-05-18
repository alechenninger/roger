package io.github.alechenninger.roger;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.UpdateDescription;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonTimestamp;
import org.bson.codecs.DecoderContext;

import java.io.Closeable;
import java.time.Clock;
import java.time.Duration;
import java.util.UUID;
import java.util.function.Consumer;

public class MongoChangeListenerFactory {
  private final Duration leaseTime;
  private final RefreshStrategy refreshStrategy;
  private final Duration maxAwaitTime;
  private final MongoListenerLockService lock;

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

  public <T> Closeable onChangeTo(MongoCollection<T> collection, Consumer<T> callback,
      StartOperationTime startOperationTime) {
    MongoChangeListener<T> listener = new MongoChangeListener<>(
        lock,
        change -> {
          if (change.getFullDocument() == null) {
            UpdateDescription update = change.getUpdateDescription();
            if (update == null) {
              return;
            }
            BsonDocumentReader reader = new BsonDocumentReader(update.getUpdatedFields());
            final T fromUpdate = collection.getCodecRegistry()
                .get(collection.getDocumentClass())
                .decode(reader, DecoderContext.builder().build());
            callback.accept(fromUpdate);
          } else {
            callback.accept(change.getFullDocument());
          }
        },
        maxAwaitTime,
        collection,
        startOperationTime);

    refreshStrategy.scheduleInBackground(listener::startOrRefresh, leaseTime);

    return listener;
  }

  interface RefreshStrategy extends Closeable {
    void scheduleInBackground(Runnable refresh, Duration leaseTime);
  }
}
