/*
 * MIT License
 *
 * Copyright (c) 2020 Alec Henninger
 */

package io.github.alechenninger.roger;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import java.io.Closeable;
import java.time.Duration;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoChangeListenerFactory {
  private final Duration leaseTime;
  private final RefreshStrategy refreshStrategy;
  private final Duration maxAwaitTime;
  private final MongoListenerLockService lock;

  private static final Logger log = LoggerFactory.getLogger(MongoChangeListenerFactory.class);

  /**
   * @param refreshStrategy Strategy which is responsible for regularly refreshing (or not) the
   *                        listener so as to ensure liveness.
   * @param maxAwaitTime How long a change cursor should wait for a change before checking the lock
   *                     status. Will be capped to {@link MongoListenerLockService#leaseTime()} of
   *                     {@code lockService}. A shorter {@code maxAwaitTime} will make the listener
   *                     more responsive to lost locks.
   * @param lockService Service responsible for maintaining a listener lock.
   */
  public MongoChangeListenerFactory(RefreshStrategy refreshStrategy,
      Duration maxAwaitTime, MongoListenerLockService lockService) {
    this.leaseTime = lockService.leaseTime();
    this.refreshStrategy = refreshStrategy;
    this.maxAwaitTime = leaseTime.compareTo(maxAwaitTime) > 0 ? maxAwaitTime : leaseTime;
    this.lock = lockService;
  }

  public static MongoChangeListenerFactory withDefaults(MongoDatabase db) {
    final MongoListenerLockService lockService = MongoListenerLockService.withDefaults(db);
    return new MongoChangeListenerFactory(
        ScheduledRefresh.auto(),
        lockService.leaseTime().dividedBy(4),
        lockService);
  }

  public <T> Closeable onChangeTo(
      MongoCollection<T> collection,
      BiConsumer<ChangeStreamDocument<T>, Long> callback,
      TimestampProvider initialStartTime) {
    MongoChangeListener<T> listener = new MongoChangeListener<>(
        lock, callback, maxAwaitTime, collection, initialStartTime);

    refreshStrategy.scheduleInBackground(listener::startOrRefresh, leaseTime);

    return listener;
  }

  interface RefreshStrategy extends Closeable {
    void scheduleInBackground(Runnable refresh, Duration leaseTime);
  }

}
