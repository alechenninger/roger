/*
 * MIT License
 *
 * Copyright (c) 2020 Alec Henninger
 */

package io.github.alechenninger.roger;

import com.mongodb.MongoInterruptedException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

public class MongoChangeListener<T> implements Closeable {
  private final MongoListenerLockService lockService;
  private final String collectionNs;
  private final BiConsumer<ChangeStreamDocument<T>, Long> callback;
  private final Duration maxAwaitTime;
  private final MongoCollection<T> collection;
  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private final TimestampProvider initialStartTime;

  public static final Marker CURSOR_MAX_WAIT = MarkerFactory.getMarker("listener.cursor_max_wait");

  private boolean closed = false;

  // Volatile due to access inside listener thread
  // Non-null once set, but may be set multiple times
  private volatile Future<?> listener;

  private static final Logger log = LoggerFactory.getLogger(MongoChangeListener.class);

  public MongoChangeListener(MongoListenerLockService lockService,
      BiConsumer<ChangeStreamDocument<T>, Long> callback, Duration maxAwaitTime,
      MongoCollection<T> collection, TimestampProvider initialStartTime) {
    this.lockService = lockService;
    this.collectionNs = collection.getNamespace().getFullName();
    this.callback = callback;
    this.maxAwaitTime = maxAwaitTime;
    this.collection = collection;
    this.initialStartTime = initialStartTime;
  }

  public synchronized void startOrRefresh() {
    if (closed) {
      throw new IllegalStateException("Cannot start again once closed.");
    }

    final Optional<ListenerLock> maybeLock = lockService.acquireOrRefreshFor(collectionNs);

    if (maybeLock.isPresent()) {
      final ListenerLock lock = maybeLock.get();
      startOrContinueListening(lock);
    } else {
      if (stopListening()) {
        log.warn("Lost lock, listener stopped");
      }
    }
  }

  public boolean isListening() {
    return listener != null && !listener.isDone();
  }

  @Override
  public synchronized void close() throws IOException {
    log.debug("Closing listener...");

    closed = true;

    stopListening();

    executor.shutdown();
    try {
      executor.awaitTermination(maxAwaitTime.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      log.info("Interrupted while waiting for listener to terminate", e);
    }
    executor.shutdownNow();

    log.info("Listener closed");
  }

  /**
   * Stops the change stream cursor from iterating. May not immediately take affect (depends on
   * {@link #maxAwaitTime} and whether or not a change is currently being processed).
   *
   * <p>Once stopped, may be resumed by {@link #startOrRefresh()}.
   *
   * @return {@code true} if a listener was currently active when this method was called
   */
  private synchronized boolean stopListening() {
    if (listener != null && !listener.isDone()) {
      listener.cancel(true);
      return true;
    }
    return false;
  }

  private synchronized void startOrContinueListening(ListenerLock lock) {
    if (isListening()) {
      log.debug("Lock refreshed and listener already running. Doing nothing.");
      return;
    }

    final Optional<BsonDocument> resumeToken = lock.resumeToken();

    log.debug("Lock acquired and listener not started. Looking for offset. " +
        "resumeToken={}", resumeToken);

    final ChangeOffset offset = resumeToken
        .<ChangeOffset>map(ResumeTokenOffset::new)
        .orElse(initialStartTime.timestamp()
            .map(OperationTimeOffset::new)
            .orElse(null));

    if (offset == null) {
      log.warn("No operation to start or resume from; cannot safely start listener");
      return;
    }

    log.info("Scheduling listener to start in background from offset {}", offset);

    listener = executor.submit(new Listen(offset, lock.version()));
  }

  interface ChangeOffset {
    void configure(ChangeStreamIterable<?> stream);
  }

  static class ResumeTokenOffset implements ChangeOffset {

    private final BsonDocument resumeToken;

    ResumeTokenOffset(BsonDocument resumeToken) {
      this.resumeToken = Objects.requireNonNull(resumeToken, "resumeToken");
    }

    @Override
    public void configure(ChangeStreamIterable<?> stream) {
      stream.resumeAfter(resumeToken);
    }

    @Override
    public String toString() {
      return "ResumeTokenOffset{" +
          "resumeToken=" + resumeToken +
          '}';
    }
  }

  static class OperationTimeOffset implements ChangeOffset {

    private final BsonTimestamp time;

    OperationTimeOffset(BsonTimestamp time) {
      this.time = Objects.requireNonNull(time, "time");
    }

    @Override
    public void configure(ChangeStreamIterable<?> stream) {
      stream.startAtOperationTime(time);
    }

    @Override
    public String toString() {
      return "OperationTimeOffset{" +
          "time=" + time +
          '}';
    }
  }

  private class Listen implements Runnable {
    private final ChangeOffset offset;
    private final long lockVersion;

    private final Logger log = LoggerFactory.getLogger(Listen.class);

    public Listen(ChangeOffset offset, long lockVersion) {
      this.offset = offset;
      this.lockVersion = lockVersion;
    }

    @Override
    public void run() {
      try {
        log.info("Starting change listener with offset {}", offset);

        ChangeStreamIterable<T> stream = collection
            .watch()
            .maxAwaitTime(maxAwaitTime.toMillis(), TimeUnit.MILLISECONDS);

        offset.configure(stream);

        MongoChangeStreamCursor<ChangeStreamDocument<T>> cursor = stream.cursor();

        // isListening checks are imperfect optimizations to attempt to catch when the lock has been
        // lost early. It can still be lost, even during/after we have processed a change. The change
        // callback must participate in locking to truly ensure order e.g. with fencing tokens.
        while (isListening()) {
          try {
            log.debug("Opening change stream cursor");

            ChangeStreamDocument<T> change = cursor.tryNext();

            if (!isListening()) {
              log.info("Listener has been stopped; aborting change processing");
              break;
            }

            if (change == null) {
              log.debug(CURSOR_MAX_WAIT, "No change. Cursor gave up waiting, but we still believe "
                  + "we have a lock, so will keep listening.");
              continue;
            }

            log.debug("Accepting change {}", change);

            callback.accept(change, lockVersion);

            BsonDocument nextResumeToken = change.getResumeToken();

            log.debug("Change processed, committing new resume position {}", nextResumeToken);

            lockService.commit(collectionNs, nextResumeToken);
          } catch (LostLockException e) {
            log.warn("Lost lock while listening to change", e);
            break;
          } catch (MongoInterruptedException e) {
            log.info("Caught interrupt while listening for changes, stopping: {}", e.getMessage());
            break;
          } catch (Exception e) {
            log.error("Uncaught exception while listening to change, stopping", e);
            break;
          }
        }

        log.info("Listener stopped. Closing cursor...");

        cursor.close();
      } catch (Exception e) {
        log.error("Uncaught exception while attempting to listen to change. "
            + "Listener not running.", e);
      }
    }
  }
}
