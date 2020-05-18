package io.github.alechenninger.roger;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class MongoChangeListener<T> implements Closeable {
  private final MongoListenerLockService lock;
  private final String collectionNamespace;
  private final Consumer<ChangeStreamDocument<T>> callback;
  private final Duration maxAwaitTime;
  private final MongoCollection<T> collection;
  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private final TimestampProvider initialStartTime;

  private boolean closed = false;

  // Volatile due to access inside listener thread
  // Non-null once set, but may be set multiple times
  private volatile Future<?> listener;

  private static final Logger log = LoggerFactory.getLogger(MongoChangeListener.class);

  public MongoChangeListener(MongoListenerLockService lock,
      Consumer<ChangeStreamDocument<T>> callback, Duration maxAwaitTime,
      MongoCollection<T> collection, TimestampProvider initialStartTime) {
    this.lock = lock;
    this.collectionNamespace = collection.getNamespace().getFullName();
    this.callback = callback;
    this.maxAwaitTime = maxAwaitTime;
    this.collection = collection;
    this.initialStartTime = initialStartTime;
  }

  public synchronized void startOrRefresh() {
    if (closed) {
      throw new IllegalStateException("Cannot start again once closed.");
    }

    // TODO: return value is a bit odd. resume token but might not actually have a resume token...
    final Optional<MongoResumeToken> resumeToken = lock.acquireOrRefreshFor(collectionNamespace);

    if (resumeToken.isPresent()) {
      final MongoResumeToken it = resumeToken.get();
      startOrContinueListening(it);
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
      executor.awaitTermination(30, TimeUnit.SECONDS);
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
   */
  private synchronized boolean stopListening() {
    if (listener != null) {
      listener.cancel(false);
      return true;
    }
    return false;
  }

  private synchronized void startOrContinueListening(MongoResumeToken resumeToken) {
    if (isListening()) {
      log.debug("Lock refreshed and listener already running. Doing nothing.");
      return;
    }

    log.debug("Lock acquired and listener not started. Looking for offset. resumeToken={}", resumeToken);

    final ChangeOffset offset = Optional.ofNullable(resumeToken.document())
        .<ChangeOffset>map(ResumeTokenOffset::new)
        .orElse(initialStartTime.timestamp()
            .map(OperationTimeOffset::new)
            .orElse(null));

    if (offset == null) {
      log.info("No operation to start or resume from; cannot safely start listener");
      return;
    }

    log.info("Scheduling listener to start in background from offset {}", offset);

    listener = executor.submit(new Listen(offset));
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

  static class MongoResumeToken {
    @Nullable
    private final BsonDocument resumeToken;

    public MongoResumeToken(@Nullable BsonDocument resumeToken) {
      this.resumeToken = resumeToken;
    }

    /**
     * The resume token document from a previous {@link ChangeStreamDocument}.
     *
     * <p>May be {@code null} if there was no resume token yet stored.
     */
    @Nullable
    public BsonDocument document() {
      return resumeToken;
    }
  }

  private class Listen implements Runnable {
    private final ChangeOffset offset;

    private final Logger log = LoggerFactory.getLogger(Listen.class);

    public Listen(ChangeOffset offset) {
      this.offset = offset;
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
            ChangeStreamDocument<T> change = cursor.next();

            if (!isListening()) {
              log.info("Got change but lost lock; aborting change processing");
              break;
            }

            log.debug("Accepting change {}", change);

            callback.accept(change);

            MongoResumeToken newResumeToken = new MongoResumeToken(change.getResumeToken());

            log.debug("Change processed, committing new resume position {}", newResumeToken);

            lock.commit(collectionNamespace, newResumeToken);
          } catch (LostLockException e) {
            log.warn("Lost lock while listening to change", e);
            break;
          } catch (Exception e) {
            log.error("Uncaught exception while listening to change", e);
            break;
          }
        }

        log.info("Stopping active listener...");

        cursor.close();
      } catch (Exception e) {
        log.error("Uncaught exception while listening to change", e);
      }
    }
  }
}
