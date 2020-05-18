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
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class MongoChangeListener<T> implements Closeable {
  private final MongoListenerLockService lock;
  private final String collectionNamespace;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final Consumer<ChangeStreamDocument<T>> callback;
  private final Duration maxAwaitTime;
  private final MongoCollection<T> collection;
  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private final StartOperationTime startOperationTime;
  private volatile Future<?> listener;

  private static final Logger log = LoggerFactory.getLogger(MongoChangeListener.class);

  public MongoChangeListener(MongoListenerLockService lock,
      Consumer<ChangeStreamDocument<T>> callback, Duration maxAwaitTime,
      MongoCollection<T> collection, StartOperationTime startOperationTime) {
    this.lock = lock;
    this.collectionNamespace = collection.getNamespace().getFullName();
    this.callback = callback;
    this.maxAwaitTime = maxAwaitTime;
    this.collection = collection;
    this.startOperationTime = startOperationTime;
  }

  public synchronized void startOrRefresh() {
    if (closed.get()) {
      throw new IllegalStateException("Cannot start again once closed.");
    }

    final Optional<MongoResumeToken> resumeToken = lock.acquireOrRefreshFor(collectionNamespace);
    if (resumeToken.isPresent()) {
      final MongoResumeToken it = resumeToken.get();
      final ChangeOffset offset = Optional.ofNullable(it.document())
          .<ChangeOffset>map(ResumeTokenOffset::new)
          .orElse(startOperationTime.startFrom()
              .map(OperationTimeOffset::new)
              .orElse(null));

      if (offset == null) {
        return;
      }

      startOrContinueListening(offset);
    } else {
      stopListening();
    }
  }

  /**
   * Stops the change stream cursor from iterating. May not immediately take affect (depends on
   * {@link #maxAwaitTime} and whether or not a change is currently being processed).
   *
   * <p>Once stopped, may be resumed by {@link #startOrRefresh()}.
   */
  private synchronized void stopListening() {
    running.set(false);
    if (listener != null) {
      listener.cancel(true);
      listener = null;
    }
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
  }

  private synchronized void startOrContinueListening(ChangeOffset offset) {
    if (listener == null || listener.isDone()) {
      listener = executor.submit(() -> {
        try {
          // Thread may execute arbitrarily in future, check we have not lost lock by then.
          // This is not perfect and does not need to be; the lock may still be lost while processing.
          // Callbacks need to account for these races themselves, and there is no way around that.
          // This is simply an optimization.
          if (running.compareAndSet(false, true)) {
            ChangeStreamIterable<T> stream = collection
                .watch()
                .maxAwaitTime(maxAwaitTime.toMillis(), TimeUnit.MILLISECONDS);

            offset.configure(stream);

            MongoChangeStreamCursor<ChangeStreamDocument<T>> cursor = stream.cursor();

            while (running.get()) {
              try {
                ChangeStreamDocument<T> change = cursor.next();

                if (!running.get()) {
                  // TODO: log
                  break;
                }

                callback.accept(change);

                MongoResumeToken newResumeToken = new MongoResumeToken(change.getResumeToken());
                lock.commit(collectionNamespace, newResumeToken);
              } catch (LostLockException e) {
                log.warn("Lost lock while listening to change", e);
                break;
              } catch (Exception e) {
                log.error("Uncaught exception while listening to change", e);
                break;
              }
            }

            cursor.close();
          }
        } catch (Exception e) {
          log.error("Uncaught exception while listening to change", e);
        }
      });
    }
  }

  @Override
  public synchronized void close() throws IOException {
    closed.set(true);
    stopListening();
    executor.shutdown();
    try {
      executor.awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace(); // TODO
    }
    executor.shutdownNow();
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
}
