package io.github.alechenninger.roger;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
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
  private final CountDownLatch initialization = new CountDownLatch(1);
  private final Duration maxAwaitTime;
  private final MongoCollection<T> collection;
  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private volatile Future<?> listener;

  private static final Logger log = LoggerFactory.getLogger(MongoChangeListener.class);

  public MongoChangeListener(MongoListenerLockService lock,
      Consumer<ChangeStreamDocument<T>> callback, Duration maxAwaitTime,
      MongoCollection<T> collection) {
    this.lock = lock;
    this.collectionNamespace = collection.getNamespace().getFullName();
    this.callback = callback;
    this.maxAwaitTime = maxAwaitTime;
    this.collection = collection;
  }

  public synchronized void startOrRefresh() {
    if (closed.get()) {
      throw new IllegalStateException("Cannot start again once closed.");
    }

    final Optional<MongoResumeToken> resumeToken = lock.acquireOrRefreshFor(collectionNamespace);
    if (resumeToken.isPresent()) {
      startOrContinueListening(resumeToken.get());
    } else {
      initialization.countDown();
      stopListening();
    }
  }

  /**
   * Blocks until at least one listener among the replicaset has a lock and cursor open.
   *
   * <p>This is necessary unless/until we can run Mongo 4+, which supports listening to changes
   * after a timestamp instead of after a resume token, allowing us to get past changes even without
   * a resume token. It's not a problem once at least one change has been listened to and committed.
   */
  public void awaitInitialization() throws InterruptedException {
    initialization.await();
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

  private synchronized void startOrContinueListening(MongoResumeToken resumeToken) {
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

            BsonDocument tokenDoc = resumeToken.document();
            if (tokenDoc != null) {
              stream.resumeAfter(tokenDoc);
            }
            // TODO: Use something like this once we are using Mongo 4+
//            } else {
//              stream.startAtOperationTime(new BsonTimestamp(0));
//            }

            MongoChangeStreamCursor<ChangeStreamDocument<T>> cursor = stream.cursor();

            // Not considered initialized until cursor is open; this means writes will definitely be
            // seen by the stream unless the cursor stops for some reason.
            // See other notes about starting change streams pre Mongo 4+.
            initialization.countDown();

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
