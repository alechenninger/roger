package io.github.alechenninger.roger;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.not;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import io.github.alechenninger.roger.MongoChangeListener.MongoResumeToken;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

public class MongoListenerLockService {
  private final Clock clock;
  private final MongoCollection<BsonDocument> collection;
  private final String listenerId;
  private final Duration leaseTime;

  public MongoListenerLockService(Clock clock, MongoCollection<BsonDocument> listenerLocks,
      String listenerId, Duration leaseTime) {
    this.clock = clock;
    this.collection = listenerLocks;
    this.listenerId = listenerId;
    this.leaseTime = leaseTime;
  }

  public static MongoListenerLockService withDefaults(MongoDatabase db) {
    return new MongoListenerLockService(
        Clock.systemUTC(),
        db.getCollection("ListenerLocks", BsonDocument.class),
        UUID.randomUUID().toString(),
        Duration.ofMinutes(5));
  }

  /**
   * Attempts to acquire the lock for the current listener ID, or refresh a lock already held by
   * the current listener ID (extending its lease). If the lock is acquired, a
   * {@link MongoResumeToken} will be returned. Otherwise, will return {@link Optional#empty()}.
   *
   * <p>Only one listener ID will hold a lock for a given {@code resource} at any time.
   *
   * <p>A listeners lease may expire however so it is necessary to still use a kind of fencing
   * token, like an increasing version number, when taking actions which require the lock.
   *
   * @param resource The resource to lock.
   * @return {@code Optional} with a {@link MongoResumeToken} if the lock is held by this listener,
   * otherwise an empty optional if the lock is held by a different listener.
   */
  public Optional<MongoResumeToken> acquireOrRefreshFor(String resource) {
    final BsonDocument found = collection.findOneAndUpdate(
        and(
            eq("_id", resource),
            or(lockIsExpired(), eq("listenerId", listenerId))),
        combine(
            set("expiresAt", clock.instant().plus(leaseTime)),
            set("listenerId", listenerId)),
        new FindOneAndUpdateOptions()
            .projection(include("resumeToken"))
            .returnDocument(ReturnDocument.AFTER)
            .upsert(true));
    return Optional.ofNullable(found)
        .map(s -> new MongoResumeToken(found.getDocument("resumeToken", null)));
  }

  public void commit(String resource, MongoResumeToken resumeToken) throws LostLockException {
    collection.updateOne(
        and(
            eq("_id", resource),
            eq("listenerId", listenerId)),
        combine(
            set("expiresAt", clock.instant().plus(leaseTime)),
            set("resumeToken", resumeToken.document())));
  }

  private Bson lockIsExpired() {
    return or(
        eq("expiresAt", null),
        not(exists("expiresAt")),
        lte("expiresAt", clock.instant()),
        // Treat all locks as expired until the first change is processed.
        // The first change is processed when there is a resumeToken stored, hence also looking for
        // missing resumeToken.
        // This is a hack to get around lack of startAtOperationTime added in 4.0.
        // If we can use mongo 4+, we can improve this.
        // The reason we want all processes to get a lock if they ask, is because unless
        // we get 4+, we have to guarantee a listener is running before the first
        // insert. The only way to guarantee–and even then it is not truly a guarantee–
        // is if every process tries to get a lock, and waits to start until it thinks
        // someone is listening.
        not(exists("resumeToken"))
    );
  }
}
