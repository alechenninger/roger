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

import com.mongodb.ErrorCategory;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import io.github.alechenninger.roger.MongoChangeListener.MongoResumeToken;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

public class MongoListenerLockService {
  private final Clock clock;
  private final MongoCollection<BsonDocument> collection;
  private final String listenerId;
  private final Duration leaseTime;

  private static final Logger log = LoggerFactory.getLogger(MongoListenerLockService.class);

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
    log.debug("Attempt acquire or refresh lock. resource={} listenerId={}", resource, listenerId);

    try {
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

      log.debug("Locked resource={} listenerId={}", resource, listenerId);

      return Optional.ofNullable(found)
          .map(s -> new MongoResumeToken(found.getDocument("resumeToken", null)));

    } catch (MongoCommandException e) {
      if (ErrorCategory.fromErrorCode(e.getErrorCode()).equals(ErrorCategory.DUPLICATE_KEY)) {
        log.debug("Lost race to lock resource={} listenerId={}", resource, listenerId);
        return Optional.empty();
      }

      log.error("Error trying to acquire or refresh lock resource={} listenerId={}",
          resource, listenerId, e);

      throw e;
    }
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
        lte("expiresAt", clock.instant())
    );
  }
}
