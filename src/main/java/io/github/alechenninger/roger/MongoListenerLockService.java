/*
 * MIT License
 *
 * Copyright (c) 2020 Alec Henninger
 */

package io.github.alechenninger.roger;

import static com.mongodb.ErrorCategory.DUPLICATE_KEY;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.not;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

import com.google.common.collect.ImmutableMap;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoCommandException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonDocument;
import org.bson.Document;
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

  public Duration leaseTime() {
    return leaseTime;
  }

  /**
   * Attempts to acquire the lock for the current listener ID, or refresh a lock already held by
   * the current listener ID (extending its lease). If the lock is acquired, a
   * {@link ListenerLock} will be returned. Otherwise, will return {@link Optional#empty()}.
   *
   * <p>Only one listener ID will hold a lock for a given {@code resource} at any time.
   *
   * <p>A listeners lease may expire however so it is necessary to still use a kind of fencing
   * token, like an increasing version number, when taking actions which require the lock.
   *
   * @param resource The resource to lock.
   * @return {@code Optional} with a {@link ListenerLock} if the lock is held by this listener,
   * otherwise an empty optional if the lock is held by a different listener.
   */
  public Optional<ListenerLock> acquireOrRefreshFor(String resource) {
    log.debug("Attempt acquire or refresh lock. resource={} listenerId={}", resource, listenerId);

    try {
      final BsonDocument found = collection
          .withWriteConcern(WriteConcern.MAJORITY)
          .findOneAndUpdate(
              and(
                  eq("_id", resource),
                  or(lockIsExpired(), eq("listenerId", listenerId))),
              singletonList(combine(
                  set("listenerId", listenerId),
                  set("version", sameIfRefreshOtherwiseIncrement()),
                  set("expiresAt", clock.instant().plus(leaseTime)))),
              new FindOneAndUpdateOptions()
                  .projection(include("resumeToken", "version"))
                  .returnDocument(ReturnDocument.AFTER)
                  .upsert(true));

      if (found == null) {
        throw new IllegalStateException(
            "No lock document upserted, but none found. This should never happen.");
      }

      final ListenerLock lock = new ListenerLock(
          found.getNumber("version"),
          found.getDocument("resumeToken", null));

      log.debug("Lock acquired or refreshed. resource={} listenerId={} lockVersion={} resumeToken={}",
          resource, listenerId, lock.version(), lock.resumeToken());

      return Optional.of(lock);
    } catch (MongoCommandException e) {
      final ErrorCategory errorCategory = ErrorCategory.fromErrorCode(e.getErrorCode());

      if (errorCategory.equals(DUPLICATE_KEY)) {
        log.debug("Lock owned by another listener. resource={} myListenerId={}", resource, listenerId);
        return Optional.empty();
      }

      log.error("Error trying to acquire or refresh lock resource={} listenerId={}",
          resource, listenerId, e);

      throw e;
    }
  }

  public void commit(String resource, BsonDocument resumeToken) throws LostLockException {
    UpdateResult result = collection
        .withWriteConcern(WriteConcern.MAJORITY)
        .updateOne(
            and(
                eq("_id", resource),
                eq("listenerId", listenerId)),
            combine(
                set("expiresAt", clock.instant().plus(leaseTime)),
                set("resumeToken", resumeToken)));

    if (result.getMatchedCount() == 0) {
      throw new LostLockException(resource, listenerId);
    }

    log.debug("Committed new resume token for lock. resource={} listenerId={} resumeToken={}",
        resource, listenerId, resumeToken);
  }

  private Bson lockIsExpired() {
    return or(
        eq("expiresAt", null),
        not(exists("expiresAt")),
        lte("expiresAt", clock.instant()));
  }

  private Document sameIfRefreshOtherwiseIncrement() {
    return new Document("$cond", new Document(ImmutableMap.of(
        "if", new Document("$ne", asList("$listenerId", listenerId)),
        "then", new Document("$ifNull", asList(
            new Document("$add", asList("$version", 1)),
            0)),
        "else", "$version")));
  }
}
