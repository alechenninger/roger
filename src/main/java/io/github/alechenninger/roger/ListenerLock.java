/*
 * MIT License
 *
 * Copyright (c) 2020 Alec Henninger
 */

package io.github.alechenninger.roger;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonDocument;
import org.bson.BsonNumber;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;

class ListenerLock {
  private final long version;

  @Nullable
  private final BsonDocument resumeToken;

  public ListenerLock(BsonNumber version, @Nullable BsonDocument resumeToken) {
    Objects.requireNonNull(version, "fencingToken");
    this.version = version.longValue();
    this.resumeToken = resumeToken;
  }

  /**
   * The resume token document from a previous {@link ChangeStreamDocument}.
   */
  public Optional<BsonDocument> resumeToken() {
    return Optional.ofNullable(resumeToken);
  }

  public long version() {
    return version;
  }

  @Override
  public String toString() {
    return "ListenerLock{" +
        "fencingToken=" + version +
        ", resumeToken=" + resumeToken +
        '}';
  }
}
