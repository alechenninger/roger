package io.github.alechenninger.roger;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonDocument;

import javax.annotation.Nullable;
import java.util.Optional;

class ListenerLock {

  @Nullable
  private final BsonDocument resumeToken;

  public ListenerLock(@Nullable BsonDocument resumeToken) {
    this.resumeToken = resumeToken;
  }

  /**
   * The resume token document from a previous {@link ChangeStreamDocument}.
   */
  public Optional<BsonDocument> resumeToken() {
    return Optional.ofNullable(resumeToken);
  }
}
