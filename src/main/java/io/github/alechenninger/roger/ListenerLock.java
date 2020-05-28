/*
 * Copyright (C) 2020  Alec Henninger
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
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
