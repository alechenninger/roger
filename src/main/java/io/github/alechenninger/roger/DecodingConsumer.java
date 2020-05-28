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
import com.mongodb.client.model.changestream.UpdateDescription;
import org.bson.BsonDocumentReader;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;

class DecodingConsumer<T> implements ChangeConsumer<T> {
  private final Codec<T> codec;
  private final BiConsumer<T, Long> callback;

  private static final Logger log = LoggerFactory.getLogger(DecodingConsumer.class);

  public DecodingConsumer(Codec<T> codec, BiConsumer<T, Long> callback) {
    this.codec = codec;
    this.callback = callback;
  }

  public static <T> DecodingConsumer<T> decoded(Codec<T> codec, BiConsumer<T, Long> callback) {
    return new DecodingConsumer<>(codec, callback);
  }

  @Override
  public void accept(ChangeStreamDocument<T> change, Long lockVersion) {
    if (change.getFullDocument() == null) {
      UpdateDescription update = change.getUpdateDescription();
      if (update == null) {
        log.info("Change had neither full document, nor update description; " +
            "nothing to process. change={}", change);
        return;
      }
      BsonDocumentReader reader = new BsonDocumentReader(update.getUpdatedFields());
      final T fromUpdate = codec.decode(reader, DecoderContext.builder().build());
      callback.accept(fromUpdate, lockVersion);
    } else {
      callback.accept(change.getFullDocument(), lockVersion);
    }
  }
}
