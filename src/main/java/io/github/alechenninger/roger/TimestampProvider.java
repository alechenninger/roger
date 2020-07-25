/*
 * MIT License
 *
 * Copyright (c) 2020 Alec Henninger
 */

package io.github.alechenninger.roger;

import org.bson.BsonTimestamp;

import java.util.Optional;

public interface TimestampProvider {
  Optional<BsonTimestamp> timestamp();
}
