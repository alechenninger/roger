package io.github.alechenninger.roger;

import org.bson.BsonTimestamp;

import java.util.Optional;

public interface StartOperationTime {
  Optional<BsonTimestamp> startFrom();
}