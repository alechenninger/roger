package io.github.alechenninger.roger;

import com.mongodb.client.model.changestream.ChangeStreamDocument;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Listener to changes in the aggregate of a repository.
 * @param <T> the type of aggregate to listen to.
 */
public interface ChangeConsumer<T> extends BiConsumer<ChangeStreamDocument<T>, Long> {
  default ChangeConsumer<T> andThen(ChangeConsumer<T> after) {
    Objects.requireNonNull(after);
    return (t, token) -> { accept(t, token); after.accept(t, token); };
  }
}
