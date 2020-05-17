package io.github.alechenninger.roger;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Listener to changes in the aggregate of a repository.
 * @param <T> the type of aggregate to listen to.
 */
public interface ChangeConsumer<T> extends Consumer<T> {
  default ChangeConsumer<T> andThen(ChangeConsumer<? super T> after) {
    Objects.requireNonNull(after);
    return (T t) -> { accept(t); after.accept(t); };
  }
}
