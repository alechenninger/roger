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
