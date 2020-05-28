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

package io.github.alechenninger.roger.testing;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class Defer implements AfterEachCallback {
  private final List<Closeable> toClose = new ArrayList<>();

  private static final Logger log = LoggerFactory.getLogger(Defer.class);

  public <T extends Closeable> T close(T closeable) {
    if (closeable != null) {
      toClose.add(closeable);
    }
    return closeable;
  }

  public <T> T that(T it, Consumer<T> cb) {
    toClose.add(() -> cb.accept(it));
    return it;
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    for (Closeable closeable : Lists.reverse(toClose)) {
      try {
        closeable.close();
      } catch (IOException e) {
        log.warn("Failed to close {}", closeable, e);
      }
    }
  }

}
