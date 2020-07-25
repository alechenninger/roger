/*
 * MIT License
 *
 * Copyright (c) 2020 Alec Henninger
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
