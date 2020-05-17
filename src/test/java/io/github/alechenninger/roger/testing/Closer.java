package io.github.alechenninger.roger.testing;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Closer implements AfterEachCallback {
  private final List<Closeable> toClose = new ArrayList<>();

  private static final Logger log = LoggerFactory.getLogger(Closer.class);

  public <T extends Closeable> T closeItAfterTest(T closeable) {
    if (closeable != null) {
      toClose.add(closeable);
    }
    return closeable;
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    for (Closeable closeable : toClose) {
      try {
        closeable.close();
      } catch (IOException e) {
        log.warn("Failed to close {}", closeable, e);
      }
    }
  }
}
