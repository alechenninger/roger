/*
 * MIT License
 *
 * Copyright (c) 2020 Alec Henninger
 */

package io.github.alechenninger.roger.testing;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

public class LogListener implements BeforeEachCallback {
  private final Logger logger;
  private final ListAppender<ILoggingEvent> listener;

  public LogListener(String listenTo) {
    Objects.requireNonNull(listenTo, "listenTo");
    logger = (Logger) LoggerFactory.getLogger(listenTo);
    listener = new ListAppender<>();
    listener.setContext(logger.getLoggerContext());
    listener.start();

    logger.addAppender(listener);
  }

  public LogListener(Class listenTo) {
    this(listenTo.getName());
  }

  public List<ILoggingEvent> logs() {
    if (listener == null) {
      throw new IllegalStateException("logs() called outside of test");
    }
    return listener.list;
  }

  public List<String> messages() {
    return logs().stream().map(ILoggingEvent::getMessage).collect(Collectors.toList());
  }

  public List<String> messages(Level level) {
    return logs().stream()
        .filter(l -> l.getLevel().isGreaterOrEqual(level))
        .map(ILoggingEvent::getMessage)
        .collect(Collectors.toList());
  }

  public List<String> messagesMarked(Marker marker) {
    return logs().stream()
        .filter(l -> l.getMarker().equals(marker))
        .map(ILoggingEvent::getMessage)
        .collect(Collectors.toList());
  }

  public boolean contains(Marker marker) {
    return logs().stream().anyMatch(l -> marker.equals(l.getMarker()));
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    listener.list.clear();
  }
}
