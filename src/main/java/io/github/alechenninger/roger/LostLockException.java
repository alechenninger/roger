/*
 * MIT License
 *
 * Copyright (c) 2020 Alec Henninger
 */

package io.github.alechenninger.roger;

public class LostLockException extends RuntimeException {
  public LostLockException(String message) {
    super(message);
  }

  public LostLockException(String resource, String listenerId) {
    super(listenerId + " lost lock to " + resource);
  }
}
