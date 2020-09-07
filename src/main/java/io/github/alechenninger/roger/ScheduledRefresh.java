/*
 * MIT License
 *
 * Copyright (c) 2020 Alec Henninger
 */

package io.github.alechenninger.roger;

import io.github.alechenninger.roger.MongoChangeListenerFactory.RefreshStrategy;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Schedules a periodic refresh.
 * @see #auto()
 */
public class ScheduledRefresh implements RefreshStrategy {
  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
  private final BiConsumer<Duration, Scheduler> scheduleIt;

  /**
   * @param scheduleIt A function which configures the schedule. Accepts two arguments. One is a
   *                   {@link Scheduler} which is what must be configured with the desired schedule.
   *                   The other argument is a lease {@link Duration} which may be used to inform
   *                   the schedule.
   */
  public ScheduledRefresh(BiConsumer<Duration, Scheduler> scheduleIt) {
    this.scheduleIt = scheduleIt;
  }

  public static ScheduledRefresh every(Duration period) {
    if (period.isNegative() || period.isZero()) {
      throw new IllegalArgumentException("Period must be > 0 but got " + period);
    }

    return new ScheduledRefresh((lease, scheduler) -> scheduler.fixedRate(Duration.ZERO, period));
  }

  /**
   * @return A {@link ScheduledRefresh} which automatically refreshes at a reasonable interval based
   * on the lease time of the lock.
   */
  public static ScheduledRefresh auto() {
    return new ScheduledRefresh((lease, scheduler) -> {
      if (lease.isNegative()) {
        throw new IllegalArgumentException("Lease time must not be negative but got " + lease);
      }

      scheduler.fixedRate(Duration.ZERO, lease.dividedBy(2));
    });
  }

  @Override
  public void scheduleInBackground(Runnable refresh, Duration leaseTime) {
    scheduleIt.accept(leaseTime, new Scheduler(executor, refresh));
  }

  @Override
  public void close() throws IOException {
    executor.shutdown();
    try {
      executor.awaitTermination(15, TimeUnit.SECONDS);
    } catch (InterruptedException ignored) {
      // TODO: log
    }
    executor.shutdownNow();
  }

  public static class Scheduler {

    private final ScheduledExecutorService executor;
    private final Runnable refresh;

    private Scheduler(ScheduledExecutorService executor, Runnable refresh) {
      this.executor = executor;
      this.refresh = refresh;
    }

    public void fixedRate(Duration initialDelay, Duration period) {
      executor.scheduleAtFixedRate(refresh,
          initialDelay.toMillis(),
          period.toMillis(),
          TimeUnit.MILLISECONDS);
    }

    public void fixedDelay(Duration initialDelay, Duration delay) {
      executor.scheduleWithFixedDelay(refresh,
          initialDelay.toMillis(),
          delay.toMillis(),
          TimeUnit.MILLISECONDS);
    }
  }
}
