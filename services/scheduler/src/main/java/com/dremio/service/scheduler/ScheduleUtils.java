/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.service.scheduler;

import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 *
 * Utility functions for Scheduled tasks
 */
public final class ScheduleUtils {

  /**
   * cancel the list of cancellables
   * @param mayInterruptIfRunning interrupt the thread if the operation is currently running
   * @param cancellables
   */
  public static void cancel(boolean mayInterruptIfRunning, Cancellable... cancellables) {
    for (Cancellable cancellable:cancellables) {
      if (cancellable != null && !cancellable.isCancelled()) {
        cancellable.cancel(mayInterruptIfRunning);
      }
    }
  }

  /**
   * Create a schedule to run a task exactly once at the given time.
   *
   * @param instant the task will run
   * @return schedule with the time in millis
   */
  public static Schedule scheduleForRunningOnceAt(final Instant instant) {
    return new Schedule() {
      @Override
      public TemporalAmount getPeriod() {
        return null;
      }

      @Override
      public Iterator<Instant> iterator() {
        return Collections.singletonList(instant)
            .iterator();
      }

      @Override
      public String getTaskName() {
        return null;
      }

      @Override
      public Long getScheduledOwnershipReleaseInMillis() {
        return null;
      }
    };
  }

  /**
   * Create a schedule to run a task exactly once at the given time.
   * for a service with given name and relinquishing leadership
   * at a given interval
   *
   * @param instant the task will run
   * @param taskName name of the task in the global world
   * @param number period to release leadership
   * @param timeUnit time unit to release leadership
   * @return schedule with the time in millis
   */
  public static Schedule scheduleForRunningOnceAt(final Instant instant, final String taskName,
                                                  long number, TimeUnit timeUnit) {
    return new Schedule() {
      @Override
      public TemporalAmount getPeriod() {
        return null;
      }

      @Override
      public Iterator<Instant> iterator() {
        return Collections.singletonList(instant)
          .iterator();
      }

      @Override
      public String getTaskName() {
        return taskName;
      }

      @Override
      public Long getScheduledOwnershipReleaseInMillis() {
        return timeUnit.toMillis(number);
      }
    };
  }

  /**
   * Create a schedule to run a task exactly once at the given time.
   * for a service with given name and relinquishing leadership
   * at a given interval
   *
   * @param instant the task will run
   * @param taskName name of the task in the global world
   * @param number period to release leadership
   * @param timeUnit time unit to release leadership
   * @param cleanupListener cleanup listener to clean up upon cancel
   * @return schedule with the time in millis
   */
  public static Schedule scheduleForRunningOnceAt(final Instant instant, final String taskName,
                                                  long number, TimeUnit timeUnit, final CleanupListener cleanupListener) {
    return new Schedule() {
      @Override
      public TemporalAmount getPeriod() {
        return null;
      }

      @Override
      public Iterator<Instant> iterator() {
        return Collections.singletonList(instant)
          .iterator();
      }

      @Override
      public String getTaskName() {
        return taskName;
      }

      @Override
      public Long getScheduledOwnershipReleaseInMillis() {
        return timeUnit.toMillis(number);
      }

      @Override
      public CleanupListener getCleanupListener() {
        return cleanupListener;
      }
    };
  }

  /**
   * Create a schedule to run a task exactly once at the given time.
   * for a service with given name and never relinquishing leadership
   *
   * @param instant the task will run
   * @param taskName name of the task in the global world
   * @return schedule with the time in millis
   */
  public static Schedule scheduleForRunningOnceAt(final Instant instant, final String taskName) {
    return new Schedule() {
      @Override
      public TemporalAmount getPeriod() {
        return null;
      }

      @Override
      public Iterator<Instant> iterator() {
        return Collections.singletonList(instant)
          .iterator();
      }

      @Override
      public String getTaskName() {
        return taskName;
      }

      @Override
      public Long getScheduledOwnershipReleaseInMillis() {
        return null;
      }
    };
  }

  /**
   * Create a schedule to run a task exactly once at the given time.
   * for a service with given name and never relinquishing leadership
   *
   * @param instant the task will run
   * @param taskName name of the task in the global world
   * @param cleanupListener cleanup listener to clean up upon cancel
   * @return schedule with the time in millis
   */
  public static Schedule scheduleForRunningOnceAt(final Instant instant, final String taskName,
                                                  final CleanupListener cleanupListener) {
    return new Schedule() {
      @Override
      public TemporalAmount getPeriod() {
        return null;
      }

      @Override
      public Iterator<Instant> iterator() {
        return Collections.singletonList(instant)
          .iterator();
      }

      @Override
      public String getTaskName() {
        return taskName;
      }

      @Override
      public Long getScheduledOwnershipReleaseInMillis() {
        return null;
      }

      @Override
      public CleanupListener getCleanupListener() { return cleanupListener; }
    };
  }

  /**
   * Create a schedule to run a task exactly once now.
   * for a service with given name and never relinquishing leadership
   * @param taskName name of the task in the global world
   * @return schedule with the time in millis
   */
  public static Schedule scheduleToRunOnceNow(final String taskName) {
    return new Schedule() {
      @Override
      public TemporalAmount getPeriod() {
        return null;
      }

      @Override
      public Iterator<Instant> iterator() {
        return Collections.singletonList(Instant.now())
          .iterator();
      }

      @Override
      public String getTaskName() {
        return taskName;
      }

      @Override
      public Long getScheduledOwnershipReleaseInMillis() {
        return null;
      }

      @Override
      public boolean isToRunOnce() {
        return true;
      }
    };
  }

  private ScheduleUtils() {
  }
}
