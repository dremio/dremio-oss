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

import com.google.common.base.Preconditions;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.TemporalAdjuster;
import java.time.temporal.TemporalAmount;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.IntSupplier;

/** Implements a staggered schedule to stagger the schedule */
public class StaggeredSchedule implements Schedule {
  private final Schedule delegate;
  private final long staggerRangeInSeconds;
  private final Random random;

  StaggeredSchedule(Schedule schedule, int staggerSeed, long staggerRange, TimeUnit timeUnit) {
    this.delegate = schedule;
    int factor = 1;
    switch (timeUnit) {
      case HOURS:
        factor = 3600;
        break;
      case MINUTES:
        factor = 60;
        break;
      case SECONDS:
        factor = 1;
        break;
      default:
        Preconditions.checkState(false, "Unsupported timeunit");
    }

    Iterator<Instant> scheduleItr = schedule.iterator();
    Instant first = scheduleItr.next();
    Preconditions.checkState(scheduleItr.hasNext(), "Schedule cannot be a single shot schedule");
    Instant second = scheduleItr.next();
    long periodInSeconds = second.getEpochSecond() - first.getEpochSecond();
    long staggerSeconds = 2 * staggerRange * factor;
    // cap the stagger to half of the task period
    this.staggerRangeInSeconds = Math.min(staggerSeconds, periodInSeconds);
    this.random = new Random(staggerSeed);
  }

  @Override
  public TemporalAmount getPeriod() {
    return delegate.getPeriod();
  }

  @Override
  public Iterator<Instant> iterator() {
    Iterator<Instant> iter = delegate.iterator();
    return new Iterator<Instant>() {
      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public Instant next() {
        Instant instant = iter.next();
        // adjust this by an amount derived from a random number
        long stagger = (long) ((random.nextDouble() - 0.5d) * staggerRangeInSeconds);
        return instant.plus(Duration.ofSeconds(stagger));
      }

      @Override
      public void remove() {
        iter.remove();
      }
    };
  }

  @Override
  public String getTaskName() {
    return delegate.getTaskName();
  }

  @Override
  public Long getScheduledOwnershipReleaseInMillis() {
    return delegate.getScheduledOwnershipReleaseInMillis();
  }

  @Override
  public boolean isDistributedSingleton() {
    return delegate.isDistributedSingleton();
  }

  @Override
  public boolean isToRunExactlyOnce() {
    return delegate.isToRunExactlyOnce();
  }

  @Override
  public SingleShotType getSingleShotType() {
    return delegate.getSingleShotType();
  }

  @Override
  public boolean isSticky() {
    return delegate.isSticky();
  }

  @Override
  public CleanupListener getCleanupListener() {
    return delegate.getCleanupListener();
  }

  @Override
  public IntSupplier getWeightProvider() {
    return delegate.getWeightProvider();
  }

  @Override
  public Function<Schedule, Schedule> getScheduleModifier() {
    return delegate.getScheduleModifier();
  }

  @Override
  public String getTaskGroupName() {
    return delegate.getTaskGroupName();
  }

  @Override
  public ZoneId getZoneId() {
    return delegate.getZoneId();
  }

  @Override
  public TemporalAdjuster getAdjuster() {
    return delegate.getAdjuster();
  }

  @Override
  public boolean isInLockStep() {
    return delegate.isInLockStep();
  }

  @Override
  public boolean isLargePeriodicity() {
    return delegate.isLargePeriodicity();
  }
}
