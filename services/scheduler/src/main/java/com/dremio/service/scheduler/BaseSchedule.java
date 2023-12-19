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

import static com.dremio.service.scheduler.ScheduleBuilderImpl.NO_ADJUSTMENT;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.TemporalAdjuster;
import java.time.temporal.TemporalAmount;
import java.util.Collections;
import java.util.Iterator;
import java.util.function.Function;

import com.google.common.base.Preconditions;

class BaseSchedule implements Schedule {
  private final TemporalAmount amount;
  private final TemporalAdjuster adjuster;
  private final Instant at;
  private final ZoneId zoneId;
  private final String taskName;
  private final Long scheduledOwnershipRelease;
  private final CleanupListener cleanupListener;
  private final String taskGroupName;
  private final Function<Schedule, Schedule> scheduleModifier;
  private final SingleShotType singleShotType;
  private final boolean inLockStep;
  private final boolean sticky;

  // constructor for multi shot local schedules
  BaseSchedule(Instant at, TemporalAmount period, TemporalAdjuster adjuster, ZoneId zoneId) {
    this(null, at, period, adjuster, zoneId, null, null, () -> {}, (x) -> null, null, false, true);
  }

  // constructor for single shot local schedules
  BaseSchedule(SingleShotType singleShotType, Instant at, ZoneId zoneId) {
    this(singleShotType, at, null, NO_ADJUSTMENT, zoneId, null, null, () -> {}, (x) -> null, null, false, true);
  }

  // constructor for multi shot distributed schedule
  BaseSchedule(Instant at, TemporalAmount period, TemporalAdjuster adjuster, ZoneId zoneId,
               String taskName, Long scheduledOwnershipRelease, CleanupListener cleanupListener,
               Function<Schedule, Schedule> scheduleModifier, String taskGroupName, boolean sticky) {
    this(null, at, period, adjuster, zoneId, taskName, scheduledOwnershipRelease, cleanupListener, scheduleModifier,
      taskGroupName, false, sticky);
  }

  // constructor for single shot distributed schedules
  BaseSchedule(SingleShotType singleShotType, Instant at, ZoneId zoneId, String taskName, boolean inLockStep) {
    this(singleShotType, at, null, NO_ADJUSTMENT, zoneId, taskName, null, () -> {}, (x) -> null, null, inLockStep,
      true);
  }

  private BaseSchedule(SingleShotType singleShotType, Instant at, TemporalAmount period, TemporalAdjuster adjuster,
                       ZoneId zoneId, String taskName, Long scheduledOwnershipRelease, CleanupListener cleanupListener,
                       Function<Schedule, Schedule> scheduleModifier, String taskGroupName, boolean inLockStep,
                       boolean sticky) {
    this.singleShotType = singleShotType;
    this.amount = period;
    this.adjuster = adjuster;
    this.at = at;
    this.zoneId = zoneId;
    this.taskName = taskName;
    this.scheduledOwnershipRelease = scheduledOwnershipRelease;
    this.cleanupListener = cleanupListener;
    this.scheduleModifier = scheduleModifier;
    this.taskGroupName = taskGroupName;
    this.inLockStep = inLockStep;
    this.sticky = sticky;
  }

  @Override
  public TemporalAmount getPeriod() {
    return amount;
  }

  @Override
  public boolean isToRunExactlyOnce() {
    return SingleShotType.RUN_ONCE_EVERY_UPGRADE.equals(singleShotType);
  }

  @Override
  public SingleShotType getSingleShotType() {
    return singleShotType;
  }

  @Override
  public boolean isSticky() {
    return sticky;
  }

  @Override
  public Iterator<Instant> iterator() {
    if (amount == null) {
      // Single shot schedules has a single item
      return Collections.singletonList(this.at).iterator();
    }
    final LocalDateTime atLocal = LocalDateTime.ofInstant(this.at, zoneId);
    final LocalDateTime adjustedAt = atLocal.with(adjuster);
    final LocalDateTime start = adjustedAt.isBefore(atLocal)
      ? atLocal.plus(amount).with(adjuster)
      : adjustedAt;

    return new Iterator<Instant>() {
      private LocalDateTime next = start;

      @Override
      public boolean hasNext() {
        return true;
      }

      @Override
      public Instant next() {
        Instant result = next.atZone(zoneId).toInstant();
        next = next.plus(amount).with(adjuster);

        return result;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("Schedule iterator does not support remove operation.");
      }
    };
  }

  @Override
  public String getTaskName() {
    return taskName;
  }

  @Override
  public Long getScheduledOwnershipReleaseInMillis() {
    Preconditions.checkState(taskName != null,
      "Name of the task for which to release scheduled ownership is missing");
    return scheduledOwnershipRelease;
  }

  @Override
  public CleanupListener getCleanupListener() {
    return cleanupListener;
  }

  @Override
  public Function<Schedule, Schedule> getScheduleModifier() {
    return scheduleModifier;
  }

  @Override
  public String getTaskGroupName() {
    return taskGroupName;
  }

  @Override
  public ZoneId getZoneId() {
    return zoneId;
  }

  @Override
  public TemporalAdjuster getAdjuster() {
    return adjuster;
  }

  @Override
  public boolean isInLockStep() {
    return inLockStep;
  }

  @Override
  public String toString() {
    final String ss = singleShotType != null ? ", Single Shot Type: " + singleShotType.name() : "";
    return "{Task Name: " + taskName + ", " +
      "Period: " + getPeriod() + ", " +
      "At: " + at.toEpochMilli() + ", " +
      "Is In Lock Step: " + inLockStep + ss  + "}";
  }
}
