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

import static com.dremio.service.scheduler.Schedule.ClusteredSingletonBuilder;

import com.google.common.base.Preconditions;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Builder Implementation to create multi shot {@code Schedule} instances for clustered singleton
 */
final class ClusteredSingletonBuilderImpl implements ClusteredSingletonBuilder {
  private final ScheduleBuilderImpl scheduleBuilder;
  private final String taskName;
  private Long scheduledOwnershipRelease;
  private CleanupListener cleanupListener;
  private Function<Schedule, Schedule> scheduleModifier;
  private String taskGroupName;
  private boolean sticky;

  ClusteredSingletonBuilderImpl(ScheduleBuilderImpl scheduleBuilder, String taskName) {
    this.scheduleBuilder = scheduleBuilder;
    this.taskName = taskName;
    this.scheduledOwnershipRelease = null;
    this.cleanupListener = () -> {};
    this.scheduleModifier = (x) -> null;
    this.taskGroupName = null;
    this.sticky = false;
  }

  ClusteredSingletonBuilderImpl(Schedule old) {
    this(old, old.getPeriod());
  }

  ClusteredSingletonBuilderImpl(Schedule old, TemporalAmount newAmount) {
    Preconditions.checkState(
        old.getSingleShotType() == null, "Modifications are not allowed for single shot types");
    Preconditions.checkState(
        !old.isInLockStep(), "Modifications are not allowed for Lock step schedules");
    Instant at = ZonedDateTime.now(old.getZoneId()).toInstant().plus(newAmount);
    this.scheduleBuilder =
        new ScheduleBuilderImpl(at, newAmount, old.getAdjuster(), old.getZoneId());
    // task name cannot be changed
    this.taskName = old.getTaskName();
    this.scheduledOwnershipRelease = old.getScheduledOwnershipReleaseInMillis();
    this.cleanupListener = old.getCleanupListener();
    this.scheduleModifier = old.getScheduleModifier();
    this.taskGroupName = old.getTaskGroupName();
    this.sticky = old.isSticky();
  }

  @Override
  public ClusteredSingletonBuilder withTimeZone(ZoneId zoneId) {
    scheduleBuilder.setZoneId(zoneId);
    return this;
  }

  @Override
  public ClusteredSingletonBuilder startingAt(Instant start) {
    scheduleBuilder.setStart(start);
    return this;
  }

  @Override
  public ClusteredSingletonBuilder asClusteredSingleton(String taskName) {
    return this;
  }

  @Override
  public ClusteredSingletonBuilder scheduleModifier(Function<Schedule, Schedule> scheduleModifier) {
    this.scheduleModifier = scheduleModifier;
    return this;
  }

  @Override
  public ClusteredSingletonBuilder taskGroup(String taskGroupName) {
    this.taskGroupName = taskGroupName;
    return this;
  }

  @Override
  public ClusteredSingletonBuilder sticky() {
    this.sticky = true;
    return this;
  }

  @Override
  public ClusteredSingletonBuilder releaseOwnershipAfter(long number, TimeUnit timeUnit) {
    this.scheduledOwnershipRelease = timeUnit.toMillis(number);
    return this;
  }

  @Override
  public ClusteredSingletonBuilder withCleanup(CleanupListener cleanupListener) {
    this.cleanupListener = cleanupListener;
    return this;
  }

  @Override
  public Schedule build() {
    return new BaseSchedule(
        scheduleBuilder.getStart(),
        scheduleBuilder.getAmount(),
        scheduleBuilder.getAdjuster(),
        scheduleBuilder.getZoneId(),
        taskName,
        scheduledOwnershipRelease,
        cleanupListener,
        scheduleModifier,
        taskGroupName,
        sticky);
  }
}
