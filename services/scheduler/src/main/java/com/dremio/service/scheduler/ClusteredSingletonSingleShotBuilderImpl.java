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

import static com.dremio.service.scheduler.Schedule.ClusteredSingletonSingleShotBuilder;
import static com.dremio.service.scheduler.Schedule.SingleShotType;

import com.google.common.base.Preconditions;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * Builder Implementation to create single shot {@code Schedule} instances for clustered singleton
 */
final class ClusteredSingletonSingleShotBuilderImpl implements ClusteredSingletonSingleShotBuilder {
  private final SingleShotBuilderImpl scheduleBuilder;
  private final String taskName;
  private boolean inLockStep;
  private SingleShotType singleShotType;

  ClusteredSingletonSingleShotBuilderImpl(SingleShotBuilderImpl scheduleBuilder, String taskName) {
    this.scheduleBuilder = scheduleBuilder;
    this.taskName = taskName;
    this.inLockStep = false;
    this.singleShotType = SingleShotType.RUN_EXACTLY_ONCE;
  }

  @Override
  public ClusteredSingletonSingleShotBuilder withTimeZone(ZoneId zoneId) {
    scheduleBuilder.setZoneId(zoneId);
    return this;
  }

  @Override
  public ClusteredSingletonSingleShotBuilder asClusteredSingleton(String taskName) {
    return this;
  }

  @Override
  public ClusteredSingletonSingleShotBuilder inLockStep() {
    this.inLockStep = true;
    return this;
  }

  @Override
  public ClusteredSingletonSingleShotBuilder setSingleShotType(SingleShotType singleShotType) {
    this.singleShotType = singleShotType;
    return this;
  }

  @Override
  public Schedule build() {
    if (inLockStep) {
      Preconditions.checkArgument(
          !singleShotType.equals(SingleShotType.RUN_ONCE_EVERY_MEMBER_DEATH),
          "Lock step schedules cannot be done for run once on death schedules");
      Preconditions.checkArgument(
          scheduleBuilder.getStart().isBefore(Instant.now().plusMillis(10L)),
          "Lock step schedules must be immediate");
      Preconditions.checkArgument(
          scheduleBuilder.getZoneId().equals(ZoneOffset.UTC),
          "Lock step schedules must be in default UTC time zone");
    }
    return new BaseSchedule(
        singleShotType,
        scheduleBuilder.getStart(),
        scheduleBuilder.getZoneId(),
        taskName,
        inLockStep);
  }
}
