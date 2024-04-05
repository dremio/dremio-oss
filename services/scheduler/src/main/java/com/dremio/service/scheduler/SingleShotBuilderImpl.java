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
import static com.dremio.service.scheduler.Schedule.SingleShotBuilder;
import static com.dremio.service.scheduler.Schedule.SingleShotType;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/** Builder to create simple single shot {@code Schedule} instances */
final class SingleShotBuilderImpl implements SingleShotBuilder {
  private final Instant start;
  private ZoneId zoneId;

  SingleShotBuilderImpl() {
    this(Instant.now(), ZoneOffset.UTC);
  }

  SingleShotBuilderImpl(Instant start) {
    this(start, ZoneOffset.UTC);
  }

  SingleShotBuilderImpl(Instant at, ZoneId zoneId) {
    this.start = at;
    this.zoneId = zoneId;
  }

  @Override
  public SingleShotBuilder withTimeZone(ZoneId zoneId) {
    this.zoneId = zoneId;
    return this;
  }

  @Override
  public ClusteredSingletonSingleShotBuilder asClusteredSingleton(String taskName) {
    return new ClusteredSingletonSingleShotBuilderImpl(this, taskName);
  }

  @Override
  public Schedule build() {
    // simple local schedules always have run only once
    return new BaseSchedule(SingleShotType.RUN_EXACTLY_ONCE, start, zoneId);
  }

  void setZoneId(ZoneId zoneId) {
    this.zoneId = zoneId;
  }

  ZoneId getZoneId() {
    return zoneId;
  }

  Instant getStart() {
    return start;
  }
}
