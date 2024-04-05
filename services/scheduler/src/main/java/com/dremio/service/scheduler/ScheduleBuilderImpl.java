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

import static com.dremio.service.scheduler.Schedule.Builder;
import static com.dremio.service.scheduler.Schedule.ClusteredSingletonBuilder;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAdjuster;
import java.time.temporal.TemporalAmount;

/** Builder to create simple {@code Schedule} instances */
final class ScheduleBuilderImpl implements Builder {
  private final TemporalAmount amount;
  private Instant start;
  private TemporalAdjuster adjuster;
  private ZoneId zoneId;

  ScheduleBuilderImpl(TemporalAmount amount) {
    this(Instant.now(), amount, NO_ADJUSTMENT, ZoneOffset.UTC);
  }

  ScheduleBuilderImpl() {
    this(Instant.now(), null, NO_ADJUSTMENT, ZoneOffset.UTC);
  }

  ScheduleBuilderImpl(Instant at, TemporalAmount amount, TemporalAdjuster adjuster, ZoneId zoneId) {
    this.start = at;
    this.amount = amount;
    this.adjuster = adjuster;
    this.zoneId = zoneId;
  }

  Builder withAdjuster(TemporalAdjuster adjuster) {
    this.adjuster = adjuster;
    return this;
  }

  @Override
  public Builder withTimeZone(ZoneId zoneId) {
    this.zoneId = zoneId;
    return this;
  }

  @Override
  public Builder startingAt(Instant start) {
    this.start = start;
    return this;
  }

  @Override
  public ClusteredSingletonBuilder asClusteredSingleton(String taskName) {
    return new ClusteredSingletonBuilderImpl(this, taskName);
  }

  @Override
  public Schedule build() {
    return new BaseSchedule(start, amount, adjuster, zoneId);
  }

  /**
   * Combine temporal adjusters by executing them sequentially. Order matters obviously...
   *
   * @param adjusters the adjusters to combine
   * @return an adjuster
   */
  static TemporalAdjuster combine(TemporalAdjuster... adjusters) {
    final ImmutableList<TemporalAdjuster> copyOf = ImmutableList.copyOf(adjusters);
    return temporal -> {
      Temporal result = temporal;
      for (TemporalAdjuster adjuster : copyOf) {
        result = result.with(adjuster);
      }
      return result;
    };
  }

  /**
   * A temporal adjuster which leaves the temporal untouched. In a Java8 world, this helper can
   * probably be removed safely and replaced by identity
   */
  static final TemporalAdjuster NO_ADJUSTMENT = temporal -> temporal;

  /**
   * Wraps an adjuster to round the temporal to the next period if the adjusted time is before the
   * temporal instant.
   *
   * @param amount the amount to add to the adjusted time if before the temporal argument of {@code
   *     TemporarlAdjuster#adjustInto()} is after the adjusted time
   * @param adjuster the adjuster to wrap
   * @return an adjuster
   */
  static TemporalAdjuster sameOrNext(final TemporalAmount amount, final TemporalAdjuster adjuster) {
    return temporal -> {
      Temporal adjusted = temporal.with(adjuster);
      return (ChronoUnit.NANOS.between(temporal, adjusted) >= 0)
          ? adjusted
          : temporal.plus(amount).with(adjuster);
    };
  }

  /**
   * Adjust a temporal to the given day of the month
   *
   * @param dayOfMonth the day of the month to adjust the temporal to
   * @return a temporal adjuster
   * @throws IllegalArgumentException if {@code dayOfMonth} is invalid
   */
  static TemporalAdjuster dayOfMonth(final int dayOfMonth) {
    ChronoField.DAY_OF_MONTH.checkValidIntValue(dayOfMonth);

    return temporal -> {
      long adjustedDayOfMonth =
          Math.min(dayOfMonth, temporal.range(ChronoField.DAY_OF_MONTH).getMaximum());
      return temporal.with(ChronoField.DAY_OF_MONTH, adjustedDayOfMonth);
    };
  }

  /**
   * Check that interval (as X hours, hours, weeks...) is strictly positive
   *
   * @param interval the value to check
   * @return interval
   */
  static int checkInterval(int interval) {
    Preconditions.checkArgument(
        interval > 0, "interval should be greater than 0, was %s", interval);
    return interval;
  }

  /** The following methods are only for package local builders */
  void setZoneId(ZoneId zoneId) {
    this.zoneId = zoneId;
  }

  void setStart(Instant start) {
    this.start = start;
  }

  TemporalAmount getAmount() {
    return amount;
  }

  Instant getStart() {
    return start;
  }

  TemporalAdjuster getAdjuster() {
    return adjuster;
  }

  ZoneId getZoneId() {
    return zoneId;
  }
}
