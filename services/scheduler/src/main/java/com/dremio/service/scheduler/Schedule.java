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

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAdjuster;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.TemporalAmount;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;


/**
 * A recurring task schedule.
 *
 * A schedule is comprised of a starting point and a set of periods
 * and is used to generate a sequence of instants where a task should be
 * run.
 */
public interface Schedule extends Iterable<Instant> {
  /**
   * Builder to create {@code Schedule} instances
   */
  final class Builder {
    private final Instant start;
    private final TemporalAmount amount;
    private final TemporalAdjuster adjuster;
    private final ZoneId zoneId;
    private final String taskName;
    private final Long scheduledOwnershipRelease;
    private final CleanupListener cleanupListener;

    private Builder(TemporalAmount amount) {
      this(Instant.now(), amount, NO_ADJUSTMENT, ZoneOffset.UTC);
    }

    private Builder(TemporalAmount amount, TemporalAdjuster adjuster) {
      this(Instant.now(), amount, adjuster, ZoneOffset.UTC);
    }

    private Builder(Instant start, TemporalAmount amount, TemporalAdjuster adjuster, ZoneId zoneId) {
      this(start, amount, adjuster, zoneId, null, null);
    }

    private Builder(Instant start,
                    TemporalAmount amount,
                    TemporalAdjuster adjuster,
                    ZoneId zoneId,
                    String taskName,
                    Long scheduledOwnershipRelease) {
      this(start, amount, adjuster, zoneId, taskName, scheduledOwnershipRelease,
        () -> { /*do nothing */ });
    }

    private Builder(Instant start,
                    TemporalAmount amount,
                    TemporalAdjuster adjuster,
                    ZoneId zoneId,
                    String taskName,
                    Long scheduledOwnershipRelease,
                    CleanupListener cleanupListener) {
      this.start = start;
      this.amount = amount;
      this.adjuster = adjuster;
      this.zoneId = zoneId;
      this.taskName = taskName;
      this.scheduledOwnershipRelease = scheduledOwnershipRelease;
      this.cleanupListener = cleanupListener;
    }

    private Builder withAdjuster(TemporalAdjuster adjuster) {
      return new Builder(this.start, this.amount, adjuster, zoneId,
        this.taskName, this.scheduledOwnershipRelease,
        this.cleanupListener);
    }

    public Builder withTimeZone(ZoneId zoneId) {
      return new Builder(this.start, this.amount, this.adjuster, zoneId,
        this.taskName, this.scheduledOwnershipRelease,
        this.cleanupListener);
    }


    public Builder startingAt(Instant start) {
      return new Builder(start, this.amount, this.adjuster, zoneId, this.taskName, this.scheduledOwnershipRelease,
        this.cleanupListener);
    }

    public final Schedule build() {
      return new BaseSchedule(
        start, amount, adjuster, zoneId,
        taskName, scheduledOwnershipRelease, cleanupListener);
    }

    public Builder asClusteredSingleton(String taskName) {
      return new Builder(
        start,
        amount,
        adjuster,
        zoneId,
        taskName,
        scheduledOwnershipRelease);
    }

    public Builder withCleanup(CleanupListener cleanupListener) {
      return new Builder(
        start,
        amount,
        adjuster,
        zoneId,
        taskName,
        scheduledOwnershipRelease,
        cleanupListener);
    }

    public Builder releaseOwnershipAfter(long number, TimeUnit timeUnit) {
      timeUnit.toMillis(number);
      return new Builder(
        start,
        amount,
        adjuster,
        zoneId,
        taskName,
        timeUnit.toMillis(number));
    }

    /**
     * Create a schedule builder where events are triggered every {@code seconds}
     *
     * @param millis the number of milliseconds between events
     * @return a schedule builder generating events every {@code seconds}
     * @throws IllegalArgumentException if {@code minutes} is negative
     */
    public static Builder everyMillis(long millis) {
      return new Builder(Duration.ofMillis(millis));
    }

    /**
     * Create a schedule builder where events are triggered every {@code seconds}
     *
     * @param seconds the number of seconds between events
     * @return a schedule builder generating events every {@code seconds}
     * @throws IllegalArgumentException if {@code minutes} is negative
     */
    public static Builder everySeconds(long seconds) {
      return new Builder(Duration.ofSeconds(seconds));
    }

    /**
     * Create a schedule builder where events are triggered every {@code minutes}
     *
     * @param minutes the number of minutes between events
     * @return a schedule builder generating events every {@code minutes}
     * @throws IllegalArgumentException if {@code minutes} is negative
     */
    public static Builder everyMinutes(long minutes) {
      return new Builder(Duration.ofMinutes(minutes));
    }

    /**
     * Create a schedule builder where events are triggered every {@code hours}
     *
     * @param hours the number of hours between events
     * @return a schedule builder generating events every {@code hours}
     * @throws IllegalArgumentException if {@code hours} is negative
     */
    public static Builder everyHours(int hours) {
      return new Builder(Duration.ofHours(checkInterval(hours)));
    }

    /**
     * Create a schedule builder where events are triggered every {@code days}
     *
     * @param days the number of days between events
     * @return a schedule builder generating events every {@code days}
     * @throws IllegalArgumentException if {@code days} is negative
     */
    public static Builder everyDays(int days) {
      return new Builder(Period.ofDays(checkInterval(days)));
    }

    /**
     * Create a schedule builder where events are triggered every {@code days}, at a specific time
     *
     * @param days the number of days between events
     * @param at the specific time to generate the events at
     * @return a schedule builder generating events every {@code days}
     * @throws IllegalArgumentException if {@code days} is negative
     * @throws NullPointerException if {@code at} is null
     */
    public static Builder everyDays(int days, LocalTime at) {
      Preconditions.checkNotNull(at);
      return everyDays(days).withAdjuster(sameOrNext(Period.ofDays(1), at));
    }

    /**
     * Create a schedule builder where events are triggered every {@code weeks}
     *
     * @param weeks the number of weeks between events
     * @return a schedule builder generating events every {@code weeks}
     * @throws IllegalArgumentException if {@code weeks} is negative
     */
    public static Builder everyWeeks(int weeks) {
      return new Builder(Period.ofWeeks(checkInterval(weeks)));
    }

    /**
     * Create a schedule build where events are triggered every {@code weeks}, at a specific day of week and time
     *
     * @param weeks the number of weeks between events
     * @param dayOfWeek the day of week when to generate events for
     * @param at the specific time to generate the events at
     * @return a schedule builder generating events every {@code weeks}
     * @throws IllegalArgumentException if {@code weeks} is negative
     * @throws NullPointerException if {@code dayOfWeek} or {@code at} is null
     */
    public static Builder everyWeeks(int weeks, DayOfWeek dayOfWeek, LocalTime at) {
      Preconditions.checkNotNull(dayOfWeek);
      Preconditions.checkNotNull(at);
      return everyWeeks(weeks).withAdjuster(sameOrNext(Period.ofWeeks(1), combine(dayOfWeek, at)));
    }

    /**
     * Create a schedule builder where events are triggered every {@code months}
     *
     * @param months the number of months between events
     * @return a schedule builder generating events every {@code months}
     * @throws IllegalArgumentException if {@code months} is negative
     */
    public static Builder everyMonths(int months) {
      return new Builder(Period.ofMonths(checkInterval(months)));
    }

    /**
     * Create a schedule builder where events are triggered every {@code months}, on a given day of the month and time
     *
     * The specific day of the month is expressed as a day of the week and a week number, which
     * is useful to represent schedule like every 2nd tuesday of the month.
     *
     * @param months the number of months between events
     * @param weekOfMonth the ordinal week of the month (1st week is 1)
     * @param dayOfWeek the day of week when to generate events for
     * @param at the specific time to generate the events at
     * @return a schedule builder generating events every {@code months}
     * @throws IllegalArgumentException if {@code months} is negative, or if {@code weekOfMonth} is invalid.
     * @throws NullPointerException if {@code dayOfWeek} or {@code at} is null
     */
    public static Builder everyMonths(int months, int weekOfMonth, DayOfWeek dayOfWeek, LocalTime at) {
      ChronoField.ALIGNED_WEEK_OF_MONTH.checkValidValue(weekOfMonth);
      Preconditions.checkNotNull(dayOfWeek);
      Preconditions.checkNotNull(at);
      return everyMonths(months).withAdjuster(sameOrNext(Period.ofMonths(1), combine(TemporalAdjusters.dayOfWeekInMonth(weekOfMonth, dayOfWeek), at)));
    }

    /**
     * Create a new schedule where events are triggered every {@code months}, on a given day of the month and time
     *
     * @param months the number of months between events
     * @param dayOfMonth the day of the month when to triggered events for. It
     *                   might be adjusted if too large for a specific month
     *                   (31 would be adjusted to 30 for an event triggered in April).
     * @param at the specific time to generate the events at
     * @return a schedule builder generating events every {@code months}
     * @throws IllegalArgumentException if {@code months} is negative, or if {@code dayOfMonth} is invalid.
     * @throws NullPointerException if {@code at} is null
     */
    public static Builder everyMonths(int months, int dayOfMonth, LocalTime at) {
      ChronoField.DAY_OF_MONTH.checkValidIntValue(dayOfMonth);
      Preconditions.checkNotNull(at);

      return everyMonths(months).withAdjuster(sameOrNext(Period.ofMonths(1), combine(dayOfMonth(dayOfMonth), at)));
    }

    /**
     * Combine temporal adjusters by executing them sequentially.
     * Order matters obviously...
     *
     * @param adjusters the adjusters to combine
     * @return an adjuster
     */
    private static TemporalAdjuster combine(TemporalAdjuster... adjusters) {
      final ImmutableList<TemporalAdjuster> copyOf = ImmutableList.copyOf(adjusters);
      return new TemporalAdjuster() {

        @Override
        public Temporal adjustInto(Temporal temporal) {
          Temporal result = temporal;
          for(TemporalAdjuster adjuster: copyOf) {
            result = result.with(adjuster);
          }
          return result;
        }
      };
    }

    /**
     * A temporal adjuster which leaves the temporal untouched.
     * In a Java8 world, this helper can probably be removed safely
     * and replaced by identity
     */
    private static final TemporalAdjuster NO_ADJUSTMENT = new TemporalAdjuster() {
      @Override
      public Temporal adjustInto(Temporal temporal) {
        return temporal;
      }
    };
    /**
     * Wraps an adjuster to round the temporal to the next period if the adjusted time is before
     * the temporal instant.
     *
     * @param amount the amount to add to the adjusted time if before the temporal argument of
     * {@code TemporarlAdjuster#adjustInto()} is after the adjusted time
     * @param adjuster the adjuster to wrap
     * @return an adjuster
     */
    private static TemporalAdjuster sameOrNext(final TemporalAmount amount, final TemporalAdjuster adjuster) {
      return new TemporalAdjuster() {

        @Override
        public Temporal adjustInto(Temporal temporal) {
          Temporal adjusted = temporal.with(adjuster);
          return (ChronoUnit.NANOS.between(temporal, adjusted) >= 0)
              ? adjusted
              : temporal.plus(amount).with(adjuster);
          }
      };
    }

    /**
     * Adjust a temporal to the given day of the month
     *
     * @param dayOfMonth the day of the month to adjust the temporal to
     * @return a temporal adjuster
     * @throws IllegalArgumentException if {@code dayOfMonth} is invalid
     */
    private static final TemporalAdjuster dayOfMonth(final int dayOfMonth) {
      ChronoField.DAY_OF_MONTH.checkValidIntValue(dayOfMonth);

      return new TemporalAdjuster() {
        @Override
        public Temporal adjustInto(Temporal temporal) {
          long adjustedDayOfMonth = Math.min(dayOfMonth, temporal.range(ChronoField.DAY_OF_MONTH).getMaximum());
          return temporal.with(ChronoField.DAY_OF_MONTH, adjustedDayOfMonth);
        }
      };
    }

    /**
     * Check that interval (as X hours, hours, weeks...) is strictly positive
     * @param interval the value to check
     * @return interval
     */
    private static int checkInterval(int interval) {
      Preconditions.checkArgument(interval > 0, "interval should be greater than 0, was %d", interval);
      return interval;
    }
  }

  /**
   * Return the amount of time between two instants created by the schedule.
   *
   * The information is approximative and not absolute as the schedule might be adjusted
   * based on schedule constraints (like every week, or every 2nd Tuesday of every other month).
   *
   * @return the amount of time between events
   */
  TemporalAmount getPeriod();

  /**
   * Return an iterator over next scheduled events
   *
   * Each sequence of events created by this method starts at the current
   * time (so the sequence doesn't contain events from the past). More precisely
   * the first event should be the closest instant greater or equals to now which satisfies the
   * schedule conditions.
   *
   * @return the iterator
   */
  @Override
  Iterator<Instant> iterator();

  /**
   * To get name of distributed singleton task
   * @return
   */
  String getTaskName();

  /**
   * Time period after which to release leadership
   * in milliseconds
   * @return
   */
  Long getScheduledOwnershipReleaseInMillis();

  /**
   * To define if it is distributed singleton task
   * @return
   */
  default boolean isDistributedSingleton() {
    return (getTaskName() != null);
  }

  /**
   * To run just once
   * @return
   */
  default boolean isToRunOnce() {
    return false;
  }

  /**
   * To get CleanupListener that will be used
   * to clean up during distributed singleton task
   * losing/abandoning leadership
   * This can/should be used by tasks that need
   * to clearly differentiate their state while they are leaders
   * or not leaders
   * @return CleanupListener
   */
  default CleanupListener getCleanupListener() { return () -> {
    // do nothing by default
    };
  }
}
