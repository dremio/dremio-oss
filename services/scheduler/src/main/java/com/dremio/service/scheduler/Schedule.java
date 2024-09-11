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
import static com.dremio.service.scheduler.ScheduleBuilderImpl.checkInterval;
import static com.dremio.service.scheduler.ScheduleBuilderImpl.combine;
import static com.dremio.service.scheduler.ScheduleBuilderImpl.dayOfMonth;
import static com.dremio.service.scheduler.ScheduleBuilderImpl.sameOrNext;

import com.google.common.base.Preconditions;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAdjuster;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.TemporalAmount;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.IntSupplier;

/**
 * A recurring task schedule.
 *
 * <p>A schedule comprises a starting point and a set of periods and is used to generate a sequence
 * of instants where a task should be run.
 */
public interface Schedule extends Iterable<Instant> {
  enum SingleShotType {
    // Run task once and only once, default
    RUN_EXACTLY_ONCE,
    // Run task once and only once in the cluster or on an upgrade
    RUN_ONCE_EVERY_UPGRADE,
    // Run task once and only once in the cluster, everytime there is a membership loss
    // Membership gains does not trigger the schedule
    RUN_ONCE_EVERY_MEMBER_DEATH,
  }

  /** Builder interface to create {@code Schedule} instances */
  interface CommonBuilder<B, C> {
    B withTimeZone(ZoneId zoneId);

    C asClusteredSingleton(String taskName);

    Schedule build();
  }

  /** Builder interface to create {@code Schedule} instances */
  interface BaseBuilder<B, C> extends CommonBuilder<B, C> {
    B startingAt(Instant start);

    /**
     * Staggers the schedules to not overload the system
     *
     * @param seed Seed used to stagger the schedule
     * @param staggerRange The tasks are scheduled within a range +- staggerRange. The max value of
     *     the stagger is capped to half of the task period
     * @param timeUnit The timeUnit to be used in staggering the tasks
     * @return The schedule
     */
    B staggered(int seed, long staggerRange, TimeUnit timeUnit);
  }

  interface SingleShotBuilder
      extends CommonBuilder<SingleShotBuilder, ClusteredSingletonSingleShotBuilder> {
    /**
     * Returns a builder that helps create a single shot schedule that runs once and only once
     * instantaneously.
     *
     * @return a schedule builder that creates a run once schedule for the task
     */
    static SingleShotBuilder now() {
      return new SingleShotBuilderImpl();
    }

    /**
     * Returns a builder that helps create a single shot schedule that runs once at a future point
     * in time.
     *
     * @param start Time at which the single shot schedule should run
     * @return a schedule builder that creates a run once future schedule
     */
    static SingleShotBuilder at(Instant start) {
      return new SingleShotBuilderImpl(start);
    }
  }

  interface Builder extends BaseBuilder<Builder, ClusteredSingletonBuilder> {
    /**
     * Create a single shot schedule which is actually a chain of single shots. So in reality it is
     * not a single shot.
     *
     * <p><strong>NOTE:</strong> This is only temporary as we need to allow the old scheduler to
     * coexist with the new for some time. The old unfortunately models multiple schedules of a task
     * as a chain of single shot schedules in order to allow occasional schedule modifications.
     * TODO: DX-68199 avoid the chain and use schedule modifier
     *
     * @return Builder for fluency
     */
    static Builder singleShotChain() {
      return new ScheduleBuilderImpl();
    }

    /**
     * Run once every switch over will run the task once but retain ownership for the task and rerun
     * again everytime the task switches over to a new node, either due to an explicit migration for
     * load balancing purpose or when the task owner shuts down or crashes.
     *
     * <p>If a cleanup is specified, the singleton does a best effort to ensure that the cleanup
     * method is called before switching over. Note that this order of invocation may not be
     * guaranteed in certain situations such as a ZK connection loss (ZK lost) and recovery. Note:
     * for now there is no differentiation between a single shot chain or a run once every
     * switchover task.
     *
     * @return Builder for fluency
     */
    static Builder runOnceEverySwitchOver() {
      return new ScheduleBuilderImpl();
    }

    /**
     * Create a schedule builder where events are triggered every {@code seconds}
     *
     * @param millis the number of milliseconds between events
     * @return a schedule builder generating events every {@code seconds}
     * @throws IllegalArgumentException if {@code minutes} is negative
     */
    static Builder everyMillis(long millis) {
      return new ScheduleBuilderImpl(Duration.ofMillis(millis));
    }

    /**
     * Create a schedule builder where events are triggered every {@code seconds}
     *
     * @param seconds the number of seconds between events
     * @return a schedule builder generating events every {@code seconds}
     * @throws IllegalArgumentException if {@code minutes} is negative
     */
    static Builder everySeconds(long seconds) {
      return new ScheduleBuilderImpl(Duration.ofSeconds(seconds));
    }

    /**
     * Create a schedule builder where events are triggered every {@code minutes}
     *
     * @param minutes the number of minutes between events
     * @return a schedule builder generating events every {@code minutes}
     * @throws IllegalArgumentException if {@code minutes} is negative
     */
    static Builder everyMinutes(long minutes) {
      return new ScheduleBuilderImpl(Duration.ofMinutes(minutes));
    }

    /**
     * Create a schedule builder where events are triggered every {@code hours}
     *
     * @param hours the number of hours between events
     * @return a schedule builder generating events every {@code hours}
     * @throws IllegalArgumentException if {@code hours} is negative
     */
    static Builder everyHours(int hours) {
      return new ScheduleBuilderImpl(Duration.ofHours(checkInterval(hours)));
    }

    /**
     * Create a schedule builder where events are triggered every {@code days}
     *
     * @param days the number of days between events
     * @return a schedule builder generating events every {@code days}
     * @throws IllegalArgumentException if {@code days} is negative
     */
    static Builder everyDays(int days) {
      return new ScheduleBuilderImpl(Period.ofDays(checkInterval(days)));
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
    static Builder everyDays(int days, LocalTime at) {
      Preconditions.checkNotNull(at);
      return ((ScheduleBuilderImpl) everyDays(days)).withAdjuster(sameOrNext(Period.ofDays(1), at));
    }

    /**
     * Create a schedule builder where events are triggered every {@code weeks}
     *
     * @param weeks the number of weeks between events
     * @return a schedule builder generating events every {@code weeks}
     * @throws IllegalArgumentException if {@code weeks} is negative
     */
    static Builder everyWeeks(int weeks) {
      return new ScheduleBuilderImpl(Period.ofWeeks(checkInterval(weeks)));
    }

    /**
     * Create a schedule build where events are triggered every {@code weeks}, at a specific day of
     * week and time
     *
     * @param weeks the number of weeks between events
     * @param dayOfWeek the day of week when to generate events for
     * @param at the specific time to generate the events at
     * @return a schedule builder generating events every {@code weeks}
     * @throws IllegalArgumentException if {@code weeks} is negative
     * @throws NullPointerException if {@code dayOfWeek} or {@code at} is null
     */
    static Builder everyWeeks(int weeks, DayOfWeek dayOfWeek, LocalTime at) {
      Preconditions.checkNotNull(dayOfWeek);
      Preconditions.checkNotNull(at);
      return ((ScheduleBuilderImpl) everyWeeks(weeks))
          .withAdjuster(sameOrNext(Period.ofWeeks(1), combine(dayOfWeek, at)));
    }

    /**
     * Create a schedule builder where events are triggered every {@code months}
     *
     * @param months the number of months between events
     * @return a schedule builder generating events every {@code months}
     * @throws IllegalArgumentException if {@code months} is negative
     */
    static Builder everyMonths(int months) {
      return new ScheduleBuilderImpl(Period.ofMonths(checkInterval(months)));
    }

    /**
     * Create a schedule builder where events are triggered every {@code months}, on a given day of
     * the month and time
     *
     * <p>The specific day of the month is expressed as a day of the week and a week number, which
     * is useful to represent schedule like every 2nd tuesday of the month.
     *
     * @param months the number of months between events
     * @param weekOfMonth the ordinal week of the month (1st week is 1)
     * @param dayOfWeek the day of week when to generate events for
     * @param at the specific time to generate the events at
     * @return a schedule builder generating events every {@code months}
     * @throws IllegalArgumentException if {@code months} is negative, or if {@code weekOfMonth} is
     *     invalid.
     * @throws NullPointerException if {@code dayOfWeek} or {@code at} is null
     */
    static Builder everyMonths(int months, int weekOfMonth, DayOfWeek dayOfWeek, LocalTime at) {
      ChronoField.ALIGNED_WEEK_OF_MONTH.checkValidValue(weekOfMonth);
      Preconditions.checkNotNull(dayOfWeek);
      Preconditions.checkNotNull(at);
      return ((ScheduleBuilderImpl) everyMonths(months))
          .withAdjuster(
              sameOrNext(
                  Period.ofMonths(1),
                  combine(TemporalAdjusters.dayOfWeekInMonth(weekOfMonth, dayOfWeek), at)));
    }

    /**
     * Create a new schedule where events are triggered every {@code months}, on a given day of the
     * month and time
     *
     * @param months the number of months between events
     * @param dayOfMonth the day of the month when to triggered events for. It might be adjusted if
     *     too large for a specific month (31 would be adjusted to 30 for an event triggered in
     *     April).
     * @param at the specific time to generate the events at
     * @return a schedule builder generating events every {@code months}
     * @throws IllegalArgumentException if {@code months} is negative, or if {@code dayOfMonth} is
     *     invalid.
     * @throws NullPointerException if {@code at} is null
     */
    static Builder everyMonths(int months, int dayOfMonth, LocalTime at) {
      ChronoField.DAY_OF_MONTH.checkValidIntValue(dayOfMonth);
      Preconditions.checkNotNull(at);

      return ((ScheduleBuilderImpl) everyMonths(months))
          .withAdjuster(sameOrNext(Period.ofMonths(1), combine(dayOfMonth(dayOfMonth), at)));
    }
  }

  interface ClusteredSingletonSingleShotBuilder
      extends CommonBuilder<
          ClusteredSingletonSingleShotBuilder, ClusteredSingletonSingleShotBuilder> {
    /**
     * Execute in lock step.
     *
     * <p>If this is set, schedules will be executed in lock step, where all nodes wait for the
     * single shot task running on a single instance to complete. Lock step schedules can be
     * duplicated which means two schedules can coexist with the same task name as long as they
     * execute in sequence.
     *
     * <p><strong>NOTE:</strong> This is an optional extension for single shot immediate distributed
     * schedules only.
     */
    ClusteredSingletonSingleShotBuilder inLockStep();

    /**
     * For clustered singleton schedules, a single shot schedule can be of a type specified by the
     * {@code SingleShotType}. By default, {@code SingleShotType.RUN_ONCE_EVERY_UPGRADE} is set.
     *
     * @return builder for fluency
     */
    ClusteredSingletonSingleShotBuilder setSingleShotType(SingleShotType singleShotType);
  }

  interface ClusteredSingletonBuilder
      extends BaseBuilder<ClusteredSingletonBuilder, ClusteredSingletonBuilder> {
    /**
     * Provides a new ClusteredSingletonBuilder from an existing schedule where the name cannot be
     * modified.
     */
    static ClusteredSingletonBuilder fromSchedule(Schedule old) {
      return new ClusteredSingletonBuilderImpl(old);
    }

    /**
     * Provides a new ClusteredSingletonBuilder from an existing schedule where the name cannot be
     * modified and amount needs to be changed
     *
     * <p>Assumption: for now modifiable schedules cannot modify schedules with temporal adjustments
     */
    static ClusteredSingletonBuilder fromSchedule(Schedule old, TemporalAmount newAmount) {
      Preconditions.checkArgument(
          old.getAdjuster().equals(NO_ADJUSTMENT),
          "Modifications not allowed for schedules with temporal adjusters such as time of day");
      return new ClusteredSingletonBuilderImpl(old, newAmount);
    }

    /**
     * Allows modification of schedule of the task.
     *
     * <p>This is a better pattern for clustered singleton schedules as this allows to model a task
     * with occasionally varying schedules as opposed to a chain of single shot schedules. For a
     * clustered singleton this will also bring in internal efficiencies, such as a service instance
     * retaining ownership for task across multiple schedules.
     *
     * @param scheduleModifier The modifier function that returns a new schedule. Return of a null
     *     schedule by this function indicates that there is no modification.
     * @return builder for fluency
     */
    ClusteredSingletonBuilder scheduleModifier(Function<Schedule, Schedule> scheduleModifier);

    /**
     * Adds a supplier that provides weight of the task.
     *
     * <p>The assumption here is that the weight of the task will periodically change.
     *
     * @param weightProvider Supplier that provides the current weight of the task
     * @return builder for fluency
     */
    ClusteredSingletonBuilder withWeightProvider(IntSupplier weightProvider);

    /**
     * Ownership release for leader election.
     *
     * <p>TODO: DX-68199 these calls will no longer be necessary once the distributed singleton is
     * entirely migrated and not under a flag.
     *
     * @param number time
     * @param timeUnit time unit
     * @return builder for fluency
     */
    ClusteredSingletonBuilder releaseOwnershipAfter(long number, TimeUnit timeUnit);

    /**
     * Cleanup listener which is called when a task is re-mastered.
     *
     * <p>Called locally whenever a local node looses booking/master status of the task.
     *
     * @param cleanupListener cleanup listener
     * @return builder for fluency
     */
    ClusteredSingletonBuilder withCleanup(CleanupListener cleanupListener);

    /**
     * Optional task group to which the task and its schedule belongs to.
     *
     * <p>If task group is not specified, default task group will be used for the task and its
     * schedule(s).
     *
     * @param taskGroupName Name of the group to which it belongs
     * @return builder for fluency
     */
    ClusteredSingletonBuilder taskGroup(String taskGroupName);

    /**
     * All schedules of this task are sticky to an instance and cannot be scheduled on another
     * instance, unless the other instance crashes.
     *
     * <p>By default schedules of a task are not sticky and can be load balanced. Schedule users
     * must explicitly state if they wish that a task must stick to an instance as long as that
     * instance is alive
     *
     * @return builder
     */
    ClusteredSingletonBuilder sticky();
  }

  /**
   * Return the amount of time between two instants created by the schedule.
   *
   * <p>The information is approximative and not absolute as the schedule might be adjusted based on
   * schedule constraints (like every week, or every 2nd Tuesday of every other month).
   *
   * @return the amount of time between events
   */
  TemporalAmount getPeriod();

  /**
   * Return an iterator over next scheduled events
   *
   * <p>Each sequence of events created by this method starts at the current time (so the sequence
   * doesn't contain events from the past). More precisely the first event should be the closest
   * instant greater or equals to now which satisfies the schedule conditions.
   *
   * @return the iterator
   */
  @Override
  Iterator<Instant> iterator();

  /**
   * To get name of distributed singleton task
   *
   * <p>Assumption: Only distributed singleton schedules have task names.
   *
   * @return task name
   */
  String getTaskName();

  /**
   * Time period after which to release leadership in milliseconds
   *
   * @return Ownership release time (will be deprecated in future)
   */
  Long getScheduledOwnershipReleaseInMillis();

  /**
   * To define if it is distributed singleton task
   *
   * @return false if normal schedule, true, if distributed singleton
   */
  default boolean isDistributedSingleton() {
    return (getTaskName() != null);
  }

  /**
   * To run exactly once.
   *
   * <p><strong>NOTE:</strong> these calls will no longer be necessary once the clustered singleton
   * is entirely migrated and not under a flag. See DX-68199 <strong>NOTE:</strong> this is
   * extremely confusing and error prone and difficult to wean out from the code as this is used to
   * distinguish between a chain of single shots vs an actual single shot.
   *
   * @return true if task run exactly once, false otherwise
   */
  boolean isToRunExactlyOnce();

  /**
   * Gets the single shot type.
   *
   * @return type of single shot, null if it is not a single shot
   */
  SingleShotType getSingleShotType();

  /**
   * Is the schedule sticky to an instance until the instance fails?
   *
   * @return true if sticky
   */
  boolean isSticky();

  /**
   * To get CleanupListener that will be used to clean up during distributed singleton task
   * losing/abandoning leadership This can/should be used by tasks that need to clearly
   * differentiate their state while they are leaders or not leaders
   *
   * <p><strong>NOTE:</strong> these calls will no longer be necessary once the distributed
   * singleton is entirely migrated and not under a flag. See DX-68199
   *
   * @return CleanupListener
   */
  default CleanupListener getCleanupListener() {
    return () -> {
      // do nothing by default
    };
  }

  default IntSupplier getWeightProvider() {
    return null;
  }

  /**
   * Returns a function that can return a modified schedule from existing schedule. This will allow
   * callers to modify the schedule.
   *
   * <p>The scheduler will call this function immediately after the task completes its run on its
   * current schedule.
   *
   * <p>Since a schedule object is immutable, this returns a new schedule object everytime there is
   * a modification, with the unmodified parts copied to the new object. If there is no modification
   * to the current schedule, a <i>null</i> should be returned, which indicates to the scheduler
   * that it should continue using the current schedule.
   *
   * @return a supplier that either returns a modified schedule OR null if no modification allowed
   */
  default Function<Schedule, Schedule> getScheduleModifier() {
    return (s) -> null;
  }

  /**
   * In order to control capacity usage and allow the creator of the task schedule to control the
   * capacity usage of all tasks within a group, the schedule creator has the option to create
   * different groups that is uniquely identified by the group name. This will allow external
   * control of capacity usage for different kind of tasks.
   *
   * <p>If the schedule has not specified a name for the {@code ScheduleTaskGroup} the default group
   * available to the scheduler service will be used, while scheduling this task.
   *
   * @return name of the task group if set for this schedule, null otherwise
   */
  default String getTaskGroupName() {
    return null;
  }

  /**
   * Time Zone information
   *
   * @return Zone Id
   */
  default ZoneId getZoneId() {
    return ZoneOffset.UTC;
  }

  /**
   * Temporal adjuster, to adjust to a specific time (such as time of day)
   *
   * @return temporal adjuster, if any
   */
  default TemporalAdjuster getAdjuster() {
    return NO_ADJUSTMENT;
  }

  /** Whether this is a lock step schedule */
  default boolean isInLockStep() {
    return false;
  }

  /**
   * Whether this schedule has a periodicity > 1 minute.
   *
   * <p>Used mainly for recovery of schedule time. Such recovery is only needed, if the period is
   * large.
   *
   * @return true if large periodicity
   */
  default boolean isLargePeriodicity() {
    return false;
  }
}
