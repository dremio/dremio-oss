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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.dremio.telemetry.api.metrics.Counter;
import com.dremio.telemetry.api.metrics.Metrics;

/**
 * Collects statistics from various sub managers of the {@code ClusteredSingletonTaskScheduler} by collecting
 * various scheduler events and converting them to various metrics.
 */
final class TaskStatsCollector implements SchedulerEvents {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(TaskStatsCollector.class);
  static final String BASE_METRIC_NAME = "schedules";
  private static final int LOG_INTERVAL_SECONDS = 600;
  private final ClusteredSingletonCommon schedulerCommon;
  private final Map<String, PerTaskStatsCollector> allTasks;
  private final Counter totalTasks;
  private final Counter totalOneShotTasks;
  private final Counter totalDoneTasks;
  private final MembershipStats groupMembershipStats;
  private volatile int currentRunSetTasks;

  TaskStatsCollector(ClusteredSingletonCommon schedulerCommon) {
    this.schedulerCommon = schedulerCommon;
    this.allTasks = new ConcurrentHashMap<>();
    this.totalTasks = Metrics.newCounter(getRootMetricsName("tasks"), Metrics.ResetType.NEVER);
    this.totalDoneTasks = Metrics.newCounter(getRootMetricsName("done_tasks"), Metrics.ResetType.NEVER);
    this.totalOneShotTasks = Metrics.newCounter(getRootMetricsName("single_shot_tasks"), Metrics.ResetType.NEVER);
    this.groupMembershipStats = new MembershipStats();
    Metrics.newGauge(getRootMetricsName("active_tasks"), allTasks::size);
    Metrics.newGauge(getRootMetricsName("run_q_size"), () -> currentRunSetTasks);
  }

  private static String getRootMetricsName(String metricName) {
    return Metrics.join(BASE_METRIC_NAME, metricName);
  }

  void start() {
    if (LOGGER.isDebugEnabled()) {
      // Log only if debug is enabled as all stats are available through JMX
      schedulerCommon.getSchedulePool().scheduleAtFixedRate(this::logStats, LOG_INTERVAL_SECONDS,
        LOG_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }
  }

  void logStats() {
    LOGGER.debug("Clustered Singleton Periodic Stats: {}{}", System.lineSeparator(), this);
  }

  @Override
  public PerTaskEvents addTask(PerTaskSchedule schedule) {
    return allTasks.computeIfAbsent(schedule.getTaskName(), (k) -> {
      totalTasks.increment();
      if (schedule.getSchedule().isToRunExactlyOnce()) {
        totalOneShotTasks.increment();
      }
      return new PerTaskStatsCollector(schedule);
    });
  }

  @Override
  public void taskDone(String taskName) {
    allTasks.computeIfPresent(taskName, (k, v) -> {
      totalDoneTasks.increment();
      return null;
    });
  }

  @Override
  public void membershipChanged(int newCount) {
    groupMembershipStats.newMemberShip(newCount);
  }

  @Override
  public void tasksAddedToMembership(int taskCount) {
    groupMembershipStats.numTasksAdded(taskCount);
  }

  @Override
  public void tasksRemovedFromMembership(int taskCount) {
    groupMembershipStats.numTasksRemoved(taskCount);
  }

  @Override
  public void runSetSize(int currentSize) {
    currentRunSetTasks = currentSize;
  }

  @Override
  public String toString() {
    String mainStats = "Total Tasks : " + totalTasks + System.lineSeparator() +
      "Total One Shot Tasks : " + totalOneShotTasks + System.lineSeparator() +
      "Total Done Tasks : " + totalDoneTasks + System.lineSeparator() +
      "Current Active Tasks : " + allTasks.size() + System.lineSeparator() +
      "Current Tasks in Run Set : " + currentRunSetTasks + System.lineSeparator() +
      "Membership Stats : " + System.lineSeparator() + groupMembershipStats +
      System.lineSeparator();
    StringBuilder sb = new StringBuilder(mainStats);
    allTasks.forEach((k, v) -> {
      sb.append("Per Task Stats For ").append(k).append(System.lineSeparator());
      sb.append(v);
    });
    return sb.toString();
  }

  private static final class MembershipStats {
    private final AtomicInteger currentMembershipCount;
    private final AtomicInteger currentOwnedTasks;
    private final AtomicInteger lastDisownedTasks;

    private MembershipStats() {
      currentMembershipCount = new AtomicInteger(0);
      currentOwnedTasks = new AtomicInteger(0);
      lastDisownedTasks = new AtomicInteger(0);
      Metrics.newGauge(getRootMetricsName("instances"), currentMembershipCount::get);
      Metrics.newGauge(getRootMetricsName("owned_tasks"), currentOwnedTasks::get);
      Metrics.newGauge(getRootMetricsName("disowned_tasks"), lastDisownedTasks::get);
    }

    public void newMemberShip(int newCount) {
      currentMembershipCount.set(newCount);
    }

    public void numTasksAdded(int taskCount) {
      currentOwnedTasks.set(taskCount);
    }

    public void numTasksRemoved(int taskCount) {
      lastDisownedTasks.set(taskCount);
    }

    @Override
    public String toString() {
        return "Current Membership Count :" + currentMembershipCount.get() + System.lineSeparator() +
        "Current Owned Tasks :" + currentOwnedTasks.get() + System.lineSeparator() +
        "Last Disowned Task count :" + lastDisownedTasks.get() + System.lineSeparator();
    }
  }
}
