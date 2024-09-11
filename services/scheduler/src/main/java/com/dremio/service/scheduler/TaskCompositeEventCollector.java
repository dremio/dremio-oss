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

import com.dremio.exec.proto.CoordinationProtos;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

class TaskCompositeEventCollector implements SchedulerEvents {
  private final List<SchedulerEvents> eventSinks;
  private final Map<String, PerTaskEventCollector> allTasks;

  TaskCompositeEventCollector(List<SchedulerEvents> eventHandlers) {
    eventSinks = ImmutableList.copyOf(eventHandlers);
    allTasks = new ConcurrentHashMap<>();
  }

  @Override
  public PerTaskEvents addTask(PerTaskSchedule schedule) {
    return allTasks.computeIfAbsent(
        schedule.getTaskName(),
        (k) -> {
          final List<PerTaskEvents> perTaskEvents =
              eventSinks.stream()
                  .map((sink) -> sink.addTask(schedule))
                  .collect(Collectors.toList());
          return new PerTaskEventCollector(perTaskEvents);
        });
  }

  @Override
  public void hitUnexpectedError() {
    eventSinks.forEach(SchedulerEvents::hitUnexpectedError);
  }

  @Override
  public void taskDone(String taskName) {
    eventSinks.forEach((x) -> x.taskDone(taskName));
  }

  @Override
  public void membershipChanged(int newCount) {
    eventSinks.forEach((x) -> x.membershipChanged(newCount));
  }

  @Override
  public void tasksAddedToMembership(int taskCount) {
    eventSinks.forEach((x) -> x.tasksAddedToMembership(taskCount));
  }

  @Override
  public void tasksRemovedFromMembership(int taskCount) {
    eventSinks.forEach((x) -> x.tasksRemovedFromMembership(taskCount));
  }

  @Override
  public void runSetSize(int currentSize) {
    eventSinks.forEach((x) -> x.runSetSize(currentSize));
  }

  @Override
  public void computedWeight(int currentWeight) {
    eventSinks.forEach((x) -> x.computedWeight(currentWeight));
  }

  private static final class PerTaskEventCollector implements SchedulerEvents.PerTaskEvents {
    private final List<PerTaskEvents> perTaskEventsSinks;

    private PerTaskEventCollector(List<PerTaskEvents> perTaskEvents) {
      this.perTaskEventsSinks = perTaskEvents;
    }

    @Override
    public void bookingAttempted() {
      perTaskEventsSinks.forEach(PerTaskMainEvents::bookingAttempted);
    }

    @Override
    public void bookingAcquired(long bookingOwnerSessionId) {
      perTaskEventsSinks.forEach(
          perTaskEvents -> perTaskEvents.bookingAcquired(bookingOwnerSessionId));
    }

    @Override
    public void bookingReleased() {
      perTaskEventsSinks.forEach(PerTaskMainEvents::bookingReleased);
    }

    @Override
    public void bookingLost() {
      perTaskEventsSinks.forEach(PerTaskMainEvents::bookingLost);
    }

    @Override
    public void bookingRechecked() {
      perTaskEventsSinks.forEach(PerTaskMainEvents::bookingRechecked);
    }

    @Override
    public void bookingRegained() {
      perTaskEventsSinks.forEach(PerTaskMainEvents::bookingRegained);
    }

    @Override
    public void contractError() {
      perTaskEventsSinks.forEach(PerTaskMainEvents::contractError);
    }

    @Override
    public void runStarted() {
      perTaskEventsSinks.forEach(PerTaskMainEvents::runStarted);
    }

    @Override
    public void runEnded(boolean success) {
      perTaskEventsSinks.forEach((sink) -> sink.runEnded(success));
    }

    @Override
    public void scheduleModified() {
      perTaskEventsSinks.forEach(PerTaskMainEvents::scheduleModified);
    }

    @Override
    public void taskOwnerQuery(CoordinationProtos.NodeEndpoint nodeEndpoint) {
      perTaskEventsSinks.forEach(perTaskEvents -> perTaskEvents.taskOwnerQuery(nodeEndpoint));
    }

    @Override
    public void noTaskOwnerFound() {
      perTaskEventsSinks.forEach(PerTaskMainEvents::noTaskOwnerFound);
    }

    @Override
    public void taskOwnerQueryFailed() {
      perTaskEventsSinks.forEach(PerTaskMainEvents::taskOwnerQueryFailed);
    }

    @Override
    public void addedToRunSet() {
      perTaskEventsSinks.forEach(PerTaskLoadEvents::addedToRunSet);
    }

    @Override
    public void crossedThreshold() {
      perTaskEventsSinks.forEach(PerTaskLoadEvents::crossedThreshold);
    }

    @Override
    public void removedFromRunSet(long cTime) {
      perTaskEventsSinks.forEach((sink) -> sink.removedFromRunSet(cTime));
    }

    @Override
    public void weightShed(int weight) {
      perTaskEventsSinks.forEach((sink) -> sink.weightShed(weight));
    }

    @Override
    public void weightGained(int weight) {
      perTaskEventsSinks.forEach((sink) -> sink.weightGained(weight));
    }

    @Override
    public void recoveryMonitoringStarted() {
      perTaskEventsSinks.forEach(PerTaskRecoveryEvents::recoveryMonitoringStarted);
    }

    @Override
    public void recoveryMonitoringStopped() {
      perTaskEventsSinks.forEach(PerTaskRecoveryEvents::recoveryMonitoringStopped);
    }

    @Override
    public void recoveryRequested() {
      perTaskEventsSinks.forEach(PerTaskRecoveryEvents::recoveryRequested);
    }

    @Override
    public void recoveryRejected(RecoveryRejectReason reason) {
      perTaskEventsSinks.forEach((sink) -> sink.recoveryRejected(reason));
    }

    @Override
    public void recovered() {
      perTaskEventsSinks.forEach(PerTaskRecoveryEvents::recovered);
    }

    @Override
    public void addedToDeathWatch() {
      perTaskEventsSinks.forEach(PerTaskRecoveryEvents::addedToDeathWatch);
    }

    @Override
    public void runOnDeath() {
      perTaskEventsSinks.forEach(PerTaskRecoveryEvents::runOnDeath);
    }

    @Override
    public void failedToRunOnDeath() {
      perTaskEventsSinks.forEach(PerTaskRecoveryEvents::failedToRunOnDeath);
    }
  }
}
