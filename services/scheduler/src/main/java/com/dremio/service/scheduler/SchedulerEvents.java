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

/**
 * Internal interface used within the clustered singleton to notify events to any one who is interested in
 * listening to various scheduler events such as the {@code TaskStatsCollector}.
 * <p>
 * <strong>NOTE:</strong> In the future this interface may be made public to allow higher layers to add their own
 * event sinks for the distributed singleton events.
 * </p>
 */
interface SchedulerEvents {
  void taskDone(String taskName);
  void membershipChanged(int newCount);
  void tasksAddedToMembership(int taskCount);
  void tasksRemovedFromMembership(int taskCount);
  void runSetSize(int currentSize);
  PerTaskEvents addTask(PerTaskSchedule schedule);
  interface PerTaskMainEvents {
    void bookingAttempted();
    void bookingAcquired();
    void bookingReleased();
    void contractError();
    void runStarted();
    void runEnded(boolean success);
    void scheduleModified();
    void taskOwnerQuery(CoordinationProtos.NodeEndpoint nodeEndpoint);
    void noTaskOwnerFound();
    void taskOwnerQueryFailed();
  }
  interface PerTaskLoadEvents {
    void addedToRunSet();
    void crossedThreshold();
    void removedFromRunSet(long cTime);
  }
  enum RecoveryRejectReason {
    TASK_DONE("already_done"),
    NOT_RECOVERY_OWNER("not_recovery_owner"),
    CANNOT_BOOK("booking_failed"),
    IN_RUN_QUEUE("in_run_q");

    private final String name;

    RecoveryRejectReason(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }
  interface PerTaskRecoveryEvents {
    void recoveryMonitoringStarted();
    void recoveryMonitoringStopped();
    void recoveryRequested();
    void recoveryRejected(RecoveryRejectReason reason);
    void recovered();
    void addedToDeathWatch();
    void runOnDeath();
    void failedToRunOnDeath();
  }
  interface PerTaskEvents extends PerTaskMainEvents, PerTaskLoadEvents, PerTaskRecoveryEvents {
  }
}
