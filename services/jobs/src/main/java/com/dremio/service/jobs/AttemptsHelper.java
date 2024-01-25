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
package com.dremio.service.jobs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.dremio.exec.proto.UserBitShared.AttemptEvent;
import com.dremio.exec.proto.UserBitShared.AttemptEvent.State;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobDetails;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.ResourceSchedulingInfo;
import com.google.common.collect.ImmutableList;

/**
 * Helper methods for computing various metrics out of {@link JobAttempt}
 */
public class AttemptsHelper {

  private final Map<AttemptEvent.State, Long> stateDurations;
  private final List<AttemptEvent> events;

  public AttemptsHelper(JobAttempt jobAttempt) {
    Map<State, Long> stateDurations = new HashMap<>();
    final List<AttemptEvent> events;
    if (jobAttempt.getStateListList() == null) {
      events = new ArrayList<>();
    } else {
      events = new ArrayList<>(JobsProtoUtil.toStuff2(jobAttempt.getStateListList()));
    }
    Collections.sort(events, Comparator.comparingLong(AttemptEvent::getStartTime));
    this.events = events;

    long timeSpent;
    for (int i=0; i < events.size(); i++) {
      if (isTerminal(events.get(i).getState())) {
        break;
      }
      if (i == events.size()-1) {
        timeSpent = System.currentTimeMillis() - events.get(i).getStartTime();
      } else {
        timeSpent = events.get(i+1).getStartTime() - events.get(i).getStartTime();
      }
      long finalTimeSpent = timeSpent;
      stateDurations.compute(events.get(i).getState(), (k,v) -> (v==null) ? finalTimeSpent : v + finalTimeSpent);
    }
    this.stateDurations = stateDurations;
  }

  private boolean isTerminal(AttemptEvent.State state) {
    return (state == State.COMPLETED ||
      state == State.CANCELED ||
      state == State.FAILED);
  }

  /**
   * True if the job has some state durations. Jobs that were run in older dremio versions (pre 4.5) will not have any.
   *
   * @return true if the job has state durations.
   */
  public boolean hasStateDurations() {
    return !stateDurations.isEmpty();
  }

  /**
   * @return wait before planning in milliseconds
   */
  public static long getCommandPoolWaitTime(JobAttempt jobAttempt) {
    final JobInfo jobInfo = jobAttempt.getInfo();
    if (jobInfo == null) {
      return 0;
    }

    return Optional.ofNullable(jobInfo.getCommandPoolWaitMillis()).orElse(0L);
  }
  /**
   * @return computed enqueued duration in milliseconds,
   *         null if {@link ResourceSchedulingInfo} is missing from the {@link JobInfo}
   */
  public static long getLegacyEnqueuedTime(JobAttempt jobAttempt) {
    final JobInfo jobInfo = jobAttempt.getInfo();
    if (jobInfo == null) {
      return 0;
    }

    final ResourceSchedulingInfo schedulingInfo = jobInfo.getResourceSchedulingInfo();
    if (schedulingInfo == null) {
      return 0;
    }

    final Long schedulingStart = schedulingInfo.getResourceSchedulingStart();
    Long schedulingEnd = schedulingInfo.getResourceSchedulingEnd();
    if (schedulingStart == null || schedulingEnd == null) {
      return 0;
    }

    if (jobAttempt.getState() == JobState.ENQUEUED) {
      // job is still enqueued, compute enqueued time up to now
      schedulingEnd = System.currentTimeMillis();
    } else {
      // schedulingEnd should always be greater than or equal to schedulingStart
      schedulingEnd = Math.max(schedulingStart, schedulingEnd);
    }

    return schedulingEnd - schedulingStart;
  }

  public static long getPoolWaitTime(JobAttempt jobAttempt) {
    return Optional.ofNullable(jobAttempt.getInfo().getCommandPoolWaitMillis()).orElse(0L);
  }

  public static long getLegacyPlanningTime(JobAttempt jobAttempt) {
    long planningScheduling = Optional.ofNullable(jobAttempt.getDetails().getTimeSpentInPlanning()).orElse(0L);
    final long enqueuedTime = getLegacyEnqueuedTime(jobAttempt);

    // take into account the fact that JobDetails.timeSpentInPlanning will only be set after the query starts executing
    planningScheduling = Math.max(planningScheduling, enqueuedTime);

    return planningScheduling - enqueuedTime;
  }

  public static long getLegacyExecutionTime(JobAttempt jobAttempt) {
    final JobInfo jobInfo = jobAttempt.getInfo();
    if (jobInfo == null) {
      return 0;
    }
    final JobDetails jobDetails = jobAttempt.getDetails();
    if (jobDetails == null) {
      return 0;
    }

    final long startTime = Optional.ofNullable(jobInfo.getStartTime()).orElse(0L);
    // finishTime is null until the query is no longer running
    final long finishTime = Optional.ofNullable(jobInfo.getFinishTime()).orElse(startTime);
    final long planningScheduling = Optional.ofNullable(jobDetails.getTimeSpentInPlanning()).orElse(0L);

    return finishTime - startTime - planningScheduling;
  }

  private Long getDuration(AttemptEvent.State state) {
    if (!stateDurations.containsKey(state)) {
      return null;
    }
    return stateDurations.get(state);
  }

  public Long getPendingTime() {
    return getDuration(State.PENDING);
  }

  public Long getMetadataRetrievalTime() {
    return getDuration(State.METADATA_RETRIEVAL);
  }

  public Long getPlanningTime() {
    return getDuration(State.PLANNING);
  }

  public Long getQueuedTime() {
    return getDuration(State.QUEUED);
  }

  public Long getEngineStartTime() {
    return getDuration(State.ENGINE_START);
  }

  public Long getExecutionPlanningTime() {
    return getDuration(State.EXECUTION_PLANNING);
  }

  public Long getStartingTime() {
    return getDuration(State.STARTING);
  }

  public Long getRunningTime() { return getDuration(State.RUNNING);
  }

  public Long getTotalTime() {
    if (events.size() > 0) {
      if (isTerminal(events.get(events.size()-1).getState())) {
        return events.get(events.size()-1).getStartTime() - events.get(0).getStartTime();
      }
      return System.currentTimeMillis() - events.get(0).getStartTime();
    }
    return null;
  }

  public Long getQueuedTimeStamp() {
    if (getQueuedTime() != null) {
      Optional<AttemptEvent> event = events.stream().filter(e -> e.getState() == State.QUEUED).findAny();
      if (event.isPresent()) {
        return event.get().getStartTime();
      }
    }
    return null;
  }

  public Long getRunningTimeStamp() {
    if (getRunningTime() != null) {
      Optional<AttemptEvent> event = events.stream().filter(e -> e.getState() == State.RUNNING).findAny();
      if (event.isPresent()) {
        return event.get().getStartTime();
      }
    }
    return null;
  }

  public List<AttemptEvent> getEvents() {
    return ImmutableList.copyOf(events);
  }

  public Long getStateTimeStamp(AttemptEvent.State state) {
    if (getDuration(state) != null) {
      Optional<AttemptEvent> event = events.stream().filter(e -> e.getState() == state).findAny();
      if (event.isPresent()) {
        return event.get().getStartTime();
      }
    }
    return null;
  }

  public Long getFinalStateTimeStamp() {
    if (!events.isEmpty()) {
      AttemptEvent lastEvent = events.get(events.size() - 1);
      if (isTerminal(lastEvent.getState())) {
        return lastEvent.getStartTime();
      }
    }
    return null;
  }
}
